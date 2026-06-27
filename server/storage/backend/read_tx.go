// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"bytes"
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// IsSafeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but IsSafeRangeBucket
// is known to never overwrite any key so range is safe.

type ReadTx interface {
	RLock()
	RUnlock()
	UnsafeReader
}

type UnsafeReader interface {
	UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error
}

// Base type for readTx and concurrentReadTx to eliminate duplicate functions between these
type baseReadTx struct {
	// mu protects accesses to the txReadBuffer
	mu  sync.RWMutex
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[BucketID]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup
}

func (baseReadTx *baseReadTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	if baseReadTx.buf.bucketDeleted(bucket) {
		return nil
	}

	baseReadTx.txMu.Lock()
	dbBucket := baseReadTx.tx.Bucket(bucket.Name())
	var c *bolt.Cursor
	if dbBucket != nil {
		c = dbBucket.Cursor()
	}
	baseReadTx.txMu.Unlock()

	bb := baseReadTx.buf.buckets[bucket.ID()]

	if bb == nil || bb.used == 0 {
		if c == nil {
			return nil
		}
		baseReadTx.txMu.Lock()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			baseReadTx.txMu.Unlock()
			if err := visitor(k, v); err != nil {
				return err
			}
			baseReadTx.txMu.Lock()
		}
		baseReadTx.txMu.Unlock()
		return nil
	}

	var kdb, vdb []byte
	if c != nil {
		baseReadTx.txMu.Lock()
		kdb, vdb = c.First()
		baseReadTx.txMu.Unlock()
	}

	j := 0
	for kdb != nil || j < bb.used {
		if kdb != nil && (j >= bb.used || bytes.Compare(kdb, bb.buf[j].key) < 0) {
			if err := visitor(kdb, vdb); err != nil {
				return err
			}
			baseReadTx.txMu.Lock()
			kdb, vdb = c.Next()
			baseReadTx.txMu.Unlock()
		} else if j < bb.used && (kdb == nil || bytes.Compare(bb.buf[j].key, kdb) < 0) {
			if bb.buf[j].val != nil {
				if err := visitor(bb.buf[j].key, bb.buf[j].val); err != nil {
					return err
				}
			}
			j++
		} else {
			if bb.buf[j].val != nil {
				if err := visitor(bb.buf[j].key, bb.buf[j].val); err != nil {
					return err
				}
			}
			j++
			baseReadTx.txMu.Lock()
			kdb, vdb = c.Next()
			baseReadTx.txMu.Unlock()
		}
	}

	return nil
}

func (baseReadTx *baseReadTx) UnsafeRange(bucketType Bucket, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if baseReadTx.buf.bucketDeleted(bucketType) {
		return nil, nil
	}
	if endKey == nil {
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bucketType.IsSafeRangeBucket() {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := baseReadTx.buf.Range(bucketType, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	bn := bucketType.ID()
	baseReadTx.txMu.RLock()
	bucket, ok := baseReadTx.buckets[bn]
	baseReadTx.txMu.RUnlock()
	lockHeld := false
	if !ok {
		baseReadTx.txMu.Lock()
		lockHeld = true
		bucket = baseReadTx.tx.Bucket(bucketType.Name())
		baseReadTx.buckets[bn] = bucket
	}

	if bucket == nil {
		if lockHeld {
			baseReadTx.txMu.Unlock()
		}
		return keys, vals
	}
	if !lockHeld {
		baseReadTx.txMu.Lock()
	}
	c := bucket.Cursor()
	baseReadTx.txMu.Unlock()

	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))
	if endKey == nil {
		if len(k2) > 0 {
			if baseReadTx.buf.tombstoned(bucketType, k2[0]) {
				return nil, nil
			}
		}
		return append(k2, keys...), append(v2, vals...)
	}
	k2, v2 = baseReadTx.filterStaleKeys(bucketType, k2, v2)
	return append(k2, keys...), append(v2, vals...)
}

func (baseReadTx *baseReadTx) filterStaleKeys(bucket Bucket, k2, v2 [][]byte) ([][]byte, [][]byte) {
	bb := baseReadTx.buf.buckets[bucket.ID()]
	if bb == nil || bb.used == 0 {
		return k2, v2
	}

	filteredK := make([][]byte, 0, len(k2))
	filteredV := make([][]byte, 0, len(v2))

	j := 0
	for i := 0; i < len(k2); i++ {
		for j < bb.used && bytes.Compare(bb.buf[j].key, k2[i]) < 0 {
			j++
		}
		if j == bb.used {
			filteredK = append(filteredK, k2[i:]...)
			filteredV = append(filteredV, v2[i:]...)
			break
		}
		if bytes.Equal(bb.buf[j].key, k2[i]) {
			continue
		}
		filteredK = append(filteredK, k2[i])
		filteredV = append(filteredV, v2[i])
	}
	if len(filteredK) == 0 {
		return nil, nil
	}
	return filteredK, filteredV
}

type readTx struct {
	baseReadTx
}

func (rt *readTx) Lock()    { rt.mu.Lock() }
func (rt *readTx) Unlock()  { rt.mu.Unlock() }
func (rt *readTx) RLock()   { rt.mu.RLock() }
func (rt *readTx) RUnlock() { rt.mu.RUnlock() }

func (rt *readTx) reset() {
	rt.buf.reset()
	rt.buckets = make(map[BucketID]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

type concurrentReadTx struct {
	baseReadTx
}

func (rt *concurrentReadTx) Lock()   {}
func (rt *concurrentReadTx) Unlock() {}

// RLock is no-op. concurrentReadTx does not need to be locked after it is created.
func (rt *concurrentReadTx) RLock() {}

// RUnlock signals the end of concurrentReadTx.
func (rt *concurrentReadTx) RUnlock() { rt.txWg.Done() }
