// Copyright 2026 The etcd Authors
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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"go.uber.org/zap"
)

func encodeBucketKey(bucketName []byte, key []byte) []byte {
	buf := make([]byte, 1+len(bucketName)+len(key))
	buf[0] = byte(len(bucketName))
	copy(buf[1:], bucketName)
	copy(buf[1+len(bucketName):], key)
	return buf
}

func decodeBucketKey(encoded []byte) (bucketName []byte, key []byte) {
	if len(encoded) == 0 {
		return nil, nil
	}
	n := int(encoded[0])
	if len(encoded) < 1+n {
		return nil, nil
	}
	return encoded[1 : 1+n], encoded[1+n:]
}

func bucketUpperBound(bucketName []byte) []byte {
	prefix := encodeBucketKey(bucketName, nil)
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	upper[len(upper)-1]++
	return upper
}

type pebbleBackend struct {
	db    *pebble.DB
	cache *pebble.Cache
	path  string
	lg    *zap.Logger

	batchTx *pebbleBatchTx
	readTx  *pebbleReadTx

	batchInterval time.Duration
	batchLimit    int

	applyMu sync.RWMutex
	mu      sync.RWMutex

	openReaders int64
	commits     int64

	syncOpts   *pebble.WriteOptions
	noSyncOpts *pebble.WriteOptions

	hooks                     Hooks
	txPostLockInsideApplyHook func()

	stopc chan struct{}
	donec chan struct{}
}

func pebbleDir(path string) string {
	if filepath.Ext(path) == ".db" {
		return path + ".pebble"
	}
	return path
}

func NewPebbleBackend(bcfg BackendConfig) (Backend, error) {
	if bcfg.Logger == nil {
		bcfg.Logger = zap.NewNop()
	}

	dir := pebbleDir(bcfg.Path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}

	cache := pebble.NewCache(128 << 20) // 128 MB cache
	opts := &pebble.Options{
		Cache:                       cache,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       32,
		LBaseMaxBytes:               64 << 20,
		Levels:                      make([]pebble.LevelOptions, 7),
	}
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = 32 << 10
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
		opts.Levels[i].FilterType = pebble.TableFilter
		if i > 0 {
			opts.Levels[i].TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		} else {
			opts.Levels[i].TargetFileSize = 16 << 20
		}
	}
	opts.EnsureDefaults()

	db, err := pebble.Open(dir, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}

	syncOpts := pebble.Sync
	noSyncOpts := pebble.NoSync
	if bcfg.UnsafeNoFsync {
		syncOpts = pebble.NoSync
	}

	pb := &pebbleBackend{
		db:            db,
		cache:         cache,
		path:          dir,
		lg:            bcfg.Logger,
		batchInterval: bcfg.BatchInterval,
		batchLimit:    bcfg.BatchLimit,
		syncOpts:      syncOpts,
		noSyncOpts:    noSyncOpts,
		hooks:         bcfg.Hooks,
		stopc:         make(chan struct{}),
		donec:         make(chan struct{}),
	}

	pb.batchTx = newPebbleBatchTx(pb)
	pb.readTx = &pebbleReadTx{backend: pb}
	go pb.run()

	return pb, nil
}

func (b *pebbleBackend) run() {
	defer close(b.donec)
	t := time.NewTimer(b.batchInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-b.stopc:
			b.batchTx.CommitAndStop()
			return
		}
		b.batchTx.Lock()
		if b.batchTx.pending > 0 {
			b.batchTx.unsafeCommit(false)
		}
		b.batchTx.Unlock()
		t.Reset(b.batchInterval)
	}
}

func (b *pebbleBackend) BatchTx() BatchTx {
	return b.batchTx
}

func (b *pebbleBackend) ReadTx() ReadTx {
	return b.readTx
}

func (b *pebbleBackend) ConcurrentReadTx() ReadTx {
	return &pebbleReadTx{
		backend: b,
	}
}

func (b *pebbleBackend) Snapshot() Snapshot {
	b.batchTx.Commit()
	snap := b.db.NewSnapshot()
	return &pebbleSnapshot{
		backend: b,
		snap:    snap,
	}
}

func (b *pebbleBackend) Hash(ignores func(bucketName, keyName []byte) bool) (uint32, error) {
	snap := b.db.NewSnapshot()
	defer snap.Close()

	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	iter, err := snap.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var curBucket string
	for iter.First(); iter.Valid(); iter.Next() {
		bName, k := decodeBucketKey(iter.Key())
		if len(bName) == 0 {
			continue
		}
		if string(bName) != curBucket {
			curBucket = string(bName)
			h.Write(bName)
		}
		if ignores != nil && ignores(bName, k) {
			continue
		}
		h.Write(k)
		h.Write(iter.Value())
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}

	return h.Sum32(), nil
}

func (b *pebbleBackend) Size() int64 {
	m := b.db.Metrics()
	return int64(m.DiskSpaceUsage())
}

func (b *pebbleBackend) SizeInUse() int64 {
	usage, err := b.db.EstimateDiskUsage(nil, []byte{0xff, 0xff, 0xff, 0xff})
	if err != nil {
		return b.Size()
	}
	return int64(usage)
}

func (b *pebbleBackend) OpenReadTxN() int64 {
	return atomic.LoadInt64(&b.openReaders)
}

func (b *pebbleBackend) Defrag() error {
	return b.db.Compact(nil, []byte{0xff, 0xff, 0xff, 0xff}, false)
}

func (b *pebbleBackend) ForceCommit() {
	b.batchTx.Lock()
	if b.batchTx.batch != nil && b.batchTx.pending > 0 {
		_ = b.batchTx.batch.Commit(b.syncOpts)
		atomic.AddInt64(&b.commits, 1)
		b.batchTx.pending = 0
		_ = b.batchTx.batch.Close()
		b.batchTx.batch = b.db.NewIndexedBatch()
	}
	b.batchTx.Unlock()
}

func (b *pebbleBackend) Close() error {
	close(b.stopc)
	<-b.donec
	if err := b.db.Close(); err != nil {
		return err
	}
	b.cache.Unref()
	return nil
}

func (b *pebbleBackend) SetTxPostLockInsideApplyHook(hook func()) {
	b.batchTx.Lock()
	defer b.batchTx.Unlock()
	b.txPostLockInsideApplyHook = hook
}

type pebbleBatchTx struct {
	backend *pebbleBackend
	mu      sync.Mutex
	batch   *pebble.Batch
	pending int
	buckets map[BucketID]Bucket
}

func newPebbleBatchTx(be *pebbleBackend) *pebbleBatchTx {
	return &pebbleBatchTx{
		backend: be,
		batch:   be.db.NewIndexedBatch(),
		buckets: make(map[BucketID]Bucket),
	}
}

func (t *pebbleBatchTx) Lock() {
	t.mu.Lock()
}

func (t *pebbleBatchTx) Unlock() {
	if t.pending > 0 {
		t.unsafeCommit(false)
	}
	t.mu.Unlock()
}

func (t *pebbleBatchTx) LockInsideApply() {
	t.mu.Lock()
	if t.backend.txPostLockInsideApplyHook != nil {
		t.backend.txPostLockInsideApplyHook()
	}
}

func (t *pebbleBatchTx) LockOutsideApply() {
	t.mu.Lock()
}

func (t *pebbleBatchTx) Commit() {
	t.Lock()
	t.unsafeCommit(false)
	t.Unlock()
}

func (t *pebbleBatchTx) CommitAndStop() {
	t.Lock()
	t.unsafeCommit(true)
	t.Unlock()
}

func (t *pebbleBatchTx) unsafeCommit(stop bool) {
	if t.batch != nil && t.pending > 0 {
		if t.backend.hooks != nil {
			t.backend.hooks.OnPreCommitUnsafe(t)
		}
		err := t.batch.Commit(t.backend.noSyncOpts)
		if err != nil {
			t.backend.lg.Fatal("failed to commit pebble batch", zap.Error(err))
		}
		atomic.AddInt64(&t.backend.commits, 1)
		t.pending = 0
	}
	if !stop {
		if t.batch != nil {
			_ = t.batch.Close()
		}
		t.batch = t.backend.db.NewIndexedBatch()
	} else if t.batch != nil {
		_ = t.batch.Close()
		t.batch = nil
	}
}

func (t *pebbleBatchTx) UnsafeCreateBucket(bucket Bucket) {
	t.buckets[bucket.ID()] = bucket
}

func (t *pebbleBatchTx) UnsafeDeleteBucket(bucket Bucket) {
	lower := encodeBucketKey(bucket.Name(), nil)
	upper := bucketUpperBound(bucket.Name())
	_ = t.batch.DeleteRange(lower, upper, nil)
	delete(t.buckets, bucket.ID())
	t.pending++
}

func (t *pebbleBatchTx) UnsafePut(bucket Bucket, key []byte, value []byte) {
	encKey := encodeBucketKey(bucket.Name(), key)
	_ = t.batch.Set(encKey, value, nil)
	t.pending++
}

func (t *pebbleBatchTx) UnsafeSeqPut(bucket Bucket, key []byte, value []byte) {
	t.UnsafePut(bucket, key, value)
}

func (t *pebbleBatchTx) UnsafeDelete(bucket Bucket, key []byte) {
	encKey := encodeBucketKey(bucket.Name(), key)
	_ = t.batch.Delete(encKey, nil)
	t.pending++
}

func (t *pebbleBatchTx) UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	if len(endKey) == 0 {
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	lower := encodeBucketKey(bucket.Name(), key)
	var upper []byte
	if len(endKey) > 0 {
		upper = encodeBucketKey(bucket.Name(), endKey)
		if bytes.Compare(lower, upper) >= 0 {
			return nil, nil
		}
	} else {
		upper = append(append([]byte(nil), lower...), 0)
	}

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil || iter == nil {
		return nil, nil
	}
	defer iter.Close()

	for iter.First(); iter.Valid() && int64(len(keys)) < limit; iter.Next() {
		_, k := decodeBucketKey(iter.Key())
		if len(endKey) == 0 && !bytes.Equal(k, key) {
			break
		}
		keys = append(keys, append([]byte(nil), k...))
		vals = append(vals, append([]byte(nil), iter.Value()...))
	}
	return keys, vals
}

func (t *pebbleBatchTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	lower := encodeBucketKey(bucket.Name(), nil)
	upper := bucketUpperBound(bucket.Name())

	iter, err := t.batch.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil || iter == nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, k := decodeBucketKey(iter.Key())
		if err := visitor(k, iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

type pebbleReadTx struct {
	backend *pebbleBackend
	mu      sync.RWMutex
	snap    *pebble.Snapshot
}

func (rt *pebbleReadTx) RLock() {
	rt.mu.RLock()
	if rt.snap == nil {
		rt.snap = rt.backend.db.NewSnapshot()
		atomic.AddInt64(&rt.backend.openReaders, 1)
	}
}

func (rt *pebbleReadTx) RUnlock() {
	if rt.snap != nil {
		_ = rt.snap.Close()
		rt.snap = nil
		atomic.AddInt64(&rt.backend.openReaders, -1)
	}
	rt.mu.RUnlock()
}

func (rt *pebbleReadTx) UnsafeRange(bucket Bucket, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	if len(endKey) == 0 {
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	lower := encodeBucketKey(bucket.Name(), key)
	var upper []byte
	if len(endKey) > 0 {
		upper = encodeBucketKey(bucket.Name(), endKey)
		if bytes.Compare(lower, upper) >= 0 {
			return nil, nil
		}
	} else {
		upper = append(append([]byte(nil), lower...), 0)
	}

	snap := rt.snap
	ownedSnap := false
	if snap == nil {
		snap = rt.backend.db.NewSnapshot()
		ownedSnap = true
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if ownedSnap {
		defer snap.Close()
	}
	if err != nil || iter == nil {
		return nil, nil
	}
	defer iter.Close()

	for iter.First(); iter.Valid() && int64(len(keys)) < limit; iter.Next() {
		_, k := decodeBucketKey(iter.Key())
		if len(endKey) == 0 && !bytes.Equal(k, key) {
			break
		}
		keys = append(keys, append([]byte(nil), k...))
		vals = append(vals, append([]byte(nil), iter.Value()...))
	}
	return keys, vals
}

func (rt *pebbleReadTx) UnsafeForEach(bucket Bucket, visitor func(k, v []byte) error) error {
	lower := encodeBucketKey(bucket.Name(), nil)
	upper := bucketUpperBound(bucket.Name())

	snap := rt.snap
	ownedSnap := false
	if snap == nil {
		snap = rt.backend.db.NewSnapshot()
		ownedSnap = true
	}
	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if ownedSnap {
		defer snap.Close()
	}
	if err != nil || iter == nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		_, k := decodeBucketKey(iter.Key())
		if err := visitor(k, iter.Value()); err != nil {
			return err
		}
	}
	return iter.Error()
}

type pebbleSnapshot struct {
	backend *pebbleBackend
	snap    *pebble.Snapshot
}

func (s *pebbleSnapshot) Size() int64 {
	return s.backend.Size()
}

func (s *pebbleSnapshot) WriteTo(w io.Writer) (n int64, err error) {
	iter, err := s.snap.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var totalWritten int64
	var header [12]byte

	for iter.First(); iter.Valid(); iter.Next() {
		bName, k := decodeBucketKey(iter.Key())
		v := iter.Value()

		binary.BigEndian.PutUint32(header[0:4], uint32(len(bName)))
		binary.BigEndian.PutUint32(header[4:8], uint32(len(k)))
		binary.BigEndian.PutUint32(header[8:12], uint32(len(v)))

		if nw, err := w.Write(header[:]); err != nil {
			return totalWritten, err
		} else {
			totalWritten += int64(nw)
		}
		if nw, err := w.Write(bName); err != nil {
			return totalWritten, err
		} else {
			totalWritten += int64(nw)
		}
		if nw, err := w.Write(k); err != nil {
			return totalWritten, err
		} else {
			totalWritten += int64(nw)
		}
		if nw, err := w.Write(v); err != nil {
			return totalWritten, err
		} else {
			totalWritten += int64(nw)
		}
	}
	return totalWritten, iter.Error()
}

func (s *pebbleSnapshot) Close() error {
	if s.snap != nil {
		err := s.snap.Close()
		s.snap = nil
		return err
	}
	return nil
}

// RestorePebbleSnapshot reads a snapshot stream produced by pebbleSnapshot.WriteTo
// and reconstructs a Pebble database at bcfg.Path.
func RestorePebbleSnapshot(r io.Reader, bcfg BackendConfig) (Backend, error) {
	if bcfg.Logger == nil {
		bcfg.Logger = zap.NewNop()
	}
	if bcfg.Path == "" {
		return nil, errors.New("backend path cannot be empty")
	}

	targetDir := pebbleDir(bcfg.Path)
	tmpPath := targetDir + ".tmp"
	if err := os.RemoveAll(tmpPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove existing tmp db dir: %w", err)
	}
	if err := os.MkdirAll(tmpPath, 0700); err != nil {
		return nil, fmt.Errorf("failed to create tmp db dir: %w", err)
	}

	cache := pebble.NewCache(64 * 1024 * 1024)
	opts := &pebble.Options{
		Cache:                       cache,
		FormatMajorVersion:          pebble.FormatRangeKeys,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20,
		MaxOpenFiles:                1000,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
	}

	db, err := pebble.Open(tmpPath, opts)
	if err != nil {
		cache.Unref()
		return nil, fmt.Errorf("failed to open pebble db for restore: %w", err)
	}

	batch := db.NewBatch()
	var header [12]byte
	var count int

	for {
		_, err := io.ReadFull(r, header[:])
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			batch.Close()
			db.Close()
			cache.Unref()
			os.RemoveAll(tmpPath)
			return nil, fmt.Errorf("failed to read snapshot header: %w", err)
		}

		bLen := binary.BigEndian.Uint32(header[0:4])
		kLen := binary.BigEndian.Uint32(header[4:8])
		vLen := binary.BigEndian.Uint32(header[8:12])
		if bLen == 0 && kLen == 0 && vLen == 0 {
			break
		}

		payload := make([]byte, bLen+kLen+vLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			batch.Close()
			db.Close()
			cache.Unref()
			os.RemoveAll(tmpPath)
			return nil, fmt.Errorf("failed to read snapshot payload: %w", err)
		}

		bName := payload[:bLen]
		k := payload[bLen : bLen+kLen]
		v := payload[bLen+kLen:]

		encodedKey := encodeBucketKey(bName, k)
		if err := batch.Set(encodedKey, v, nil); err != nil {
			batch.Close()
			db.Close()
			cache.Unref()
			os.RemoveAll(tmpPath)
			return nil, fmt.Errorf("failed to set key in batch: %w", err)
		}

		count++
		if count%10000 == 0 {
			if err := batch.Commit(pebble.Sync); err != nil {
				batch.Close()
				db.Close()
				cache.Unref()
				os.RemoveAll(tmpPath)
				return nil, fmt.Errorf("failed to commit batch during restore: %w", err)
			}
			batch.Close()
			batch = db.NewBatch()
		}
	}

	if batch.Count() > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			batch.Close()
			db.Close()
			cache.Unref()
			os.RemoveAll(tmpPath)
			return nil, fmt.Errorf("failed to commit final batch during restore: %w", err)
		}
	}
	bcfg.Logger.Info("RestorePebbleSnapshot restored keys", zap.Int("count", count))
	batch.Close()

	if err := db.Close(); err != nil {
		cache.Unref()
		os.RemoveAll(tmpPath)
		return nil, fmt.Errorf("failed to close restore db: %w", err)
	}
	cache.Unref()

	if err := os.RemoveAll(targetDir); err != nil && !os.IsNotExist(err) {
		os.RemoveAll(tmpPath)
		return nil, fmt.Errorf("failed to remove existing db dir: %w", err)
	}
	if err := os.Rename(tmpPath, targetDir); err != nil {
		os.RemoveAll(tmpPath)
		return nil, fmt.Errorf("failed to rename restored db dir: %w", err)
	}

	return NewPebbleBackend(bcfg)
}
