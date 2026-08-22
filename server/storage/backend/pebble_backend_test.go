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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

type testBucket struct {
	id   BucketID
	name []byte
}

func (b testBucket) ID() BucketID            { return b.id }
func (b testBucket) Name() []byte            { return b.name }
func (b testBucket) String() string          { return string(b.name) }
func (b testBucket) IsSafeRangeBucket() bool { return true }

func TestPebbleBackendBasic(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble.db")
	bcfg := DefaultBackendConfig(zap.NewNop())
	bcfg.Path = dir
	be, err := NewPebbleBackend(bcfg)
	require.NoError(t, err)
	defer be.Close()

	b1 := testBucket{id: 1, name: []byte("bucket1")}
	b2 := testBucket{id: 2, name: []byte("bucket2")}

	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(b1)
	tx.UnsafeCreateBucket(b2)
	tx.UnsafePut(b1, []byte("k1"), []byte("v1"))
	tx.UnsafePut(b1, []byte("k2"), []byte("v2"))
	tx.UnsafePut(b2, []byte("k1"), []byte("v2-b2"))
	tx.Unlock()
	be.ForceCommit()

	rtx := be.ConcurrentReadTx()
	rtx.RLock()
	keys, vals := rtx.UnsafeRange(b1, []byte("k1"), nil, 1)
	rtx.RUnlock()
	require.Equal(t, 1, len(keys))
	require.Equal(t, []byte("k1"), keys[0])
	require.Equal(t, []byte("v1"), vals[0])

	rtx = be.ConcurrentReadTx()
	rtx.RLock()
	keys, vals = rtx.UnsafeRange(b2, []byte("k1"), nil, 1)
	rtx.RUnlock()
	require.Equal(t, 1, len(keys))
	require.Equal(t, []byte("k1"), keys[0])
	require.Equal(t, []byte("v2-b2"), vals[0])

	// ForEach
	var feKeys, feVals [][]byte
	rtx = be.ConcurrentReadTx()
	rtx.RLock()
	err = rtx.UnsafeForEach(b1, func(k, v []byte) error {
		feKeys = append(feKeys, append([]byte(nil), k...))
		feVals = append(feVals, append([]byte(nil), v...))
		return nil
	})
	rtx.RUnlock()
	require.NoError(t, err)
	require.Equal(t, 2, len(feKeys))
	require.Equal(t, []byte("k1"), feKeys[0])
	require.Equal(t, []byte("k2"), feKeys[1])

	// Hash
	h, err := be.Hash(nil)
	require.NoError(t, err)
	require.NotZero(t, h)

	// Snapshot
	snap := be.Snapshot()
	require.NotNil(t, snap)
	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.Positive(t, n)
	require.NoError(t, snap.Close())

	// Defrag
	require.NoError(t, be.Defrag())
}

func TestPebbleBatchTxUnsafeRange(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble-batch.db")
	bcfg := DefaultBackendConfig(zap.NewNop())
	bcfg.Path = dir
	be, err := NewPebbleBackend(bcfg)
	require.NoError(t, err)
	defer be.Close()

	b := testBucket{id: 1, name: []byte("key")}
	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(b)
	tx.UnsafePut(b, []byte{0, 0, 0, 0, 0, 0, 0, 2, '_', 0, 0, 0, 0, 0, 0, 0, 0}, []byte("v2"))
	tx.UnsafePut(b, []byte{0, 0, 0, 0, 0, 0, 0, 3, '_', 0, 0, 0, 0, 0, 0, 0, 0}, []byte("v3"))
	tx.Unlock()
	be.ForceCommit()

	tx = be.BatchTx()
	tx.LockOutsideApply()
	defer tx.Unlock()

	last := []byte{0, 0, 0, 0, 0, 0, 0, 0, '_', 0, 0, 0, 0, 0, 0, 0, 0}
	end := []byte{0, 0, 0, 0, 0, 0, 0, 3}
	keys, vals := tx.UnsafeRange(b, last, end, 100)
	t.Logf("UnsafeRange returned %d keys: %v %v", len(keys), keys, vals)
	require.Equal(t, 1, len(keys))
}

func TestPebbleSnapshotRestore(t *testing.T) {
	origDir := filepath.Join(t.TempDir(), "pebble-orig.db")
	bcfg := DefaultBackendConfig(zap.NewNop())
	bcfg.Path = origDir
	be, err := NewPebbleBackend(bcfg)
	require.NoError(t, err)
	defer be.Close()

	bKey := testBucket{id: 1, name: []byte("key")}
	bLease := testBucket{id: 2, name: []byte("lease")}
	bMeta := testBucket{id: 3, name: []byte("meta")}

	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(bKey)
	tx.UnsafeCreateBucket(bLease)
	tx.UnsafeCreateBucket(bMeta)
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("key-%05d", i))
		v := []byte(fmt.Sprintf("value-%05d", i))
		tx.UnsafePut(bKey, k, v)
		if i%2 == 0 {
			tx.UnsafePut(bLease, k, v)
		}
		if i%5 == 0 {
			tx.UnsafePut(bMeta, k, v)
		}
	}
	tx.Unlock()
	be.ForceCommit()

	origHash, err := be.Hash(nil)
	require.NoError(t, err)

	// Stream snapshot to buffer
	snap := be.Snapshot()
	require.NotNil(t, snap)
	var buf bytes.Buffer
	n, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	require.Positive(t, n)
	require.NoError(t, snap.Close())
	t.Logf("Wrote snapshot %d bytes, buf.Len()=%d", n, buf.Len())

	// Restore snapshot into new backend
	restoreDir := filepath.Join(t.TempDir(), "pebble-restored.db")
	restoreBcfg := DefaultBackendConfig(zaptest.NewLogger(t))
	restoreBcfg.Path = restoreDir
	restoredBe, err := RestorePebbleSnapshot(&buf, restoreBcfg)
	require.NoError(t, err)
	defer restoredBe.Close()

	// Verify Hash matches exactly
	restoredHash, err := restoredBe.Hash(nil)
	require.NoError(t, err)
	t.Logf("origHash=%x restoredHash=%x", origHash, restoredHash)
	require.Equal(t, origHash, restoredHash)

	// Verify all keys can be read from restored backend
	rtx := restoredBe.ReadTx()
	rtx.RLock()
	for i := 0; i < 200; i++ {
		k := []byte(fmt.Sprintf("key-%05d", i))
		expectedV := []byte(fmt.Sprintf("value-%05d", i))
		_, v := rtx.UnsafeRange(bKey, k, nil, 0)
		require.Equal(t, 1, len(v))
		require.Equal(t, expectedV, v[0])
	}
	rtx.RUnlock()

	// Verify write operations work on restored backend
	wtx := restoredBe.BatchTx()
	wtx.Lock()
	wtx.UnsafePut(bKey, []byte("new-key"), []byte("new-val"))
	wtx.Unlock()
	restoredBe.ForceCommit()

	rtx2 := restoredBe.ConcurrentReadTx()
	rtx2.RLock()
	_, newV := rtx2.UnsafeRange(bKey, []byte("new-key"), nil, 0)
	require.Equal(t, 1, len(newV))
	require.Equal(t, []byte("new-val"), newV[0])
	rtx2.RUnlock()
}
