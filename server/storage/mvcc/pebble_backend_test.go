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

package mvcc

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

func TestPebbleBackendWithMVCC(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "pebble-mvcc")
	bcfg := backend.DefaultBackendConfig(zap.NewNop())
	bcfg.Path = dir
	be, err := backend.NewPebbleBackend(bcfg)
	require.NoError(t, err)
	defer be.Close()

	tx := be.BatchTx()
	tx.Lock()
	for _, b := range schema.AllBuckets {
		tx.UnsafeCreateBucket(b)
	}
	tx.Unlock()
	be.ForceCommit()

	st := New(zap.NewNop(), be, &lease.FakeLessor{}, StoreConfig{})
	defer st.Close()

	tw := st.Write(traceutil.TODO())
	rev := tw.Put([]byte("key1"), []byte("val1"), lease.NoLease)
	tw.End()
	require.Equal(t, int64(2), rev)

	rr, err := st.Range(context.Background(), []byte("key1"), nil, RangeOptions{})
	require.NoError(t, err)
	require.NotNil(t, rr)
	require.Equal(t, 1, len(rr.KVs))
	require.Equal(t, []byte("key1"), rr.KVs[0].Key)
	require.Equal(t, []byte("val1"), rr.KVs[0].Value)
}
