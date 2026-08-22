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
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/testing/protocmp"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

func TestStoragePutMultipleTimes(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			for _, mode := range []string{"normal", "txn"} {
				t.Run(fmt.Sprintf("Mode=%s", mode), func(t *testing.T) {
					kv, cleanup := store.setup(t)
					defer cleanup()
					testStoragePutMultipleTimes(t, kv, mode)
				})
			}
		})
	}
}

func testStoragePutMultipleTimes(t *testing.T, s WatchableKV, mode string) {
	for i := 0; i < 10; i++ {
		base := int64(i + 1)
		var rev int64
		if mode == "normal" {
			rev = s.Put([]byte("foo"), []byte("bar"), lease.LeaseID(base))
		} else {
			tw := s.Write(traceutil.TODO())
			rev = tw.Put([]byte("foo"), []byte("bar"), lease.LeaseID(base))
			tw.End()
		}

		if rev != base+1 {
			t.Errorf("#%d: rev = %d, want %d", i, rev, base+1)
		}

		r, err := s.Range(context.Background(), []byte("foo"), nil, RangeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		wkvs := []*mvccpb.KeyValue{
			{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: base + 1, Version: base, Lease: base},
		}
		if !cmp.Equal(r.KVs, wkvs, protocmp.Transform()) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, r.KVs, wkvs)
		}
	}
}

func TestStorageDeleteRange(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			testStorageDeleteRange(t, store.setup)
		})
	}
}

func testStorageDeleteRange(t *testing.T, setup func(testing.TB) (WatchableKV, func())) {
	tests := []struct {
		key, end []byte
		wrev     int64
		wN       int64
	}{
		{[]byte("foo"), nil, 5, 1},
		{[]byte("foo"), []byte("foo1"), 5, 1},
		{[]byte("foo"), []byte("foo2"), 5, 2},
		{[]byte("foo"), []byte("foo3"), 5, 3},
		{[]byte("foo3"), []byte("foo8"), 4, 0},
		{[]byte("foo3"), nil, 4, 0},
	}

	for i, tt := range tests {
		s, cleanup := setup(t)
		s.Put([]byte("foo"), []byte("bar"), lease.NoLease)
		s.Put([]byte("foo1"), []byte("bar1"), lease.NoLease)
		s.Put([]byte("foo2"), []byte("bar2"), lease.NoLease)

		n, rev := s.DeleteRange(tt.key, tt.end)
		if n != tt.wN || rev != tt.wrev {
			t.Errorf("#%d: n = %d, rev = %d, want (%d, %d)", i, n, rev, tt.wN, tt.wrev)
		}
		cleanup()
	}
}

func TestStorageDeleteMultipleTimes(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageDeleteMultipleTimes(t, s)
		})
	}
}

func testStorageDeleteMultipleTimes(t *testing.T, s WatchableKV) {
	s.Put([]byte("foo"), []byte("bar"), lease.NoLease)

	n, rev := s.DeleteRange([]byte("foo"), nil)
	if n != 1 || rev != 3 {
		t.Fatalf("n = %d, rev = %d, want (1, 3)", n, rev)
	}

	for i := 0; i < 10; i++ {
		n, rev := s.DeleteRange([]byte("foo"), nil)
		if n != 0 || rev != 3 {
			t.Fatalf("#%d: n = %d, rev = %d, want (0, 3)", i, n, rev)
		}
	}
}

func TestStoragePutWithSameLease(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStoragePutWithSameLease(t, s)
		})
	}
}

func testStoragePutWithSameLease(t *testing.T, s WatchableKV) {
	leaseID := int64(1)

	rev := s.Put([]byte("foo"), []byte("bar"), lease.LeaseID(leaseID))
	if rev != 2 {
		t.Errorf("rev = %d, want %d", 2, rev)
	}

	rev2 := s.Put([]byte("foo"), []byte("bar"), lease.LeaseID(leaseID))
	if rev2 != 3 {
		t.Errorf("rev = %d, want %d", 3, rev2)
	}

	r, err := s.Range(context.Background(), []byte("foo"), nil, RangeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wkvs := []*mvccpb.KeyValue{
		{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 3, Version: 2, Lease: leaseID},
	}
	if !cmp.Equal(r.KVs, wkvs, protocmp.Transform()) {
		t.Errorf("kvs = %+v, want %+v", r.KVs, wkvs)
	}
}

func TestStorageOperationInSequence(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageOperationInSequence(t, s)
		})
	}
}

func testStorageOperationInSequence(t *testing.T, s WatchableKV) {
	for i := 0; i < 10; i++ {
		base := int64(i*2 + 1)

		rev := s.Put([]byte("foo"), []byte("bar"), lease.NoLease)
		if rev != base+1 {
			t.Errorf("#%d: put rev = %d, want %d", i, rev, base+1)
		}

		r, err := s.Range(context.Background(), []byte("foo"), nil, RangeOptions{Rev: base + 1})
		if err != nil {
			t.Fatal(err)
		}
		wkvs := []*mvccpb.KeyValue{
			{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: base + 1, ModRevision: base + 1, Version: 1, Lease: int64(lease.NoLease)},
		}
		if !cmp.Equal(r.KVs, wkvs, protocmp.Transform()) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, r.KVs, wkvs)
		}
		if r.Rev != base+1 {
			t.Errorf("#%d: range rev = %d, want %d", i, r.Rev, base+1)
		}

		n, delRev := s.DeleteRange([]byte("foo"), nil)
		if n != 1 || delRev != base+2 {
			t.Errorf("#%d: n = %d, rev = %d, want (1, %d)", i, n, delRev, base+2)
		}

		r, err = s.Range(context.Background(), []byte("foo"), nil, RangeOptions{Rev: base + 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.KVs) != 0 {
			t.Errorf("#%d: kvs = %+v, want empty", i, r.KVs)
		}
		if r.Rev != base+2 {
			t.Errorf("#%d: range rev = %d, want %d", i, r.Rev, base+2)
		}
	}
}

func TestStorageTxnBlockWriteOperations(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageTxnBlockWriteOperations(t, s)
		})
	}
}

func testStorageTxnBlockWriteOperations(t *testing.T, s WatchableKV) {
	tests := []func(){
		func() { s.Put([]byte("foo"), nil, lease.NoLease) },
		func() { s.DeleteRange([]byte("foo"), nil) },
	}
	for i, tt := range tests {
		tf := tt
		txn := s.Write(traceutil.TODO())
		done := make(chan struct{}, 1)
		go func() {
			tf()
			done <- struct{}{}
		}()
		select {
		case <-done:
			t.Fatalf("#%d: operation failed to be blocked", i)
		case <-time.After(10 * time.Millisecond):
		}

		txn.End()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("#%d: operation failed to be unblocked", i)
		}
	}
}

func TestStorageTxnNonBlockRange(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageTxnNonBlockRange(t, s)
		})
	}
}

func testStorageTxnNonBlockRange(t *testing.T, s WatchableKV) {
	txn := s.Write(traceutil.TODO())
	defer txn.End()

	donec := make(chan struct{})
	go func() {
		defer close(donec)
		_, _ = s.Range(context.Background(), []byte("foo"), nil, RangeOptions{})
	}()
	select {
	case <-donec:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("range operation blocked on write txn")
	}
}

func TestStorageCompactReserveLastValue(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageCompactReserveLastValue(t, s)
		})
	}
}

func testStorageCompactReserveLastValue(t *testing.T, s WatchableKV) {
	s.Put([]byte("foo"), []byte("bar0"), 1)
	s.Put([]byte("foo"), []byte("bar1"), 2)
	s.DeleteRange([]byte("foo"), nil)
	s.Put([]byte("foo"), []byte("bar2"), 3)

	tests := []struct {
		rev  int64
		wkvs []*mvccpb.KeyValue
	}{
		{
			1,
			[]*mvccpb.KeyValue{
				{Key: []byte("foo"), Value: []byte("bar0"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 1},
			},
		},
		{
			2,
			[]*mvccpb.KeyValue{
				{Key: []byte("foo"), Value: []byte("bar1"), CreateRevision: 2, ModRevision: 3, Version: 2, Lease: 2},
			},
		},
		{
			3,
			nil,
		},
		{
			4,
			[]*mvccpb.KeyValue{
				{Key: []byte("foo"), Value: []byte("bar2"), CreateRevision: 5, ModRevision: 5, Version: 1, Lease: 3},
			},
		},
	}
	for i, tt := range tests {
		donec, err := s.Compact(traceutil.TODO(), tt.rev)
		if err != nil {
			t.Errorf("#%d: unexpected compact error %v", i, err)
		}
		<-donec

		r, err := s.Range(context.Background(), []byte("foo"), nil, RangeOptions{Rev: tt.rev + 1})
		if err != nil {
			t.Errorf("#%d: unexpected range error %v", i, err)
		}
		if !cmp.Equal(r.KVs, tt.wkvs, protocmp.Transform()) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, r.KVs, tt.wkvs)
		}
	}
}

func TestStorageCompactBad(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageCompactBad(t, s)
		})
	}
}

func testStorageCompactBad(t *testing.T, s WatchableKV) {
	s.Put([]byte("foo"), []byte("bar0"), lease.NoLease)
	s.Put([]byte("foo"), []byte("bar1"), lease.NoLease)
	s.Put([]byte("foo"), []byte("bar2"), lease.NoLease)

	tests := []struct {
		rev  int64
		werr error
	}{
		{0, nil},
		{1, nil},
		{1, ErrCompacted},
		{4, nil},
		{5, ErrFutureRev},
		{100, ErrFutureRev},
	}
	for i, tt := range tests {
		donec, err := s.Compact(traceutil.TODO(), tt.rev)
		if err != nil {
			if !errors.Is(err, tt.werr) {
				t.Errorf("#%d: compact error = %v, want %v", i, err, tt.werr)
			}
		} else {
			<-donec
			if tt.werr != nil {
				t.Errorf("#%d: compact returned no error, want %v", i, tt.werr)
			}
		}
	}
}

func TestStorageRange(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageRange(t, s)
		})
	}
}

func testStorageRange(t *testing.T, s WatchableKV) {
	kvs := populate3SampleKVs(s)

	wrev := int64(4)
	tests := []struct {
		key, end []byte
		wkvs     []*mvccpb.KeyValue
	}{
		{[]byte("doo"), []byte("foo"), nil},
		{[]byte("foo"), []byte("foo"), nil},
		{[]byte("doo"), nil, nil},
		{[]byte("foo"), []byte("foo3"), kvs},
		{[]byte("foo"), []byte("foo1"), kvs[:1]},
		{[]byte("foo"), nil, kvs[:1]},
		{[]byte(""), []byte(""), kvs},
	}

	for i, tt := range tests {
		r, err := s.Range(context.Background(), tt.key, tt.end, RangeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if r.Rev != wrev {
			t.Errorf("#%d: rev = %d, want %d", i, r.Rev, wrev)
		}
		if !cmp.Equal(r.KVs, tt.wkvs, protocmp.Transform()) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, r.KVs, tt.wkvs)
		}
	}
}

func TestStorageRangeRev(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageRangeRev(t, s)
		})
	}
}

func testStorageRangeRev(t *testing.T, s WatchableKV) {
	kvs := populate3SampleKVs(s)

	tests := []struct {
		rev  int64
		wrev int64
		wkvs []*mvccpb.KeyValue
	}{
		{-1, 4, kvs},
		{0, 4, kvs},
		{2, 4, kvs[:1]},
		{3, 4, kvs[:2]},
		{4, 4, kvs},
	}

	for i, tt := range tests {
		r, err := s.Range(context.Background(), []byte("foo"), []byte("foo3"), RangeOptions{Rev: tt.rev})
		if err != nil {
			t.Fatal(err)
		}
		if r.Rev != tt.wrev {
			t.Errorf("#%d: rev = %d, want %d", i, r.Rev, tt.wrev)
		}
		if !cmp.Equal(r.KVs, tt.wkvs, protocmp.Transform()) {
			t.Errorf("#%d: kvs = %+v, want %+v", i, r.KVs, tt.wkvs)
		}
	}
}

func TestStorageRangeBadRev(t *testing.T) {
	for _, store := range storageDrivers() {
		t.Run(store.name, func(t *testing.T) {
			s, cleanup := store.setup(t)
			defer cleanup()
			testStorageRangeBadRev(t, s)
		})
	}
}

func testStorageRangeBadRev(t *testing.T, s WatchableKV) {
	populate3SampleKVs(s)
	donec, err := s.Compact(traceutil.TODO(), 4)
	if err != nil {
		t.Fatalf("compact error (%v)", err)
	}
	<-donec

	tests := []struct {
		rev  int64
		werr error
	}{
		{-1, nil},
		{0, nil},
		{1, ErrCompacted},
		{2, ErrCompacted},
		{4, nil},
		{5, ErrFutureRev},
		{100, ErrFutureRev},
	}
	for i, tt := range tests {
		_, err := s.Range(context.Background(), []byte("foo"), []byte("foo3"), RangeOptions{Rev: tt.rev})
		if !errors.Is(err, tt.werr) {
			t.Errorf("#%d: error = %v, want %v", i, err, tt.werr)
		}
	}
}

func populate3SampleKVs(s WatchableKV) []*mvccpb.KeyValue {
	s.Put([]byte("foo"), []byte("bar"), 1)
	s.Put([]byte("foo1"), []byte("bar1"), 2)
	s.Put([]byte("foo2"), []byte("bar2"), 3)
	return []*mvccpb.KeyValue{
		{Key: []byte("foo"), Value: []byte("bar"), CreateRevision: 2, ModRevision: 2, Version: 1, Lease: 1},
		{Key: []byte("foo1"), Value: []byte("bar1"), CreateRevision: 3, ModRevision: 3, Version: 1, Lease: 2},
		{Key: []byte("foo2"), Value: []byte("bar2"), CreateRevision: 4, ModRevision: 4, Version: 1, Lease: 3},
	}
}

type storeDriver struct {
	name  string
	setup func(t testing.TB) (WatchableKV, func())
}

func storageDrivers() []storeDriver {
	return []storeDriver{
		{
			name:  "bbolt",
			setup: setupBboltStore,
		},
		{
			name:  "pebble",
			setup: setupPebbleStore,
		},
		{
			name:  "badger",
			setup: setupBadgerStore,
		},
	}
}

func setupBboltStore(t testing.TB) (WatchableKV, func()) {
	b, _ := betesting.NewDefaultTmpBackend(t)
	s := New(zap.NewNop(), b, &lease.FakeLessor{}, StoreConfig{
		CompactionBatchLimit:    1000,
		CompactionSleepInterval: 1 * time.Millisecond,
	})
	return s, func() {
		_ = s.Close()
		_ = b.Close()
	}
}

func setupPebbleStore(t testing.TB) (WatchableKV, func()) {
	dir := filepath.Join(t.TempDir(), "pebble-test")
	s, err := NewPebbleStore(zap.NewNop(), dir, &lease.FakeLessor{}, StoreConfig{
		CompactionBatchLimit:    1000,
		CompactionSleepInterval: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	return s, func() {
		_ = s.Close()
	}
}

func setupBadgerStore(t testing.TB) (WatchableKV, func()) {
	dir := filepath.Join(t.TempDir(), "badger-test")
	s, err := NewBadgerStore(zap.NewNop(), dir, &lease.FakeLessor{}, StoreConfig{
		CompactionBatchLimit:    1000,
		CompactionSleepInterval: 1 * time.Millisecond,
	})
	require.NoError(t, err)
	return s, func() {
		_ = s.Close()
	}
}
