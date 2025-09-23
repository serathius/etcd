// Copyright 2025 The etcd Authors
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

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/google/go-cmp/cmp"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	cache "go.etcd.io/etcd/cache/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func TestCacheWithoutPrefixGet(t *testing.T) {
	tcs := []struct {
		name                          string
		initialEvents, followupEvents []*clientv3.Event
	}{
		{"watch-early (no pre-events)", nil, TestGetEvents},
		{"watch-mid (partial pre-events)", filterEvents(TestGetEvents, revLessThan(4)), filterEvents(TestGetEvents, revGreaterEqual(4))},
		{"watch-late (all pre-events)", TestGetEvents, nil},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			integration.BeforeTest(t)
			clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
			t.Cleanup(func() { clus.Terminate(t) })
			client, kv := clus.Client(0), clus.Client(0).KV

			testGet(t, kv, func() Getter {
				c, err := cache.New(client, "")
				if err != nil {
					t.Fatalf("cache.New: %v", err)
				}
				t.Cleanup(c.Close)
				if err := c.WaitReady(t.Context()); err != nil {
					t.Fatalf("cache not ready: %v", err)
				}
				return c
			}, tc.initialEvents, tc.followupEvents)
		})
	}
}

func TestGet(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })

	client := clus.Client(0)
	kv := client.KV

	testGet(t, kv, func() Getter { return kv }, TestGetEvents, nil)
}

func testGet(t *testing.T, kv clientv3.KV, getReader func() Getter, initialEvents, followupEvents []*clientv3.Event) {
	ctx := t.Context()
	t.Log("Setup")
	initialRev := applyEvents(ctx, t, kv, initialEvents)

	reader := getReader()
	if c, ok := reader.(*cache.Cache); ok {
		if err := c.WaitForRevision(ctx, initialRev); err != nil {
			t.Fatalf("cache never caught up to rev %d: %v", initialRev, err)
		}
	}

	followupRev := applyEvents(ctx, t, kv, followupEvents)
	if c, ok := reader.(*cache.Cache); ok {
		if err := c.WaitForRevision(ctx, followupRev); err != nil {
			t.Fatalf("cache never caught up to rev %d: %v", followupRev, err)
		}
	}

	t.Log("Validate")
	for _, tc := range getTestCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			op := clientv3.OpGet(tc.key, tc.opts...)
			requestedRev := op.Rev()
			resp, err := reader.Get(ctx, tc.key, tc.opts...)
			if tc.expectErr != nil {
				if !errors.Is(err, tc.expectErr) {
					t.Fatalf("expected %v for Get %q; got %v", tc.expectErr, tc.key, err)
				}
				return
			}
			if err != nil {
				if _, ok := reader.(*cache.Cache); ok && requestedRev > 0 && requestedRev < initialRev && errors.Is(err, rpctypes.ErrCompacted) {
					t.Logf("expected ErrCompacted: requestedRev=%d < initialCompleteRev=%d", requestedRev, initialRev)
					return
				}
				t.Fatalf("Get %q failed: %v", tc.key, err)
			}
			if diff := cmp.Diff(tc.wantKVs, resp.Kvs); diff != "" {
				t.Fatalf("unexpected KVs (-want +got):\n%s", diff)
			}
			if resp.Header.Revision != tc.wantRevision {
				t.Fatalf("revision: got %d, want %d", resp.Header.Revision, tc.wantRevision)
			}
		})
	}
}

var TestGetEvents = []*clientv3.Event{
	Rev2PutFooA, Rev3PutFooB, Rev4PutFooC, Rev5PutFooD, Rev6DeleteFooD, Rev7TxnPutFooA, Rev7TxnPutFooB, Rev8PutFooA,
}

var (
	Rev2PutFooA = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("a1"),
			CreateRevision: 2,
			ModRevision:    2,
			Version:        1,
		},
	}
	Rev3PutFooB = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/b"),
			Value:          []byte("b1"),
			CreateRevision: 3,
			ModRevision:    3,
			Version:        1,
		},
	}
	Rev4PutFooC = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/c"),
			Value:          []byte("c1"),
			CreateRevision: 4,
			ModRevision:    4,
			Version:        1,
		},
	}
	Rev5PutFooD = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/d"),
			Value:          []byte("d1"),
			CreateRevision: 5,
			ModRevision:    5,
			Version:        1,
		},
	}
	Rev6DeleteFooD = &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte("/foo/d"),
			ModRevision: 6,
		},
	}
	Rev7TxnPutFooA = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("a2"),
			CreateRevision: 2,
			ModRevision:    7,
			Version:        2,
		},
	}
	Rev7TxnPutFooB = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/b"),
			Value:          []byte("b2"),
			CreateRevision: 3,
			ModRevision:    7,
			Version:        2,
		},
	}
	Rev8PutFooA = &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("a3"),
			CreateRevision: 2,
			ModRevision:    8,
			Version:        3,
		},
	}
)

type getTestCase struct {
	name         string
	key          string
	opts         []clientv3.OpOption
	wantKVs      []*mvccpb.KeyValue
	wantRevision int64
	expectErr    error
}

var getTestCases = []getTestCase{
	{
		name:         "single key /foo/a",
		key:          "/foo/a",
		opts:         []clientv3.OpOption{clientv3.WithSerializable()},
		wantKVs:      []*mvccpb.KeyValue{Rev8PutFooA.Kv},
		wantRevision: 8,
	},
	{
		name:         "single key /foo/a at rev=2",
		key:          "/foo/a",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRev(2)},
		wantKVs:      []*mvccpb.KeyValue{Rev2PutFooA.Kv},
		wantRevision: 8,
	},
	{
		name:         "single key /foo/a  at rev=7",
		key:          "/foo/a",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRev(7)},
		wantKVs:      []*mvccpb.KeyValue{Rev7TxnPutFooA.Kv},
		wantRevision: 8,
	},
	{
		name:         "single key /foo/a at rev=8",
		key:          "/foo/a",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRev(8)},
		wantKVs:      []*mvccpb.KeyValue{Rev8PutFooA.Kv},
		wantRevision: 8,
	},
	{
		name:      "single key /foo/a at rev=9 (future), returns error",
		key:       "/foo/a",
		opts:      []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRev(9)},
		expectErr: rpctypes.ErrFutureRev,
	},
	{
		name:         "non-existing key",
		key:          "/doesnotexist",
		opts:         []clientv3.OpOption{clientv3.WithSerializable()},
		wantKVs:      nil,
		wantRevision: 8,
	},
	{
		name:         "non-existing key at rev=4",
		key:          "/doesnotexist",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRev(4)},
		wantKVs:      nil,
		wantRevision: 8,
	},
	{
		name:      "non-existing key at rev=9 (future), returns error",
		key:       "/doesnotexist",
		opts:      []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRev(9)},
		expectErr: rpctypes.ErrFutureRev,
	},
	{
		name:         "prefix /foo",
		key:          "/foo",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix()},
		wantKVs:      []*mvccpb.KeyValue{Rev8PutFooA.Kv, Rev7TxnPutFooB.Kv, Rev4PutFooC.Kv},
		wantRevision: 8,
	},
	{
		name:         "prefix /foo at rev=5",
		key:          "/foo",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix(), clientv3.WithRev(5)},
		wantKVs:      []*mvccpb.KeyValue{Rev2PutFooA.Kv, Rev3PutFooB.Kv, Rev4PutFooC.Kv, Rev5PutFooD.Kv},
		wantRevision: 8,
	},
	{
		name:         "prefix /foo/b at rev=4",
		key:          "/foo/b",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix(), clientv3.WithRev(4)},
		wantKVs:      []*mvccpb.KeyValue{Rev3PutFooB.Kv},
		wantRevision: 8,
	},
	{
		name:         "prefix /foo/b at rev=7",
		key:          "/foo/b",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix(), clientv3.WithRev(7)},
		wantKVs:      []*mvccpb.KeyValue{Rev7TxnPutFooB.Kv},
		wantRevision: 8,
	},
	{
		name:      "prefix /foo at rev=9 (future), returns error",
		key:       "/foo",
		opts:      []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix(), clientv3.WithRev(9)},
		wantKVs:   []*mvccpb.KeyValue{Rev2PutFooA.Kv, Rev3PutFooB.Kv, Rev4PutFooC.Kv, Rev5PutFooD.Kv},
		expectErr: rpctypes.ErrFutureRev,
	},
	{
		name:         "range [/foo/a, /foo/c)",
		key:          "/foo/a",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRange("/foo/c")},
		wantKVs:      []*mvccpb.KeyValue{Rev8PutFooA.Kv, Rev7TxnPutFooB.Kv},
		wantRevision: 8,
	},
	{
		name:         "range [/foo/a, /foo/d) at rev=5",
		key:          "/foo/a",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRange("/foo/d"), clientv3.WithRev(5)},
		wantKVs:      []*mvccpb.KeyValue{Rev2PutFooA.Kv, Rev3PutFooB.Kv, Rev4PutFooC.Kv},
		wantRevision: 8,
	},
	{
		name:      "range [/foo/a, /foo/c) at rev=9 (future), returns error",
		key:       "/foo/a",
		opts:      []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRange("/foo/c"), clientv3.WithRev(9)},
		expectErr: rpctypes.ErrFutureRev,
	},
	{
		name:         "fromKey /foo/b",
		key:          "/foo/b",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithFromKey()},
		wantKVs:      []*mvccpb.KeyValue{Rev7TxnPutFooB.Kv, Rev4PutFooC.Kv},
		wantRevision: 8,
	},
	{
		name:         "fromKey /foo/b at rev=7",
		key:          "/foo/b",
		opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithFromKey(), clientv3.WithRev(7)},
		wantKVs:      []*mvccpb.KeyValue{Rev7TxnPutFooB.Kv, Rev4PutFooC.Kv},
		wantRevision: 8,
	},
	{
		name:      "fromKey /foo/b at rev=9 (future), returns error",
		key:       "/foo/b",
		opts:      []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithFromKey(), clientv3.WithRev(9)},
		expectErr: rpctypes.ErrFutureRev,
	},
}

func TestCacheWithPrefixGetInScope(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	cli := clus.Client(0)

	testWithPrefixGet(t, cli, func() Getter {
		c, err := cache.New(cli, "/foo")
		if err != nil {
			t.Fatalf("cache.New: %v", err)
		}
		t.Cleanup(c.Close)
		if err := c.WaitReady(t.Context()); err != nil {
			t.Fatalf("cache.WaitReady: %v", err)
		}
		return c
	})
}

func TestWithPrefixGet(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	cli := clus.Client(0)

	testWithPrefixGet(t, cli, func() Getter { return cli.KV })
}

func testWithPrefixGet(t *testing.T, cli *clientv3.Client, getReader func() Getter) {
	ctx := t.Context()
	seedResp, err := cli.Put(ctx, "/foo/a", "val")
	if err != nil {
		t.Fatalf("seed put: %v", err)
	}
	seedRev := seedResp.Header.Revision

	reader := getReader()

	expectedFooA := &mvccpb.KeyValue{
		Key:            []byte("/foo/a"),
		Value:          []byte("val"),
		CreateRevision: seedRev,
		ModRevision:    seedRev,
		Version:        1,
	}

	testCases := []struct {
		name         string
		key          string
		opts         []clientv3.OpOption
		wantKVs      []*mvccpb.KeyValue
		wantRevision int64
	}{
		{
			name:         "single key within cache prefix",
			key:          "/foo/a",
			opts:         []clientv3.OpOption{clientv3.WithSerializable()},
			wantKVs:      []*mvccpb.KeyValue{expectedFooA},
			wantRevision: seedRev,
		},
		{
			name:         "prefix query within cache prefix",
			key:          "/foo",
			opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix()},
			wantKVs:      []*mvccpb.KeyValue{expectedFooA},
			wantRevision: seedRev,
		},
		{
			name:         "range query within cache prefix",
			key:          "/foo/a",
			opts:         []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRange("/foo/b")},
			wantKVs:      []*mvccpb.KeyValue{expectedFooA},
			wantRevision: seedRev,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resp, err := reader.Get(ctx, tc.key, tc.opts...)
			if err != nil {
				t.Fatalf("Get(%q): %v", tc.key, err)
			}

			if diff := cmp.Diff(tc.wantKVs, resp.Kvs); diff != "" {
				t.Errorf("unexpected KVs (-want +got):\n%s", diff)
			}

			if resp.Header.Revision != tc.wantRevision {
				t.Errorf("Header.Revision=%d; want: %d", resp.Header.Revision, tc.wantRevision)
			}
		})
	}
}

func TestCacheWithPrefixGetOutOfScope(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	cli := clus.Client(0)
	c, err := cache.New(cli, "/foo")
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	defer c.Close()
	ctx := t.Context()
	if err := c.WaitReady(ctx); err != nil {
		t.Fatalf("cache.WaitReady: %v", err)
	}

	cases := []struct {
		name string
		key  string
		opts []clientv3.OpOption
	}{
		{
			name: "single key outside prefix",
			key:  "/bar/a",
			opts: []clientv3.OpOption{clientv3.WithSerializable()},
		},
		{
			name: "prefix() outside cache prefix",
			key:  "/bar",
			opts: []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithPrefix()},
		},
		{
			name: "range crossing cache boundary",
			key:  "/foo/a",
			opts: []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithRange("/zzz")},
		},
		{
			name: "fromKey disallowed with cache prefix",
			key:  "/foo/a",
			opts: []clientv3.OpOption{clientv3.WithSerializable(), clientv3.WithFromKey()},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Get(ctx, tc.key, tc.opts...)
			if !errors.Is(err, cache.ErrKeyRangeInvalid) {
				t.Fatalf("expected ErrKeyRangeInvalid; got %v", err)
			}
		})
	}
}

func TestCacheUnsupportedGetOptions(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	c, err := cache.New(client, "", cache.WithHistoryWindowSize(1))
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	defer c.Close()
	if err := c.WaitReady(t.Context()); err != nil {
		t.Fatalf("cache not ready: %v", err)
	}

	unsupported := []struct {
		name string
		opts []clientv3.OpOption
	}{
		{"WithCountOnly", []clientv3.OpOption{clientv3.WithCountOnly()}},
		{"WithLimit", []clientv3.OpOption{clientv3.WithLimit(1)}},
		{"WithSort", []clientv3.OpOption{clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend)}},
		{"WithPrevKV", []clientv3.OpOption{clientv3.WithPrevKV()}},
		{"WithMinModRevision", []clientv3.OpOption{clientv3.WithMinModRev(2)}},
		{"WithMaxModRevision", []clientv3.OpOption{clientv3.WithMaxModRev(10)}},
		{"WithMinCreateRevision", []clientv3.OpOption{clientv3.WithMinCreateRev(3)}},
		{"WithMaxCreateRevision", []clientv3.OpOption{clientv3.WithMaxCreateRev(5)}},
		{"NoSerializable", nil},
	}

	for _, tc := range unsupported {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.Get(t.Context(), "foo", tc.opts...)
			if !errors.Is(err, cache.ErrUnsupportedRequest) {
				t.Errorf("Get with %s: expected ErrUnsupportedRequest, got %v", tc.name, err)
			}
		})
	}
}

type Getter interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

func applyEvents(ctx context.Context, t *testing.T, kv clientv3.KV, evs []*clientv3.Event) int64 {
	var lastRev int64
	for _, batches := range batchEventsByRevision(evs) {
		lastRev = applyEventBatch(ctx, t, kv, batches)
	}
	return lastRev
}

func batchEventsByRevision(events []*clientv3.Event) [][]*clientv3.Event {
	var batches [][]*clientv3.Event
	if len(events) == 0 {
		return batches
	}
	start := 0
	for end := 1; end < len(events); end++ {
		if events[end].Kv.ModRevision != events[start].Kv.ModRevision {
			batches = append(batches, events[start:end])
			start = end
		}
	}
	batches = append(batches, events[start:])
	return batches
}

func applyEventBatch(ctx context.Context, t *testing.T, kv clientv3.KV, batch []*clientv3.Event) int64 {
	ops := make([]clientv3.Op, 0, len(batch))
	for _, event := range batch {
		switch event.Type {
		case clientv3.EventTypePut:
			ops = append(ops, clientv3.OpPut(string(event.Kv.Key), string(event.Kv.Value)))
		case clientv3.EventTypeDelete:
			ops = append(ops, clientv3.OpDelete(string(event.Kv.Key)))
		default:
			t.Fatalf("unsupported event type: %v", event.Type)
		}
	}
	resp, err := kv.Txn(ctx).Then(ops...).Commit()
	if err != nil {
		t.Fatalf("Txn failed: %v", err)
	}
	return resp.Header.Revision
}

func filterEvents(evs []*clientv3.Event, pred func(int64) bool) []*clientv3.Event {
	var out []*clientv3.Event
	for _, ev := range evs {
		if pred(ev.Kv.ModRevision) {
			out = append(out, ev)
		}
	}
	return out
}

func revLessThan(n int64) func(int64) bool     { return func(r int64) bool { return r < n } }
func revGreaterEqual(n int64) func(int64) bool { return func(r int64) bool { return r >= n } }
