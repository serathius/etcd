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
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func TestWatch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	testWatch(t, client.KV, client.Watcher)
}

func testWatch(t *testing.T, kv clientv3.KV, watcher Watcher) {
	ctx := t.Context()
	rev2PutFooA := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("1"),
			CreateRevision: 2,
			ModRevision:    2,
			Version:        1,
		},
	}
	rev3PutFooB := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/b"),
			Value:          []byte("2"),
			CreateRevision: 3,
			ModRevision:    3,
			Version:        1,
		},
	}
	rev4DeleteFooA := &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte("/foo/a"),
			ModRevision: 4,
		},
	}
	rev5PutFooA := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("3"),
			CreateRevision: 5,
			ModRevision:    5,
			Version:        1,
		},
	}
	rev5DeleteFooB := &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte("/foo/b"),
			ModRevision: 5,
		},
	}
	rev6PutFooC := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/c"),
			Value:          []byte("x"),
			CreateRevision: 6,
			ModRevision:    6,
			Version:        1,
		},
	}
	rev7PutFooBar := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/bar"),
			Value:          []byte("y"),
			CreateRevision: 7,
			ModRevision:    7,
			Version:        1,
		},
	}
	rev8PutFooBaz := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/baz"),
			Value:          []byte("z"),
			CreateRevision: 8,
			ModRevision:    8,
			Version:        1,
		},
	}
	rev9PutFooYoo := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/yoo"),
			Value:          []byte("1"),
			CreateRevision: 9,
			ModRevision:    9,
			Version:        1,
		},
	}
	rev10PutZoo := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/zoo"),
			Value:          []byte("1"),
			CreateRevision: 10,
			ModRevision:    10,
			Version:        1,
		},
	}
	rev11PutFooFuture := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/future"),
			Value:          []byte("42"),
			CreateRevision: 11,
			ModRevision:    11,
			Version:        1,
		},
	}
	rev12PutFooTx1 := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/tx1"),
			Value:          []byte("a"),
			CreateRevision: 12,
			ModRevision:    12,
			Version:        1,
		},
	}
	rev12DeleteFooFuture := &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte("/foo/future"),
			ModRevision: 12,
		},
	}
	rev12PutFooTx2 := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/tx2"),
			Value:          []byte("b"),
			CreateRevision: 12,
			ModRevision:    12,
			Version:        1,
		},
	}

	tcs := []struct {
		name       string
		key        string
		opts       []clientv3.OpOption
		wantEvents []*clientv3.Event
	}{
		{
			name:       "Watch single key existing /foo/c",
			key:        "/foo/c",
			opts:       []clientv3.OpOption{clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev6PutFooC},
		},
		{
			name:       "Watch single key non‑existent /doesnotexist",
			key:        "/doesnotexist",
			opts:       []clientv3.OpOption{clientv3.WithRev(2)},
			wantEvents: nil,
		},
		{
			name:       "Watch range empty",
			key:        "",
			opts:       []clientv3.OpOption{clientv3.WithRange(""), clientv3.WithRev(2)},
			wantEvents: nil,
		},
		{
			name:       "Watch range [/foo/a, /foo/b)",
			key:        "/foo/a",
			opts:       []clientv3.OpOption{clientv3.WithRange("/foo/b"), clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev2PutFooA, rev4DeleteFooA, rev5PutFooA},
		},
		{
			name:       "Watch with prefix /foo/b",
			key:        "/foo/b",
			opts:       []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev3PutFooB, rev5DeleteFooB, rev7PutFooBar, rev8PutFooBaz},
		},
		{
			name:       "Watch with prefix non-existent /doesnotexist",
			key:        "/doesnotexist",
			opts:       []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(2)},
			wantEvents: nil,
		},
		{
			name:       "Watch with prefix empty string",
			key:        "",
			opts:       []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev2PutFooA, rev3PutFooB, rev4DeleteFooA, rev5PutFooA, rev5DeleteFooB, rev6PutFooC, rev7PutFooBar, rev8PutFooBaz, rev9PutFooYoo, rev10PutZoo, rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
		{
			name:       "Watch from key /foo/b",
			key:        "/foo/b",
			opts:       []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev3PutFooB, rev5DeleteFooB, rev6PutFooC, rev7PutFooBar, rev8PutFooBaz, rev9PutFooYoo, rev10PutZoo, rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
		{
			name:       "Watch from empty key",
			key:        "",
			opts:       []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev2PutFooA, rev3PutFooB, rev4DeleteFooA, rev5PutFooA, rev5DeleteFooB, rev6PutFooC, rev7PutFooBar, rev8PutFooBaz, rev9PutFooYoo, rev10PutZoo, rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
		{
			name:       "Watch from non-existent key /doesnotexist",
			key:        "/doesnotexist",
			opts:       []clientv3.OpOption{clientv3.WithFromKey(), clientv3.WithRev(2)},
			wantEvents: []*clientv3.Event{rev2PutFooA, rev3PutFooB, rev4DeleteFooA, rev5PutFooA, rev5DeleteFooB, rev6PutFooC, rev7PutFooBar, rev8PutFooBaz, rev9PutFooYoo, rev10PutZoo, rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
		{
			name:       "Watch from rev 4 with single key /foo/a",
			key:        "/foo/a",
			opts:       []clientv3.OpOption{clientv3.WithRev(4)},
			wantEvents: []*clientv3.Event{rev4DeleteFooA, rev5PutFooA},
		},
		{
			name:       "Watch from rev 6 with single key /foo/a",
			key:        "/foo/a",
			opts:       []clientv3.OpOption{clientv3.WithRev(6)},
			wantEvents: nil,
		},
		{
			name: "Watch from rev 5 with prefix /foo",
			key:  "/foo",
			opts: []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(5)},
			wantEvents: []*clientv3.Event{
				rev5PutFooA, rev5DeleteFooB, rev6PutFooC, rev7PutFooBar, rev8PutFooBaz, rev9PutFooYoo, rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2,
			},
		},
		{
			name: "Watch from rev 10 with prefix /foo",
			key:  "/foo",
			opts: []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(10)},
			wantEvents: []*clientv3.Event{
				rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2,
			},
		},
		{
			name: "Watch from rev 4 with range [/foo/a, /foo/c)",
			key:  "/foo/a",
			opts: []clientv3.OpOption{clientv3.WithRange("/foo/c"), clientv3.WithRev(4)},
			wantEvents: []*clientv3.Event{
				rev4DeleteFooA, rev5PutFooA, rev5DeleteFooB, rev7PutFooBar, rev8PutFooBaz,
			},
		},
		{
			name:       "Latest‑revision watcher for /foo",
			key:        "/foo",
			opts:       []clientv3.OpOption{clientv3.WithPrefix()},
			wantEvents: []*clientv3.Event{rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
		{
			name:       "Watch from rev 11 with single key /foo/future",
			key:        "/foo",
			opts:       []clientv3.OpOption{clientv3.WithRev(11), clientv3.WithPrefix()},
			wantEvents: []*clientv3.Event{rev11PutFooFuture, rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
		{
			name:       "Watch from rev 12 with txn prefix /foo",
			key:        "/foo",
			opts:       []clientv3.OpOption{clientv3.WithRev(12), clientv3.WithPrefix()},
			wantEvents: []*clientv3.Event{rev12PutFooTx1, rev12DeleteFooFuture, rev12PutFooTx2},
		},
	}

	t.Log("Write the first batch of events rev 2-10")
	if _, err := kv.Put(ctx, string(rev2PutFooA.Kv.Key), string(rev2PutFooA.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(rev3PutFooB.Kv.Key), string(rev3PutFooB.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Delete(ctx, string(rev4DeleteFooA.Kv.Key)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := kv.Txn(ctx).Then(clientv3.OpPut(string(rev5PutFooA.Kv.Key), string(rev5PutFooA.Kv.Value)), clientv3.OpDelete(string(rev5DeleteFooB.Kv.Key))).Commit(); err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if _, err := kv.Put(ctx, string(rev6PutFooC.Kv.Key), string(rev6PutFooC.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(rev7PutFooBar.Kv.Key), string(rev7PutFooBar.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(rev8PutFooBaz.Kv.Key), string(rev8PutFooBaz.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(rev9PutFooYoo.Kv.Key), string(rev9PutFooYoo.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(rev10PutZoo.Kv.Key), string(rev10PutZoo.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	t.Log("Open watches")
	watches := make([]clientv3.WatchChan, len(tcs))
	for i, tc := range tcs {
		watches[i] = watcher.Watch(ctx, tc.key, tc.opts...)
	}
	time.Sleep(50 * time.Millisecond)

	t.Log("Write the second batch of events rev 11‑12")
	if _, err := kv.Put(ctx, string(rev11PutFooFuture.Kv.Key), string(rev11PutFooFuture.Kv.Value)); err != nil {
		t.Fatalf("Put /foo/future: %v", err)
	}
	if _, err := kv.Txn(ctx).Then(
		clientv3.OpPut(string(rev12PutFooTx1.Kv.Key), string(rev12PutFooTx1.Kv.Value)),
		clientv3.OpDelete(string(rev12DeleteFooFuture.Kv.Key)),
		clientv3.OpPut(string(rev12PutFooTx2.Kv.Key), string(rev12PutFooTx2.Kv.Value)),
	).Commit(); err != nil {
		t.Fatalf("Txn rev12: %v", err)
	}

	t.Log("Validate")
	for i, tc := range tcs {
		i, tc := i, tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			events, _ := collectAndAssertAtomicEvents(t, watches[i])
			if diff := cmp.Diff(tc.wantEvents, events); diff != "" {
				t.Errorf("unexpected events (-want +got):\n%s", diff)
			}
		})
	}
}

type Watcher interface {
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

func collectAndAssertAtomicEvents(t *testing.T, watch clientv3.WatchChan) (events []*clientv3.Event, ok bool) {
	deadline := time.After(time.Second)
	var lastRevision int64

	for {
		select {
		case resp, ok := <-watch:
			if !ok {
				return events, false
			}
			if len(resp.Events) != 0 && resp.Events[0].Kv.ModRevision == lastRevision {
				t.Fatalf("same revision found as in previous response: %d", lastRevision)
			}
			for _, ev := range resp.Events {
				if ev.Kv.ModRevision < lastRevision {
					t.Fatalf("revision went backwards: last %d, now %d", lastRevision, ev.Kv.ModRevision)
				}
				events = append(events, ev)
				lastRevision = ev.Kv.ModRevision
			}
		case <-deadline:
			return events, true
		case <-time.After(100 * time.Millisecond):
			return events, true
		}
	}
}
