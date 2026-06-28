// Copyright 2015 The etcd Authors
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
	"fmt"
	"testing"

	"go.uber.org/zap/zaptest"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/lease"
	betesting "go.etcd.io/etcd/server/v3/storage/backend/testing"
)

func BenchmarkKVWatcherMemoryUsage(b *testing.B) {
	be, _ := betesting.NewDefaultTmpBackend(b)
	watchable := New(zaptest.NewLogger(b), be, &lease.FakeLessor{}, StoreConfig{})

	defer cleanup(watchable, be)

	w := watchable.NewWatchStream()
	defer w.Close()

	b.ReportAllocs()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		w.Watch(b.Context(), 0, []byte(fmt.Sprint("foo", i)), nil, 0)
	}
}

func BenchmarkWatcherGroupMatching(b *testing.B) {
	wg := newWatcherGroup()

	// Create 100 range watchers (simulating namespaces ns-0 to ns-99)
	for i := 0; i < 100; i++ {
		ns := fmt.Sprintf("/pods/ns-%d/", i)
		nsEnd := fmt.Sprintf("/pods/ns-%d0", i) // range end
		wa := &watcher{
			key:    []byte(ns),
			end:    []byte(nsEnd),
			minRev: 1,
			id:     WatchID(i),
			ch:     make(chan WatchResponse, 1000),
		}
		wg.add(wa)
	}

	// Create 50 events across these namespaces
	evs := make([]*mvccpb.Event, 50)
	for i := 0; i < 50; i++ {
		nsIdx := i % 100
		key := fmt.Sprintf("/pods/ns-%d/pod-%d", nsIdx, i)
		evs[i] = &mvccpb.Event{
			Type: mvccpb.Event_PUT,
			Kv: &mvccpb.KeyValue{
				Key:            []byte(key),
				ModRevision:    2,
				CreateRevision: 2,
			},
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		wb := newWatcherBatch(&wg, evs)
		if wb == nil {
			b.Fatal("nil batch")
		}
	}
}

