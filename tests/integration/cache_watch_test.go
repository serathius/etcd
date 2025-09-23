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
	"fmt"
	"strings"
	"testing"
	"time"

	cache "go.etcd.io/etcd/cache/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func TestCacheWithoutPrefixWatch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	c, err := cache.New(client, "", cache.WithHistoryWindowSize(32))
	if err != nil {
		t.Fatalf("New(...): %v", err)
	}
	t.Cleanup(c.Close)
	if err := c.WaitReady(t.Context()); err != nil {
		t.Fatalf("cache not ready: %v", err)
	}
	testWatch(t, client.KV, c)
}

func TestCacheWithPrefixWatch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	ctx := t.Context()

	tests := []struct {
		name           string
		key            string
		opts           []clientv3.OpOption
		expectCanceled bool
	}{
		{
			name:           "single key within prefix",
			key:            "/foo/a",
			opts:           nil,
			expectCanceled: false,
		},
		{
			name:           "single key outside prefix returns error",
			key:            "/bar/a",
			opts:           nil,
			expectCanceled: true,
		},
		{
			name:           "prefix() within cache prefix",
			key:            "/foo",
			opts:           []clientv3.OpOption{clientv3.WithPrefix()},
			expectCanceled: false,
		},
		{
			name:           "prefix() outside cache prefix returns error",
			key:            "/bar",
			opts:           []clientv3.OpOption{clientv3.WithPrefix()},
			expectCanceled: true,
		},
		{
			name:           "range within prefix",
			key:            "/foo/a",
			opts:           []clientv3.OpOption{clientv3.WithRange("/foo/b")},
			expectCanceled: false,
		},
		{
			name:           "range crosses cache prefix boundary returns error",
			key:            "/foo/a",
			opts:           []clientv3.OpOption{clientv3.WithRange("/zzz")},
			expectCanceled: true,
		},
		{
			name:           "fromKey not allowed when cache has prefix returns error",
			key:            "/foo/a",
			opts:           []clientv3.OpOption{clientv3.WithFromKey()},
			expectCanceled: true,
		},
	}

	const testPutKey = "/foo/a"

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c, err := cache.New(client, "/foo")
			if err != nil {
				t.Fatalf("New(...): %v", err)
			}
			defer c.Close()
			if err := c.WaitReady(ctx); err != nil {
				t.Fatal(err)
			}

			watchCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			ch := c.Watch(watchCtx, tc.key, tc.opts...)

			if !tc.expectCanceled {
				if _, err := client.Put(ctx, testPutKey, "val"); err != nil {
					t.Fatalf("Put(%q): %v", testPutKey, err)
				}
			}

			select {
			case resp, ok := <-ch:
				if tc.expectCanceled {
					if !ok || !resp.Canceled {
						t.Fatalf("expected canceled watch, got %+v (closed=%v)", resp, !ok)
					}
					return
				}

				if !ok || resp.Canceled {
					t.Fatalf("expected active watch (not canceled), got %+v (closed=%v)", resp, !ok)
				}
				if len(resp.Events) == 0 {
					t.Fatalf("watch returned no events, expected at least the test event")
				}
				if string(resp.Events[0].Kv.Key) != testPutKey {
					t.Fatalf("got event for key %q, want %q", resp.Events[0].Kv.Key, testPutKey)
				}
			case <-watchCtx.Done():
				if tc.expectCanceled {
					t.Fatalf("watch did not cancel within timeout")
				} else {
					t.Fatalf("active watch did not deliver event within timeout")
				}
			}
		})
	}
}

func TestCacheLaggingWatcher(t *testing.T) {
	const prefix = "/test/"
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	tests := []struct {
		name                string
		window              int
		eventCount          int
		wantExactEventCount int
		wantAtMaxEventCount int
		wantClosed          bool
	}{
		{
			name:                "all event fit",
			window:              10,
			eventCount:          9,
			wantExactEventCount: 9,
			wantClosed:          false,
		},
		{
			name:                "events fill window",
			window:              10,
			eventCount:          10,
			wantExactEventCount: 10,
			wantClosed:          false,
		},
		{
			name:                "event fill pipeline",
			window:              10,
			eventCount:          11,
			wantExactEventCount: 11,
			wantClosed:          false,
		},
		{
			name:                "pipeline overflow",
			window:              10,
			eventCount:          12,
			wantAtMaxEventCount: 1, // Either 0 or 1.
			wantClosed:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := cache.New(
				client, prefix,
				cache.WithHistoryWindowSize(tt.window),
				cache.WithPerWatcherBufferSize(0),
				cache.WithResyncInterval(10*time.Millisecond),
			)
			if err != nil {
				t.Fatalf("New(...): %v", err)
			}
			defer c.Close()

			if err := c.WaitReady(t.Context()); err != nil {
				t.Fatalf("cache not ready: %v", err)
			}
			ch := c.Watch(t.Context(), prefix, clientv3.WithPrefix())

			generateEvents(t, client, prefix, tt.eventCount)
			gotEvents, ok := collectAndAssertAtomicEvents(t, ch)
			closed := !ok

			if tt.wantExactEventCount != 0 && tt.wantExactEventCount != len(gotEvents) {
				t.Errorf("gotEvents=%v, wantEvents=%v", len(gotEvents), tt.wantExactEventCount)
			}
			if tt.wantAtMaxEventCount != 0 && len(gotEvents) > tt.wantAtMaxEventCount {
				t.Errorf("gotEvents=%v, wantEvents<%v", len(gotEvents), tt.wantAtMaxEventCount)
			}
			if closed != tt.wantClosed {
				t.Errorf("closed=%v, wantClosed=%v", closed, tt.wantClosed)
			}
		})
	}
}

func TestCacheUnsupportedWatchOptions(t *testing.T) {
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
		opt  clientv3.OpOption
	}{
		{"WithPrevKV", clientv3.WithPrevKV()},
		{"WithFragment", clientv3.WithFragment()},
		{"WithProgressNotify", clientv3.WithProgressNotify()},
		{"WithCreatedNotify", clientv3.WithCreatedNotify()},
		{"WithFilterPut", clientv3.WithFilterPut()},
		{"WithFilterDelete", clientv3.WithFilterDelete()},
	}

	for _, tc := range unsupported {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ch := c.Watch(t.Context(), "foo", tc.opt)

			resp, ok := <-ch
			if !ok {
				t.Fatalf("channel closed without yielding a response")
			}
			if !resp.Canceled {
				t.Errorf("expected Canceled=true, got %+v", resp)
			}
			if !strings.Contains(resp.Err().Error(), cache.ErrUnsupportedRequest.Error()) {
				t.Errorf("expected ErrUnsupportedWatch text %q, got %v",
					cache.ErrUnsupportedRequest.Error(), resp.Err())
			}
		})
	}
}

func generateEvents(t *testing.T, client *clientv3.Client, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		if _, err := client.Put(t.Context(), key, fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
}
