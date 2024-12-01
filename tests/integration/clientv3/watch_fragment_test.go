// Copyright 2018 The etcd Authors
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

//go:build !cluster_proxy

package clientv3test

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/testutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	integration2 "go.etcd.io/etcd/tests/v3/framework/integration"
	"strings"
	"testing"
	"time"
)

// testWatchFragment triggers watch response that spans over multiple
// revisions exceeding server request limits when combined.
func TestWatchFragment(t *testing.T) {

	tcs := []struct {
		name              string
		clientRecvMsgSize int
		fragmentEnabled   bool
		expectEventCount  int
		revisionCount     int
		eventsPerRevision int
		eventSize         int
		expertError       string
	}{
		{
			name:              "Within limit, without fragmentation, watch response can arrive",
			revisionCount:     10,
			eventsPerRevision: 1,
			eventSize:         1024 * 1024,
			expectEventCount:  10,
		},
		{
			name:              "Outside limit without fragmentation, watch fails",
			revisionCount:     10,
			eventsPerRevision: 1,
			eventSize:         1024 * 1024,
			clientRecvMsgSize: 1.5 * 1024 * 1024,
			expertError:       "code = ResourceExhausted desc = grpc: received message larger than max (",
		},
		{
			name:              "Within limit, with fragmentation, watch response can arrive",
			revisionCount:     10,
			eventsPerRevision: 1,
			eventSize:         1024 * 1024,
			fragmentEnabled:   true,
			expectEventCount:  10,
		},
		{
			name:              "Outside limit, with fragmentation, watch response can arrive",
			revisionCount:     10,
			eventsPerRevision: 1,
			eventSize:         1024 * 1024,
			fragmentEnabled:   true,
			clientRecvMsgSize: 1.5 * 1024 * 1024,
			expectEventCount:  10,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			integration2.BeforeTest(t)
			cfg := &integration2.ClusterConfig{
				Size:                     1,
				MaxRequestBytes:          uint(tc.clientRecvMsgSize),
				ClientMaxCallRecvMsgSize: tc.clientRecvMsgSize,
			}
			clus := integration2.NewCluster(t, cfg)
			defer clus.Terminate(t)

			cli := clus.Client(0)
			for i := 0; i < tc.revisionCount; i++ {
				var ops []clientv3.Op
				for j := 0; j < tc.eventsPerRevision; j++ {
					ops = append(ops, clientv3.OpPut(fmt.Sprintf("foo-%d-%d", i, j), strings.Repeat("a", tc.eventSize)))
				}
				_, err := cli.Txn(context.TODO()).Then(ops...).Commit()
				require.NoError(t, err)
			}

			opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(1)}
			if tc.fragmentEnabled {
				opts = append(opts, clientv3.WithFragment())
			}
			wch := cli.Watch(context.TODO(), "foo", opts...)

			// expect 10 MiB watch response
			select {
			case ws := <-wch:
				assert.Len(t, ws.Events, tc.expectEventCount)
				if tc.expertError != "" {
					assert.ErrorContains(t, ws.Err(), tc.expertError)
				} else {
					assert.NoError(t, ws.Err())
				}
			case <-time.After(testutil.RequestTimeout):
				t.Fatalf("took too long to receive events")
			}
		})
	}
}
