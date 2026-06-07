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

package snapshot_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func TestCompactionBlockUnderPartition(t *testing.T) {
	integration.BeforeTest(t)
	t.Setenv("CLUSTER_DEBUG", "1")

	t.Run("DefaultDelayBlocksCompaction", func(t *testing.T) {
		runCompactionPartitionTest(t, false)
	})

	t.Run("ZeroDelayAllowsCompaction", func(t *testing.T) {
		etcdserver.ReleaseDelayAfterSnapshot = 0
		defer func() {
			etcdserver.ReleaseDelayAfterSnapshot = 30 * time.Second
		}()
		runCompactionPartitionTest(t, true)
	})
}

func runCompactionPartitionTest(t *testing.T, expectCompaction bool) {
	clus := integration.NewCluster(t, &integration.ClusterConfig{
		Size:                   3,
		SnapshotCount:          10,
		SnapshotCatchUpEntries: 10,
	})
	defer clus.Terminate(t)

	lead := clus.WaitLeader(t)
	follower := (lead + 1) % 3
	other := (lead + 2) % 3

	// Isolate follower
	clus.Members[follower].InjectPartition(t, clus.Members[lead], clus.Members[other])
	partitioned1 := true
	defer func() {
		if partitioned1 {
			clus.Members[follower].RecoverPartition(t, clus.Members[lead], clus.Members[other])
		}
	}()

	// Put 15 keys to leader (exceeds SnapshotCount=10)
	ccfg := clientv3.Config{Endpoints: []string{clus.Members[lead].GRPCURL}}
	cli, err := integration.NewClient(t, ccfg)
	require.NoError(t, err)
	defer cli.Close()

	for i := 0; i < 15; i++ {
		_, err = cli.Put(t.Context(), fmt.Sprintf("key-%d", i), "val")
		require.NoError(t, err)
	}

	// Reconnect follower
	clus.Members[follower].RecoverPartition(t, clus.Members[lead], clus.Members[other])
	partitioned1 = false

	// Wait for follower to catch up via snapshot
	fcli, err := integration.NewClient(t, clientv3.Config{Endpoints: []string{clus.Members[follower].GRPCURL}})
	require.NoError(t, err)
	defer fcli.Close()

	require.Eventually(t, func() bool {
		_, err := fcli.Get(t.Context(), "key-14")
		return err == nil
	}, 10*time.Second, 100*time.Millisecond)

	// Isolate follower again
	clus.Members[follower].InjectPartition(t, clus.Members[lead], clus.Members[other])
	partitioned2 := true
	defer func() {
		if partitioned2 {
			clus.Members[follower].RecoverPartition(t, clus.Members[lead], clus.Members[other])
		}
	}()

	// Put another 15 keys to leader to trigger compaction again
	for i := 15; i < 30; i++ {
		_, err = cli.Put(t.Context(), fmt.Sprintf("key-%d", i), "val")
		require.NoError(t, err)
	}

	// Wait for a short time to let compaction run
	time.Sleep(1 * time.Second)

	// Check if the leader compacted its logs
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = clus.Members[lead].LogObserver.Expect(ctx, "compacted Raft logs", 3)
	if expectCompaction {
		require.NoError(t, err, "Expected 3 compaction logs on leader (compaction not blocked)")
	} else {
		require.Error(t, err, "Expected at most 2 compaction logs on leader (compaction blocked)")
	}
}
