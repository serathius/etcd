// Copyright 2025 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package report

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.etcd.io/raft/v3/raftpb"
)

func TestQuorumWALEntries(t *testing.T) {
	tcs := []struct {
		name             string
		memberWALEntries [][]raftpb.Entry
		clusterSize      int
		expectErr        string
		expectEntries    []raftpb.Entry
	}{
		{
			name: "Success when one member cluster has entries",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
			},
			clusterSize: 1,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Success when three members agree on entries",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
			},
			clusterSize: 3,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Success when three members have no entries",
			memberWALEntries: [][]raftpb.Entry{
				{},
				{},
				{},
			},
			clusterSize:   3,
			expectEntries: []raftpb.Entry{},
		},
		{
			name: "Success when one member's WAL is unavailable in three node cluster",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
			},
			clusterSize: 3,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Success when one member returned empty WAL in three-node cluster",

			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
			},
			clusterSize: 3,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Error when two members' WALs are unavailable in three-node cluster",

			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
			},
			clusterSize: 3,
			expectErr:   "unexpected differences between wal entries",
		},
		{
			name: "Success if members didn't observe whole history",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
				},
				{
					{Index: 1, Data: []byte("1")},
				},
			},
			clusterSize: 3,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Success if only one member observed the history",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{},
				{},
			},
			clusterSize: 3,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Success when one member observed a different last entry",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("4")},
				},
			},
			clusterSize: 3,
			expectEntries: []raftpb.Entry{
				{Index: 1, Data: []byte("1")},
				{Index: 2, Data: []byte("2")},
				{Index: 3, Data: []byte("3")},
			},
		},
		{
			name: "Error when one member didn't observe the whole history and others observed a different last entry",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("4")},
				},
			},
			clusterSize: 3,
			expectErr:   "unexpected differences between wal entries",
		},
		{
			name: "Error when three members observed different last entry",
			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("4")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("5")},
				},
			},
			clusterSize: 3,
			expectErr:   "unexpected differences between wal entries",
		},
		{
			name: "Error when one member's WAL is unavailable and others differ on last entry",

			memberWALEntries: [][]raftpb.Entry{
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("4")},
				},
			},
			clusterSize: 3,
			expectErr:   "unexpected differences between wal entries",
		},
		{
			name: "Error when one member observed empty history and others differ on last entry",
			memberWALEntries: [][]raftpb.Entry{
				{},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("3")},
				},
				{
					{Index: 1, Data: []byte("1")},
					{Index: 2, Data: []byte("2")},
					{Index: 3, Data: []byte("4")},
				},
			},
			clusterSize: 3,
			expectErr:   "unexpected differences between wal entries",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			requests, err := quorumWALEntries(tc.memberWALEntries, tc.clusterSize)
			if tc.expectErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.expectErr)
			}
			if tc.expectEntries == nil {
				require.Nil(t, requests)
			} else {
				require.Equal(t, tc.expectEntries, requests)
			}
		})
	}
}
