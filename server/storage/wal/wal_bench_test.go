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

package wal

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/raft/v3/raftpb"
)

var k8sBatchDistribution = []int{
	1, 1, 1, 1, 1, 1, 30, 51, 30, 26, 78, 56, 44, 84, 59, 66, 60, 22, 100, 118,
	157, 163,
}

func BenchmarkWrite100EntryWithoutBatch(b *testing.B) { benchmarkWriteEntry(b, 100, []int{1}) }
func BenchmarkWrite100EntryBatch10(b *testing.B)      { benchmarkWriteEntry(b, 100, []int{10}) }
func BenchmarkWrite100EntryBatch100(b *testing.B)     { benchmarkWriteEntry(b, 100, []int{100}) }
func BenchmarkWrite100EntryBatch500(b *testing.B)     { benchmarkWriteEntry(b, 100, []int{500}) }
func BenchmarkWrite100EntryBatch1000(b *testing.B)    { benchmarkWriteEntry(b, 100, []int{1000}) }

func BenchmarkWrite1000EntryWithoutBatch(b *testing.B) { benchmarkWriteEntry(b, 1000, []int{1}) }
func BenchmarkWrite1000EntryBatch10(b *testing.B)      { benchmarkWriteEntry(b, 1000, []int{10}) }
func BenchmarkWrite1000EntryBatch100(b *testing.B)     { benchmarkWriteEntry(b, 1000, []int{100}) }
func BenchmarkWrite1000EntryBatch500(b *testing.B)     { benchmarkWriteEntry(b, 1000, []int{500}) }
func BenchmarkWrite1000EntryBatch1000(b *testing.B)    { benchmarkWriteEntry(b, 1000, []int{1000}) }

func BenchmarkWrite10KEntryWithoutBatch(b *testing.B) { benchmarkWriteEntry(b, 10145, []int{1}) }
func BenchmarkWrite10KEntryBatch10(b *testing.B)      { benchmarkWriteEntry(b, 10145, []int{10}) }
func BenchmarkWrite10KEntryBatch50(b *testing.B)      { benchmarkWriteEntry(b, 10145, []int{50}) }
func BenchmarkWrite10KEntryBatch100(b *testing.B)     { benchmarkWriteEntry(b, 10145, []int{100}) }
func BenchmarkWrite10KEntryBatch500(b *testing.B)     { benchmarkWriteEntry(b, 10145, []int{500}) }

func BenchmarkWrite10KEntryK8sDistribution(b *testing.B) {
	benchmarkWriteEntry(b, 10145, k8sBatchDistribution)
}

func benchmarkWriteEntry(b *testing.B, size int, batchSamples []int) {
	p := b.TempDir()

	w, err := Create(zaptest.NewLogger(b), p, []byte("somedata"))
	require.NoErrorf(b, err, "err = %v, want nil", err)
	if os.Getenv("WAL_BENCH_NO_SYNC") == "1" {
		w.unsafeNoSync = true
	}
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i % 256)
	}
	e := &raftpb.Entry{Data: data}

	b.ResetTimer()
	b.SetBytes(int64(proto.Size(e)))

	i := 0
	batchIdx := 0
	for i < b.N {
		batchLimit := 1
		if len(batchSamples) > 0 {
			batchLimit = batchSamples[batchIdx]
			batchIdx = (batchIdx + 1) % len(batchSamples)
		}

		for j := 0; j < batchLimit && i < b.N; j++ {
			err := w.saveEntry(e)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
		w.sync()
	}
}
