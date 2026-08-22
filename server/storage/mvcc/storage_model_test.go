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
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
)

type modelTestOp struct {
	req           StorageRequest
	res           StorageResponse
	expectFailure bool
}

type modelTestCase struct {
	name       string
	operations []modelTestOp
}

var commonModelScenarios = []modelTestCase{
	{
		name: "MVCC Put and Get latest",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")},
				res: StorageResponse{Rev: 2},
			},
			{
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 0},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k1"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1},
					},
					Count: 1,
				},
			},
			{
				// Getting non-existent key returns empty
				req: StorageRequest{Op: OpGet, Key: []byte("k2"), Rev: 0},
				res: StorageResponse{KVs: nil, Count: 0},
			},
		},
	},
	{
		name: "MVCC Put, Update, Range and Delete",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")},
				res: StorageResponse{Rev: 2},
			},
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k2"), Value: []byte("v2")},
				res: StorageResponse{Rev: 3},
			},
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k1"), Value: []byte("v1-updated")},
				res: StorageResponse{Rev: 4},
			},
			{
				req: StorageRequest{Op: OpRange, Key: []byte("k1"), End: []byte("k3")},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k1"), Value: []byte("v1-updated"), CreateRevision: 2, ModRevision: 4, Version: 2},
						{Key: []byte("k2"), Value: []byte("v2"), CreateRevision: 3, ModRevision: 3, Version: 1},
					},
					Count: 2,
				},
			},
			{
				req: StorageRequest{Op: OpDelete, Key: []byte("k1")},
				res: StorageResponse{Rev: 5},
			},
			{
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 0},
				res: StorageResponse{KVs: nil, Count: 0},
			},
			{
				req: StorageRequest{Op: OpGet, Key: []byte("k2"), Rev: 0},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k2"), Value: []byte("v2"), CreateRevision: 3, ModRevision: 3, Version: 1},
					},
					Count: 1,
				},
			},
		},
	},
	{
		name: "MVCC Multi-operation Txn",
		operations: []modelTestOp{
			{
				req: StorageRequest{
					Op: OpTxn,
					TxnOps: []TxnSubOp{
						{Type: TxnSubOpPut, Key: []byte("k1"), Value: []byte("v1")},
						{Type: TxnSubOpPut, Key: []byte("k2"), Value: []byte("v2")},
					},
				},
				res: StorageResponse{Rev: 2},
			},
			{
				req: StorageRequest{Op: OpRange, Key: []byte("k1"), End: []byte("k3")},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k1"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1},
						{Key: []byte("k2"), Value: []byte("v2"), CreateRevision: 2, ModRevision: 2, Version: 1},
					},
					Count: 2,
				},
			},
			{
				req: StorageRequest{
					Op: OpTxn,
					TxnOps: []TxnSubOp{
						{Type: TxnSubOpDeleteRange, Key: []byte("k1"), End: []byte("k2")},
						{Type: TxnSubOpPut, Key: []byte("k3"), Value: []byte("v3")},
					},
				},
				res: StorageResponse{Rev: 3},
			},
			{
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 0},
				res: StorageResponse{KVs: nil, Count: 0},
			},
			{
				req: StorageRequest{Op: OpGet, Key: []byte("k3"), Rev: 0},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k3"), Value: []byte("v3"), CreateRevision: 3, ModRevision: 3, Version: 1},
					},
					Count: 1,
				},
			},
		},
	},
	{
		name: "Backend Put, Range and Delete across buckets",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "meta", Key: []byte("bkey1"), Value: []byte("bval1")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "lease", Key: []byte("lkey1"), Value: []byte("lval1")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendRange, Bucket: "meta", Key: []byte("bkey1")},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("bkey1")},
					BackendVals: [][]byte{[]byte("bval1")},
				},
			},
			{
				req: StorageRequest{Op: OpBackendRange, Bucket: "lease", Key: []byte("lkey1")},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("lkey1")},
					BackendVals: [][]byte{[]byte("lval1")},
				},
			},
			{
				// Delete from backend meta
				req: StorageRequest{Op: OpBackendDelete, Bucket: "meta", Key: []byte("bkey1")},
				res: StorageResponse{},
			},
			{
				// After delete, backend range returns empty
				req: StorageRequest{Op: OpBackendRange, Bucket: "meta", Key: []byte("bkey1")},
				res: StorageResponse{BackendKeys: nil, BackendVals: nil},
			},
			{
				// Lease bucket remains unaffected
				req: StorageRequest{Op: OpBackendRange, Bucket: "lease", Key: []byte("lkey1")},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("lkey1")},
					BackendVals: [][]byte{[]byte("lval1")},
				},
			},
		},
	},
	{
		name: "Backend ForEach and Hash",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "meta", Key: []byte("k1"), Value: []byte("v1")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "meta", Key: []byte("k2"), Value: []byte("v2")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "meta", Key: []byte("k1"), Value: []byte("v1-updated")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendForEach, Bucket: "meta"},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("k1"), []byte("k2")},
					BackendVals: [][]byte{[]byte("v1-updated"), []byte("v2")},
				},
			},
			{
				req: StorageRequest{Op: OpBackendHash},
				res: StorageResponse{},
			},
		},
	},
	{
		name: "Cross-Layer Transaction (MVCC + Backend Meta)",
		operations: []modelTestOp{
			{
				req: StorageRequest{
					Op:      OpCrossLayerTxn,
					Bucket:  "meta",
					Key:     []byte("k1"),
					Value:   []byte("v1"),
					MetaKey: []byte("consistent_index"),
					MetaVal: []byte("0000000000000002"),
				},
				res: StorageResponse{Rev: 2},
			},
			{
				// MVCC read observes k1 at rev 2
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 0},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k1"), Value: []byte("v1"), CreateRevision: 2, ModRevision: 2, Version: 1},
					},
					Count: 1,
				},
			},
			{
				// Backend read observes consistent_index in meta bucket
				req: StorageRequest{Op: OpBackendRange, Bucket: "meta", Key: []byte("consistent_index")},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("consistent_index")},
					BackendVals: [][]byte{[]byte("0000000000000002")},
				},
			},
		},
	},
	{
		name: "Compaction and historical error handling",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")},
				res: StorageResponse{Rev: 2},
			},
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k1"), Value: []byte("v2")},
				res: StorageResponse{Rev: 3},
			},
			{
				req: StorageRequest{Op: OpCompact, Rev: 2},
				res: StorageResponse{},
			},
			{
				// Reading rev 1 (compacted) returns ErrCompacted
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 1},
				res: StorageResponse{Err: ErrCompacted},
			},
			{
				// Reading future rev 10 returns ErrFutureRev
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 10},
				res: StorageResponse{Err: ErrFutureRev},
			},
		},
	},
}

var negativeModelScenarios = []modelTestCase{
	{
		name: "Model rejects wrong MVCC value",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")},
				res: StorageResponse{Rev: 2},
			},
			{
				req: StorageRequest{Op: OpGet, Key: []byte("k1"), Rev: 0},
				res: StorageResponse{
					KVs: []*mvccpb.KeyValue{
						{Key: []byte("k1"), Value: []byte("wrong-val"), CreateRevision: 2, ModRevision: 2, Version: 1},
					},
				},
				expectFailure: true,
			},
		},
	},
	{
		name: "Model rejects wrong backend value",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "meta", Key: []byte("bkey1"), Value: []byte("bval1")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendRange, Bucket: "meta", Key: []byte("bkey1")},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("bkey1")},
					BackendVals: [][]byte{[]byte("bval-wrong")},
				},
				expectFailure: true,
			},
		},
	},
	{
		name: "Model rejects observing deleted backend key",
		operations: []modelTestOp{
			{
				req: StorageRequest{Op: OpBackendPut, Bucket: "meta", Key: []byte("bkey1"), Value: []byte("bval1")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendDelete, Bucket: "meta", Key: []byte("bkey1")},
				res: StorageResponse{},
			},
			{
				req: StorageRequest{Op: OpBackendRange, Bucket: "meta", Key: []byte("bkey1")},
				res: StorageResponse{
					BackendKeys: [][]byte{[]byte("bkey1")},
					BackendVals: [][]byte{[]byte("bval1")},
				},
				expectFailure: true,
			},
		},
	},
}

func findBucketByName(name string) backend.Bucket {
	for _, b := range schema.AllBuckets {
		if string(b.Name()) == name {
			return b
		}
	}
	panic(fmt.Sprintf("unknown bucket %q", name))
}

func executeStorageOp(t *testing.T, s *storage, req StorageRequest) StorageResponse {
	var res StorageResponse
	switch req.Op {
	case OpPut:
		tw := s.store.Write(traceutil.TODO())
		rev := tw.Put(req.Key, req.Value, lease.NoLease)
		tw.End()
		res.Rev = rev

	case OpGet:
		rr, err := s.store.Range(context.Background(), req.Key, nil, RangeOptions{Rev: req.Rev})
		res.Err = err
		if err == nil && rr != nil {
			res.KVs = rr.KVs
			res.Count = rr.Count
		}

	case OpRange:
		rr, err := s.store.Range(context.Background(), req.Key, req.End, RangeOptions{
			Rev:       req.Rev,
			Limit:     req.Limit,
			CountOnly: req.CountOnly,
		})
		res.Err = err
		if err == nil && rr != nil {
			res.KVs = rr.KVs
			res.Count = rr.Count
		}

	case OpDelete:
		tw := s.store.Write(traceutil.TODO())
		n, rev := tw.DeleteRange(req.Key, nil)
		tw.End()
		if n > 0 {
			res.Rev = rev
		}

	case OpDeleteRange:
		tw := s.store.Write(traceutil.TODO())
		n, rev := tw.DeleteRange(req.Key, req.End)
		tw.End()
		if n > 0 {
			res.Rev = rev
		}

	case OpTxn:
		tw := s.store.Write(traceutil.TODO())
		var finalRev int64
		var anyMod bool
		for _, sub := range req.TxnOps {
			switch sub.Type {
			case TxnSubOpPut:
				rev := tw.Put(sub.Key, sub.Value, lease.NoLease)
				finalRev = rev
				anyMod = true
			case TxnSubOpDeleteRange:
				n, rev := tw.DeleteRange(sub.Key, sub.End)
				if n > 0 {
					finalRev = rev
					anyMod = true
				}
			}
		}
		tw.End()
		if anyMod && finalRev > 0 {
			res.Rev = finalRev
		}

	case OpCompact:
		donec, err := s.store.Compact(traceutil.TODO(), req.Rev)
		if err == nil && donec != nil {
			<-donec
		}
		res.Err = err

	case OpBackendPut:
		bk := findBucketByName(req.Bucket)
		tx := s.backend.BatchTx()
		tx.LockInsideApply()
		tx.UnsafePut(bk, req.Key, req.Value)
		tx.Unlock()

	case OpBackendDelete:
		bk := findBucketByName(req.Bucket)
		tx := s.backend.BatchTx()
		tx.LockInsideApply()
		tx.UnsafeDelete(bk, req.Key)
		tx.Unlock()

	case OpBackendRange:
		bk := findBucketByName(req.Bucket)
		rtx := s.backend.ConcurrentReadTx()
		rtx.RLock()
		resKeys, resVals := rtx.UnsafeRange(bk, req.Key, req.End, req.Limit)
		rtx.RUnlock()
		if len(resKeys) > 0 {
			res.BackendKeys = make([][]byte, len(resKeys))
			res.BackendVals = make([][]byte, len(resVals))
			for i := range resKeys {
				res.BackendKeys[i] = append([]byte(nil), resKeys[i]...)
				res.BackendVals[i] = append([]byte(nil), resVals[i]...)
			}
		}

	case OpBackendForEach:
		bk := findBucketByName(req.Bucket)
		rtx := s.backend.ConcurrentReadTx()
		rtx.RLock()
		resMap := make(map[string][]byte)
		err := rtx.UnsafeForEach(bk, func(k, v []byte) error {
			resMap[string(k)] = append([]byte(nil), v...)
			return nil
		})
		rtx.RUnlock()
		res.Err = err
		for k, v := range resMap {
			res.BackendKeys = append(res.BackendKeys, []byte(k))
			res.BackendVals = append(res.BackendVals, v)
		}
		sortKVs(res.BackendKeys, res.BackendVals)

	case OpBackendHash:
		h, err := s.backend.Hash(nil)
		res.Hash = h
		res.Err = err

	case OpCrossLayerTxn:
		bk := findBucketByName(req.Bucket)
		tw := s.store.Write(traceutil.TODO())
		rev := tw.Put(req.Key, req.Value, lease.NoLease)
		s.backend.BatchTx().UnsafePut(bk, req.MetaKey, req.MetaVal)
		tw.End()
		res.Rev = rev
	}
	return res
}

// TestStorageModel executes model test cases directly against the Porcupine model.
func TestStorageModel(t *testing.T) {
	allScenarios := append(append([]modelTestCase(nil), commonModelScenarios...), negativeModelScenarios...)
	for _, tc := range allScenarios {
		t.Run(tc.name, func(t *testing.T) {
			state := storagePorcupineModel.Init()
			for opIdx, op := range tc.operations {
				ok, newState := storagePorcupineModel.Step(state, op.req, op.res)
				if op.expectFailure {
					require.Falsef(t, ok, "Op #%d (%s) expected to fail but succeeded", opIdx, storagePorcupineModel.DescribeOperation(op.req, op.res))
				} else {
					require.Truef(t, ok, "Op #%d (%s) failed unexpectedly on state: %s", opIdx, storagePorcupineModel.DescribeOperation(op.req, op.res), storagePorcupineModel.DescribeState(state))
					state = newState
				}
			}
		})
	}
}

func isBackendOp(op StorageOpType) bool {
	switch op {
	case OpBackendPut, OpBackendDelete, OpBackendRange, OpBackendForEach, OpBackendHash, OpCrossLayerTxn:
		return true
	default:
		return false
	}
}

// TestStorageModelAgainstBackend executes the deterministic test scenarios against real backend drivers (bbolt).
func TestStorageModelAgainstBackend(t *testing.T) {
	for _, driver := range DefaultStorageDrivers {
		t.Run("Backend="+driver.Name, func(t *testing.T) {
			for _, tc := range commonModelScenarios {
				t.Run(tc.name, func(t *testing.T) {
					dir := t.TempDir()
					s, err := driver.Setup(dir)
					require.NoError(t, err)
					defer s.Close()

					modelState := storagePorcupineModel.Init()

					for opIdx, op := range tc.operations {
						if isBackendOp(op.req.Op) && s.backend == nil {
							t.Skipf("Driver %s has no backend instance initialized yet", driver.Name)
							return
						}
						actualRes := executeStorageOp(t, s, op.req)

						// 1. Verify model accepts actual response
						ok, newState := storagePorcupineModel.Step(modelState, op.req, actualRes)
						if !ok {
							t.Fatalf("Model rejected actual response on op #%d: %s. State: %s",
								opIdx, storagePorcupineModel.DescribeOperation(op.req, actualRes), storagePorcupineModel.DescribeState(modelState))
						}
						modelState = newState

						// 2. Clear diff diagnostics between expected and actual response
						diffOpts := []cmp.Option{
							cmpopts.IgnoreUnexported(mvccpb.KeyValue{}),
							cmpopts.EquateErrors(),
							cmp.Comparer(func(x, y []byte) bool {
								return bytes.Equal(x, y)
							}),
						}
						if op.res.Hash == 0 {
							diffOpts = append(diffOpts, cmpopts.IgnoreFields(StorageResponse{}, "Hash"))
						}
						if diff := cmp.Diff(op.res, actualRes, diffOpts...); diff != "" {
							t.Fatalf("Op #%d (%s) response mismatch (-want +got):\n%s",
								opIdx, storagePorcupineModel.DescribeOperation(op.req, actualRes), diff)
						}
					}
				})
			}
		})
	}
}

func TestStorageStateEqual(t *testing.T) {
	s1 := storagePorcupineModel.Init().(*storageLinearState)
	s2 := storagePorcupineModel.Init().(*storageLinearState)

	require.True(t, storagePorcupineModel.Equal(s1, s2))

	s1.items["k1"] = &mvccpb.KeyValue{Key: []byte("k1"), Value: []byte("v1")}
	require.False(t, storagePorcupineModel.Equal(s1, s2))

	s2.items["k1"] = &mvccpb.KeyValue{Key: []byte("k1"), Value: []byte("v1")}
	require.True(t, storagePorcupineModel.Equal(s1, s2))

	s1.backendBuckets["meta"] = map[string][]byte{"idx": []byte("1")}
	require.False(t, storagePorcupineModel.Equal(s1, s2))

	s2.backendBuckets["meta"] = map[string][]byte{"idx": []byte("1")}
	require.True(t, storagePorcupineModel.Equal(s1, s2))
}
