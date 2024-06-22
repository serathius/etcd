// Copyright 2023 The etcd Authors
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

package validate

import (
	"fmt"

	"github.com/anishathalye/porcupine"

	"go.etcd.io/etcd/tests/v3/robustness/model"
	"go.etcd.io/etcd/tests/v3/robustness/report"
)

func patchLinearizableOperations(reports []report.ClientReport, persistedRequests []model.EtcdRequest) []porcupine.Operation {
	allOperations := relevantOperations(reports)
	clientRequestCount := countClientRequests(reports)
	requestRevision := requestRevision(reports)
	requestReturnTime := requestReturnTime(reports, clientRequestCount, persistedRequests)
	persistedRequestsCount := countPersistedRequests(persistedRequests)
	return patchOperations(allOperations, clientRequestCount, requestReturnTime, persistedRequestsCount, requestRevision)
}

func relevantOperations(reports []report.ClientReport) []porcupine.Operation {
	var ops []porcupine.Operation
	for _, r := range reports {
		for _, op := range r.KeyValue {
			request := op.Input.(model.EtcdRequest)
			resp := op.Output.(model.MaybeEtcdResponse)
			// Remove failed read requests as they are not relevant for linearization.
			if resp.Error == "" || !request.IsRead() {
				ops = append(ops, op)
			}
		}
	}
	return ops
}

func patchOperations(operations []porcupine.Operation, clientRequestCount, requestReturnTime, persistedRequestCount, requestRevision requestsStats) []porcupine.Operation {
	newOperations := make([]porcupine.Operation, 0, len(operations))

	for _, op := range operations {
		request := op.Input.(model.EtcdRequest)
		resp := op.Output.(model.MaybeEtcdResponse)
		// Patch only error requests
		if resp.Error == "" {
			newOperations = append(newOperations, op)
			continue
		}
		switch request.Type {
		case model.Txn:
			txnPersisted := false
			var txnRevision int64 = 0
			allOperations := append(request.Txn.OperationsOnSuccess, request.Txn.OperationsOnFailure...)
			if len(allOperations) == 0 {
				panic("invalid operation")
			}
			for _, operation := range append(request.Txn.OperationsOnSuccess, request.Txn.OperationsOnFailure...) {
				switch operation.Type {
				case model.PutOperation:
					kv := keyValue{Key: operation.Put.Key, Value: operation.Put.Value}
					if _, persisted := persistedRequestCount.Put[kv]; persisted {
						txnPersisted = true
					}
					if count := clientRequestCount.Put[kv]; count <= 1 {
						if count == 0 {
							panic("broken validation assumption, persisted request not sent by client")
						}
						if t, ok := requestReturnTime.Put[kv]; ok && t < op.Return {
							op.Return = t
						}
						if revision, ok := requestRevision.Put[kv]; ok {
							txnRevision = revision
						}
					}
				case model.DeleteOperation:
					if _, persisted := persistedRequestCount.Delete[operation.Delete]; persisted {
						txnPersisted = true
					}
					if count := clientRequestCount.Delete[operation.Delete]; count <= 1 {
						if count == 0 {
							panic("broken validation assumption, persisted request not sent by client")
						}
						if t, ok := requestReturnTime.Delete[operation.Delete]; ok && t < op.Return {
							op.Return = t
						}
						if revision, ok := requestRevision.Delete[operation.Delete]; ok {
							txnRevision = revision
						}
					}
				case model.RangeOperation:
				default:
					panic(fmt.Sprintf("unknown operation type %q", operation.Type))
				}
			}
			if !txnPersisted {
				continue
			}
			resp := model.MaybeEtcdResponse{Persisted: true}
			if txnRevision != 0 {
				resp.PersistedRevision = txnRevision
			}
			op.Output = resp
		case model.LeaseGrant:
			if _, persisted := persistedRequestCount.LeaseGrant[*request.LeaseGrant]; !persisted {
				continue
			}
			if count := clientRequestCount.LeaseGrant[*request.LeaseGrant]; count <= 1 {
				if count == 0 {
					panic("broken validation assumption, persisted request not sent by client")
				}
				if t, ok := requestReturnTime.LeaseGrant[*request.LeaseGrant]; ok && t < op.Return {
					op.Return = t
				}
			}
			op.Output = model.MaybeEtcdResponse{Persisted: true}
		case model.LeaseRevoke:
		case model.Compact:
			if _, persisted := persistedRequestCount.Compact[*request.Compact]; !persisted {
				continue
			}
			if count := clientRequestCount.Compact[*request.Compact]; count <= 1 {
				if count == 0 {
					panic("broken validation assumption, persisted request not sent by client")
				}
				if t, ok := requestReturnTime.Compact[*request.Compact]; ok && t < op.Return {
					op.Return = t
				}
			}
			op.Output = model.MaybeEtcdResponse{Persisted: true}
		case model.Defragment:
		case model.Range:
		default:
			panic(fmt.Sprintf("unknown request type %q", request.Type))
		}
		// Leave operation as it is as we cannot discard it.
		newOperations = append(newOperations, op)
	}
	return newOperations
}

func countClientRequests(reports []report.ClientReport) requestsStats {
	counter := requestsStats{
		LeaseGrant: map[model.LeaseGrantRequest]int64{},
		Put:        map[keyValue]int64{},
		Delete:     map[model.DeleteOptions]int64{},
		Compact:    map[model.CompactRequest]int64{},
	}
	for _, client := range reports {
		for _, op := range client.KeyValue {
			request := op.Input.(model.EtcdRequest)
			countRequest(&counter, request)
		}
	}
	return counter
}

func countPersistedRequests(requests []model.EtcdRequest) requestsStats {
	counter := requestsStats{
		LeaseGrant: map[model.LeaseGrantRequest]int64{},
		Put:        map[keyValue]int64{},
		Delete:     map[model.DeleteOptions]int64{},
		Compact:    map[model.CompactRequest]int64{},
	}
	for _, request := range requests {
		countRequest(&counter, request)
	}
	return counter
}

func countRequest(counter *requestsStats, request model.EtcdRequest) {
	switch request.Type {
	case model.Txn:
		for _, operation := range append(request.Txn.OperationsOnSuccess, request.Txn.OperationsOnFailure...) {
			switch operation.Type {
			case model.PutOperation:
				kv := keyValue{Key: operation.Put.Key, Value: operation.Put.Value}
				counter.Put[kv] += 1
			case model.DeleteOperation:
				counter.Delete[operation.Delete] += 1
			case model.RangeOperation:
			default:
				panic(fmt.Sprintf("unknown operation type %q", operation.Type))
			}
		}
	case model.LeaseGrant:
		counter.LeaseGrant[*request.LeaseGrant] += 1
	case model.LeaseRevoke:
	case model.Compact:
		counter.Compact[*request.Compact] += 1
	case model.Defragment:
	case model.Range:
	default:
		panic(fmt.Sprintf("unknown request type %q", request.Type))
	}
}

func requestReturnTime(reports []report.ClientReport, clientRequestCount requestsStats, persistedRequests []model.EtcdRequest) requestsStats {
	earliestTime := requestsStats{
		LeaseGrant: map[model.LeaseGrantRequest]int64{},
		Put:        map[keyValue]int64{},
		Delete:     map[model.DeleteOptions]int64{},
		Compact:    map[model.CompactRequest]int64{},
	}
	var lastReturnTime int64 = 0
	for _, client := range reports {
		for _, op := range client.KeyValue {
			earliestOperationTime(&earliestTime, op)
			if op.Return > lastReturnTime {
				lastReturnTime = op.Return
			}
		}
		for _, watch := range client.Watch {
			for _, resp := range watch.Responses {
				earliestWatchResponseTime(&earliestTime, resp)
			}
		}
	}
	for i := len(persistedRequests) - 1; i >= 0; i-- {
		request := persistedRequests[i]
		lastReturnTime--
		switch request.Type {
		case model.Txn:
			for _, operation := range append(request.Txn.OperationsOnSuccess, request.Txn.OperationsOnFailure...) {
				switch operation.Type {
				case model.PutOperation:
					kv := keyValue{Key: operation.Put.Key, Value: operation.Put.Value}
					if count := clientRequestCount.Put[kv]; count == 1 {
						t, ok := earliestTime.Put[kv]
						if ok {
							lastReturnTime = min(t, lastReturnTime)
						}
						earliestTime.Put[kv] = lastReturnTime
					}
				case model.DeleteOperation:
					if count := clientRequestCount.Delete[operation.Delete]; count == 1 {
						t, ok := earliestTime.Delete[operation.Delete]
						if ok {
							lastReturnTime = min(t, lastReturnTime)
						}
						earliestTime.Delete[operation.Delete] = lastReturnTime
					}
				case model.RangeOperation:
				default:
					panic(fmt.Sprintf("unknown operation type %q", operation.Type))
				}
			}
		case model.LeaseGrant:
			if count := clientRequestCount.LeaseGrant[*request.LeaseGrant]; count == 1 {
				t, ok := earliestTime.LeaseGrant[*request.LeaseGrant]
				if ok {
					lastReturnTime = min(t, lastReturnTime)
				}
				earliestTime.LeaseGrant[*request.LeaseGrant] = lastReturnTime
			}
		case model.LeaseRevoke:
		case model.Compact:
			if count := clientRequestCount.Compact[*request.Compact]; count == 1 {
				t, ok := earliestTime.Compact[*request.Compact]
				if ok {
					lastReturnTime = min(t, lastReturnTime)
				}
				earliestTime.Compact[*request.Compact] = lastReturnTime
			}
		case model.Defragment:
		case model.Range:
		default:
			panic(fmt.Sprintf("Unknown request type: %q", request.Type))
		}
	}

	return earliestTime
}

func earliestOperationTime(earliestTime *requestsStats, op porcupine.Operation) {
	request := op.Input.(model.EtcdRequest)
	switch request.Type {
	case model.Txn:
		for _, operation := range append(request.Txn.OperationsOnSuccess, request.Txn.OperationsOnFailure...) {
			switch operation.Type {
			case model.PutOperation:
				kv := keyValue{Key: operation.Put.Key, Value: operation.Put.Value}
				if t, ok := earliestTime.Put[kv]; !ok || t > op.Return {
					earliestTime.Put[kv] = op.Return
				}
			case model.DeleteOperation:
				if t, ok := earliestTime.Delete[operation.Delete]; !ok || t > op.Return {
					earliestTime.Delete[operation.Delete] = op.Return
				}
			case model.RangeOperation:
			default:
				panic(fmt.Sprintf("unknown operation type %q", operation.Type))
			}
		}
	case model.LeaseGrant:
		if t, ok := earliestTime.LeaseGrant[*request.LeaseGrant]; !ok || t > op.Return {
			earliestTime.LeaseGrant[*request.LeaseGrant] = op.Return
		}
	case model.LeaseRevoke:
	case model.Compact:
		if t, ok := earliestTime.Compact[*request.Compact]; !ok || t > op.Return {
			earliestTime.Compact[*request.Compact] = op.Return
		}
	case model.Defragment:
	case model.Range:
	default:
		panic(fmt.Sprintf("unknown request type %q", request.Type))
	}
}

func earliestWatchResponseTime(earliestTime *requestsStats, resp model.WatchResponse) {
	for _, event := range resp.Events {
		switch event.Type {
		case model.RangeOperation:
		case model.PutOperation:
			kv := keyValue{Key: event.Key, Value: event.Value}
			if t, ok := earliestTime.Put[kv]; !ok || t > resp.Time.Nanoseconds() {
				earliestTime.Put[kv] = resp.Time.Nanoseconds()
			}
		case model.DeleteOperation:
			delete := model.DeleteOptions{Key: event.Key}
			if t, ok := earliestTime.Delete[delete]; !ok || t > resp.Time.Nanoseconds() {
				earliestTime.Delete[delete] = resp.Time.Nanoseconds()
			}
		default:
			panic(fmt.Sprintf("unknown event type %q", event.Type))
		}
	}
}

func requestRevision(reports []report.ClientReport) requestsStats {
	requestRevision := requestsStats{
		LeaseGrant: map[model.LeaseGrantRequest]int64{},
		Put:        map[keyValue]int64{},
		Delete:     map[model.DeleteOptions]int64{},
		Compact:    map[model.CompactRequest]int64{},
	}
	for _, client := range reports {
		for _, watch := range client.Watch {
			for _, resp := range watch.Responses {
				for _, event := range resp.Events {
					switch event.Type {
					case model.RangeOperation:
					case model.PutOperation:
						kv := keyValue{Key: event.Key, Value: event.Value}
						requestRevision.Put[kv] = event.Revision
					case model.DeleteOperation:
						delete := model.DeleteOptions{Key: event.Key}
						requestRevision.Delete[delete] = event.Revision
					default:
						panic(fmt.Sprintf("unknown event type %q", event.Type))
					}
				}
			}
		}
	}
	return requestRevision
}

// Skips range requests as they are not persisted in WAL, skip LeaseRevokeRequest as they can be executed by server.
type requestsStats struct {
	LeaseGrant map[model.LeaseGrantRequest]int64
	Put        map[keyValue]int64
	Delete     map[model.DeleteOptions]int64
	Compact    map[model.CompactRequest]int64
}

type keyValue struct {
	Key   string
	Value model.ValueOrHash
}
