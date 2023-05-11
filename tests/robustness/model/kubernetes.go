package model

import (
	"context"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
)

type MvccKVClient interface {
	Get(ctx context.Context, key string) (fullKey []byte, value []byte, modRevision int64, headerRev int64, err error)
	List(ctx context.Context, key string, opts []clientv3.OpOption) (kvs []*KV, hasMore bool, count int64, headerRev int64, err error)
	Count(ctx context.Context, key string) (count int64, err error)
	OptimisticCreate(ctx context.Context, key string, data []byte, ttl int64) (headerRev int64, err error)
	OptimisticUpdate(ctx context.Context, key string, newData []byte, ttl int64, expectedRV int64) (kv *KV, succeeded bool, txnRV int64, err error)
	OptimisticDelete(ctx context.Context, key string, expectedRV int64) (bool, *KV, error)
	Compact(ctx context.Context, t int64, rev int64) (curTime int64, curRev int64, err error)
	GrantLease(ctx context.Context, ttl int64) (leaseID int64, err error)
	Watch(ctx context.Context, cancelFunc context.CancelFunc, key string, startRV int64, withPrefix bool, withProgressNotify bool, errCh chan<- error) <-chan *Event
	GetLeaseManager() LeaseManager
}

type LeaseManager interface {
	GetLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error)
	GetLeaseAttachedObjectCount() int64
	GetReuseDurationSecondsLocked(ttl int64) int64
}

type KV struct {
	Key   []byte
	Value []byte
	RV    int64
}

type Event struct {
	Key              string
	Value            []byte
	PrevValue        []byte
	RV               int64
	IsDeleted        bool
	IsCreated        bool
	IsProgressNotify bool
}

type mvccFake struct {
	mux sync.RWMutex
	etcdState
}

func (f *mvccFake) Get(ctx context.Context, key string) (fullKey []byte, value []byte, modRevision int64, headerRev int64, err error) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	var resp EtcdResponse
	f.etcdState, resp = f.etcdState.step(getRequest(key))
	if len(resp.Txn.Results[0].KVs) == 0 {
		return nil, nil, 0, resp.Revision, nil
	}
	kv := resp.Txn.Results[0].KVs[0]
	// TODO: Disable hashing in ValueOrHash.
	return []byte(kv.Key), []byte(kv.Value.Value), kv.ModRevision, resp.Revision, nil
}

func (f *mvccFake) List(ctx context.Context, key string, opts []clientv3.OpOption) (kvs []*KV, hasMore bool, count int64, headerRev int64, err error) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	var resp EtcdResponse
	// TODO: Transfer opts into arguments
	f.etcdState, resp = f.etcdState.step(rangeRequest(key, true, 0))
	for _, kv := range resp.Txn.Results[0].KVs {
		kvs = append(kvs, &KV{
			Key: []byte(kv.Key),
			// TODO: Disable hashing in ValueOrHash.
			Value: []byte(kv.Value.Value),
			RV:    kv.ModRevision,
		})
	}
	return kvs, resp.Txn.Results[0].Count > int64(len(resp.Txn.Results[0].KVs)), resp.Txn.Results[0].Count, resp.Revision, nil
}

func (f *mvccFake) Count(ctx context.Context, key string) (count int64, err error) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	var resp EtcdResponse
	f.etcdState, resp = f.etcdState.step(rangeRequest(key, true, 0))
	return resp.Txn.Results[0].Count, nil
}

func (f *mvccFake) OptimisticCreate(ctx context.Context, key string, data []byte, ttl int64) (headerRev int64, err error) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	var resp EtcdResponse
	// TODO: Handle TTL
	request := txnRequest([]EtcdCondition{{Key: key, ExpectedRevision: 0}}, []EtcdOperation{{Type: Put, Key: key, Value: ValueOrHash{Value: string(data)}}}, nil)
	f.etcdState, resp = f.etcdState.step(request)
	return resp.Revision, nil
}

func (f *mvccFake) OptimisticUpdate(ctx context.Context, key string, newData []byte, ttl int64, expectedRV int64) (kv *KV, succeeded bool, txnRV int64, err error) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	var resp EtcdResponse
	// TODO: Handle TTL
	request := txnRequest([]EtcdCondition{{Key: key, ExpectedRevision: expectedRV}}, []EtcdOperation{{Type: Put, Key: key, Value: ValueOrHash{Value: string(newData)}}}, []EtcdOperation{{Type: Range, Key: key}})
	f.etcdState, resp = f.etcdState.step(request)
	succeeded = !resp.Txn.Failure
	if !succeeded {
		result := resp.Txn.Results[0].KVs[0]
		kv = &KV{
			Key:   []byte(result.Key),
			Value: []byte(result.Value.Value),
			RV:    result.ModRevision,
		}
	}
	return kv, succeeded, resp.Revision, nil
}

func (f *mvccFake) OptimisticDelete(ctx context.Context, key string, expectedRV int64) (succeeded bool, kv *KV, err error) {
	f.mux.RLock()
	defer f.mux.RUnlock()
	var resp EtcdResponse
	request := txnRequest([]EtcdCondition{{Key: key, ExpectedRevision: expectedRV}}, []EtcdOperation{{Type: Delete, Key: key}}, []EtcdOperation{{Type: Range, Key: key}})
	f.etcdState, resp = f.etcdState.step(request)
	succeeded = !resp.Txn.Failure
	if !succeeded {
		result := resp.Txn.Results[0].KVs[0]
		kv = &KV{
			Key: []byte(result.Key),
			// TODO: Disable hashing in ValueOrHash.
			Value: []byte(result.Value.Value),
			RV:    result.ModRevision,
		}
	}
	return succeeded, nil, nil
}

func (f *mvccFake) Compact(ctx context.Context, t int64, rev int64) (curTime int64, curRev int64, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *mvccFake) GrantLease(ctx context.Context, ttl int64) (leaseID int64, err error) {
	//TODO implement me
	panic("implement me")
}

func (f *mvccFake) Watch(ctx context.Context, cancelFunc context.CancelFunc, key string, startRV int64, withPrefix bool, withProgressNotify bool, errCh chan<- error) <-chan *Event {
	//TODO implement me
	panic("implement me")
}

func (f *mvccFake) GetLeaseManager() LeaseManager {
	//TODO implement me
	panic("implement me")
}

var _ MvccKVClient = (*mvccFake)(nil)
