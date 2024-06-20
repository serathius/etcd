package clientv3

import (
	"context"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func NewKubernetes(c *Client) Kubernetes {
	return &kubernetes{kv: RetryKVClient(c), watcher: newWatchFromWatchClient(pb.NewWatchClient(c.conn), c)}
}

type Kubernetes interface {
	Get(ctx context.Context, key string, opts GetOptions) (KubernetesGetResponse, error)
	List(ctx context.Context, prefix string, opts ListOptions) (KubernetesListResponse, error)
	Count(ctx context.Context, prefix string) (int64, error)
	OptimisticPut(ctx context.Context, key string, value []byte, opts PutOptions) (KubernetesPutResponse, error)
	OptimisticDelete(ctx context.Context, key string, opts DeleteOptions) (KubernetesDeleteResponse, error)
	Watch(ctx context.Context, key string, opts WatchOptions) KubernetesWatchChan
	RequestProgress(ctx context.Context, opts RequestProgressOptions) error
}

type WatchID string

type GetOptions struct {
	Revision int64
}

type ListOptions struct {
	Revision int64
	Limit    int64
	Continue string
}

type WatchOptions struct {
	StreamKey string
	Revision  int64
	Prefix    bool
}

type PutOptions struct {
	ExpectedRevision int64
	LeaseID          LeaseID
}

type DeleteOptions struct {
	ExpectedRevision int64
}

type RequestProgressOptions struct {
	StreamKey string
}

type KubernetesGetResponse struct {
	KV       *mvccpb.KeyValue
	Revision int64
}

type KubernetesListResponse struct {
	KVs      []*mvccpb.KeyValue
	Count    int64
	Revision int64
}

type KubernetesPutResponse struct {
	KV        *mvccpb.KeyValue
	Succeeded bool
	Revision  int64
}

type KubernetesDeleteResponse struct {
	KV        *mvccpb.KeyValue
	Succeeded bool
	Revision  int64
}

type KubernetesWatchChan <-chan KubernetesWatchEvent

type KubernetesEventType string

const (
	Added    KubernetesEventType = "ADDED"
	Modified KubernetesEventType = "MODIFIED"
	Deleted  KubernetesEventType = "DELETED"
	Bookmark KubernetesEventType = "BOOKMARK"
	Error    KubernetesEventType = "ERROR"
)

type KubernetesWatchEvent struct {
	Type KubernetesEventType

	Error error
	Revision int64
	Key string
	Value []byte
	PreviousValue []byte
}

type kubernetes struct {
	kv      pb.KVClient
	watcher *watcher
}


func (k kubernetes) Get(ctx context.Context, key string, opts GetOptions) (resp KubernetesGetResponse, err error) {
	rangeResp, err := k.kv.Range(ctx, &pb.RangeRequest{
		Key:      []byte(key),
		Revision: opts.Revision,
		Limit:    1,
	})
	if err != nil {
		return resp, toErr(ctx, err)
	}
	resp.Revision = rangeResp.Header.Revision
	if len(rangeResp.Kvs) == 1 {
		resp.KV = rangeResp.Kvs[0]
	}
	return resp, nil
}

func (k kubernetes) List(ctx context.Context, prefix string, opts ListOptions) (resp KubernetesListResponse, err error) {
	rangeStart := prefix + opts.Continue
	rangeEnd := GetPrefixRangeEnd(prefix)

	rangeResp, err := k.kv.Range(ctx, &pb.RangeRequest{
		Key:      []byte(rangeStart),
		RangeEnd: []byte(rangeEnd),
		Limit:    opts.Limit,
		Revision: opts.Revision,
	})
	if err != nil {
		return resp, toErr(ctx, err)
	}
	resp.KVs = rangeResp.Kvs
	resp.Count = rangeResp.Count
	resp.Revision = rangeResp.Header.Revision
	return resp, nil
}

func (k kubernetes) Count(ctx context.Context, prefix string) (int64, error) {
	resp, err := k.kv.Range(ctx, &pb.RangeRequest{
		Key:       []byte(prefix),
		RangeEnd:  []byte(GetPrefixRangeEnd(prefix)),
		CountOnly: true,
	})
	if err != nil {
		return 0, toErr(ctx, err)
	}
	return resp.Count, nil
}

func (k kubernetes) OptimisticPut(ctx context.Context, key string, value []byte, opts PutOptions) (resp KubernetesPutResponse, err error) {
	put := &pb.RequestOp{Request: &pb.RequestOp_RequestPut{RequestPut: &pb.PutRequest{Key: []byte(key), Value: value, Lease: int64(opts.LeaseID)}}}

	txnResp, err := k.optimisticTxn(ctx, key, opts.ExpectedRevision, put)
	if err != nil {
		return resp, toErr(ctx, err)
	}
	resp.Succeeded = txnResp.Succeeded
	resp.Revision = txnResp.Header.Revision
	if !txnResp.Succeeded {
		resp.KV = kvFromTxnResponse(txnResp.Responses[0])
	}
	return resp, nil
}

func (k kubernetes) OptimisticDelete(ctx context.Context, key string, opts DeleteOptions) (resp KubernetesDeleteResponse, err error) {
	del := &pb.RequestOp{Request: &pb.RequestOp_RequestDeleteRange{RequestDeleteRange: &pb.DeleteRangeRequest{Key: []byte(key)}}}

	txnResp, err := k.optimisticTxn(ctx, key, opts.ExpectedRevision, del)
	if err != nil {
		return resp, toErr(ctx, err)
	}
	resp.Succeeded = txnResp.Succeeded
	resp.Revision = txnResp.Header.Revision
	if !txnResp.Succeeded {
		resp.KV = kvFromTxnResponse(txnResp.Responses[0])
	}
	return resp, nil
}

func (k kubernetes) optimisticTxn(ctx context.Context, key string, expectRevision int64, onSuccess *pb.RequestOp) (*pb.TxnResponse, error) {
	txn := &pb.TxnRequest{
		Compare: []*pb.Compare{&pb.Compare{
			Result:      pb.Compare_EQUAL,
			Target:      pb.Compare_MOD,
			Key:         []byte(key),
			TargetUnion: &pb.Compare_ModRevision{ModRevision: expectRevision},
		}},
		Success: []*pb.RequestOp{onSuccess},
		Failure: []*pb.RequestOp{{Request: &pb.RequestOp_RequestRange{RequestRange: &pb.RangeRequest{Key: []byte(key), Limit: 1}}}},
	}
	return k.kv.Txn(ctx, txn)
}

func kvFromTxnResponse(resp *pb.ResponseOp) *mvccpb.KeyValue {
	getResponse := resp.GetResponseRange()
	if len(getResponse.Kvs) == 1 {
		return getResponse.Kvs[0]
	}
	return nil
}

func (k kubernetes) Watch(ctx context.Context, key string, opts WatchOptions) KubernetesWatchChan {
	ctx, cancel := context.WithCancel(ctx)
	ctx = WithRequireLeader(ctx)
	if opts.StreamKey == "" {
		opts.StreamKey = streamKeyFromCtx(ctx)
	}

	wr := &watchRequest{
		ctx:            ctx,
		key:            key,
		end:            GetPrefixRangeEnd(key),
		rev:            opts.Revision,
		progressNotify: true,
		prevKV:         true,
		retc:           make(chan chan WatchResponse, 1),
	}
	ch :=  k.watcher.watch(ctx, opts.StreamKey, wr)

	Kch := make(chan KubernetesWatchEvent, 1)
	go func() {
		for resp := range ch {
				if resp.Err() != nil {
						event := KubernetesWatchEvent{
							Type: Error,
							Error: resp.Err(),
						}
						select {
						  case Kch <- event:
							case <-ctx.Done():
						}
						continue
					}
					if resp.IsProgressNotify() {
						event := KubernetesWatchEvent{
							Type: Bookmark,
							Revision: resp.Header.GetRevision(),
						}
						select {
							case Kch <- event:
							case <-ctx.Done():
						}
						continue
					}
					for _, e := range resp.Events {
						parsedEvent, err := parseEvent(e)
						if err != nil {
							event := KubernetesWatchEvent{
								Type: Error,
								Error: err,
							}
							cancel()
							select {
								case Kch <- event:
								case <-ctx.Done():
							}
						  break
						}
						select {
						case Kch <- *parsedEvent:
						case <-ctx.Done():
						}
					}
			}
			close(Kch)
	}()
	return Kch
}

func parseEvent(e *Event) (*KubernetesWatchEvent, error) {
	if !e.IsCreate() && e.PrevKv == nil {
		// If the previous value is nil, error. One example of how this is possible is if the previous value has been compacted already.
			return nil, fmt.Errorf("etcd event received with PrevKv=nil (key=%q, modRevision=%d, type=%s)", string(e.Kv.Key), e.Kv.ModRevision, e.Type.String())
	}
	ret := &KubernetesWatchEvent{
		Key:       string(e.Kv.Key),
		Value:     e.Kv.Value,
		Revision:  e.Kv.ModRevision,
	}
	if e.PrevKv != nil {
		ret.PreviousValue = e.PrevKv.Value
	}
	switch {
	case e.Type == EventTypeDelete:
		ret.Type = Deleted
	case e.IsCreate():
		ret.Type = Added
	default:
		ret.Type = Modified
	}
	return ret, nil
}

func (k kubernetes) RequestProgress(ctx context.Context, opts RequestProgressOptions) error {
	return k.watcher.requestProgress(ctx, opts.StreamKey)
}
