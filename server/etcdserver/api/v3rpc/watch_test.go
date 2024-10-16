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

package v3rpc

import (
	"bytes"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/auth"
	"go.etcd.io/etcd/server/v3/config"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/mvcc"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc/metadata"
	"math"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestSendFragment(t *testing.T) {
	tt := []struct {
		wr              *pb.WatchResponse
		maxRequestBytes int
		fragments       int
		werr            error
	}{
		{ // large limit should not fragment
			wr:              createResponse(100, 1),
			maxRequestBytes: math.MaxInt32,
			fragments:       1,
		},
		{ // large limit for two messages, expect no fragment
			wr:              createResponse(10, 2),
			maxRequestBytes: 50,
			fragments:       1,
		},
		{ // limit is small but only one message, expect no fragment
			wr:              createResponse(1024, 1),
			maxRequestBytes: 1,
			fragments:       1,
		},
		{ // exceed limit only when combined, expect fragments
			wr:              createResponse(11, 5),
			maxRequestBytes: 20,
			fragments:       5,
		},
		{ // 5 events with each event exceeding limits, expect fragments
			wr:              createResponse(15, 5),
			maxRequestBytes: 10,
			fragments:       5,
		},
		{ // 4 events with some combined events exceeding limits
			wr:              createResponse(10, 4),
			maxRequestBytes: 35,
			fragments:       2,
		},
	}

	for i := range tt {
		fragmentedResp := make([]*pb.WatchResponse, 0)
		testSend := func(wr *pb.WatchResponse) error {
			fragmentedResp = append(fragmentedResp, wr)
			return nil
		}
		err := sendFragments(tt[i].wr, tt[i].maxRequestBytes, testSend)
		if !errors.Is(err, tt[i].werr) {
			t.Errorf("#%d: expected error %v, got %v", i, tt[i].werr, err)
		}
		got := len(fragmentedResp)
		if got != tt[i].fragments {
			t.Errorf("#%d: expected response number %d, got %d", i, tt[i].fragments, got)
		}
		if got > 0 && fragmentedResp[got-1].Fragment {
			t.Errorf("#%d: expected fragment=false in last response, got %+v", i, fragmentedResp[got-1])
		}
	}
}

func createResponse(dataSize, events int) (resp *pb.WatchResponse) {
	resp = &pb.WatchResponse{Events: make([]*mvccpb.Event, events)}
	for i := range resp.Events {
		resp.Events[i] = &mvccpb.Event{
			Kv: &mvccpb.KeyValue{
				Key: bytes.Repeat([]byte("a"), dataSize),
			},
		}
	}
	return resp
}

func TestWatchServerContextCancel(t *testing.T) {
	lg := zaptest.NewLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	cfg := config.ServerConfig{}
	watchable := watchableMock{
		respStream: make(chan mvcc.WatchResponse),
	}
	s := newWatchServer(lg, 123, 456, cfg, nil, &watchable, nil)
	watchStream := watchStreamMock{
		ctx:      ctx,
		requests: make(chan *pb.WatchRequest, 1),
	}
	err := s.Watch(&watchStream)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWatchServerWatchCreate(t *testing.T) {
	lg := zaptest.NewLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	cfg := config.ServerConfig{}
	watchable := watchableMock{
		respStream: make(chan mvcc.WatchResponse),
		rev:        88,
	}
	auth := authStoreMock{}
	raft := raftStatusMock{
		term: 42,
	}
	s := newWatchServer(lg, 123, 456, cfg, &raft, &watchable, &auth)
	watchStream := watchStreamMock{
		ctx:       ctx,
		requests:  make(chan *pb.WatchRequest, 1),
		responses: make(chan *pb.WatchResponse, 1),
	}
	watchStream.requests <- &pb.WatchRequest{
		RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{
				Key:     []byte("a"),
				WatchId: 1,
			},
		},
	}
	err := s.Watch(&watchStream)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, watchStream.responses, 1)
	expectResp := &pb.WatchResponse{
		Header: &pb.ResponseHeader{
			ClusterId: 123,
			MemberId:  456,
			Revision:  88,
			RaftTerm:  42,
		},
		WatchId: 1,
		Created: true,
	}
	require.Equal(t, expectResp, <-watchStream.responses)
}

func TestWatchServerCancel(t *testing.T) {
	lg := zaptest.NewLogger(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	defer cancel()

	cfg := config.ServerConfig{}
	wg := sync.WaitGroup{}
	watchServer := newTestWatchServer(ctx, lg, cfg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := watchServer.Watch(&watchServer.stream)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	for i := 0; i < 20; i++ {
		watchServer.stream.requests <- &pb.WatchRequest{
			RequestUnion: &pb.WatchRequest_CancelRequest{
				CancelRequest: &pb.WatchCancelRequest{
					WatchId: int64(i),
				},
			},
		}
	}
	wg.Wait()
}

func newTestWatchServer(ctx context.Context, lg *zap.Logger, cfg config.ServerConfig) *testWatchServer {
	watchServer := testWatchServer{
		raft: raftStatusMock{
			term: 42,
		},
		watchable: watchableMock{
			respStream: make(chan mvcc.WatchResponse, 1),
			rev:        88,
		},
		auth: authStoreMock{},
		stream: watchStreamMock{
			ctx:       ctx,
			requests:  make(chan *pb.WatchRequest, 0),
			responses: make(chan *pb.WatchResponse, 0),
		},
	}
	watchServer.WatchServer = newWatchServer(lg, 123, 456, cfg, watchServer.raft, &watchServer.watchable, &watchServer.auth)
	return &watchServer

}

type testWatchServer struct {
	pb.WatchServer
	stream    watchStreamMock
	raft      raftStatusMock
	watchable watchableMock
	auth      authStoreMock
}

type raftStatusMock struct {
	term uint64
}

func (r raftStatusMock) MemberID() types.ID {
	//TODO implement me
	panic("implement me")
}

func (r raftStatusMock) Leader() types.ID {
	//TODO implement me
	panic("implement me")
}

func (r raftStatusMock) CommittedIndex() uint64 {
	//TODO implement me
	panic("implement me")
}

func (r raftStatusMock) AppliedIndex() uint64 {
	//TODO implement me
	panic("implement me")
}

func (r raftStatusMock) Term() uint64 {
	return r.term
}

type authStoreMock struct {
}

func (a *authStoreMock) AuthEnable() error {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) AuthDisable() {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) IsAuthEnabled() bool {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) Authenticate(ctx context.Context, username, password string) (*pb.AuthenticateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) Recover(be auth.AuthBackend) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserAdd(r *pb.AuthUserAddRequest) (*pb.AuthUserAddResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserDelete(r *pb.AuthUserDeleteRequest) (*pb.AuthUserDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserChangePassword(r *pb.AuthUserChangePasswordRequest) (*pb.AuthUserChangePasswordResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserGrantRole(r *pb.AuthUserGrantRoleRequest) (*pb.AuthUserGrantRoleResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserGet(r *pb.AuthUserGetRequest) (*pb.AuthUserGetResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserRevokeRole(r *pb.AuthUserRevokeRoleRequest) (*pb.AuthUserRevokeRoleResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) RoleAdd(r *pb.AuthRoleAddRequest) (*pb.AuthRoleAddResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) RoleGrantPermission(r *pb.AuthRoleGrantPermissionRequest) (*pb.AuthRoleGrantPermissionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) RoleGet(r *pb.AuthRoleGetRequest) (*pb.AuthRoleGetResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) RoleRevokePermission(r *pb.AuthRoleRevokePermissionRequest) (*pb.AuthRoleRevokePermissionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) RoleDelete(r *pb.AuthRoleDeleteRequest) (*pb.AuthRoleDeleteResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) UserList(r *pb.AuthUserListRequest) (*pb.AuthUserListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) RoleList(r *pb.AuthRoleListRequest) (*pb.AuthRoleListResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) IsPutPermitted(authInfo *auth.AuthInfo, key []byte) error {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) IsRangePermitted(authInfo *auth.AuthInfo, key, rangeEnd []byte) error {
	return nil
}

func (a *authStoreMock) IsDeleteRangePermitted(authInfo *auth.AuthInfo, key, rangeEnd []byte) error {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) IsAdminPermitted(authInfo *auth.AuthInfo) error {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) GenTokenPrefix() (string, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) Revision() uint64 {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) CheckPassword(username, password string) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) Close() error {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) AuthInfoFromTLS(ctx context.Context) *auth.AuthInfo {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) WithRoot(ctx context.Context) context.Context {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) HasRole(user, role string) bool {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) BcryptCost() int {
	//TODO implement me
	panic("implement me")
}

func (a *authStoreMock) AuthInfoFromCtx(ctx context.Context) (*auth.AuthInfo, error) {
	return nil, nil
}

func (a *authStoreMock) AuthStore() auth.AuthStore {
	return a
}

type watchableMock struct {
	respStream chan mvcc.WatchResponse
	rev        int64
}

func (w watchableMock) FirstRev() int64 {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Rev() int64 {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Range(ctx context.Context, key, end []byte, ro mvcc.RangeOptions) (r *mvcc.RangeResult, err error) {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) DeleteRange(key, end []byte) (n, rev int64) {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Read(mode mvcc.ReadTxMode, trace *traceutil.Trace) mvcc.TxnRead {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Write(trace *traceutil.Trace) mvcc.TxnWrite {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) HashStorage() mvcc.HashStorage {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Commit() {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Restore(b backend.Backend) error {
	//TODO implement me
	panic("implement me")
}

func (w watchableMock) Close() error {
	//TODO implement me
	panic("implement me")
}

func (w *watchableMock) NewWatchStream() mvcc.WatchStream {
	return mvccWatchStreamMock{
		respStream: w.respStream,
		rev:        w.rev,
	}
}

type mvccWatchStreamMock struct {
	respStream chan mvcc.WatchResponse
	rev        int64
}

func (m mvccWatchStreamMock) Watch(id mvcc.WatchID, key, end []byte, startRev int64, fcs ...mvcc.FilterFunc) (mvcc.WatchID, error) {
	return 1, nil
}

func (m mvccWatchStreamMock) Chan() <-chan mvcc.WatchResponse {
	return m.respStream
}

func (m mvccWatchStreamMock) RequestProgress(id mvcc.WatchID) {
	//TODO implement me
	panic("implement me")
}

func (m mvccWatchStreamMock) RequestProgressAll() bool {
	//TODO implement me
	panic("implement me")
}

func (m mvccWatchStreamMock) Cancel(id mvcc.WatchID) error {
	return nil
}

func (m mvccWatchStreamMock) Close() {
	close(m.respStream)
}

func (m mvccWatchStreamMock) Rev() int64 {
	return m.rev
}

type watchStreamMock struct {
	ctx       context.Context
	requests  chan *pb.WatchRequest
	responses chan *pb.WatchResponse
}

func (w watchStreamMock) Send(response *pb.WatchResponse) error {
	select {
	case w.responses <- response:
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	}
}

func (w watchStreamMock) Recv() (*pb.WatchRequest, error) {
	select {
	case resp := <-w.requests:
		return resp, nil
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	}
}

func (w watchStreamMock) SetHeader(md metadata.MD) error {
	//TODO implement me
	panic("implement me")
}

func (w watchStreamMock) SendHeader(md metadata.MD) error {
	//TODO implement me
	panic("implement me")
}

func (w watchStreamMock) SetTrailer(md metadata.MD) {
	//TODO implement me
	panic("implement me")
}

func (w watchStreamMock) Context() context.Context {
	return w.ctx
}

func (w watchStreamMock) SendMsg(m any) error {
	//TODO implement me
	panic("implement me")
}

func (w watchStreamMock) RecvMsg(m any) error {
	//TODO implement me
	panic("implement me")
}
