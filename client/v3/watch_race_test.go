package clientv3

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockWatchClient struct {
	pb.WatchClient
	streams  []*mockWatchStream
	mu       sync.Mutex
	stopChan chan struct{} // Add stopChan for cleanup
}

func (m *mockWatchClient) Watch(ctx context.Context, opts ...grpc.CallOption) (pb.Watch_WatchClient, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &mockWatchStream{
		ctx:      ctx,
		failChan: make(chan struct{}),
		index:    len(m.streams),
		stopChan: m.stopChan,
	}
	m.streams = append(m.streams, s)
	return s, nil
}

type mockWatchStream struct {
	pb.Watch_WatchClient
	ctx       context.Context
	sendCount int
	index     int // Track stream index
	mu        sync.Mutex
	failChan  chan struct{}
	stopChan  chan struct{} // Add stopChan for cleanup
}

func (s *mockWatchStream) Context() context.Context {
	return s.ctx
}

func (s *mockWatchStream) Send(req *pb.WatchRequest) error {
	s.mu.Lock()
	s.sendCount++
	count := s.sendCount
	s.mu.Unlock()

	if s.index == 0 && count == 1 {
		// Trigger failure ONLY on first stream's first send
		close(s.failChan)
	}
	return nil
}

func (s *mockWatchStream) Recv() (*pb.WatchResponse, error) {
	select {
	case <-s.failChan:
		return nil, status.Error(codes.Unavailable, "stream error") // Simulate stream death
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-s.stopChan:
		return nil, status.Error(codes.Canceled, "test done") // Trigger halt error for cleanup
	}
}

func TestWatchRaceMultipleCreateRequests(t *testing.T) {
	mock := &mockWatchClient{
		stopChan: make(chan struct{}),
	}
	w := &watcher{
		remote:  mock,
		streams: make(map[string]*watchGRPCStream),
		lg:      zap.NewNop(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Call Watch in a goroutine because it will block waiting for Created response
	// which our mock never sends.
	go func() {
		w.Watch(ctx, "foo")
	}()

	// Wait for streams to be created
	var streams []*mockWatchStream
	for i := 0; i < 20; i++ {
		mock.mu.Lock()
		streams = mock.streams
		mock.mu.Unlock()
		if len(streams) >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if len(streams) < 2 {
		t.Fatalf("expected at least 2 streams, got %d", len(streams))
	}

	// Verify send counts
	mock.mu.Lock()
	s1 := mock.streams[0]
	s2 := mock.streams[1]
	mock.mu.Unlock()

	s1.mu.Lock()
	s1Count := s1.sendCount
	s1.mu.Unlock()

	s2.mu.Lock()
	s2Count := s2.sendCount
	s2.mu.Unlock()

	if s1Count != 1 {
		t.Errorf("stream 1 sendCount = %d, expected 1", s1Count)
	}
	if s2Count != 1 {
		t.Errorf("stream 2 sendCount = %d, expected 1", s2Count)
	}

	total := s1Count + s2Count
	if total != 2 {
		t.Errorf("total sendCount = %d, expected 2", total)
	}

	// Trigger cleanup
	cancel()
	close(mock.stopChan)

	// Wait for streams to be cleaned up to avoid goroutine leaks
	for i := 0; i < 20; i++ {
		w.mu.Lock()
		count := len(w.streams)
		w.mu.Unlock()
		if count == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
