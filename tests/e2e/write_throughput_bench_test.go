package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	stdruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
)


func BenchmarkWriteThroughput(b *testing.B) {
	e2e.SkipInShortMode(b)

	nsCount := 50
	podPerNs := 3000
	payloadSize := 10145
	nodeCount := 5000 // matching dims for SetupPreseededDatabase name hashing
	totalPods := nsCount * podPerNs
	data := PrepareBenchmarkData(nsCount, podPerNs, payloadSize)

	SetupPreseededDatabase(b, nsCount, totalPods, nodeCount, data)

	dataDir := os.Getenv("BENCHMARK_ETCD_DATA_DIR")
	if dataDir == "" {
		b.Fatal("BENCHMARK_ETCD_DATA_DIR not set after SetupPreseededDatabase")
	}

	ctx := context.Background()
	client, cleanup := createStore(b, dataDir)
	defer cleanup.Close()

	resp, err := client.KV.Get(ctx, "/pods/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("Validated database state: total keys under /pods/ prefix = %d", resp.Count)

	if err := PopulateInitialRevisions(ctx, client, &data); err != nil {
		b.Fatal(err)
	}

	RunBenchmarkWriteThroughput(ctx, b, client, data, nil, true)
}


func PrepareBenchmarkData(nsCount, podPerNs int, payloadSize int) BenchmarkData {
	totalKeys := nsCount * podPerNs
	keys := make([]string, 0, totalKeys)
	for i := 0; i < nsCount; i++ {
		ns := fmt.Sprintf("ns-%d", i)
		for j := 0; j < podPerNs; j++ {
			keys = append(keys, fmt.Sprintf("/pods/%s/pod-%d", ns, j))
		}
	}
	val := make([]byte, payloadSize)
	revisions := make([]atomic.Int64, totalKeys)
	return BenchmarkData{
		Keys:      keys,
		Val:       val,
		Revisions: revisions,
	}
}

func SetupPreseededDatabase(b *testing.B, nsCount, totalPods, nodeCount int, data BenchmarkData) {
	archivePath := fmt.Sprintf("/tmp/etcd_db_%d_%d_%d.tar.gz", nsCount, totalPods, nodeCount)

	var dataDir string
	var isPreseeded bool
	if _, err := os.Stat(archivePath); err == nil {
		dataDir = b.TempDir()
		cmd := exec.Command("tar", "-xzf", archivePath, "-C", dataDir)
		if err := cmd.Run(); err != nil {
			b.Fatalf("failed to unarchive pre-seeded database: %v", err)
		}
		isPreseeded = true
		os.Setenv("ETCD_DATA_PRESEEDED", "true")
	} else {
		dataDir = b.TempDir()
		os.Setenv("ETCD_DATA_PRESEEDED", "false")
	}
	os.Setenv("BENCHMARK_ETCD_DATA_DIR", dataDir)

	b.Cleanup(func() {
		os.Unsetenv("BENCHMARK_ETCD_DATA_DIR")
		os.Unsetenv("ETCD_DATA_PRESEEDED")
	})

	if !isPreseeded {
		ctx := context.Background()
		client, cleanup := createStore(b, dataDir)

		if err := preseedDatabase(ctx, client, data); err != nil {
			b.Fatalf("failed to seed database: %v", err)
		}

		cleanup.Close()

		cmd := exec.Command("tar", "-czf", archivePath, "-C", dataDir, ".")
		if out, err := cmd.CombinedOutput(); err != nil {
			b.Fatalf("failed to archive database: %v. Output: %s", err, string(out))
		}
		os.Setenv("ETCD_DATA_PRESEEDED", "true")
	}
}

func createStore(tb testing.TB, dataDir string) (*clientv3.Client, storeCleanup) {
	ctx := context.Background()
	epc, err := e2e.NewEtcdProcessCluster(ctx, tb,
		e2e.WithClusterSize(1),
		e2e.WithQuotaBackendBytes(8589934592),
		e2e.WithLogLevel("warn"),
		e2e.WithDataDirPath(dataDir),
		e2e.WithKeepDataDir(true),
		e2e.EPClusterOption(func(cfg *e2e.EtcdProcessClusterConfig) {
			cfg.ServerConfig.BackendBatchInterval = time.Second
		}),
	)
	if err != nil {
		tb.Fatal(err)
	}

	cfg := clientv3.Config{
		Endpoints:   epc.EndpointsGRPC(),
		DialTimeout: 5 * time.Second,
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		tb.Fatal(err)
	}

	return client, storeCleanup{client: client, epc: epc}
}

type storeCleanup struct {
	client *clientv3.Client
	epc    *e2e.EtcdProcessCluster
}

func (c storeCleanup) Close() {
	c.client.Close()
	c.epc.Close()
}


func PopulateInitialRevisions(ctx context.Context, client *clientv3.Client, data *BenchmarkData) error {
	resp, err := client.KV.Get(ctx, "/pods/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	keyToIndex := make(map[string]int, len(data.Keys))
	for idx, key := range data.Keys {
		keyToIndex[key] = idx
	}
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if idx, ok := keyToIndex[key]; ok {
			data.Revisions[idx].Store(kv.ModRevision)
		}
	}
	return nil
}

func RunBenchmarkWriteThroughput(ctx context.Context, b *testing.B, client *clientv3.Client, data BenchmarkData, tracker *WatchLatencyTracker, compact bool) {
	if os.Getenv("ETCD_DATA_PRESEEDED") != "true" {
		if err := preseedDatabase(ctx, client, data); err != nil {
			b.Fatal(err)
		}
		if compact {
			rv, err := getCurrentRevision(ctx, client)
			if err != nil {
				panic(fmt.Sprintf("Failed to get current resource version for seeding compaction: %v", err))
			}
			if rv > 0 {
				if _, err := client.Compact(ctx, rv, clientv3.WithCompactPhysical()); err != nil && !strings.Contains(err.Error(), "compacted") {
					panic(fmt.Sprintf("Failed to compact etcd to revision %d after database seeding: %v", rv, err))
				}
			}
		}
	}

	for _, trafficType := range []string{trafficDeleteCreate, trafficPatch} {
		b.Run(fmt.Sprintf("Traffic=%s", trafficType), func(b *testing.B) {
			parallelismOptions := []int{25}
			if pStr := os.Getenv("BENCHMARK_PARALLELISM"); pStr != "" {
				if parsed, err := strconv.Atoi(pStr); err == nil {
					parallelismOptions = []int{parsed}
				}
			}
			for _, parallelism := range parallelismOptions {
				b.Run(fmt.Sprintf("Parallelism=%d", parallelism), func(b *testing.B) {
					loadTypes := []string{loadNone, loadWatcher}
					for _, loadType := range loadTypes {
						b.Run(fmt.Sprintf("Background=%s", loadType), func(b *testing.B) {
							if compact {
								rv, err := getCurrentRevision(ctx, client)
								if err != nil {
									panic(fmt.Sprintf("Failed to get current resource version for compaction: %v", err))
								}
								if rv > 0 {
									if _, err := client.Compact(ctx, rv, clientv3.WithCompactPhysical()); err != nil && !strings.Contains(err.Error(), "compacted") {
										panic(fmt.Sprintf("Failed to compact etcd to revision %d before benchmark: %v", rv, err))
									}
								}
							}
							stdruntime.GC()
							b.SetParallelism(parallelism)
							if tracker != nil {
								rv, _ := getCurrentRevision(ctx, client)
								tracker.Reset(uint64(rv), time.Now())
							}
							runBenchmarkWriteThroughput(ctx, b, client, data, trafficType, loadType, tracker)
						})
					}
				})
			}
		})
	}
}


func preseedDatabase(ctx context.Context, client *clientv3.Client, data BenchmarkData) error {
	errCh := make(chan error, len(data.Keys))
	var wg sync.WaitGroup
	limitCh := make(chan struct{}, 20) // limit to 20 concurrent writers

	for _, key := range data.Keys {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			limitCh <- struct{}{}
			defer func() { <-limitCh }()

			txnResp, err := client.Txn(ctx).If(
				clientv3.Compare(clientv3.ModRevision(k), "=", 0),
			).Then(
				clientv3.OpPut(k, string(data.Val)),
			).Commit()
			if err != nil {
				errCh <- fmt.Errorf("failed to pre-seed key %q: %w", k, err)
			} else if !txnResp.Succeeded {
				// Already exists, ignore
			}
		}(key)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func getCurrentRevision(ctx context.Context, client *clientv3.Client) (int64, error) {
	resp, err := client.KV.Get(ctx, "/", clientv3.WithKeysOnly(), clientv3.WithLimit(1))
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

func runBenchmarkWriteThroughput(ctx context.Context, b *testing.B, client *clientv3.Client, data BenchmarkData, trafficType string, loadType string, tracker *WatchLatencyTracker) {
	stopBackgroundLoadCh := make(chan struct{})
	var workersWg sync.WaitGroup
	var stopOnce sync.Once
	stopBackgroundLoad := func() {
		stopOnce.Do(func() {
			close(stopBackgroundLoadCh)
			workersWg.Wait()
		})
	}
	defer stopBackgroundLoad()

	var writes atomic.Uint64
	var watchEvents atomic.Uint64
	var index atomic.Uint64
	var latestRV atomic.Int64

	var writeTracker *WatchLatencyTracker
	var clientTracker *WatchLatencyTracker

	if tracker != nil {
		writeTracker = tracker
		if loadType == loadWatcher {
			clientTracker = NewWatchLatencyTracker(RealClock{})
			rv, _ := getCurrentRevision(ctx, client)
			clientTracker.Reset(uint64(rv), time.Now())
			latestRV.Store(rv)
		}
	} else if loadType == loadWatcher {
		clientTracker = NewWatchLatencyTracker(RealClock{})
		writeTracker = clientTracker
		rv, _ := getCurrentRevision(ctx, client)
		clientTracker.Reset(uint64(rv), time.Now())
		latestRV.Store(rv)
	}

	if loadType == loadWatcher {
		startBackgroundWatchers(ctx, client, data, 1, &workersWg, stopBackgroundLoadCh, &watchEvents, clientTracker, latestRV.Load())
	}

	var mu sync.Mutex
	var writeDurations []time.Duration

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var localDurations []time.Duration
		for pb.Next() {
			i := int(index.Add(1)) % len(data.Keys)
			start := time.Now()
			wCount := runTraffic(ctx, b, client, data, trafficType, i, &latestRV, writeTracker)
			duration := time.Since(start)
			writes.Add(wCount)
			if wCount > 0 {
				localDurations = append(localDurations, duration)
			}
		}
		mu.Lock()
		writeDurations = append(writeDurations, localDurations...)
		mu.Unlock()
	})
	b.StopTimer()
	elapsedSeconds := b.Elapsed().Seconds()

	finalRV := latestRV.Load()
	if finalRV > 0 {
		if clientTracker != nil {
			if err := clientTracker.WaitForResourceVersion(uint64(finalRV), 30*time.Second); err != nil {
				b.Fatalf("Timed out waiting for client watchers to consume target RV %d: %v", finalRV, err)
			}
		} else if tracker != nil {
			if err := tracker.WaitForResourceVersion(uint64(finalRV), 30*time.Second); err != nil {
				b.Fatalf("Timed out waiting for cacher reflector to consume target RV %d: %v", finalRV, err)
			}
		}
	}

	b.ReportMetric(float64(writes.Load())/elapsedSeconds, "writes/s")
	if len(writeDurations) > 0 {
		slices.Sort(writeDurations)
		p50 := writeDurations[len(writeDurations)*50/100]
		p90 := writeDurations[len(writeDurations)*90/100]
		p99 := writeDurations[len(writeDurations)*99/100]
		b.ReportMetric(p50.Seconds(), "write-latency-p50-s")
		b.ReportMetric(p90.Seconds(), "write-latency-p90-s")
		b.ReportMetric(p99.Seconds(), "write-latency-p99-s")
	}

	stopBackgroundLoad()

	if loadType == loadWatcher {
		b.ReportMetric(float64(watchEvents.Load())/elapsedSeconds, "watch-events/s")
	}
	if clientTracker != nil {
		if p99 := clientTracker.GetP99Latency(); p99 > 0 {
			b.ReportMetric(p99.Seconds(), "watch-latency-p99-s")
		}
	}
}


func startBackgroundWatchers(ctx context.Context, client *clientv3.Client, data BenchmarkData, count int, wg *sync.WaitGroup, stopCh <-chan struct{}, eventCounter *atomic.Uint64, tracker *WatchLatencyTracker, resourceVersion int64) {
	for i := 0; i < count; i++ {
		wg.Add(1)
		go watchLoop(ctx, client, resourceVersion+1, eventCounter, tracker, stopCh, wg)
	}
}

func watchLoop(ctx context.Context, client *clientv3.Client, resourceVersion int64, eventCounter *atomic.Uint64, tracker *WatchLatencyTracker, stopCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := []clientv3.OpOption{
		clientv3.WithRev(resourceVersion),
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
	}
	wch := client.Watch(ctx, "/pods/", opts...)
	for {
		select {
		case <-stopCh:
			return
		case <-ctx.Done():
			return
		case wres, ok := <-wch:
			if !ok {
				return
			}
			if wres.Err() != nil {
				return
			}
			for _, ev := range wres.Events {
				eventCounter.Add(1)
				if tracker != nil {
					tracker.HandleEvent(ev)
				}
			}
		}
	}
}

type WatchLatencyTracker struct {
	clock                  Clock
	mu                     sync.Mutex
	durations              []time.Duration
	startResourceVersion   uint64
	highestResourceVersion uint64
	startTime              time.Time
}

func NewWatchLatencyTracker(clk Clock) *WatchLatencyTracker {
	return &WatchLatencyTracker{
		clock: clk,
	}
}

func (t *WatchLatencyTracker) Reset(rv uint64, startTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.durations = nil
	t.startResourceVersion = rv
	t.highestResourceVersion = rv
	t.startTime = startTime
}

func (t *WatchLatencyTracker) RecordWrite(payload []byte) {
	if len(payload) >= 8 {
		binary.BigEndian.PutUint64(payload[0:8], uint64(t.clock.Now().UnixNano()))
	}
}

func (t *WatchLatencyTracker) HandleEvent(ev *clientv3.Event) {
	if ev.Type == clientv3.EventTypeDelete {
		return
	}
	rv := ev.Kv.ModRevision
	t.mu.Lock()
	if uint64(rv) > t.highestResourceVersion {
		t.highestResourceVersion = uint64(rv)
	}
	t.mu.Unlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	if uint64(rv) < t.startResourceVersion {
		return
	}
	if len(ev.Kv.Value) < 8 {
		return
	}
	tNano := binary.BigEndian.Uint64(ev.Kv.Value[0:8])
	writeTime := time.Unix(0, int64(tNano))
	if writeTime.Before(t.startTime) {
		return
	}
	delay := t.clock.Since(writeTime)
	t.durations = append(t.durations, delay)
}

func (t *WatchLatencyTracker) WaitForResourceVersion(targetRV uint64, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		t.mu.Lock()
		reached := t.highestResourceVersion >= targetRV
		t.mu.Unlock()
		if reached {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for resource version %d", targetRV)
}

func (t *WatchLatencyTracker) GetP99Latency() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.durations) < 100 {
		return 0
	}
	slices.Sort(t.durations)
	idx := len(t.durations)*99/100 - 1
	return t.durations[idx]
}

func runTraffic(ctx context.Context, b *testing.B, client *clientv3.Client, data BenchmarkData, trafficType string, index int, latestRV *atomic.Int64, tracker *WatchLatencyTracker) (writes uint64) {
	key := data.Keys[index]
	switch trafficType {
	case trafficDeleteCreate:
		expectedRev := data.Revisions[index].Load()
		txnDelete := client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", expectedRev),
		).Then(
			clientv3.OpDelete(key),
		)
		txnResp, err := txnDelete.Commit()
		if err != nil {
			panic(fmt.Sprintf("Unexpected error on Delete %q: %v", key, err))
		}
		if txnResp.Succeeded {
			writes++
		}

		valCopy := make([]byte, len(data.Val))
		copy(valCopy, data.Val)
		if tracker != nil {
			tracker.RecordWrite(valCopy)
		}
		txnCreate := client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", 0),
		).Then(
			clientv3.OpPut(key, string(valCopy)),
		)
		txnResp, err = txnCreate.Commit()
		if err != nil {
			panic(fmt.Sprintf("Unexpected error on Create %q: %v", key, err))
		}
		if txnResp.Succeeded {
			writes++
			latestRV.Store(txnResp.Header.Revision)
			data.Revisions[index].Store(txnResp.Header.Revision)
		} else {
			panic(fmt.Sprintf("Create transaction failed for key %q (already exists)", key))
		}

	case trafficPatch:
		expectedRev := data.Revisions[index].Load()
		valCopy := make([]byte, len(data.Val))
		copy(valCopy, data.Val)
		if tracker != nil {
			tracker.RecordWrite(valCopy)
		}
		txn := client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(key), "=", expectedRev),
		).Then(
			clientv3.OpPut(key, string(valCopy)),
		)
		txnResp, err := txn.Commit()
		if err != nil {
			panic(fmt.Sprintf("Unexpected error on Patch %q: %v", key, err))
		}
		if txnResp.Succeeded {
			writes++
			latestRV.Store(txnResp.Header.Revision)
			data.Revisions[index].Store(txnResp.Header.Revision)
		} else {
			panic(fmt.Sprintf("Patch transaction failed for key %q (revision mismatch: expected %d)", key, expectedRev))
		}

	default:
		panic(fmt.Sprintf("Unknown traffic type: %s", trafficType))
	}
	return writes
}


type BenchmarkData struct {
	Keys      []string
	Val       []byte
	Revisions []atomic.Int64
}

type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

type RealClock struct{}

func (RealClock) Now() time.Time                  { return time.Now() }
func (RealClock) Since(t time.Time) time.Duration { return time.Since(t) }

const (
	trafficDeleteCreate = "DeleteCreate"
	trafficPatch        = "Patch"

	loadNone               = "None"
	loadWatcher            = "Watcher"
	loadLister             = "Lister"
	loadListerExactRV      = "ListerExactRV"
	loadListerNotOlderThan = "ListerNotOlderThan"
	loadWatchList          = "WatchList"
)
