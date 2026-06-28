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
)

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

	loadNone            = "None"
	loadWatcher         = "Watcher"
	loadLister          = "Lister"
	loadListerExactRV   = "ListerExactRV"
	loadListerNotOlderThan = "ListerNotOlderThan"
	loadWatchList       = "WatchList"
)

type BenchmarkData struct {
	Keys      []string
	Val       []byte
	Revisions []atomic.Int64
}


func RunBenchmarkWriteThroughput(ctx context.Context, b *testing.B, store storage.Interface, data BenchmarkData, hasIndex bool, tracker *WatchLatencyTracker, compactFn func(context.Context, uint64) error) {
	if os.Getenv("ETCD_DATA_PRESEEDED") != "true" {
		require.NoError(b, PrecreateBenchmarkPods(ctx, store, data))
		if compactFn != nil {
			rv, err := store.GetCurrentResourceVersion(ctx)
			if err != nil {
				panic(fmt.Sprintf("Failed to get current resource version for seeding compaction: %v", err))
			}
			if rv > 0 {
				if err := compactFn(ctx, rv); err != nil && !strings.Contains(err.Error(), "compacted") {
					panic(fmt.Sprintf("Failed to compact etcd to revision %d after database seeding: %v", rv, err))
				}
			}
		}
	}
	require.NoError(b, waitForConsistent(ctx, store))

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
						useIndexOptions := []bool{false}
						if hasIndex && loadType != loadNone {
							useIndexOptions = []bool{false, true}
						}
						for _, readIndexed := range useIndexOptions {
							b.Run(fmt.Sprintf("Background=%s/UseIndex=%v", loadType, readIndexed), func(b *testing.B) {
								if compactFn != nil {
									rv, err := store.GetCurrentResourceVersion(ctx)
									if err != nil {
										panic(fmt.Sprintf("Failed to get current resource version for compaction: %v", err))
									}
									if rv > 0 {
										if err := compactFn(ctx, rv); err != nil && !strings.Contains(err.Error(), "compacted") {
											panic(fmt.Sprintf("Failed to compact etcd to revision %d before benchmark: %v", rv, err))
										}
									}
								}
								require.NoError(b, waitForConsistent(ctx, store))
								stdruntime.GC()
								b.SetParallelism(parallelism)
								if tracker != nil {
									rv, _ := store.GetCurrentResourceVersion(ctx)
									tracker.Reset(rv, time.Now())
								}
								runBenchmarkWriteThroughput(ctx, b, store, data, trafficType, loadType, readIndexed, tracker)
							})
						}
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

func SetupPreseededDatabase(b *testing.B, nsCount, totalPods, nodeCount int, data BenchmarkData, seedFn func(ctx context.Context, store storage.Interface) error, createStoreFn func(b testing.TB, dataDir string) (storage.Interface, func())) {
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
		store, stopStore := createStoreFn(b, dataDir)

		if err := seedFn(ctx, store); err != nil {
			b.Fatalf("failed to seed database: %v", err)
		}

		stopStore()

		cmd := exec.Command("tar", "-czf", archivePath, "-C", dataDir, ".")
		if out, err := cmd.CombinedOutput(); err != nil {
			b.Fatalf("failed to archive database: %v. Output: %s", err, string(out))
		}
		os.Setenv("ETCD_DATA_PRESEEDED", "true")
	}
}

func runBenchmarkWriteThroughput(ctx context.Context, b *testing.B, store storage.Interface, data BenchmarkData, trafficType string, loadType string, readIndexed bool, tracker *WatchLatencyTracker) {
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
	var listCalls atomic.Uint64
	var listObjects atomic.Uint64
	var index atomic.Uint64
	var latestRV atomic.Pointer[string]
	initialRV := "0"
	latestRV.Store(&initialRV)

	// Determine trackers
	var writeTracker *WatchLatencyTracker  // Tracker used to record writes (sets annotation)
	var clientTracker *WatchLatencyTracker // Tracker used by background watchers (client-side latency)
	startRVStr := ""

	if tracker != nil {
		writeTracker = tracker
		if loadType == loadWatcher {
			// Cacher case with background watchers: we need a separate client tracker
			clientTracker = NewWatchLatencyTracker(clock.RealClock{})
			// Reset the clientTracker with current RV
			rv, _ := store.GetCurrentResourceVersion(ctx)
			clientTracker.Reset(rv, time.Now())
			startRVStr = strconv.FormatUint(rv, 10)
		}
	} else if loadType == loadWatcher {
		// Etcd3 case with background watchers: one tracker does both recording writes and client tracking
		clientTracker = NewWatchLatencyTracker(clock.RealClock{})
		writeTracker = clientTracker
		// Reset the clientTracker with current RV
		rv, _ := store.GetCurrentResourceVersion(ctx)
		clientTracker.Reset(rv, time.Now())
		startRVStr = strconv.FormatUint(rv, 10)
	}

	listerCount := 10
	if countStr := os.Getenv("BENCHMARK_LISTER_COUNT"); countStr != "" {
		if parsed, err := strconv.Atoi(countStr); err == nil {
			listerCount = parsed
		}
	}

	switch loadType {
	case loadNone:
	case loadWatcher:
		watcherCount := 10
		if countStr := os.Getenv("BENCHMARK_WATCHER_COUNT"); countStr != "" {
			if parsed, err := strconv.Atoi(countStr); err == nil {
				watcherCount = parsed
			}
		}
		startBackgroundWatchers(ctx, store, data, watcherCount, readIndexed, &workersWg, stopBackgroundLoadCh, &watchEvents, clientTracker, startRVStr)
	case loadLister:
		startBackgroundListers(ctx, store, data, listerCount, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects, "", &latestRV)
	case loadListerExactRV:
		startBackgroundListers(ctx, store, data, listerCount, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects, metav1.ResourceVersionMatchExact, &latestRV)
	case loadListerNotOlderThan:
		startBackgroundListers(ctx, store, data, listerCount, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects, metav1.ResourceVersionMatchNotOlderThan, &latestRV)
	case loadWatchList:
		startBackgroundWatchListers(ctx, store, data, listerCount, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects)
	default:
		panic(fmt.Sprintf("Unknown load type: %s", loadType))
	}
	writes.Store(0)
	watchEvents.Store(0)
	listCalls.Store(0)
	listObjects.Store(0)

	etcd3metrics.Register()
	statsBefore := getEtcdRequestStats()

	var mu sync.Mutex
	var writeDurations []time.Duration

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var localDurations []time.Duration
		for pb.Next() {
			i := int(index.Add(1)) % len(data.PodKeys)
			start := time.Now()
			wCount := runTraffic(ctx, b, store, data, trafficType, i, &latestRV, writeTracker)
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
	rv := ""
	if rvPtr := latestRV.Load(); rvPtr != nil {
		rv = *rvPtr
	}
	require.NoError(b, waitForResourceVersion(ctx, store, rv))
	if rv != "" && rv != "0" {
		targetRVVal, err := strconv.ParseUint(rv, 10, 64)
		if err == nil {
			if clientTracker != nil && (loadType != loadWatcher || !readIndexed) {
				if err := clientTracker.WaitForResourceVersion(targetRVVal, 30*time.Second); err != nil {
					b.Fatalf("Timed out waiting for client watchers to consume target RV %d: %v", targetRVVal, err)
				}
			} else if tracker != nil {
				if err := tracker.WaitForResourceVersion(targetRVVal, 30*time.Second); err != nil {
					b.Fatalf("Timed out waiting for cacher reflector to consume target RV %d: %v", targetRVVal, err)
				}
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

	statsAfter := getEtcdRequestStats()

	stopBackgroundLoad()

	switch loadType {
	case loadWatcher:
		b.ReportMetric(float64(watchEvents.Load())/elapsedSeconds, "watch-events/s")
	case loadLister, loadListerExactRV, loadListerNotOlderThan, loadWatchList:
		b.ReportMetric(float64(listCalls.Load())/elapsedSeconds, "list-calls/s")
		b.ReportMetric(float64(listObjects.Load())/elapsedSeconds, "list-objs/s")
	}

	// Report cacher internal watchCache latency if available
	if tracker != nil {
		if p99 := tracker.GetP99Latency(); p99 > 0 {
			b.ReportMetric(p99.Seconds(), "watch-cache-latency-p99-s")
		}
	}
	// Report client-observed watch latency if available
	if clientTracker != nil {
		if p99 := clientTracker.GetP99Latency(); p99 > 0 {
			b.ReportMetric(p99.Seconds(), "watch-latency-p99-s")
		}
	}

	numWrites := writes.Load()

	if numWrites > 0 {
		creates := statsAfter.create - statsBefore.create
		deletes := statsAfter.delete - statsBefore.delete
		updates := statsAfter.update - statsBefore.update
		gets := statsAfter.get - statsBefore.get
		totalReqs := creates + deletes + updates + gets

		b.ReportMetric(float64(creates)/float64(numWrites), "etcd-creates/write-cycle")
		b.ReportMetric(float64(deletes)/float64(numWrites), "etcd-deletes/write-cycle")
		b.ReportMetric(float64(updates)/float64(numWrites), "etcd-updates/write-cycle")
		b.ReportMetric(float64(gets)/float64(numWrites), "etcd-gets/write-cycle")
		b.ReportMetric(float64(totalReqs)/float64(numWrites), "etcd-total-reqs/write-cycle")
	}
}

func waitForConsistent(ctx context.Context, store storage.Interface) error {
	rvVal, err := store.GetCurrentResourceVersion(ctx)
	if err != nil {
		return fmt.Errorf("unexpected error getting resource version: %w", err)
	}
	rv := strconv.FormatUint(rvVal, 10)

	listOut := &corev1.PodList{}
	err = store.GetList(ctx, "/pods/", storage.ListOptions{
		ResourceVersion:      rv,
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
		Recursive:            true,
		Predicate: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.Everything(),
			Limit: 1,
		},
	}, listOut)
	if err != nil {
		return fmt.Errorf("unexpected error waiting for consistency: %w", err)
	}
	return nil
}

func waitForResourceVersion(ctx context.Context, store storage.Interface, rv string) error {
	if rv == "0" || rv == "" {
		return nil
	}
	var err error
	for range 10 {
		listOut := &corev1.PodList{}
		err = store.GetList(ctx, "/pods/", storage.ListOptions{
			ResourceVersion:      rv,
			ResourceVersionMatch: metav1.ResourceVersionMatchExact,
			Recursive:            true,
			Predicate: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
		}, listOut)
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "Too large resource version") {
			return fmt.Errorf("unexpected error waiting for consistency at rv %s: %w", rv, err)
		}
	}
	return fmt.Errorf("timed out waiting for consistency at rv %s: %w", rv, err)
}

type WatchLatencyTracker struct {
	clock                  clock.Clock
	mu                     sync.Mutex
	durations              []time.Duration
	startResourceVersion   uint64
	highestResourceVersion uint64
	startTime              time.Time
}

func NewWatchLatencyTracker(clk clock.Clock) *WatchLatencyTracker {
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
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return wait.PollUntilContextCancel(ctx, 10*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.highestResourceVersion >= targetRV, nil
	})
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

func startBackgroundWatchers(ctx context.Context, client *clientv3.Client, data BenchmarkData, count int, wg *sync.WaitGroup, stopCh <-chan struct{}, eventCounter *atomic.Uint64, tracker *WatchLatencyTracker, resourceVersion int64) {
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			opts := []clientv3.OpOption{
				clientv3.WithRev(resourceVersion + 1),
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
							tracker.HandleEvent(&ev)
						}
					}
				}
			}
		}()
	}
}


type etcdStats struct {
	create uint64
	delete uint64
	update uint64
	get    uint64
}

func getEtcdRequestStats() etcdStats {
	stats := etcdStats{}
	metricFamilies, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		return stats
	}
	for _, mf := range metricFamilies {
		if mf.GetName() == "etcd_requests_total" {
			for _, m := range mf.Metric {
				var operation string
				for _, label := range m.Label {
					if label.GetName() == "operation" {
						operation = label.GetValue()
					}
				}
				if m.Counter != nil {
					val := uint64(m.Counter.GetValue())
					switch operation {
					case "create":
						stats.create = val
					case "delete":
						stats.delete = val
					case "update":
						stats.update = val
					case "get":
						stats.get = val
					}
				}
			}
		}
	}
	return stats
}
