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

package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"
)

// watchLatencyCmd represents the watch latency command
var watchLatencyCmd = &cobra.Command{
	Use:   "watch-latency",
	Short: "Benchmark watch latency",
	Long: `Benchmarks the latency for watches by measuring
	the latency between writing to a key and receiving the
	associated watch response.`,
	Run: watchLatencyFunc,
}

var (
	watchLPutRate           int
	watchLKeySize           int
	watchLValueSize         int
	watchLWatchersPerStream int
	watchLPrevKV            bool
	watchLWatchRate         int
	watchLRevision          int64
	watchLDuration          time.Duration
)

func init() {
	RootCmd.AddCommand(watchLatencyCmd)
	watchLatencyCmd.Flags().IntVar(&watchLWatchersPerStream, "watchers-per-stream", 10, "Total watchers per stream")
	watchLatencyCmd.Flags().BoolVar(&watchLPrevKV, "prevkv", false, "PrevKV enabled on watch requests")

	watchLatencyCmd.Flags().IntVar(&watchLPutRate, "put-rate", 100, "Number of keys to put per second")
	watchLatencyCmd.Flags().IntVar(&watchLKeySize, "key-size", 32, "Key size of watch response")
	watchLatencyCmd.Flags().IntVar(&watchLValueSize, "val-size", 32, "Value size of watch response")
	watchLatencyCmd.Flags().IntVar(&watchLWatchRate, "watch-rate", 10, "Number of watches to create per second")
	watchLatencyCmd.Flags().Int64Var(&watchLRevision, "watch-revision", 0, "Revision to watch from")
	watchLatencyCmd.Flags().DurationVar(&watchLDuration, "duration", 30*time.Second, "Duration of the benchmark")
}

func watchLatencyFunc(cmd *cobra.Command, _ []string) {
	key := string(mustRandBytes(watchLKeySize))
	value := string(mustRandBytes(watchLValueSize))
	putClient := mustCreateConn()

	putLimiter := rate.NewLimiter(rate.Limit(watchLPutRate), watchLPutRate)
	watchLimiter := rate.NewLimiter(rate.Limit(watchLWatchRate), watchLWatchRate)

	var putTimes sync.Map // map[int64]time.Time (revision -> put time)

	putReport := newReport(cmd.Name() + "-put")
	putReportResults := putReport.Run()
	watchReport := newReport(cmd.Name() + "-watch")
	watchReportResults := watchReport.Run()

	ctx, cancel := context.WithTimeout(context.Background(), watchLDuration)
	defer cancel()

	var putWg sync.WaitGroup
	var lastRev atomic.Int64
	putWg.Add(1)
	go performPuts(ctx, &putWg, putClient, key, value, putLimiter, &putTimes, putReport, &lastRev)

	var watchWg sync.WaitGroup
	var eventWg sync.WaitGroup

	watchWg.Add(1)
	go createWatches(ctx, &watchWg, &eventWg, putClient, key, watchLimiter, &putTimes, watchReport, &lastRev)

	putWg.Wait()
	watchWg.Wait()

	// Wait a bit for in-flight events to arrive
	time.Sleep(3 * time.Second)
	eventWg.Wait()

	close(putReport.Results())
	fmt.Printf("\nPut summary:\n%s", <-putReportResults)

	close(watchReport.Results())
	fmt.Printf("\nWatch events summary:\n%s", <-watchReportResults)
}

func performPuts(ctx context.Context, putWg *sync.WaitGroup, putClient clientv3.KV, key, value string, putLimiter *rate.Limiter, putTimes *sync.Map, putReport report.Report, lastRev *atomic.Int64) {
	defer putWg.Done()
	for {
		if err := putLimiter.Wait(ctx); err != nil {
			break
		}
		start := time.Now()
		resp, err := putClient.Put(ctx, key, value)
		if err != nil {
			// If context is canceled, it's expected
			if ctx.Err() != nil {
				break
			}
			fmt.Fprintf(os.Stderr, "Failed to Put for watch latency benchmark: %v\n", err)
			os.Exit(1)
		}
		end := time.Now()
		lastRev.Store(resp.Header.Revision)
		putTimes.Store(resp.Header.Revision, end)
		putReport.Results() <- report.Result{Start: start, End: end}
	}
}

type streamInfo struct {
	watcher  clientv3.Watcher
	watchers int
}

func createWatches(ctx context.Context, watchWg *sync.WaitGroup, eventWg *sync.WaitGroup, putClient clientv3.KV, key string, watchLimiter *rate.Limiter, putTimes *sync.Map, watchReport report.Report, lastRev *atomic.Int64) {
	defer watchWg.Done()

	clients := mustCreateClients(totalClients, totalConns)
	var streams []*streamInfo
	var streamsMu sync.Mutex

	opts := []clientv3.OpOption{}
	if watchLPrevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}

	for {
		if err := watchLimiter.Wait(ctx); err != nil {
			return
		}

		// Get latest revision from local atomic variable
		latestRev := lastRev.Load()
		if latestRev == 0 {
			// No writes have succeeded yet, wait briefly and try again
			time.Sleep(10 * time.Millisecond)
			continue
		}

		watchRev := watchLRevision

		watchOpts := append(opts, clientv3.WithRev(watchRev))

		// Find or create a stream
		streamsMu.Lock()
		var targetStream *streamInfo
		for _, s := range streams {
			if s.watchers < watchLWatchersPerStream {
				targetStream = s
				break
			}
		}
		if targetStream == nil {
			client := clients[len(streams)%len(clients)]
			targetStream = &streamInfo{
				watcher:  clientv3.NewWatcher(client),
				watchers: 0,
			}
			streams = append(streams, targetStream)
		}
		targetStream.watchers++
		streamsMu.Unlock()

		// Create watch with 1 second timeout
		watchCtx, watchCancel := context.WithTimeout(ctx, time.Second)
		wch := targetStream.watcher.Watch(watchCtx, key, watchOpts...)

		eventWg.Add(1)
		startTime := time.Now()
		go func(s *streamInfo, startTime time.Time) {
			consumeEvents(watchCtx, eventWg, wch, putTimes, watchReport, startTime)
			watchCancel() // Ensure resources are freed
			streamsMu.Lock()
			s.watchers--
			streamsMu.Unlock()
		}(targetStream, startTime)
	}
}

func consumeEvents(ctx context.Context, eventWg *sync.WaitGroup, ch clientv3.WatchChan, putTimes *sync.Map, watchReport report.Report, watchStartTime time.Time) {
	defer eventWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case wresp, ok := <-ch:
			if !ok {
				return
			}
			for _, ev := range wresp.Events {
				receiveTime := time.Now()
				if putTime, ok := putTimes.Load(ev.Kv.ModRevision); ok {
					start := putTime.(time.Time)
					if start.Before(watchStartTime) {
						start = watchStartTime
					}
					if receiveTime.Before(start) {
						start = receiveTime
					}
					watchReport.Results() <- report.Result{Start: start, End: receiveTime}
				}
			}
		}
	}
}
