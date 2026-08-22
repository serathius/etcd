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
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	clientv3 "go.etcd.io/etcd/client/v3"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"
)

//go:embed testdata/exemplar_pod.pb
var exemplarPodData []byte

// putCmd represents the put command
var putCmd = &cobra.Command{
	Use:   "put",
	Short: "Benchmark put",

	Run: putFunc,
}

var (
	keySize         int
	valSize         int
	valType         string
	valSizeVariance float64

	putTimeout time.Duration
	putRate    int
	initWrites int64
	totalPuts  int64

	keySpaceSize int
	prefix       string

	compactInterval   time.Duration
	compactIndexDelta int64

	defragInterval time.Duration
)

func init() {
	RootCmd.AddCommand(putCmd)
	putCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of put request")
	putCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of put request")
	putCmd.Flags().StringVar(&valType, "val-type", "random", "Value type: 'random' for random bytes, 'pod' for realistic Kubernetes Pod pb")
	putCmd.Flags().Float64Var(&valSizeVariance, "val-size-variance", 0.0, "Fractional variance in value size (e.g. 0.66 for +/-66% range [1/3, 5/3] of val-size)")
	putCmd.Flags().IntVar(&putRate, "rate", 0, "Maximum puts per second (0 is no limit)")

	putCmd.Flags().Int64Var(&initWrites, "init-writes", 0, "Minimum number of writes")
	putCmd.Flags().Int64Var(&totalPuts, "total-puts", 100000, "Total number of puts")
	putCmd.Flags().DurationVar(&putTimeout, "duration", 30*time.Second, "Benchmark duration (e.g. 10s, 1m)")

	putCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
	putCmd.Flags().StringVar(&prefix, "prefix", "", "Prefix for keys")
	putCmd.Flags().DurationVar(&compactInterval, "compact-interval", 0, `Interval to compact database (do not duplicate this with etcd's 'auto-compaction-retention' flag) (e.g. --compact-interval=5m compacts every 5-minute)`)
	putCmd.Flags().DurationVar(&defragInterval, "defrag-interval", 0, `Interval to defrag database (e.g. --defrag-interval=5m defrags every 5-minute)`)
	putCmd.Flags().Int64Var(&compactIndexDelta, "compact-index-delta", 1000, "Delta between current revision and compact revision (e.g. current revision 10000, compact at 9000)")
}

func putFunc(cmd *cobra.Command, _ []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)\n", keySpaceSize)
		os.Exit(1)
	}
	if putTimeout <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --duration, got (%v)\n", putTimeout)
		os.Exit(1)
	}

	if putRate == 0 {
		putRate = math.MaxInt32
	}
	limit := rate.NewLimiter(rate.Limit(putRate), 1)
	clients := mustCreateClients(totalClients, totalConns)

	if valSizeVariance < 0 || valSizeVariance >= 1.0 {
		fmt.Fprintf(os.Stderr, "expected --val-size-variance in [0.0, 1.0), got (%v)\n", valSizeVariance)
		os.Exit(1)
	}

	minValLen := valSize
	maxValLen := valSize
	if valSizeVariance > 0 {
		minValLen = int(float64(valSize) * (1.0 - valSizeVariance))
		maxValLen = int(math.Ceil(float64(valSize) * (1.0 + valSizeVariance)))
		if minValLen < 1 {
			minValLen = 1
		}
		if maxValLen < minValLen {
			maxValLen = minValLen
		}
	}

	var basePodTemplate []byte
	if valType == "pod" {
		if len(exemplarPodData) == 0 {
			fmt.Fprintln(os.Stderr, "missing exemplar_pod.pb")
			os.Exit(1)
		}
		basePodTemplate = make([]byte, maxValLen)
		for off := 0; off < maxValLen; {
			n := copy(basePodTemplate[off:], exemplarPodData)
			off += n
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), putTimeout)
	defer cancel()

	sigc := make(chan os.Signal, 2)
	signal.Notify(sigc, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigc)
	go func() {
		<-sigc
		cancel()
		<-sigc
		os.Exit(1)
	}()

	if initWrites > 0 {
		counter := atomic.Int64{}
		bar = pb.Start64(int64(initWrites))
		for i := range clients {
			wg.Add(1)
			go func(c *v3.Client, clientID int) {
				defer wg.Done()
				k := make([]byte, keySize)
				valBuf := make([]byte, maxValLen)
				var rnd *rand.Rand
				rnd = rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)*1000000))

				for {
					binary.PutVarint(k, int64(rnd.Intn(keySpaceSize)))

					curSz := valSize
					if maxValLen > minValLen {
						curSz = minValLen + rnd.Intn(maxValLen-minValLen+1)
					}

					if curSz > 0 {
						if valType == "pod" {
							copy(valBuf[:curSz], basePodTemplate[:curSz])
							randBytesCount := max(1, curSz/10)
							offset := rnd.Intn(curSz - randBytesCount + 1)
							rnd.Read(valBuf[offset : offset+randBytesCount])
						} else {
							rnd.Read(valBuf[:curSz])
						}
					}

					_, err := c.Do(ctx, v3.OpPut(prefix+string(k), string(valBuf[:curSz])))
					if errors.Is(err, context.Canceled) {
						return
					}
					bar.Increment()
					if counter.Add(1) >= initWrites {
						return
					}
				}
			}(clients[i], i)
		}
		wg.Wait()
		bar.Finish()
	}
	select {
	case <-ctx.Done():
		return
	default:
	}

	counter := atomic.Int64{}
	bar = pb.Start64(int64(totalPuts))

	r := newReport(cmd.Name())
	for i := range clients {
		wg.Add(1)
		go func(c *v3.Client, clientID int) {
			defer wg.Done()
			k := make([]byte, keySize)
			valBuf := make([]byte, maxValLen)
			var rnd *rand.Rand
			rnd = rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)*1000000))

			for {
				if putRate > 0 {
					if err := limit.Wait(ctx); err != nil {
						return
					}
				}
				binary.PutVarint(k, int64(rnd.Intn(keySpaceSize)))

				curSz := valSize
				if maxValLen > minValLen {
					curSz = minValLen + rnd.Intn(maxValLen-minValLen+1)
				}

				if curSz > 0 {
					if valType == "pod" {
						copy(valBuf[:curSz], basePodTemplate[:curSz])
						randBytesCount := max(1, curSz/10)
						offset := rnd.Intn(curSz - randBytesCount + 1)
						rnd.Read(valBuf[offset : offset+randBytesCount])
					} else {
						rnd.Read(valBuf[:curSz])
					}
				}

				st := time.Now()
				_, err := c.Do(ctx, v3.OpPut(prefix+string(k), string(valBuf[:curSz])))
				if errors.Is(err, context.Canceled) {
					return
				}

				r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
				if counter.Add(1) > totalPuts {
					cancel()
					return
				}

				bar.Increment()
			}
		}(clients[i], i)
	}

	var compactReport report.Report
	var compactRc <-chan string
	if compactInterval > 0 {
		compactReport = newReport("compact")
		compactRc = compactReport.Run()
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := mustCreateConn()
			defer client.Close()
			ticker := time.NewTicker(compactInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					compactKV(ctx, client, compactReport)
				}
			}
		}()
	}

	var defragReport report.Report
	var defragRc <-chan string
	if defragInterval > 0 {
		defragReport = newReport("defrag")
		defragRc = defragReport.Run()
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := mustCreateConn()
			defer client.Close()
			ticker := time.NewTicker(defragInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					defrag(ctx, client, defragReport)
				}
			}
		}()
	}

	rc := r.Run()
	wg.Wait()
	bar.Finish()
	close(r.Results())

	if compactReport != nil {
		close(compactReport.Results())
	}
	if defragReport != nil {
		close(defragReport.Results())
	}
	fmt.Printf("PUT operation")
	fmt.Println(<-rc)

	if compactReport != nil {
		fmt.Printf("COMPACT operation")
		fmt.Println(<-compactRc)
	}
	if defragReport != nil {
		fmt.Printf("DEFRAG operation")
		fmt.Println(<-defragRc)
	}
}

func compactKV(ctx context.Context, client *v3.Client, r report.Report) {
	resp, err := client.KV.Get(ctx, "foo")
	if err != nil {
		return
	}
	st := time.Now()
	revToCompact := max(0, resp.Header.Revision-compactIndexDelta)
	_, err = client.KV.Compact(ctx, revToCompact, clientv3.WithCompactPhysical())
	if errors.Is(err, context.Canceled) {
		return
	}
	r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
}

func defrag(ctx context.Context, client *v3.Client, r report.Report) {
	st := time.Now()
	_, err := client.Defragment(ctx, client.Endpoints()[0])
	if errors.Is(err, context.Canceled) {
		return
	}
	r.Results() <- report.Result{Err: err, Start: st, End: time.Now()}
}
