// Copyright 2017 The etcd Authors
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
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/spf13/cobra"
	"golang.org/x/time/rate"

	clientv3 "go.etcd.io/etcd/client/v3"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/pkg/v3/report"
	"go.etcd.io/etcd/pkg/v3/stringutil"
)

// txnPutCmd represents the txnPut command
var txnPutCmd = &cobra.Command{
	Use:   "txn-put",
	Short: "Benchmark txn-put",

	Run: txnPutFunc,
}

var (
	txnPutTotal int
	qpsRate     int
)

func init() {
	RootCmd.AddCommand(txnPutCmd)
	txnPutCmd.Flags().IntVar(&keySize, "key-size", 8, "Key size of txn put")
	txnPutCmd.Flags().IntVar(&valSize, "val-size", 8, "Value size of txn put")
	txnPutCmd.Flags().IntVar(&qpsRate, "rate", 0, "Maximum txns per second (0 is no limit)")

	txnPutCmd.Flags().IntVar(&keySpaceSize, "key-space-size", 1, "Maximum possible keys")
}

func txnPutFunc(cmd *cobra.Command, _ []string) {
	if keySpaceSize <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --key-space-size, got (%v)", keySpaceSize)
		os.Exit(1)
	}

	if qpsRate <= 0 {
		fmt.Fprintf(os.Stderr, "expected positive --rate, got (%v)", qpsRate)
		os.Exit(1)
	}
	clients := mustCreateClients(totalClients, totalConns)
	resp, err := clients[0].Get(context.Background(), "\x00", clientv3.WithFromKey(), clientv3.WithKeysOnly())
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't get all keys: %v", err)
		os.Exit(1)
	}
	fmt.Printf("Got %d keys\n", len(resp.Kvs))
	if len(resp.Kvs) > keySpaceSize {
		fmt.Fprintf(os.Stderr, "didn't expect keys more than in key space, got %d, want: %d", len(resp.Kvs), keySpaceSize)
		os.Exit(1)
	}
	keys := []string{}
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
	}
	if len(keys) < keySpaceSize {
		needKeys := keySpaceSize - len(keys)
		fmt.Printf("Need to add %d keys\n", needKeys)
		keys = append(keys, stringutil.RandomStrings(uint(keySize), needKeys)...)
	}

	value := stringutil.RandString(uint(valSize))
	ops := make([]v3.Op, len(keys))
	for i := range ops {
		ops[i] = v3.OpPut(keys[i], value)
	}
	duration := time.Minute
	bar = pb.New(int(duration.Seconds()) * qpsRate)
	bar.Start()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	takeN := int(math.Ceil(float64(qpsRate) / 500))
	putLimiter := rate.NewLimiter(rate.Limit(qpsRate), takeN)
	putReport := newReport(cmd.Name())
	for i := range clients {
		c := clients[i]
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				err := putLimiter.WaitN(ctx, takeN)
				if err != nil {
					return
				}
				wg.Go(func() {
					for range takeN {
						select {
						case <-ctx.Done():
							return
						default:
						}
						op := ops[rand.Intn(len(ops))]
						start := time.Now()
						_, err := c.Txn(context.Background()).Then(op).Commit()
						putReport.Results() <- report.Result{Err: err, Start: start, End: time.Now()}
						bar.Increment()
					}
				})
			}
		})
	}

	putResult := putReport.Run()
	wg.Wait()
	bar.Finish()
	close(putReport.Results())
	fmt.Println(<-putResult)
}
