// Copyright 2022 The etcd Authors
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

package robustness

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/pkg/v3/stringutil"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/model"
)

var (
	DefaultLeaseTTL   int64 = 7200
	RequestTimeout          = 40 * time.Millisecond
	MultiOpTxnOpCount       = 4
)

type TrafficRequestType string

const (
	Get           TrafficRequestType = "get"
	Put           TrafficRequestType = "put"
	LargePut      TrafficRequestType = "largePut"
	Delete        TrafficRequestType = "delete"
	MultiOpTxn    TrafficRequestType = "multiOpTxn"
	PutWithLease  TrafficRequestType = "putWithLease"
	LeaseRevoke   TrafficRequestType = "leaseRevoke"
	CompareAndSet TrafficRequestType = "compareAndSet"
	Defragment    TrafficRequestType = "defragment"
)

func simulateTraffic(ctx context.Context, t *testing.T, lg *zap.Logger, clus *e2e.EtcdProcessCluster, config trafficConfig, finish <-chan struct{}) []porcupine.Operation {
	mux := sync.Mutex{}
	endpoints := clus.EndpointsGRPC()

	ids := identity.NewIdProvider()
	lm := identity.NewLeaseIdStorage()
	h := model.History{}
	limiter := rate.NewLimiter(rate.Limit(config.maximalQPS), 200)

	startTime := time.Now()
	cc, err := NewClient(endpoints, ids, startTime)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	wg := sync.WaitGroup{}
	for i := 0; i < config.clientCount; i++ {
		wg.Add(1)
		c, err := NewClient([]string{endpoints[i%len(endpoints)]}, ids, startTime)
		if err != nil {
			t.Fatal(err)
		}
		go func(c *recordingClient, clientId int) {
			defer wg.Done()
			defer c.Close()

			config.traffic.Run(ctx, clientId, c, limiter, ids, lm, finish)
			mux.Lock()
			h = h.Merge(c.history.History)
			mux.Unlock()
		}(c, i)
	}
	wg.Wait()
	endTime := time.Now()

	// Ensure that last operation is succeeds
	time.Sleep(time.Second)
	err = cc.Put(ctx, "tombstone", "true")
	if err != nil {
		t.Error(err)
	}
	h = h.Merge(cc.history.History)

	operations := h.Operations()
	lg.Info("Recorded operations", zap.Int("count", len(operations)))

	qps := float64(len(operations)) / float64(endTime.Sub(startTime)) * float64(time.Second)
	lg.Info("Average traffic", zap.Float64("qps", qps))
	if qps < config.minimalQPS {
		t.Errorf("Requiring minimal %f qps for test results to be reliable, got %f qps", config.minimalQPS, qps)
	}
	return operations
}

type trafficConfig struct {
	name            string
	minimalQPS      float64
	maximalQPS      float64
	clientCount     int
	traffic         Traffic
	requestProgress bool // Request progress notifications while watching this traffic
}

type Traffic interface {
	Run(ctx context.Context, clientId int, c *recordingClient, limiter *rate.Limiter, ids identity.Provider, lm identity.LeaseIdStorage, finish <-chan struct{})
}

type traffic struct {
	keyCount     int
	writes       []requestChance
	leaseTTL     int64
	largePutSize int
}

type requestChance struct {
	operation TrafficRequestType
	chance    int
}

type kubernetesTraffic struct {
	keyCount int
}

func (t kubernetesTraffic) Run(ctx context.Context, clientId int, c *recordingClient, limiter *rate.Limiter, ids identity.Provider, lm identity.LeaseIdStorage, finish <-chan struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-finish:
			return
		default:
		}
		resource := "pods"

		pods, err := t.Range(ctx, c, "/registry/"+resource+"/", true)
		if err != nil {
			continue
		}
		limiter.Wait(ctx)
		if len(pods) < t.keyCount {
			err = t.Create(ctx, c, "/registry/"+resource+"/default/"+stringutil.RandString(5), fmt.Sprintf("%d", ids.RequestId()))
			continue
		} else {
			randomPod := pods[rand.Intn(len(pods))]
			err = t.Update(ctx, c, string(randomPod.Key), fmt.Sprintf("%d", ids.RequestId()), randomPod.ModRevision)
		}
		if err != nil {
			continue
		}
		limiter.Wait(ctx)
	}
}

func (t kubernetesTraffic) Range(ctx context.Context, c *recordingClient, key string, withPrefix bool) ([]*mvccpb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	resp, err := c.Range(ctx, key, withPrefix)
	cancel()
	return resp, err
}

func (t kubernetesTraffic) Create(ctx context.Context, c *recordingClient, key, value string) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	err := c.CompareAndSet(ctx, key, value, 0)
	cancel()
	return err
}

func (t kubernetesTraffic) Update(ctx context.Context, c *recordingClient, key, value string, expectedRevision int64) error {
	ctx, cancel := context.WithTimeout(ctx, RequestTimeout)
	err := c.CompareAndSet(ctx, key, value, expectedRevision)
	cancel()
	return err
}

func (t traffic) Run(ctx context.Context, clientId int, c *recordingClient, limiter *rate.Limiter, ids identity.Provider, lm identity.LeaseIdStorage, finish <-chan struct{}) {

	for {
		select {
		case <-ctx.Done():
			return
		case <-finish:
			return
		default:
		}
		key := fmt.Sprintf("%d", rand.Int()%t.keyCount)
		// Execute one read per one write to avoid operation history include too many failed writes when etcd is down.
		resp, err := t.Read(ctx, c, key)
		if err != nil {
			continue
		}
		limiter.Wait(ctx)
		err = t.Write(ctx, c, limiter, key, ids, lm, clientId, resp)
		if err != nil {
			continue
		}
		limiter.Wait(ctx)
	}
}

func (t traffic) Read(ctx context.Context, c *recordingClient, key string) (*mvccpb.KeyValue, error) {
	getCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	resp, err := c.Get(getCtx, key)
	cancel()
	return resp, err
}

func (t traffic) Write(ctx context.Context, c *recordingClient, limiter *rate.Limiter, key string, id identity.Provider, lm identity.LeaseIdStorage, cid int, lastValues *mvccpb.KeyValue) error {
	writeCtx, cancel := context.WithTimeout(ctx, RequestTimeout)

	var err error
	switch t.pickWriteRequest() {
	case Put:
		err = c.Put(writeCtx, key, fmt.Sprintf("%d", id.RequestId()))
	case LargePut:
		err = c.Put(writeCtx, key, randString(t.largePutSize))
	case Delete:
		err = c.Delete(writeCtx, key)
	case MultiOpTxn:
		err = c.Txn(writeCtx, nil, t.pickMultiTxnOps(id))
	case CompareAndSet:
		var expectRevision int64
		if lastValues != nil {
			expectRevision = lastValues.ModRevision
		}
		err = c.CompareAndSet(writeCtx, key, fmt.Sprintf("%d", id.RequestId()), expectRevision)
	case PutWithLease:
		leaseId := lm.LeaseId(cid)
		if leaseId == 0 {
			leaseId, err = c.LeaseGrant(writeCtx, t.leaseTTL)
			if err == nil {
				lm.AddLeaseId(cid, leaseId)
				limiter.Wait(ctx)
			}
		}
		if leaseId != 0 {
			putCtx, putCancel := context.WithTimeout(ctx, RequestTimeout)
			err = c.PutWithLease(putCtx, key, fmt.Sprintf("%d", id.RequestId()), leaseId)
			putCancel()
		}
	case LeaseRevoke:
		leaseId := lm.LeaseId(cid)
		if leaseId != 0 {
			err = c.LeaseRevoke(writeCtx, leaseId)
			//if LeaseRevoke has failed, do not remove the mapping.
			if err == nil {
				lm.RemoveLeaseId(cid)
			}
		}
	case Defragment:
		err = c.Defragment(writeCtx)
	default:
		panic("invalid operation")
	}
	cancel()
	return err
}

func (t traffic) pickWriteRequest() TrafficRequestType {
	sum := 0
	for _, op := range t.writes {
		sum += op.chance
	}
	roll := rand.Int() % sum
	for _, op := range t.writes {
		if roll < op.chance {
			return op.operation
		}
		roll -= op.chance
	}
	panic("unexpected")
}

func (t traffic) pickMultiTxnOps(ids identity.Provider) (ops []clientv3.Op) {
	keys := rand.Perm(t.keyCount)
	opTypes := make([]model.OperationType, 4)

	atLeastOnePut := false
	for i := 0; i < MultiOpTxnOpCount; i++ {
		opTypes[i] = t.pickOperationType()
		if opTypes[i] == model.Put {
			atLeastOnePut = true
		}
	}
	// Ensure at least one put to make operation unique
	if !atLeastOnePut {
		opTypes[0] = model.Put
	}

	for i, opType := range opTypes {
		key := fmt.Sprintf("%d", keys[i])
		switch opType {
		case model.Range:
			ops = append(ops, clientv3.OpGet(key))
		case model.Put:
			value := fmt.Sprintf("%d", ids.RequestId())
			ops = append(ops, clientv3.OpPut(key, value))
		case model.Delete:
			ops = append(ops, clientv3.OpDelete(key))
		default:
			panic("unsuported operation type")
		}
	}
	return ops
}

func (t traffic) pickOperationType() model.OperationType {
	roll := rand.Int() % 100
	if roll < 10 {
		return model.Delete
	}
	if roll < 50 {
		return model.Range
	}
	return model.Put
}

func randString(size int) string {
	data := strings.Builder{}
	data.Grow(size)
	for i := 0; i < size; i++ {
		data.WriteByte(byte(int('a') + rand.Intn(26)))
	}
	return data.String()
}
