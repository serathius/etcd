// Copyright 2024 The etcd Authors
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

package embed

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func TestEmptyClientTLSInfo_createMetricsListener(t *testing.T) {
	e := &Etcd{
		cfg: Config{
			ClientTLSInfo: transport.TLSInfo{},
		},
	}

	murl := url.URL{
		Scheme: "https",
		Host:   "localhost:8080",
	}
	_, err := e.createMetricsListener(murl)
	require.ErrorIsf(t, err, ErrMissingClientTLSInfoForMetricsURL, "expected error %v, got %v", ErrMissingClientTLSInfoForMetricsURL, err)
}

func TestStartEtcdPebble(t *testing.T) {
	tdir := t.TempDir()
	cfg := NewConfig()

	testURLConfig := newConfigTestURLs()
	applyTestURLConfig(cfg, testURLConfig)

	cfg.Dir = tdir
	cfg.StorageEngine = "pebble"
	e, err := StartEtcd(cfg)
	require.NoError(t, err)
	defer e.Close()

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		t.Fatal("etcd server with pebble timed out waiting to become ready")
	}

	client := v3client.New(e.Server)
	defer client.Close()

	// 1. Put / Get
	_, err = client.Put(t.Context(), "foo", "bar")
	require.NoError(t, err)
	resp, err := client.Get(t.Context(), "foo")
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.Kvs))
	require.Equal(t, []byte("bar"), resp.Kvs[0].Value)

	// 2. Lease
	lresp, err := client.Grant(t.Context(), 10)
	require.NoError(t, err)
	require.NotZero(t, lresp.ID)
	_, err = client.Put(t.Context(), "leased-key", "val", clientv3.WithLease(lresp.ID))
	require.NoError(t, err)

	// 3. Compact
	_, err = client.Compact(t.Context(), resp.Kvs[0].ModRevision)
	require.NoError(t, err)
}
