package version

import (
	"encoding/json"
	"path"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
	"go.etcd.io/etcd/server/v3/mvcc/buckets"
)

const (
	storePrefix = "/0"
)

func mustCreateBackendBuckets(be backend.Backend) {
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafeCreateBucket(buckets.Cluster)
}

func mustSaveClusterVersionToStore(lg *zap.Logger, s v2store.Store, ver *semver.Version) {
	if _, err := s.Set(StoreClusterVersionKey(), false, ver.String(), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		lg.Panic(
			"failed to save cluster version to store",
			zap.String("path", StoreClusterVersionKey()),
			zap.Error(err),
		)
	}
}

func clusterVersionFromStore(lg *zap.Logger, st v2store.Store) *semver.Version {
	e, err := st.Get(path.Join(storePrefix, "version"), false, false)
	if err != nil {
		if membership.IsKeyNotFound(err) {
			return nil
		}
		lg.Panic(
			"failed to get cluster version from store",
			zap.String("path", path.Join(storePrefix, "version")),
			zap.Error(err),
		)
	}
	return semver.Must(semver.NewVersion(*e.Node.Value))
}

// The field is populated since etcd v3.5.
func mustSaveClusterVersionToBackend(be backend.Backend, ver *semver.Version) {
	ckey := backendClusterVersionKey()

	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(buckets.Cluster, ckey, []byte(ver.String()))
}

// The field is populated since etcd v3.5.
func mustSaveDowngradeToBackend(lg *zap.Logger, be backend.Backend, downgrade *DowngradeInfo) {
	dkey := backendDowngradeKey()
	dvalue, err := json.Marshal(downgrade)
	if err != nil {
		lg.Panic("failed to marshal downgrade information", zap.Error(err))
	}
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(buckets.Cluster, dkey, dvalue)
}

// The field is populated since etcd v3.5.
func clusterVersionFromBackend(lg *zap.Logger, be backend.Backend) *semver.Version {
	ckey := backendClusterVersionKey()
	tx := be.ReadTx()
	tx.RLock()
	defer tx.RUnlock()
	keys, vals := tx.UnsafeRange(buckets.Cluster, ckey, nil, 0)
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != 1 {
		lg.Panic(
			"unexpected number of keys when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	return semver.Must(semver.NewVersion(string(vals[0])))
}

// The field is populated since etcd v3.5.
func downgradeInfoFromBackend(lg *zap.Logger, be backend.Backend) *DowngradeInfo {
	dkey := backendDowngradeKey()
	tx := be.ReadTx()
	tx.Lock()
	defer tx.Unlock()
	keys, vals := tx.UnsafeRange(buckets.Cluster, dkey, nil, 0)
	if len(keys) == 0 {
		return nil
	}

	if len(keys) != 1 {
		lg.Panic(
			"unexpected number of keys when getting cluster version from backend",
			zap.Int("number-of-key", len(keys)),
		)
	}
	var d DowngradeInfo
	if err := json.Unmarshal(vals[0], &d); err != nil {
		lg.Panic("failed to unmarshal downgrade information", zap.Error(err))
	}

	// verify the downgrade info from backend
	if d.Enabled {
		if _, err := semver.NewVersion(d.TargetVersion); err != nil {
			lg.Panic(
				"unexpected version format of the downgrade target version from backend",
				zap.String("target-version", d.TargetVersion),
			)
		}
	}
	return &d
}

func StoreClusterVersionKey() string {
	return path.Join(storePrefix, "version")
}

func backendClusterVersionKey() []byte {
	return []byte("clusterVersion")
}

func backendDowngradeKey() []byte {
	return []byte("downgrade")
}
