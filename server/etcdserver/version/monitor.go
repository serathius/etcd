package version

import (
	"sync"

	"github.com/coreos/go-semver/semver"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/v2store"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

// Monitor contains logic used by cluster leader to monitor version changes and decide on cluster version or downgrade progress.
type Monitor struct {
	lg *zap.Logger

	s  Server
	v2store v2store.Store
	be      backend.Backend

	sync.Mutex // guards the fields below
	version    *semver.Version
	downgradeInfo *DowngradeInfo
}

// Server lists EtcdServer methods needed by Monitor
type Server interface {
	GetVersions() map[string]*version.Versions
	UpdateClusterVersion(string)
	DowngradeCancel()
}

func NewMonitor(lg *zap.Logger) *Monitor {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &Monitor{
		lg: lg,
		downgradeInfo: &DowngradeInfo{Enabled: false},
	}
}

func (c *Monitor) SetStore(st v2store.Store) { c.v2store = st }

func (c *Monitor) SetBackend(be backend.Backend) {
	c.be = be
	mustCreateBackendBuckets(c.be)
}

func (c *Monitor) SetServer(s Server) {
	c.s = s
}

func (m *Monitor) SetVersion(ver *semver.Version, onSet func(*zap.Logger, *semver.Version), shouldApplyV3 membership.ShouldApplyV3) {
	m.Lock()
	defer m.Unlock()
	if m.version != nil {
		m.lg.Info(
			"updated cluster version",
			zap.String("from", version.Cluster(m.version.String())),
			zap.String("to", version.Cluster(ver.String())),
		)
	} else {
		m.lg.Info(
			"set initial cluster version",
			zap.String("cluster-version", version.Cluster(ver.String())),
		)
	}
	oldVer := m.version
	m.version = ver
	mustDetectDowngrade(m.lg, m.version, m.downgradeInfo)
	if m.v2store != nil {
		mustSaveClusterVersionToStore(m.lg, m.v2store, ver)
	}
	if m.be != nil && shouldApplyV3 {
		mustSaveClusterVersionToBackend(m.be, ver)
	}
	if oldVer != nil {
		ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(oldVer.String())}).Set(0)
	}
	ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(ver.String())}).Set(1)
	onSet(m.lg, ver)
}

func (m *Monitor) Recover(onSet func(*zap.Logger, *semver.Version)) {
	m.Lock()
	defer m.Unlock()

	if m.be != nil {
		m.version = clusterVersionFromBackend(m.lg, m.be)
	} else {
		m.version = clusterVersionFromStore(m.lg, m.v2store)
	}

	if m.be != nil {
		m.downgradeInfo = downgradeInfoFromBackend(m.lg, m.be)
	}
	d := &DowngradeInfo{Enabled: false}
	if m.downgradeInfo != nil {
		d = &DowngradeInfo{Enabled: m.downgradeInfo.Enabled, TargetVersion: m.downgradeInfo.TargetVersion}
	}
	mustDetectDowngrade(m.lg, m.version, d)
	onSet(m.lg, m.version)

	if m.version != nil {
		m.lg.Info(
			"set cluster version from store",
			zap.String("cluster-version", version.Cluster(m.version.String())),
		)
	}
}

func (m *Monitor) Version() *semver.Version {
	m.Lock()
	defer m.Unlock()
	if m.version == nil {
		return nil
	}
	return semver.Must(semver.NewVersion(m.version.String()))
}

// IsValidVersionChange checks the two scenario when version is valid to change:
// 1. Downgrade: cluster version is 1 minor version higher than local version,
// cluster version should change.
// 2. Cluster start: when not all members version are available, cluster version
// is set to MinVersion(3.0), when all members are at higher version, cluster version
// is lower than local version, cluster version should change
func IsValidVersionChange(cv *semver.Version, lv *semver.Version) bool {
	cv = &semver.Version{Major: cv.Major, Minor: cv.Minor}
	lv = &semver.Version{Major: lv.Major, Minor: lv.Minor}

	if isValidDowngrade(cv, lv) || (cv.Major == lv.Major && cv.LessThan(*lv)) {
		return true
	}
	return false
}

// DowngradeInfo returns the downgrade status of the cluster
func (m *Monitor) DowngradeInfo() *DowngradeInfo {
	m.Lock()
	defer m.Unlock()
	if m.downgradeInfo == nil {
		return &DowngradeInfo{Enabled: false}
	}
	d := &DowngradeInfo{Enabled: m.downgradeInfo.Enabled, TargetVersion: m.downgradeInfo.TargetVersion}
	return d
}

func (m *Monitor) SetDowngradeInfo(d *DowngradeInfo, shouldApplyV3 membership.ShouldApplyV3) {
	m.Lock()
	defer m.Unlock()

	if m.be != nil && shouldApplyV3 {
		mustSaveDowngradeToBackend(m.lg, m.be, d)
	}

	m.downgradeInfo = d

	if d.Enabled {
		m.lg.Info(
			"The server is ready to downgrade",
			zap.String("target-version", d.TargetVersion),
			zap.String("server-version", version.Version),
		)
	}
}

// UpdateClusterVersionIfNeeded updates the cluster version if all members agrees on a higher one.
// It prints out log if there is a member with a higher version than the
// local version.
func (m *Monitor) UpdateClusterVersionIfNeeded() {
	v := m.decideClusterVersion()
	if v != nil {
		// only keep major.minor version for comparison
		v = &semver.Version{
			Major: v.Major,
			Minor: v.Minor,
		}
	}

	// if the current version is nil:
	// 1. use the decided version if possible
	// 2. or use the min cluster version
	if m.version == nil {
		verStr := version.MinClusterVersion
		if v != nil {
			verStr = v.String()
		}
		m.s.UpdateClusterVersion(verStr)
		return
	}

	if v != nil && IsValidVersionChange(m.version, v) {
		m.s.UpdateClusterVersion(v.String())
	}
}

func (m *Monitor) CancelDowngradeIfNeeded() {
	d := m.downgradeInfo
	if !d.Enabled {
		return
	}

	targetVersion := d.TargetVersion
	v := semver.Must(semver.NewVersion(targetVersion))
	if m.versionsMatchTarget(v) {
		m.lg.Info("the cluster has been downgraded", zap.String("cluster-version", targetVersion))
		m.s.DowngradeCancel()
	}
}

// decideClusterVersion decides the cluster version based on the versions map.
// The returned version is the min server version in the map, or nil if the min
// version in unknown.
func (m *Monitor) decideClusterVersion() *semver.Version {
	vers := m.s.GetVersions()
	var cv *semver.Version
	lv := semver.Must(semver.NewVersion(version.Version))

	for mid, ver := range vers {
		if ver == nil {
			return nil
		}
		v, err := semver.NewVersion(ver.Server)
		if err != nil {
			m.lg.Warn(
				"failed to parse server version of remote member",
				zap.String("remote-member-id", mid),
				zap.String("remote-member-version", ver.Server),
				zap.Error(err),
			)
			return nil
		}
		if lv.LessThan(*v) {
			m.lg.Warn(
				"leader found higher-versioned member",
				zap.String("local-member-version", lv.String()),
				zap.String("remote-member-id", mid),
				zap.String("remote-member-version", ver.Server),
			)
		}
		if cv == nil {
			cv = v
		} else if v.LessThan(*cv) {
			cv = v
		}
	}
	return cv
}

// versionsMatchTarget returns true if all server versions are equal to target version, otherwise return false.
// It can be used to decide the whether the cluster finishes downgrading to target version.
func (m *Monitor) versionsMatchTarget(targetVersion *semver.Version) bool {
	vers := m.s.GetVersions()
	for mid, ver := range vers {
		if ver == nil {
			return false
		}
		v, err := semver.NewVersion(ver.Cluster)
		if err != nil {
			m.lg.Warn(
				"failed to parse server version of remote member",
				zap.String("remote-member-id", mid),
				zap.String("remote-member-version", ver.Server),
				zap.Error(err),
			)
			return false
		}
		if !targetVersion.Equal(*v) {
			m.lg.Warn("remotes server has mismatching etcd version",
				zap.String("remote-member-id", mid),
				zap.String("current-server-version", v.String()),
				zap.String("target-version", targetVersion.String()),
			)
			return false
		}
	}
	return true
}
