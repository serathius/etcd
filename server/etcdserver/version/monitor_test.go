package version

import (
	"reflect"
	"testing"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"

	"go.etcd.io/etcd/api/v3/version"
)

var testLogger = zap.NewExample()

func TestDecideClusterVersion(t *testing.T) {
	tests := []struct {
		vers  map[string]*version.Versions
		wdver *semver.Version
	}{
		{
			map[string]*version.Versions{"a": {Server: "2.0.0"}},
			semver.Must(semver.NewVersion("2.0.0")),
		},
		// unknown
		{
			map[string]*version.Versions{"a": nil},
			nil,
		},
		{
			map[string]*version.Versions{"a": {Server: "2.0.0"}, "b": {Server: "2.1.0"}, "c": {Server: "2.1.0"}},
			semver.Must(semver.NewVersion("2.0.0")),
		},
		{
			map[string]*version.Versions{"a": {Server: "2.1.0"}, "b": {Server: "2.1.0"}, "c": {Server: "2.1.0"}},
			semver.Must(semver.NewVersion("2.1.0")),
		},
		{
			map[string]*version.Versions{"a": nil, "b": {Server: "2.1.0"}, "c": {Server: "2.1.0"}},
			nil,
		},
	}

	for i, tt := range tests {
		monitor := NewMonitor(testLogger)
		monitor.SetServer(&storageMock{versions: tt.vers})
		dver := monitor.decideClusterVersion()
		if !reflect.DeepEqual(dver, tt.wdver) {
			t.Errorf("#%d: ver = %+v, want %+v", i, dver, tt.wdver)
		}
	}
}

func TestVersionMatchTarget(t *testing.T) {
	tests := []struct {
		name             string
		targetVersion    *semver.Version
		versionMap       map[string]*version.Versions
		expectedFinished bool
	}{
		{
			"When downgrade finished",
			&semver.Version{Major: 3, Minor: 4},
			map[string]*version.Versions{
				"mem1": {Server: "3.4.1", Cluster: "3.4.0"},
				"mem2": {Server: "3.4.2-pre", Cluster: "3.4.0"},
				"mem3": {Server: "3.4.2", Cluster: "3.4.0"},
			},
			true,
		},
		{
			"When cannot parse peer version",
			&semver.Version{Major: 3, Minor: 4},
			map[string]*version.Versions{
				"mem1": {Server: "3.4.1", Cluster: "3.4"},
				"mem2": {Server: "3.4.2-pre", Cluster: "3.4.0"},
				"mem3": {Server: "3.4.2", Cluster: "3.4.0"},
			},
			false,
		},
		{
			"When downgrade not finished",
			&semver.Version{Major: 3, Minor: 4},
			map[string]*version.Versions{
				"mem1": {Server: "3.4.1", Cluster: "3.4.0"},
				"mem2": {Server: "3.4.2-pre", Cluster: "3.4.0"},
				"mem3": {Server: "3.5.2", Cluster: "3.5.0"},
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			monitor := NewMonitor(testLogger)
			monitor.SetServer(&storageMock{versions: tt.versionMap})
			actual := monitor.versionsMatchTarget(tt.targetVersion)
			if actual != tt.expectedFinished {
				t.Errorf("expected downgrade finished is %v; got %v", tt.expectedFinished, actual)
			}
		})
	}
}


func TestIsVersionChangable(t *testing.T) {
	v0 := semver.Must(semver.NewVersion("2.4.0"))
	v1 := semver.Must(semver.NewVersion("3.4.0"))
	v2 := semver.Must(semver.NewVersion("3.5.0"))
	v3 := semver.Must(semver.NewVersion("3.5.1"))
	v4 := semver.Must(semver.NewVersion("3.6.0"))

	tests := []struct {
		name           string
		currentVersion *semver.Version
		localVersion   *semver.Version
		expectedResult bool
	}{
		{
			name:           "When local version is one minor lower than cluster version",
			currentVersion: v2,
			localVersion:   v1,
			expectedResult: true,
		},
		{
			name:           "When local version is one minor and one patch lower than cluster version",
			currentVersion: v3,
			localVersion:   v1,
			expectedResult: true,
		},
		{
			name:           "When local version is one minor higher than cluster version",
			currentVersion: v1,
			localVersion:   v2,
			expectedResult: true,
		},
		{
			name:           "When local version is two minor higher than cluster version",
			currentVersion: v1,
			localVersion:   v4,
			expectedResult: true,
		},
		{
			name:           "When local version is one major higher than cluster version",
			currentVersion: v0,
			localVersion:   v1,
			expectedResult: false,
		},
		{
			name:           "When local version is equal to cluster version",
			currentVersion: v1,
			localVersion:   v1,
			expectedResult: false,
		},
		{
			name:           "When local version is one patch higher than cluster version",
			currentVersion: v2,
			localVersion:   v3,
			expectedResult: false,
		},
		{
			name:           "When local version is two minor lower than cluster version",
			currentVersion: v4,
			localVersion:   v1,
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ret := IsValidVersionChange(tt.currentVersion, tt.localVersion); ret != tt.expectedResult {
				t.Errorf("Expected %v; Got %v", tt.expectedResult, ret)
			}
		})
	}
}

type storageMock struct {
	versions       map[string]*version.Versions
	clusterVersion *semver.Version
	downgradeInfo  *DowngradeInfo
}

var _ Server = (*storageMock)(nil)

func (s *storageMock) UpdateClusterVersion(version string) {
	s.clusterVersion = semver.New(version)
}

func (s *storageMock) DowngradeCancel() {
	s.downgradeInfo = nil
}

func (s *storageMock) GetVersions() map[string]*version.Versions {
	return s.versions
}
