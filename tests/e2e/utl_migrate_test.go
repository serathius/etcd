package e2e

import (
	"fmt"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"strings"
	"testing"
	"time"
)

func TestEtctlutlMigrate(t *testing.T) {
	lastReleaseBinary := binDir + "/etcd-last-release"

	tcs := []struct {
		name          string
		targetVersion string
		binary        string
		force         bool

		expectSubString string
	}{
		{
			name:            "Migrate last release to 3.5 is no-op",
			binary:          lastReleaseBinary,
			targetVersion:   "3.5",
			expectSubString: "storage version up-to-date\t" + `{"storage-version": "3.5.0"}`,
		},
		{
			name:            "Upgrade last release to 3.6 should succeed",
			binary:          lastReleaseBinary,
			targetVersion:   "3.6",
			expectSubString: "upgraded storage version\t" + `{"storage-version": "3.6.0"}`,
		},
		{
			name:            "Migrate current release to 3.6 is no-op",
			targetVersion:   "3.6",
			expectSubString: "storage version up-to-date\t" + `{"storage-version": "3.6.0"}`,
		},
		{
			name:            "Downgrade current release to 3.5 should fail without force",
			targetVersion:   "3.5",
			expectSubString: "Downgrades are not yet supported\t" + `{"storage-version": "3.6.0", "target-storage-version": "3.5.0"}`,
		},
		{
			name:            "Downgrade current release to 3.5 with force should work",
			targetVersion:   "3.5",
			force:           true,
			expectSubString: "forcfully set storage version\t" + `{"storage-version": "3.5.0"}`,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			BeforeTest(t)
			if tc.binary != "" && !fileutil.Exist(tc.binary) {
				t.Skipf("%q does not exist", lastReleaseBinary)
			}
			dataDirPath := t.TempDir()

			epc, err := newEtcdProcessCluster(t, &etcdProcessClusterConfig{
				execPath:     tc.binary,
				dataDirPath:  dataDirPath,
				clusterSize:  1,
				initialToken: "new",
				keepDataDir:  true,
				// Set low snapshotCount to ensure wal snapshot is done
				snapshotCount: 1,
			})
			if err != nil {
				t.Fatalf("could not start etcd process cluster (%v)", err)
			}
			defer func() {
				if errC := epc.Close(); errC != nil {
					t.Fatalf("error closing etcd processes (%v)", errC)
				}
			}()

			dialTimeout := 10 * time.Second
			prefixArgs := []string{ctlBinPath, "--endpoints", strings.Join(epc.EndpointsV3(), ","), "--dial-timeout", dialTimeout.String()}

			t.Log("Write keys to ensure wal snapshot is created and all v3.5 fields are set...")
			for i := 0; i < 10; i++ {
				if err = spawnWithExpect(append(prefixArgs, "put", fmt.Sprintf("%d", i), "value"), "OK"); err != nil {
					t.Fatal(err)
				}
			}

			t.Log("Stopping the server...")
			if err = epc.procs[0].Stop(); err != nil {
				t.Fatal(err)
			}

			t.Log("etcdutl migrate...")
			args := []string{utlBinPath, "migrate", "--data-dir", dataDirPath, "--target-version", tc.targetVersion}
			if tc.force {
				args = append(args, "--force")
			}
			err = spawnWithExpect(args, tc.expectSubString)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
