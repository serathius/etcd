// Copyright 2021 The etcd Authors
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

package etcdutl

import (
	"fmt"
	"github.com/coreos/go-semver/semver"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/pkg/v3/cobrautl"
	"go.etcd.io/etcd/server/v3/datadir"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"
	"strings"
)

// NewMigrateCommand prints out the version of etcd.
func NewMigrateCommand() *cobra.Command {
	o := newMigrateOptions()
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrates schema of etcd data dir files to make them compatible with different etcd version",
		Run:  func(cmd *cobra.Command, args []string) {
			cfg, err := o.Config()
			if err != nil {
				cobrautl.ExitWithError(cobrautl.ExitBadArgs, err)
			}
			migrateCommandFunc(cfg)
		},
	}
	o.AddFlags(cmd)
	return cmd
}

type migrateOptions struct {
	dataDir string
	targetVersion string
	force bool
}

func newMigrateOptions() *migrateOptions {
	return &migrateOptions{
		dataDir: "",
		targetVersion: "",
		force: false,
	}
}

func (o *migrateOptions) AddFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.dataDir, "data-dir", o.dataDir, "Path to the etcd data dir")
	cmd.MarkFlagRequired("data-dir")
	cmd.MarkFlagDirname("data-dir")

	cmd.Flags().StringVar(&o.targetVersion, "target-version", o.targetVersion, "Target version, format X.Y for example 3.6")
	cmd.MarkFlagRequired("target-version")

	cmd.Flags().BoolVar(&o.force, "force", o.force, "Ignore migration failure and forcefully override storage version. `")
}

func (o *migrateOptions) Config() (*migrateConfig, error) {
	c := &migrateConfig{
		force: o.force,
	}
	var err error
	dotCount := strings.Count(o.targetVersion, ".")
	if dotCount != 1 {
		return nil, fmt.Errorf(`wrong target-version format, expected "3.6", got %q`, o.targetVersion)
	}
	c.targetVersion, err = semver.NewVersion(o.targetVersion + ".0")
	if err != nil {
		return nil, fmt.Errorf("failed to parse target-version: %w", err)
	}

	dbPath := datadir.ToBackendFileName(o.dataDir)
	c.be = backend.NewDefaultBackend(dbPath)

	return c, nil
}

type migrateConfig struct {
	be backend.Backend
	targetVersion *semver.Version
	force bool
}

func migrateCommandFunc(c *migrateConfig) {
	defer c.be.Close()
	lg := GetLogger()
	tx := c.be.BatchTx()
	if c.force {
		defer func() {
			r := recover()
			if r != nil {
				tx.Lock()
				schema.UnsafeSetStorageVersion(tx, c.targetVersion)
				tx.Unlock()
				c.be.ForceCommit()
				lg.Warn("forcfully set storage version", zap.String("storage-version", c.targetVersion.String()))
			}
		}()
	}
	err := schema.Migrate(lg, tx, *c.targetVersion)
	if err != nil {
		lg.Fatal("failed to migrate", zap.Error(err))
	}
	c.be.ForceCommit()
}
