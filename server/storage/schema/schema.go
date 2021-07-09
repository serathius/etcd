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

package schema

import (
	"fmt"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"

	"go.etcd.io/etcd/server/v3/storage/backend"
)

var (
	V3_5 = semver.Version{Major: 3, Minor: 5}
	V3_6 = semver.Version{Major: 3, Minor: 6}
)

// Migrate updates storage version to provided target version.
func Migrate(lg *zap.Logger, tx backend.BatchTx, target semver.Version) error {
	ver, err := detectStorageVersion(lg, tx)
	if err != nil {
		return fmt.Errorf("cannot determine storage version: %w", err)
	}
	if ver.Major != target.Major {
		lg.Panic("Chaning major storage version is not supported",
			zap.String("storage-version", ver.String()),
			zap.String("target-storage-version", target.String()),
		)
	}
	if ver.Minor > target.Minor {
		lg.Panic("Downgrades are not yet supported",
			zap.String("storage-version", ver.String()),
			zap.String("target-storage-version", target.String()),
		)
	}
	if ver.Minor == target.Minor {
		lg.Info("storage version up-to-date", zap.String("storage-version", ver.String()))
		return nil
	}
	for ver.Minor != target.Minor {
		next := semver.Version{Major: ver.Major}
		upgrade := ver.Minor < target.Minor
		if upgrade {
			next.Minor = ver.Minor + 1
		} else {
			next.Minor = ver.Minor - 1
		}
		err := migrateOnce(lg, tx, next, upgrade)
		if err != nil {
			return err
		}
		ver = &next
		lg.Info("upgraded storage version", zap.String("storage-version", ver.String()))
	}
	return nil
}

func detectStorageVersion(lg *zap.Logger, tx backend.ReadTx) (*semver.Version, error) {
	tx.Lock()
	defer tx.Unlock()
	v := UnsafeReadStorageVersion(tx)
	if v != nil {
		return v, nil
	}
	confstate := UnsafeConfStateFromBackend(lg, tx)
	if confstate == nil {
		return nil, fmt.Errorf("missing %q key", MetaConfStateName)
	}
	_, term := UnsafeReadConsistentIndex(tx)
	if term == 0 {
		return nil, fmt.Errorf("missing %q key", MetaTermKeyName)
	}
	copied := V3_5
	return &copied, nil
}

func migrateOnce(lg *zap.Logger, tx backend.BatchTx, next semver.Version, upgrade bool) error {
	ms, found := schema[next]
	if !found {
		lg.Panic("storage version is not supported", zap.String("storage-version", next.String()))
	}
	var err error
	tx.Lock()
	defer tx.Unlock()
	for _, m := range ms {
		if upgrade {
			err = m.UnsafeUpgrade(tx)
		} else {
			err = m.UnsafeDowngrade(tx)
		}
		if err != nil {
			return err
		}
	}
	// Storage version is available since v3.6
	if next != V3_5 {
		UnsafeSetStorageVersion(tx, &next)
	}
	return nil
}

var schema = map[semver.Version][]Migration{
	V3_6: {
		&newField{bucket: Meta, fieldName: MetaStorageVersionName, fieldValue: []byte("")},
	},
}

type Migration interface {
	UnsafeUpgrade(backend.BatchTx) error
	UnsafeDowngrade(backend.BatchTx) error
}

type newField struct {
	bucket     backend.Bucket
	fieldName  []byte
	fieldValue []byte
}

func (m *newField) UnsafeUpgrade(tx backend.BatchTx) error {
	tx.UnsafePut(m.bucket, m.fieldName, m.fieldValue)
	return nil
}
func (m *newField) UnsafeDowngrade(tx backend.BatchTx) error {
	tx.UnsafeDelete(m.bucket, m.fieldName)
	return nil
}
