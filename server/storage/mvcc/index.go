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

package mvcc

import (
	"bytes"
	"fmt"
	"github.com/VictorLowther/ibtree"
	"go.uber.org/zap"
)

type index interface {
	Get(key []byte, atRev int64) (rev, created Revision, ver int64, err error)
	Range(key, end []byte, atRev int64) ([][]byte, []Revision)
	Revisions(key, end []byte, atRev int64, limit int) ([]Revision, int)
	CountRevisions(key, end []byte, atRev int64) int
	Put(key []byte, rev Revision)
	Tombstone(key []byte, rev Revision) error
	Compact(rev int64) map[Revision]struct{}
	Keep(rev int64) map[Revision]struct{}
}

type treeIndex struct {
	baseRev      int64
	revisionTree []*ibtree.Tree[keyRev]
	lg           *zap.Logger
}

type keyRev struct {
	key          []byte
	mod, created Revision
	version      int64
}

var lessThen ibtree.LessThan[keyRev] = func(k keyRev, k2 keyRev) bool {
	return compare(k, k2) == -1
}

func compare(k keyRev, k2 keyRev) int {
	return bytes.Compare(k.key, k2.key)
}

func compareKey(k []byte) ibtree.CompareAgainst[keyRev] {
	return func(k2 keyRev) int {
		return bytes.Compare(k2.key, k)
	}
}

func lessThanKey(k []byte) ibtree.Test[keyRev] {
	return func(k2 keyRev) bool {
		return bytes.Compare(k2.key, k) < 0
	}
}

func greaterThanEqualKey(k []byte) ibtree.Test[keyRev] {
	return func(k2 keyRev) bool {
		return bytes.Compare(k2.key, k) >= 0
	}
}

func newTreeIndex(lg *zap.Logger) *treeIndex {
	return &treeIndex{
		baseRev: -1,
		lg:      lg,
	}
}

func (ti *treeIndex) Put(key []byte, rev Revision) {
	if ti.baseRev == -1 {
		ti.baseRev = rev.Main - 1
		ti.revisionTree = []*ibtree.Tree[keyRev]{
			ibtree.New[keyRev](lessThen),
		}
	}
	if rev.Main != ti.rev()+1 {
		panic(fmt.Sprintf("append only, lastRev: %d, putRev: %d", ti.rev(), rev.Main))
	}
	prevTree := ti.revisionTree[len(ti.revisionTree)-1]
	item, found := prevTree.Get(compareKey(key))
	created := rev
	var version int64 = 1
	if found {
		created = item.created
		version = item.version + 1
	}
	ti.revisionTree = append(ti.revisionTree, prevTree.Insert(keyRev{
		key:     key,
		mod:     rev,
		created: created,
		version: version,
	}))
}

func (ti *treeIndex) rev() int64 {
	return ti.baseRev + int64(len(ti.revisionTree)) - 1
}

func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created Revision, ver int64, err error) {
	idx := atRev - ti.baseRev
	if idx < 0 || idx >= int64(len(ti.revisionTree)) {
		return Revision{}, Revision{}, 0, ErrRevisionNotFound
	}
	tree := ti.revisionTree[idx]

	keyRev, found := tree.Get(compareKey(key))
	if !found {
		return Revision{}, Revision{}, 0, ErrRevisionNotFound
	}
	return keyRev.mod, keyRev.created, keyRev.version, nil
}

// Revisions returns limited number of revisions from key(included) to end(excluded)
// at the given rev. The returned slice is sorted in the order of key. There is no limit if limit <= 0.
// The second return parameter isn't capped by the limit and reflects the total number of revisions.
func (ti *treeIndex) Revisions(key, end []byte, atRev int64, limit int) (revs []Revision, total int) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, 0
		}
		return []Revision{rev}, 1
	}
	idx := atRev - ti.baseRev
	tree := ti.revisionTree[idx]
	tree.Range(lessThanKey(key), greaterThanEqualKey(end), func(kr keyRev) bool {
		if limit <= 0 || len(revs) < limit {
			revs = append(revs, kr.mod)
		}
		total++
		return true
	})
	return revs, total
}

// CountRevisions returns the number of revisions
// from key(included) to end(excluded) at the given rev.
func (ti *treeIndex) CountRevisions(key, end []byte, atRev int64) int {
	if end == nil {
		_, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return 0
		}
		return 1
	}
	idx := atRev - ti.baseRev
	tree := ti.revisionTree[idx]
	total := 0
	tree.Range(lessThanKey(key), greaterThanEqualKey(end), func(kr keyRev) bool {
		total++
		return true
	})
	return total
}

func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []Revision) {
	if end == nil {
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []Revision{rev}
	}
	idx := atRev - ti.baseRev
	tree := ti.revisionTree[idx]
	tree.Range(lessThanKey(key), greaterThanEqualKey(end), func(kr keyRev) bool {
		revs = append(revs, kr.mod)
		keys = append(keys, kr.key)
		return true
	})
	return keys, revs
}

func (ti *treeIndex) Tombstone(key []byte, rev Revision) error {
	if rev.Main != ti.rev()+1 {
		panic(fmt.Sprintf("append only, lastRev: %d, putRev: %d", ti.rev(), rev.Main))
	}
	prevTree := ti.revisionTree[len(ti.revisionTree)-1]
	newTree, _, found := prevTree.Delete(keyRev{
		key: key,
	})
	if !found {
		return ErrRevisionNotFound
	}
	ti.revisionTree = append(ti.revisionTree, newTree)
	return nil
}

func (ti *treeIndex) Compact(rev int64) map[Revision]struct{} {
	available := make(map[Revision]struct{})
	ti.lg.Info("compact tree index", zap.Int64("revision", rev))
	idx := rev - ti.baseRev
	ti.revisionTree = ti.revisionTree[idx:]
	ti.baseRev = rev
	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[Revision]struct{} {
	available := make(map[Revision]struct{})
	idx := rev - ti.baseRev
	tree := ti.revisionTree[idx]
	for it := tree.All(); it.Next(); {
		keyRev := it.Item()
		available[keyRev.mod] = struct{}{}
	}
	return available
}
