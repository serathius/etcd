// Copyright 2026 The etcd Authors
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
	"context"
	"errors"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	badgeroptions "github.com/dgraph-io/badger/v4/options"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

type badgerStore struct {
	ReadView
	WriteView

	db    *badger.DB
	index index
	lg    *zap.Logger
	le    lease.Lessor
	cfg   StoreConfig

	mu             sync.RWMutex
	compactMu      sync.Mutex
	revMu          sync.RWMutex
	currentRev     int64
	compactMainRev int64

	watcherMu sync.RWMutex
	unsynced  watcherGroup
	synced    watcherGroup
	victims   []watcherBatch
	stopc     chan struct{}
	wg        sync.WaitGroup
}

func NewBadgerStore(lg *zap.Logger, dir string, le lease.Lessor, cfg StoreConfig) (WatchableKV, error) {
	if lg == nil {
		lg = zap.NewNop()
	}

	opts := badger.DefaultOptions(dir).
		WithLogger(nil).
		WithSyncWrites(false).      // Etcd has its own WAL for durability.
		WithDetectConflicts(false). // Etcd already guarantees linarized execution via Raft
		WithNumMemtables(5).
		WithNumLevelZeroTables(8).
		WithNumLevelZeroTablesStall(16).
		WithValueThreshold(1 << 20). // 1MB threshold -> store values inline in LSM SSTables with compression
		WithCompression(badgeroptions.ZSTD).
		WithNumVersionsToKeep(1)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	bs := &badgerStore{
		db:             db,
		index:          newTreeIndex(lg),
		lg:             lg,
		le:             le,
		cfg:            cfg,
		currentRev:     1,
		compactMainRev: 0,
		unsynced:       newWatcherGroup(),
		synced:         newWatcherGroup(),
		stopc:          make(chan struct{}),
	}
	bs.ReadView = &badgerReadView{bs}
	bs.WriteView = &badgerWriteView{bs}

	bs.wg.Add(1)
	go bs.syncWatchersLoop()

	return bs, nil
}

func (bs *badgerStore) FirstRev() int64 {
	bs.revMu.RLock()
	defer bs.revMu.RUnlock()
	return bs.compactMainRev
}

func (bs *badgerStore) Rev() int64 {
	bs.revMu.RLock()
	defer bs.revMu.RUnlock()
	return bs.currentRev
}

func (bs *badgerStore) Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead {
	bs.revMu.RLock()
	curRev := bs.currentRev
	firstRev := bs.compactMainRev
	bs.revMu.RUnlock()

	var txn *badger.Txn
	if mode == ConcurrentReadTxMode {
		txn = bs.db.NewTransaction(false)
	}

	return &badgerTxnRead{
		bs:       bs,
		txn:      txn,
		rev:      curRev,
		firstRev: firstRev,
		trace:    trace,
	}
}

func (bs *badgerStore) Write(trace *traceutil.Trace) TxnWrite {
	bs.mu.Lock()
	bs.revMu.RLock()
	curRev := bs.currentRev
	firstRev := bs.compactMainRev
	bs.revMu.RUnlock()

	txn := bs.db.NewTransaction(true)

	return &badgerTxnWrite{
		badgerTxnRead: badgerTxnRead{
			bs:       bs,
			txn:      txn,
			rev:      curRev,
			firstRev: firstRev,
			trace:    trace,
		},
		beginRev: curRev,
		changes:  make([]*mvccpb.KeyValue, 0, 4),
	}
}

func (bs *badgerStore) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	bs.compactMu.Lock()
	defer bs.compactMu.Unlock()

	bs.revMu.Lock()
	if rev <= bs.compactMainRev && rev > 0 {
		bs.revMu.Unlock()
		return nil, ErrCompacted
	}
	if rev > bs.currentRev {
		bs.revMu.Unlock()
		return nil, ErrFutureRev
	}
	bs.compactMainRev = rev
	bs.revMu.Unlock()

	keep := bs.index.Compact(rev)

	donec := make(chan struct{})
	go func() {
		defer close(donec)
		startKey := encodePebbleRevKey(Revision{Main: 0, Sub: 0}, false)
		endKey := encodePebbleRevKey(Revision{Main: rev + 1, Sub: 0}, false)

		txn := bs.db.NewTransaction(false)
		defer txn.Discard()

		opt := badger.DefaultIteratorOptions
		opt.Prefix = dataPrefix
		iter := txn.NewIterator(opt)
		defer iter.Close()

		var toDelete [][]byte
		for iter.Seek(startKey); iter.Valid(); iter.Next() {
			item := iter.Item()
			k := item.KeyCopy(nil)
			if bytes.Compare(k, endKey) >= 0 {
				break
			}
			r, ok := decodePebbleRevKey(k)
			if !ok || r.Main > rev {
				break
			}
			if _, ok := keep[r]; !ok {
				toDelete = append(toDelete, k)
			}
		}
		iter.Close()
		txn.Discard()

		if len(toDelete) > 0 {
			wb := bs.db.NewWriteBatch()
			for _, k := range toDelete {
				_ = wb.Delete(k)
			}
			_ = wb.Flush()
			wb.Cancel()
		}
	}()

	return donec, nil
}

func (bs *badgerStore) HashStorage() HashStorage {
	return nil
}

func (bs *badgerStore) Commit() {
	_ = bs.db.Sync()
}

func (bs *badgerStore) Defrag() error {
	_ = bs.db.Sync()
	return bs.db.Flatten(4)
}

func (bs *badgerStore) Restore(b backend.Backend) error {
	return errors.New("not implemented")
}

func (bs *badgerStore) Close() error {
	close(bs.stopc)
	bs.wg.Wait()
	return bs.db.Close()
}

func (bs *badgerStore) NewWatchStream() WatchStream {
	return &watchStream{
		watchable: bs,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

type badgerReadView struct {
	bs *badgerStore
}

func (rv *badgerReadView) FirstRev() int64 { return rv.bs.FirstRev() }
func (rv *badgerReadView) Rev() int64      { return rv.bs.Rev() }

func (rv *badgerReadView) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	tr := rv.bs.Read(ConcurrentReadTxMode, traceutil.TODO())
	defer tr.End()
	return tr.Range(ctx, key, end, ro)
}

type badgerWriteView struct {
	bs *badgerStore
}

func (wv *badgerWriteView) DeleteRange(key, end []byte) (int64, int64) {
	tw := wv.bs.Write(traceutil.TODO())
	defer tw.End()
	return tw.DeleteRange(key, end)
}

func (wv *badgerWriteView) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw := wv.bs.Write(traceutil.TODO())
	defer tw.End()
	return tw.Put(key, value, lease)
}

type badgerTxnRead struct {
	bs       *badgerStore
	txn      *badger.Txn
	rev      int64
	firstRev int64
	trace    *traceutil.Trace
}

func (tr *badgerTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *badgerTxnRead) Rev() int64      { return tr.rev }

func (tr *badgerTxnRead) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	targetRev := ro.Rev
	if targetRev > tr.rev {
		return &RangeResult{Count: -1, Rev: 0}, ErrFutureRev
	}
	if targetRev <= 0 {
		targetRev = tr.rev
	}
	if targetRev < tr.bs.FirstRev() {
		return &RangeResult{Count: -1, Rev: 0}, ErrCompacted
	}

	limit := int(ro.Limit)
	if ro.CountOnly {
		limit = 0
	}

	_, revs, _, _, totalCount := tr.bs.index.Range(key, end, targetRev, limit, true)
	if ro.CountOnly {
		return &RangeResult{Count: totalCount, Rev: tr.rev}, nil
	}

	if len(revs) == 0 {
		return &RangeResult{
			KVs:   nil,
			Count: totalCount,
			Rev:   tr.rev,
		}, nil
	}

	kvs := make([]*mvccpb.KeyValue, len(revs))
	readFn := func(txn *badger.Txn) {
		for i, r := range revs {
			k := encodePebbleRevKey(r, false)
			item, err := txn.Get(k)
			if err == nil {
				val, err := item.ValueCopy(nil)
				if err == nil {
					kv := &mvccpb.KeyValue{}
					if err := proto.Unmarshal(val, kv); err == nil {
						kvs[i] = kv
					}
				}
			}
		}
	}

	if tr.txn != nil {
		readFn(tr.txn)
	} else {
		_ = tr.bs.db.View(func(txn *badger.Txn) error {
			readFn(txn)
			return nil
		})
	}

	if targetRev < tr.bs.FirstRev() {
		return &RangeResult{Count: -1, Rev: 0}, ErrCompacted
	}

	filtered := make([]*mvccpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		if kv != nil {
			filtered = append(filtered, kv)
		}
	}

	return &RangeResult{
		KVs:   filtered,
		Count: totalCount,
		Rev:   tr.rev,
	}, nil
}

func (tr *badgerTxnRead) End() {
	if tr.txn != nil {
		tr.txn.Discard()
	}
}

type badgerTxnWrite struct {
	badgerTxnRead
	beginRev int64
	changes  []*mvccpb.KeyValue
}

func (tw *badgerTxnWrite) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		rev++
	}
	tr := &badgerTxnRead{
		bs:       tw.bs,
		txn:      tw.txn,
		rev:      rev,
		firstRev: tw.firstRev,
		trace:    tw.trace,
	}
	return tr.Range(ctx, key, end, ro)
}

func (tw *badgerTxnWrite) Put(key, value []byte, leaseID lease.LeaseID) int64 {
	rev := tw.beginRev + 1
	c := rev
	ver := int64(1)

	if _, created, prevVer, err := tw.bs.index.Get(key, rev); err == nil {
		c = created.Main
		ver = prevVer + 1
	}

	idxRev := Revision{Main: rev, Sub: int64(len(tw.changes))}
	kv := &mvccpb.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: c,
		ModRevision:    rev,
		Version:        ver,
		Lease:          int64(leaseID),
	}

	d, _ := proto.Marshal(kv)
	dKey := encodePebbleRevKey(idxRev, false)
	_ = tw.txn.Set(dKey, d)

	tw.bs.index.Put(key, idxRev)
	tw.changes = append(tw.changes, kv)
	return rev
}

func (tw *badgerTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {
		rrev++
	}
	keys, _, _, _, _ := tw.bs.index.Range(key, end, rrev, 0, false)
	if len(keys) == 0 {
		return 0, tw.beginRev
	}

	rev := tw.beginRev + 1
	for _, existingKey := range keys {
		delKV := &mvccpb.KeyValue{
			Key:         existingKey,
			ModRevision: rev,
		}
		idxRev := Revision{Main: rev, Sub: int64(len(tw.changes))}
		d, _ := proto.Marshal(&mvccpb.KeyValue{Key: existingKey})
		tombstoneKey := encodePebbleRevKey(idxRev, true)
		_ = tw.txn.Set(tombstoneKey, d)

		_ = tw.bs.index.Tombstone(existingKey, idxRev)
		tw.changes = append(tw.changes, delKV)
	}

	return int64(len(keys)), rev
}

func (tw *badgerTxnWrite) Changes() []*mvccpb.KeyValue {
	return tw.changes
}

func (tw *badgerTxnWrite) End() {
	if len(tw.changes) > 0 {
		_ = tw.txn.Commit()

		tw.bs.revMu.Lock()
		tw.bs.currentRev = tw.beginRev + 1
		tw.bs.revMu.Unlock()

		tw.bs.notify(tw.beginRev+1, tw.changes)
	} else {
		tw.txn.Discard()
	}
	tw.bs.mu.Unlock()
}

func (bs *badgerStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{
		key:      key,
		end:      end,
		startRev: startRev,
		minRev:   startRev,
		id:       id,
		ch:       ch,
		fcs:      fcs,
	}

	bs.watcherMu.Lock()
	bs.revMu.RLock()
	synced := startRev > bs.currentRev || startRev == 0
	if synced {
		wa.minRev = bs.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
		bs.synced.add(wa)
	} else {
		bs.unsynced.add(wa)
	}
	bs.revMu.RUnlock()
	bs.watcherMu.Unlock()

	return wa, func() {
		bs.watcherMu.Lock()
		bs.synced.delete(wa)
		bs.unsynced.delete(wa)
		bs.watcherMu.Unlock()
	}
}

func (bs *badgerStore) rev() int64 {
	return bs.Rev()
}

func (bs *badgerStore) progress(w *watcher) {
	select {
	case w.ch <- WatchResponse{WatchID: w.id, Revision: bs.Rev()}:
	default:
	}
}

func (bs *badgerStore) progressAll(watchers map[WatchID]*watcher) bool {
	rev := bs.Rev()
	for _, w := range watchers {
		select {
		case w.ch <- WatchResponse{WatchID: w.id, Revision: rev}:
		default:
		}
	}
	return true
}

func (bs *badgerStore) syncWatchersLoop() {
	defer bs.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-bs.stopc:
			return
		case <-ticker.C:
			bs.syncWatchers()
		}
	}
}

func (bs *badgerStore) syncWatchers() {
	bs.watcherMu.Lock()
	if bs.unsynced.size() == 0 {
		bs.watcherMu.Unlock()
		return
	}

	bs.revMu.RLock()
	curRev := bs.currentRev
	compactRev := bs.compactMainRev
	bs.revMu.RUnlock()

	wg, minRev := bs.unsynced.choose(512, curRev, compactRev)
	bs.watcherMu.Unlock()

	if minRev > curRev {
		return
	}

	var evs []*mvccpb.Event
	startKey := encodePebbleRevKey(Revision{Main: minRev, Sub: 0}, false)
	endKey := encodePebbleRevKey(Revision{Main: curRev + 1, Sub: 0}, false)

	_ = bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		opts.PrefetchSize = 100
		opts.Prefix = dataPrefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(startKey); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if bytes.Compare(k, endKey) >= 0 {
				break
			}
			r, ok := decodePebbleRevKey(k)
			if !ok {
				continue
			}
			_ = item.Value(func(val []byte) error {
				kv := &mvccpb.KeyValue{}
				if err := proto.Unmarshal(val, kv); err == nil {
					ty := mvccpb.Event_PUT
					if isPebbleTombstone(k) {
						ty = mvccpb.Event_DELETE
						kv.ModRevision = r.Main
					}
					evs = append(evs, &mvccpb.Event{
						Type: ty,
						Kv:   kv,
					})
				}
				return nil
			})
		}
		return nil
	})

	wb := newWatcherBatch(wg, evs)
	bs.watcherMu.Lock()
	for w := range wg.watchers {
		if eb, ok := wb[w]; ok {
			w.ch <- WatchResponse{
				WatchID:  w.id,
				Events:   eb.evs,
				Revision: curRev,
			}
		}
		w.minRev = curRev + 1
		bs.unsynced.delete(w)
		bs.synced.add(w)
	}
	bs.watcherMu.Unlock()
}

func (bs *badgerStore) notify(rev int64, changes []*mvccpb.KeyValue) {
	if len(changes) == 0 {
		return
	}

	evs := make([]*mvccpb.Event, len(changes))
	for i, change := range changes {
		evs[i] = &mvccpb.Event{Kv: change}
		if change.CreateRevision == 0 {
			evs[i].Type = mvccpb.Event_DELETE
			evs[i].Kv.ModRevision = rev
		} else {
			evs[i].Type = mvccpb.Event_PUT
		}
	}

	bs.watcherMu.Lock()
	defer bs.watcherMu.Unlock()

	for _, ev := range evs {
		for w := range bs.synced.watcherSetByKey(string(ev.Kv.Key)) {
			select {
			case w.ch <- WatchResponse{WatchID: w.id, Events: []*mvccpb.Event{ev}, Revision: rev}:
			default:
			}
		}
	}
}
