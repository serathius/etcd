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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/pkg/v3/schedule"
	"go.etcd.io/etcd/pkg/v3/traceutil"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
)

var (
	dataPrefix = []byte("/d/")
)

func encodePebbleRevKey(r Revision, tombstone bool) []byte {
	size := len(dataPrefix) + 17
	if tombstone {
		size++
	}
	buf := make([]byte, size)
	copy(buf, dataPrefix)
	binary.BigEndian.PutUint64(buf[len(dataPrefix):], uint64(r.Main))
	buf[len(dataPrefix)+8] = '_'
	binary.BigEndian.PutUint64(buf[len(dataPrefix)+9:], uint64(r.Sub))
	if tombstone {
		buf[len(dataPrefix)+17] = 't'
	}
	return buf
}

func isPebbleTombstone(b []byte) bool {
	return len(b) == len(dataPrefix)+18 && b[len(dataPrefix)+17] == 't'
}

func decodePebbleRevKey(b []byte) (Revision, bool) {
	if len(b) < len(dataPrefix)+17 || !bytes.HasPrefix(b, dataPrefix) || b[len(dataPrefix)+8] != '_' {
		return Revision{}, false
	}
	main := int64(binary.BigEndian.Uint64(b[len(dataPrefix):]))
	sub := int64(binary.BigEndian.Uint64(b[len(dataPrefix)+9:]))
	return Revision{Main: main, Sub: sub}, true
}

type pebbleStore struct {
	ReadView
	WriteView

	db        *pebble.DB
	cache     *pebble.Cache
	index     index
	lg        *zap.Logger
	le        lease.Lessor
	cfg       StoreConfig
	fifoSched schedule.Scheduler

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

func NewPebbleStore(lg *zap.Logger, dir string, le lease.Lessor, cfg StoreConfig) (WatchableKV, error) {
	if lg == nil {
		lg = zap.NewNop()
	}

	cache := pebble.NewCache(128 * 1024 * 1024)
	opts := &pebble.Options{
		Cache:                       cache,
		Logger:                      zapPebbleLogger{lg: lg},
		MemTableSize:                64 * 1024 * 1024,
		MemTableStopWritesThreshold: 8,
		L0CompactionFileThreshold:   8,
		MaxConcurrentCompactions:    func() int { return 4 },
		DisableWAL:                  true, // Etcd has its own WAL for durability.
	}
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := range opts.Levels {
		opts.Levels[i].BlockSize = 32 * 1024
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
		opts.Levels[i].FilterType = pebble.TableFilter
		opts.Levels[i].EnsureDefaults()
	}
	opts.EnsureDefaults()

	db, err := pebble.Open(dir, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}

	ps := &pebbleStore{
		db:             db,
		cache:          cache,
		index:          newTreeIndex(lg),
		lg:             lg,
		le:             le,
		cfg:            cfg,
		currentRev:     1,
		compactMainRev: 0,
		unsynced:       newWatcherGroup(),
		synced:         newWatcherGroup(),
		stopc:          make(chan struct{}),
		fifoSched:      schedule.NewFIFOScheduler(lg),
	}
	ps.ReadView = &pebbleReadView{ps}
	ps.WriteView = &pebbleWriteView{ps}

	ps.wg.Add(1)
	go ps.syncWatchersLoop()

	return ps, nil
}

type zapPebbleLogger struct {
	lg *zap.Logger
}

func (l zapPebbleLogger) Infof(format string, args ...any) {
}

func (l zapPebbleLogger) Errorf(format string, args ...any) {
	l.lg.Error(fmt.Sprintf(format, args...))
}

func (l zapPebbleLogger) Fatalf(format string, args ...any) {
	l.lg.Fatal(fmt.Sprintf(format, args...))
}

func (ps *pebbleStore) FirstRev() int64 {
	ps.revMu.RLock()
	defer ps.revMu.RUnlock()
	return ps.compactMainRev
}

func (ps *pebbleStore) Rev() int64 {
	ps.revMu.RLock()
	defer ps.revMu.RUnlock()
	return ps.currentRev
}

func (ps *pebbleStore) Read(mode ReadTxMode, trace *traceutil.Trace) TxnRead {
	ps.revMu.RLock()
	curRev := ps.currentRev
	firstRev := ps.compactMainRev
	ps.revMu.RUnlock()

	var snap *pebble.Snapshot
	if mode == ConcurrentReadTxMode {
		snap = ps.db.NewSnapshot()
	}

	return &pebbleTxnRead{
		ps:       ps,
		snap:     snap,
		rev:      curRev,
		firstRev: firstRev,
		trace:    trace,
	}
}

func (ps *pebbleStore) Write(trace *traceutil.Trace) TxnWrite {
	ps.mu.Lock()
	ps.revMu.RLock()
	curRev := ps.currentRev
	firstRev := ps.compactMainRev
	ps.revMu.RUnlock()

	batch := ps.db.NewIndexedBatch()

	return &pebbleTxnWrite{
		pebbleTxnRead: pebbleTxnRead{
			ps:       ps,
			batch:    batch,
			rev:      curRev,
			firstRev: firstRev,
			trace:    trace,
		},
		batch:    batch,
		beginRev: curRev,
		changes:  make([]*mvccpb.KeyValue, 0, 4),
	}
}

func (ps *pebbleStore) Compact(trace *traceutil.Trace, rev int64) (<-chan struct{}, error) {
	ps.compactMu.Lock()
	defer ps.compactMu.Unlock()

	ps.revMu.Lock()
	if rev <= ps.compactMainRev && rev > 0 {
		ps.revMu.Unlock()
		return nil, ErrCompacted
	}
	if rev > ps.currentRev {
		ps.revMu.Unlock()
		return nil, ErrFutureRev
	}
	prevCompactRev := ps.compactMainRev
	ps.compactMainRev = rev
	ps.revMu.Unlock()

	keep := ps.index.Compact(rev)

	donec := make(chan struct{})
	j := schedule.NewJob("pebble_compact", func(ctx context.Context) {
		defer close(donec)
		if ctx.Err() != nil {
			return
		}

		startKey := encodePebbleRevKey(Revision{Main: prevCompactRev, Sub: 0}, false)
		endKey := encodePebbleRevKey(Revision{Main: rev + 1, Sub: 0}, false)

		iter, err := ps.db.NewIter(&pebble.IterOptions{
			LowerBound: startKey,
			UpperBound: endKey,
		})
		if err != nil {
			return
		}

		batch := ps.db.NewBatch()
		defer batch.Close()

		count := 0
		for valid := iter.First(); valid; valid = iter.Next() {
			select {
			case <-ctx.Done():
				_ = iter.Close()
				return
			case <-ps.stopc:
				_ = iter.Close()
				return
			default:
			}

			k := iter.Key()
			r, ok := decodePebbleRevKey(k)
			if !ok || r.Main > rev {
				break
			}
			if _, ok := keep[r]; !ok {
				_ = batch.Delete(k, pebble.NoSync)
				count++
				if count%1000 == 0 {
					_ = ps.db.Apply(batch, pebble.NoSync)
					batch.Reset()
					if ps.cfg.CompactionSleepInterval > 0 {
						time.Sleep(ps.cfg.CompactionSleepInterval)
					}
				}
			}
		}
		_ = iter.Close()
		if count%1000 != 0 {
			_ = ps.db.Apply(batch, pebble.NoSync)
		}
	})

	ps.fifoSched.Schedule(j)
	return donec, nil
}

func (ps *pebbleStore) HashStorage() HashStorage {
	return nil
}

func (ps *pebbleStore) Commit() {
	_ = ps.db.Flush()
}

func (ps *pebbleStore) Restore(b backend.Backend) error {
	return errors.New("not implemented")
}

func (ps *pebbleStore) Close() error {
	close(ps.stopc)
	ps.fifoSched.Stop()
	ps.wg.Wait()
	err := ps.db.Close()
	if ps.cache != nil {
		ps.cache.Unref()
	}
	return err
}

func (ps *pebbleStore) NewWatchStream() WatchStream {
	return &watchStream{
		watchable: ps,
		ch:        make(chan WatchResponse, chanBufLen),
		cancels:   make(map[WatchID]cancelFunc),
		watchers:  make(map[WatchID]*watcher),
	}
}

type pebbleReadView struct {
	ps *pebbleStore
}

func (rv *pebbleReadView) FirstRev() int64 { return rv.ps.FirstRev() }
func (rv *pebbleReadView) Rev() int64      { return rv.ps.Rev() }

func (rv *pebbleReadView) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	tr := rv.ps.Read(ConcurrentReadTxMode, traceutil.TODO())
	defer tr.End()
	return tr.Range(ctx, key, end, ro)
}

type pebbleWriteView struct {
	ps *pebbleStore
}

func (wv *pebbleWriteView) DeleteRange(key, end []byte) (int64, int64) {
	tw := wv.ps.Write(traceutil.TODO())
	defer tw.End()
	return tw.DeleteRange(key, end)
}

func (wv *pebbleWriteView) Put(key, value []byte, lease lease.LeaseID) int64 {
	tw := wv.ps.Write(traceutil.TODO())
	defer tw.End()
	return tw.Put(key, value, lease)
}

type pebbleTxnRead struct {
	ps       *pebbleStore
	snap     *pebble.Snapshot
	batch    *pebble.Batch
	rev      int64
	firstRev int64
	trace    *traceutil.Trace
}

func (tr *pebbleTxnRead) FirstRev() int64 { return tr.firstRev }
func (tr *pebbleTxnRead) Rev() int64      { return tr.rev }

func (tr *pebbleTxnRead) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	targetRev := ro.Rev
	if targetRev > tr.rev {
		return &RangeResult{Count: -1, Rev: 0}, ErrFutureRev
	}
	if targetRev <= 0 {
		targetRev = tr.rev
	}
	if targetRev < tr.ps.FirstRev() {
		return &RangeResult{Count: -1, Rev: 0}, ErrCompacted
	}

	limit := int(ro.Limit)
	if ro.CountOnly {
		limit = 0
	}

	_, revs, _, _, totalCount := tr.ps.index.Range(key, end, targetRev, limit, true)
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
	var reader pebble.Reader
	if tr.batch != nil {
		reader = tr.batch
	} else if tr.snap != nil {
		reader = tr.snap
	} else {
		reader = tr.ps.db
	}

	for i, r := range revs {
		k := encodePebbleRevKey(r, false)
		val, closer, err := reader.Get(k)
		if err == nil {
			valCopy := append([]byte(nil), val...)
			_ = closer.Close()
			kv := &mvccpb.KeyValue{}
			if err := proto.Unmarshal(valCopy, kv); err == nil {
				kvs[i] = kv
			}
		}
	}

	if targetRev < tr.ps.FirstRev() {
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

func (tr *pebbleTxnRead) End() {
	if tr.snap != nil {
		_ = tr.snap.Close()
	}
}

type pebbleTxnWrite struct {
	pebbleTxnRead
	batch    *pebble.Batch
	beginRev int64
	changes  []*mvccpb.KeyValue
}

func (tw *pebbleTxnWrite) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	rev := tw.beginRev
	if len(tw.changes) > 0 {
		rev++
	}
	tr := &pebbleTxnRead{
		ps:       tw.ps,
		batch:    tw.batch,
		rev:      rev,
		firstRev: tw.firstRev,
		trace:    tw.trace,
	}
	return tr.Range(ctx, key, end, ro)
}

func (tw *pebbleTxnWrite) Put(key, value []byte, leaseID lease.LeaseID) int64 {
	rev := tw.beginRev + 1
	c := rev
	ver := int64(1)

	if _, created, prevVer, err := tw.ps.index.Get(key, rev); err == nil {
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
	_ = tw.batch.Set(dKey, d, pebble.NoSync)

	tw.ps.index.Put(key, idxRev)
	tw.changes = append(tw.changes, kv)
	return rev
}

func (tw *pebbleTxnWrite) DeleteRange(key, end []byte) (int64, int64) {
	rrev := tw.beginRev
	if len(tw.changes) > 0 {
		rrev++
	}
	keys, _, _, _, _ := tw.ps.index.Range(key, end, rrev, 0, false)
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
		_ = tw.batch.Set(tombstoneKey, d, pebble.NoSync)

		_ = tw.ps.index.Tombstone(existingKey, idxRev)
		tw.changes = append(tw.changes, delKV)
	}

	return int64(len(keys)), rev
}

func (tw *pebbleTxnWrite) Changes() []*mvccpb.KeyValue {
	return tw.changes
}

func (tw *pebbleTxnWrite) End() {
	if len(tw.changes) > 0 {
		_ = tw.ps.db.Apply(tw.batch, pebble.NoSync)

		tw.ps.revMu.Lock()
		tw.ps.currentRev = tw.beginRev + 1
		tw.ps.revMu.Unlock()

		tw.ps.notify(tw.beginRev+1, tw.changes)
	}
	_ = tw.batch.Close()
	tw.ps.mu.Unlock()
}

func (ps *pebbleStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, cancelFunc) {
	wa := &watcher{
		key:      key,
		end:      end,
		startRev: startRev,
		minRev:   startRev,
		id:       id,
		ch:       ch,
		fcs:      fcs,
	}

	ps.watcherMu.Lock()
	ps.revMu.RLock()
	synced := startRev > ps.currentRev || startRev == 0
	if synced {
		wa.minRev = ps.currentRev + 1
		if startRev > wa.minRev {
			wa.minRev = startRev
		}
		ps.synced.add(wa)
	} else {
		ps.unsynced.add(wa)
	}
	ps.revMu.RUnlock()
	ps.watcherMu.Unlock()

	return wa, func() {
		ps.watcherMu.Lock()
		ps.synced.delete(wa)
		ps.unsynced.delete(wa)
		ps.watcherMu.Unlock()
	}
}

func (ps *pebbleStore) rev() int64 {
	return ps.Rev()
}

func (ps *pebbleStore) progress(w *watcher) {
	select {
	case w.ch <- WatchResponse{WatchID: w.id, Revision: ps.Rev()}:
	default:
	}
}

func (ps *pebbleStore) progressAll(watchers map[WatchID]*watcher) bool {
	rev := ps.Rev()
	for _, w := range watchers {
		select {
		case w.ch <- WatchResponse{WatchID: w.id, Revision: rev}:
		default:
		}
	}
	return true
}

func (ps *pebbleStore) syncWatchersLoop() {
	defer ps.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ps.stopc:
			return
		case <-ticker.C:
			ps.syncWatchers()
		}
	}
}

func (ps *pebbleStore) syncWatchers() {
	ps.watcherMu.Lock()
	if ps.unsynced.size() == 0 {
		ps.watcherMu.Unlock()
		return
	}

	ps.revMu.RLock()
	curRev := ps.currentRev
	compactRev := ps.compactMainRev
	ps.revMu.RUnlock()

	wg, minRev := ps.unsynced.choose(512, curRev, compactRev)
	ps.watcherMu.Unlock()

	if minRev > curRev {
		return
	}

	var evs []*mvccpb.Event
	startKey := encodePebbleRevKey(Revision{Main: minRev, Sub: 0}, false)
	endKey := encodePebbleRevKey(Revision{Main: curRev + 1, Sub: 0}, false)

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	if err == nil {
		for valid := iter.First(); valid; valid = iter.Next() {
			val := iter.Value()
			k := iter.Key()
			r, ok := decodePebbleRevKey(k)
			if !ok {
				continue
			}
			kv := &mvccpb.KeyValue{}
			_ = proto.Unmarshal(val, kv)
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
		_ = iter.Close()
	}

	wb := newWatcherBatch(wg, evs)
	ps.watcherMu.Lock()
	for w := range wg.watchers {
		if eb, ok := wb[w]; ok {
			w.ch <- WatchResponse{
				WatchID:  w.id,
				Events:   eb.evs,
				Revision: curRev,
			}
		}
		w.minRev = curRev + 1
		ps.unsynced.delete(w)
		ps.synced.add(w)
	}
	ps.watcherMu.Unlock()
}

func (ps *pebbleStore) notify(rev int64, changes []*mvccpb.KeyValue) {
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

	ps.watcherMu.Lock()
	defer ps.watcherMu.Unlock()

	for _, ev := range evs {
		for w := range ps.synced.watcherSetByKey(string(ev.Kv.Key)) {
			select {
			case w.ch <- WatchResponse{WatchID: w.id, Events: []*mvccpb.Event{ev}, Revision: rev}:
			default:
			}
		}
	}
}
