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

package etcdserver

import (
	"expvar"
	"fmt"
	"log"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/pkg/v3/contention"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	serverstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	// The max throughput of etcd will not exceed 100MB/s (100K * 1KB value).
	// Assuming the RTT is around 10ms, 1MB max size is large enough.
	maxSizePerMsg = 1 * 1024 * 1024
	// Never overflow the rafthttp buffer, which is 4096.
	// TODO: a better const?
	maxInflightMsgs = 4096 / 8
)

var (
	// protects raftStatus
	raftStatusMu sync.Mutex
	// indirection for expvar func interface
	// expvar panics when publishing duplicate name
	// expvar does not support remove a registered name
	// so only register a func that calls raftStatus
	// and change raftStatus as we need.
	raftStatus func() raft.Status
)

func init() {
	expvar.Publish("raft.status", expvar.Func(func() any {
		raftStatusMu.Lock()
		defer raftStatusMu.Unlock()
		if raftStatus == nil {
			return nil
		}
		return raftStatus()
	}))
}

// toApply contains entries, snapshot to be applied. Once
// an toApply is consumed, the entries will be persisted to
// raft storage concurrently; the application must read
// notifyc before assuming the raft messages are stable.
type toApply struct {
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
	// notifyc synchronizes etcd server applies with the raft node
	notifyc chan struct{}
	// raftAdvancedC notifies EtcdServer.apply that
	// 'raftLog.applied' has advanced by r.Advance
	// it should be used only when entries contain raftpb.EntryConfChange
	raftAdvancedC chan struct{}
}

func (ap *toApply) NotifySnapshotPersisted() {
	ap.notifyc <- struct{}{}
}

func (ap *toApply) NotifyRaftLogPersisted() {
	ap.notifyc <- struct{}{}
}

func (ap *toApply) NotifyRaftAdvanced() {
	ap.raftAdvancedC <- struct{}{}
}

func (ap *toApply) WaitForApply(stopped chan struct{}) bool {
	// Candidate or follower needs to wait for all pending configuration
	// changes to be applied before sending messages.
	// Otherwise we might incorrectly count votes (e.g. votes from removed members).
	// Also slow machine's follower raft-layer could proceed to become the leader
	// on its own single-node cluster, before toApply-layer applies the config change.
	// We simply wait for ALL pending entries to be applied for now.
	// We might improve this later on if it causes unnecessary long blocking issues.
	// blocks until 'applyAll' calls 'applyWait.Trigger'
	// to be in sync with scheduled config-change job
	// (assume notifyc has cap of 1)
	select {
	case ap.notifyc <- struct{}{}:
	case <-stopped:
		return true
	}
	return false
}

type raftNode struct {
	lg *zap.Logger

	tickMu *sync.Mutex
	raftNodeConfig

	// a chan to send/receive snapshot
	msgSnapC chan raftpb.Message

	// a chan to send out apply
	applyc chan toApply

	// a chan to send out readState
	readStateC chan raft.ReadState

	// utility
	ticker *time.Ticker
	// contention detectors for raft heartbeat message
	td *contention.TimeoutDetector

	stopped chan struct{}
	done    chan struct{}
}

type raftNodeConfig struct {
	lg *zap.Logger

	// to check if msg receiver is removed from cluster
	isIDRemoved func(id uint64) bool
	raft.Node
	raftStorage *raft.MemoryStorage
	storage     serverstorage.Storage
	heartbeat   time.Duration // for logging
	// transport specifies the transport to send and receive msgs to members.
	// Sending messages MUST NOT block. It is okay to drop messages, since
	// clients should timeout and reissue their messages.
	// If transport is nil, server will panic.
	transport rafthttp.Transporter
}

func newRaftNode(cfg raftNodeConfig) *raftNode {
	var lg raft.Logger
	if cfg.lg != nil {
		lg = NewRaftLoggerZap(cfg.lg)
	} else {
		lcfg := logutil.DefaultZapLoggerConfig
		var err error
		lg, err = NewRaftLogger(&lcfg)
		if err != nil {
			log.Fatalf("cannot create raft logger %v", err)
		}
	}
	raft.SetLogger(lg)
	r := &raftNode{
		lg:             cfg.lg,
		tickMu:         new(sync.Mutex),
		raftNodeConfig: cfg,
		// set up contention detectors for raft heartbeat message.
		// expect to send a heartbeat within 2 heartbeat intervals.
		td:         contention.NewTimeoutDetector(2 * cfg.heartbeat),
		readStateC: make(chan raft.ReadState, 1),
		msgSnapC:   make(chan raftpb.Message, maxInFlightMsgSnap),
		applyc:     make(chan toApply),
		stopped:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	if r.heartbeat == 0 {
		r.ticker = &time.Ticker{}
	} else {
		r.ticker = time.NewTicker(r.heartbeat)
	}
	return r
}

// raft.Node does not have locks in Raft package
func (r *raftNode) tick() {
	r.tickMu.Lock()
	r.Tick()
	r.tickMu.Unlock()
}

// start prepares and starts raftNode in a new goroutine. It is no longer safe
// to modify the fields after it has been started.
func (r *raftNode) start(rh *raftReadyHandler) {
	type hardStateMessage struct {
		message   raftpb.Message
		hardState raftpb.HardState
	}
	toAppend := make(chan hardStateMessage, 1)
	toApply := make(chan raftpb.Message, 1)

	go func() {
		for {
			select {
			case messageHardState := <-toAppend:
				m := messageHardState.message

				// Must save the snapshot file and WAL snapshot entry before saving any other entries or hardstate to
				// ensure that recovery after a snapshot restore is possible.
				if m.Snapshot != nil {
					// gofail: var raftBeforeSaveSnap struct{}
					if err := r.storage.SaveSnap(*m.Snapshot); err != nil {
						r.lg.Fatal("failed to save Raft snapshot", zap.Error(err))
					}
					// gofail: var raftAfterSaveSnap struct{}
				}

				// gofail: var raftBeforeSave struct{}
				if err := r.storage.Save(messageHardState.hardState, m.Entries); err != nil {
					r.lg.Fatal("failed to save Raft hard state and entries", zap.Error(err))
				}
				if !raft.IsEmptyHardState(messageHardState.hardState) {
					proposalsCommitted.Set(float64(messageHardState.hardState.Commit))
				}
				// gofail: var raftAfterSave struct{}

				if m.Snapshot != nil {
					// Force WAL to fsync its hard state before Release() releases
					// old data from the WAL. Otherwise could get an error like:
					// panic: tocommit(107) is out of range [lastIndex(84)]. Was the raft log corrupted, truncated, or lost?
					// See https://github.com/etcd-io/etcd/issues/10219 for more details.
					if err := r.storage.Sync(); err != nil {
						r.lg.Fatal("failed to sync Raft snapshot", zap.Error(err))
					}

					// etcdserver now claim the snapshot has been persisted onto the disk
					// gofail: var raftBeforeApplySnap struct{}
					r.raftStorage.ApplySnapshot(*m.Snapshot)
					// gofail: var raftAfterApplySnap struct{}

					if err := r.storage.Release(*m.Snapshot); err != nil {
						r.lg.Fatal("failed to release Raft wal", zap.Error(err))
					}
					// gofail: var raftAfterWALRelease struct{}
				}
				r.raftStorage.Append(m.Entries)
				r.transport.Send(m.Responses)
			case <-r.stopped:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case m := <-toApply:
				ap, stopped := r.handleApply(rh, m.Entries, m.Snapshot)
				if stopped {
					return
				}
				ap.NotifyRaftLogPersisted()
				confChanged := includesConfigChange(m.Entries)
				if confChanged {
					if ap.WaitForApply(r.stopped) {
						return
					}
				}
				// gofail: var raftBeforeFollowerSend struct{}
				r.transport.Send(m.Responses)
				// gofail: var raftBeforeAdvance struct{}
				if confChanged {
					ap.NotifyRaftAdvanced()
				}
			case <-r.stopped:
				return
			}
		}
	}()

	go func() {
		defer r.onStop()

		for {
			select {
			case <-r.ticker.C:
				r.tick()
			case rd := <-r.Ready():
				if rd.SoftState != nil {
					r.handleSoftState(rh, rd.SoftState, rd.RaftState)
				}
				if len(rd.ReadStates) != 0 {
					if r.handleReadyStates(rd.ReadStates) {
						return
					}
				}
				for _, m := range rd.Messages {
					switch m.To {
					case raft.LocalApplyThread:
						toApply <- m
					case raft.LocalAppendThread:
						toAppend <- hardStateMessage{
							hardState: rd.HardState,
							message:   m,
						}
					default:
						r.transport.Send([]raftpb.Message{m})
					}
				}
			case <-r.stopped:
				return
			}
		}
	}()
}

func (r *raftNode) handleSoftState(rh *raftReadyHandler, ss *raft.SoftState, state raft.StateType) bool {
	newLeader := ss.Lead != raft.None && rh.getLead() != ss.Lead
	if newLeader {
		leaderChanges.Inc()
	}

	if ss.Lead == raft.None {
		hasLeader.Set(0)
	} else {
		hasLeader.Set(1)
	}

	rh.updateLead(ss.Lead)
	rh.updateLeadership(newLeader)
	r.td.Reset()
	islead := state == raft.StateLeader
	if islead {
		isLeader.Set(1)
	} else {
		isLeader.Set(0)
	}
	return islead
}

func (r *raftNode) handleReadyStates(rs []raft.ReadState) bool {
	internalTimeout := time.Second
	select {
	case r.readStateC <- rs[len(rs)-1]:
	case <-time.After(internalTimeout):
		r.lg.Warn("timed out sending read state", zap.Duration("timeout", internalTimeout))
	case <-r.stopped:
		return true
	}
	return false
}

func (r *raftNode) handleApply(rh *raftReadyHandler, committedEntries []raftpb.Entry, snapshot *raftpb.Snapshot) (*toApply, bool) {
	notifyc := make(chan struct{}, 1)
	raftAdvancedC := make(chan struct{}, 1)
	ap := toApply{
		entries:       committedEntries,
		notifyc:       notifyc,
		raftAdvancedC: raftAdvancedC,
	}
	if snapshot != nil {
		ap.snapshot = *snapshot
	}

	var ci uint64
	if len(ap.entries) != 0 {
		ci = ap.entries[len(ap.entries)-1].Index
	}
	if ap.snapshot.Metadata.Index > ci {
		ci = ap.snapshot.Metadata.Index
	}
	if ci != 0 {
		rh.updateCommittedIndex(ci)
	}
	select {
	case r.applyc <- ap:
	case <-r.stopped:
		return nil, true
	}
	return &ap, false
}

func (r *raftNode) handleHardStateAndSnapshot(snapshot *raftpb.Snapshot, hardState raftpb.HardState, entries []raftpb.Entry) {
}

func (r *raftNode) handleSnapshot(snapshot raftpb.Snapshot) {
}

func includesConfigChange(entries []raftpb.Entry) bool {
	for _, ent := range entries {
		if ent.Type == raftpb.EntryConfChange {
			return true
		}
	}
	return false
}

func (r *raftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	sentAppResp := false
	for i := len(ms) - 1; i >= 0; i-- {
		if r.isIDRemoved(ms[i].To) {
			ms[i].To = 0
			continue
		}

		if ms[i].Type == raftpb.MsgAppResp {
			if sentAppResp {
				ms[i].To = 0
			} else {
				sentAppResp = true
			}
		}

		if ms[i].Type == raftpb.MsgSnap {
			// There are two separate data store: the store for v2, and the KV for v3.
			// The msgSnap only contains the most recent snapshot of store without KV.
			// So we need to redirect the msgSnap to etcd server main loop for merging in the
			// current store snapshot and KV snapshot.
			select {
			case r.msgSnapC <- ms[i]:
			default:
				// drop msgSnap if the inflight chan if full.
			}
			ms[i].To = 0
		}
		if ms[i].Type == raftpb.MsgHeartbeat {
			ok, exceed := r.td.Observe(ms[i].To)
			if !ok {
				// TODO: limit request rate.
				r.lg.Warn(
					"leader failed to send out heartbeat on time; took too long, leader is overloaded likely from slow disk",
					zap.String("to", fmt.Sprintf("%x", ms[i].To)),
					zap.Duration("heartbeat-interval", r.heartbeat),
					zap.Duration("expected-duration", 2*r.heartbeat),
					zap.Duration("exceeded-duration", exceed),
				)
				heartbeatSendFailures.Inc()
			}
		}
	}
	return ms
}

func (r *raftNode) apply() chan toApply {
	return r.applyc
}

func (r *raftNode) stop() {
	close(r.stopped)
	// Block until the stop has been acknowledged by start()
	<-r.done
}

func (r *raftNode) onStop() {
	r.Stop()
	r.ticker.Stop()
	r.transport.Stop()
	if err := r.storage.Close(); err != nil {
		r.lg.Panic("failed to close Raft storage", zap.Error(err))
	}
	close(r.done)
}

// for testing
func (r *raftNode) pauseSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Pause()
}

func (r *raftNode) resumeSending() {
	p := r.transport.(rafthttp.Pausable)
	p.Resume()
}

// advanceTicks advances ticks of Raft node.
// This can be used for fast-forwarding election
// ticks in multi data-center deployments, thus
// speeding up election process.
func (r *raftNode) advanceTicks(ticks int) {
	for i := 0; i < ticks; i++ {
		r.tick()
	}
}
