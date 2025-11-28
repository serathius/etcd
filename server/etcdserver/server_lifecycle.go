package etcdserver

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/membershippb"
	"go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/pkg/v3/notify"
	"go.etcd.io/etcd/pkg/v3/schedule"
	"go.etcd.io/etcd/pkg/v3/wait"
	"go.etcd.io/etcd/server/v3/etcdserver/api/membership"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/features"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/storage/backend"
	"go.etcd.io/etcd/server/v3/storage/schema"
	"go.uber.org/zap"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	serverversion "go.etcd.io/etcd/server/v3/etcdserver/version"
)

// Functions moved from server.go

func (s *EtcdServer) Start() {
	s.start()
	s.GoAttach(func() { s.adjustTicks() })
	s.GoAttach(func() { s.publishV3(s.Cfg.ReqTimeout()) })
	s.GoAttach(s.purgeFile)
	s.GoAttach(func() { monitorFileDescriptor(s.Logger(), s.stopping) })
	s.GoAttach(s.monitorClusterVersions)
	s.GoAttach(s.monitorStorageVersion)
	s.GoAttach(s.linearizableReadLoop)
	s.GoAttach(s.monitorKVHash)
	s.GoAttach(s.monitorCompactHash)
	s.GoAttach(s.monitorDowngrade)
}

func (s *EtcdServer) start() {
	lg := s.Logger()

	if s.Cfg.SnapshotCount == 0 {
		lg.Info(
			"updating snapshot-count to default",
			zap.Uint64("given-snapshot-count", s.Cfg.SnapshotCount),
			zap.Uint64("updated-snapshot-count", DefaultSnapshotCount),
		)
		s.Cfg.SnapshotCount = DefaultSnapshotCount
	}
	if s.Cfg.SnapshotCatchUpEntries == 0 {
		lg.Info(
			"updating snapshot catch-up entries to default",
			zap.Uint64("given-snapshot-catchup-entries", s.Cfg.SnapshotCatchUpEntries),
			zap.Uint64("updated-snapshot-catchup-entries", DefaultSnapshotCatchUpEntries),
		)
		s.Cfg.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries
	}

	s.w = wait.New()
	s.applyWait = wait.NewTimeList()
	s.done = make(chan struct{})
	s.stop = make(chan struct{})
	s.stopping = make(chan struct{}, 1)
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.readwaitc = make(chan struct{}, 1)
	s.readNotifier = newNotifier()
	s.leaderChanged = notify.NewNotifier()
	if s.ClusterVersion() != nil {
		lg.Info(
			"starting etcd server",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("local-server-version", version.Version),
			zap.String("cluster-id", s.Cluster().ID().String()),
			zap.String("cluster-version", version.Cluster(s.ClusterVersion().String())),
		)
		membership.ClusterVersionMetrics.With(prometheus.Labels{"cluster_version": version.Cluster(s.ClusterVersion().String())}).Set(1)
	} else {
		lg.Info(
			"starting etcd server",
			zap.String("local-member-id", s.MemberID().String()),
			zap.String("local-server-version", version.Version),
			zap.String("cluster-version", "to_be_decided"),
		)
	}

	// TODO: if this is an empty log, writes all peer infos
	// into the first entry
	go s.run()
}

func (s *EtcdServer) run() {
	lg := s.Logger()

	sn, err := s.r.raftStorage.Snapshot()
	if err != nil {
		lg.Panic("failed to get snapshot from Raft storage", zap.Error(err))
	}

	// asynchronously accept toApply packets, dispatch progress in-order
	sched := schedule.NewFIFOScheduler(lg)

	rh := &raftReadyHandler{
		getLead:    func() (lead uint64) { return s.getLead() },
		updateLead: func(lead uint64) { s.setLead(lead) },
		updateLeadership: func(newLeader bool) {
			if !s.isLeader() {
				if s.lessor != nil {
					s.lessor.Demote()
				}
				if s.compactor != nil {
					s.compactor.Pause()
				}
			} else {
				if newLeader {
					t := time.Now()
					s.leadTimeMu.Lock()
					s.leadElectedTime = t
					s.leadTimeMu.Unlock()
				}
				if s.compactor != nil {
					s.compactor.Resume()
				}
			}
			if newLeader {
				s.leaderChanged.Notify()
			}
			// TODO: remove the nil checking
			// current test utility does not provide the stats
			if s.stats != nil {
				s.stats.BecomeLeader()
			}
		},
		updateCommittedIndex: func(ci uint64) {
			cci := s.getCommittedIndex()
			if ci > cci {
				s.setCommittedIndex(ci)
			}
		},
	}
	s.r.start(rh)

	ep := etcdProgress{
		confState:           sn.Metadata.ConfState,
		diskSnapshotIndex:   sn.Metadata.Index,
		memorySnapshotIndex: sn.Metadata.Index,
		appliedt:            sn.Metadata.Term,
		appliedi:            sn.Metadata.Index,
	}

	defer func() {
		s.wgMu.Lock() // block concurrent waitgroup adds in GoAttach while stopping
		close(s.stopping)
		s.wgMu.Unlock()
		s.cancel()
		sched.Stop()

		// wait for goroutines before closing raft so wal stays open
		s.wg.Wait()

		// must stop raft after scheduler-- etcdserver can leak rafthttp pipelines
		// by adding a peer after raft stops the transport
		s.r.stop()

		s.Cleanup()

		close(s.done)
	}()

	var expiredLeaseC <-chan []*lease.Lease
	if s.lessor != nil {
		expiredLeaseC = s.lessor.ExpiredLeasesC()
	}

	for {
		select {
		case ap := <-s.r.apply():
			f := schedule.NewJob("server_applyAll", func(context.Context) { s.applyAll(&ep, &ap) })
			sched.Schedule(f)
		case leases := <-expiredLeaseC:
			s.revokeExpiredLeases(leases)
		case err := <-s.errorc:
			lg.Warn("server error", zap.Error(err))
			lg.Warn("data-dir used by this member must be removed")
			return
		case <-s.stop:
			return
		}
	}
}

func (s *EtcdServer) Stop() {
	lg := s.Logger()
	if err := s.TryTransferLeadershipOnShutdown(); err != nil {
		lg.Warn("leadership transfer failed", zap.String("local-member-id", s.MemberID().String()), zap.Error(err))
	}
	s.HardStop()
}

func (s *EtcdServer) HardStop() {
	select {
	case s.stop <- struct{}{}:
	case <-s.done:
		return
	}
	<-s.done
}

func (s *EtcdServer) Cleanup() {
	// kv, lessor and backend can be nil if running without v3 enabled
	// or running unit tests.
	if s.lessor != nil {
		s.lessor.Stop()
	}
	if s.kv != nil {
		s.kv.Close()
	}
	if s.authStore != nil {
		s.authStore.Close()
	}
	if s.be != nil {
		s.be.Close()
	}
	if s.compactor != nil {
		s.compactor.Stop()
	}
}

func (s *EtcdServer) ReadyNotify() <-chan struct{} { return s.readych }

func (s *EtcdServer) StopNotify() <-chan struct{} { return s.done }

func (s *EtcdServer) StoppingNotify() <-chan struct{} { return s.stopping }

func (s *EtcdServer) stopWithDelay(d time.Duration, err error) {
	select {
	case <-time.After(d):
	case <-s.done:
	}
	select {
	case s.errorc <- err:
	default:
	}
}

func (s *EtcdServer) monitorClusterVersions() {
	lg := s.Logger()
	monitor := serverversion.NewMonitor(lg, NewServerVersionAdapter(s))
	for {
		select {
		case <-s.firstCommitInTerm.Receive():
		case <-time.After(monitorVersionInterval):
		case <-s.stopping:
			lg.Info("server has stopped; stopping cluster version's monitor")
			return
		}

		if s.Leader() != s.MemberID() {
			continue
		}
		err := monitor.UpdateClusterVersionIfNeeded()
		if err != nil {
			s.lg.Error("Failed to monitor cluster version", zap.Error(err))
		}
	}
}

func (s *EtcdServer) monitorStorageVersion() {
	lg := s.Logger()
	monitor := serverversion.NewMonitor(lg, NewServerVersionAdapter(s))
	for {
		select {
		case <-time.After(monitorVersionInterval):
		case <-s.clusterVersionChanged.Receive():
		case <-s.stopping:
			lg.Info("server has stopped; stopping storage version's monitor")
			return
		}
		monitor.UpdateStorageVersionIfNeeded()
	}
}

func (s *EtcdServer) monitorKVHash() {
	t := s.Cfg.CorruptCheckTime
	if t == 0 {
		return
	}
	checkTicker := time.NewTicker(t)
	defer checkTicker.Stop()

	lg := s.Logger()
	lg.Info(
		"enabled corruption checking",
		zap.String("local-member-id", s.MemberID().String()),
		zap.Duration("interval", t),
	)
	for {
		select {
		case <-s.stopping:
			lg.Info("server has stopped; stopping kv hash's monitor")
			return
		case <-checkTicker.C:
		}
		backend.VerifyBackendConsistency(s.be, lg, false, schema.AllBuckets...)
		if !s.isLeader() {
			continue
		}
		if err := s.corruptionChecker.PeriodicCheck(); err != nil {
			lg.Warn("failed to check hash KV", zap.Error(err))
		}
	}
}

func (s *EtcdServer) monitorCompactHash() {
	if !s.FeatureEnabled(features.CompactHashCheck) {
		return
	}
	t := s.Cfg.CompactHashCheckTime
	for {
		select {
		case <-time.After(t):
		case <-s.stopping:
			lg := s.Logger()
			lg.Info("server has stopped; stopping compact hash's monitor")
			return
		}
		if !s.isLeader() {
			continue
		}
		s.corruptionChecker.CompactHashCheck()
	}
}

func (s *EtcdServer) monitorDowngrade() {
	monitor := serverversion.NewMonitor(s.Logger(), NewServerVersionAdapter(s))
	t := s.Cfg.DowngradeCheckTime
	if t == 0 {
		return
	}
	for {
		select {
		case <-time.After(t):
		case <-s.stopping:
			return
		}

		if !s.isLeader() {
			continue
		}
		monitor.CancelDowngradeIfNeeded()
	}
}

func (s *EtcdServer) adjustTicks() {
	lg := s.Logger()
	clusterN := len(s.cluster.Members())

	// single-node fresh start, or single-node recovers from snapshot
	if clusterN == 1 {
		ticks := s.Cfg.ElectionTicks - 1
		lg.Info(
			"started as single-node; fast-forwarding election ticks",
			zap.String("local-member-id", s.MemberID().String()),
			zap.Int("forward-ticks", ticks),
			zap.String("forward-duration", tickToDur(ticks, s.Cfg.TickMs)),
			zap.Int("election-ticks", s.Cfg.ElectionTicks),
			zap.String("election-timeout", tickToDur(s.Cfg.ElectionTicks, s.Cfg.TickMs)),
		)
		s.r.advanceTicks(ticks)
		return
	}

	if !s.Cfg.InitialElectionTickAdvance {
		lg.Info("skipping initial election tick advance", zap.Int("election-ticks", s.Cfg.ElectionTicks))
		return
	}
	lg.Info("starting initial election tick advance", zap.Int("election-ticks", s.Cfg.ElectionTicks))

	// retry up to "rafthttp.ConnReadTimeout", which is 5-sec
	// until peer connection reports; otherwise:
	// 1. all connections failed, or
	// 2. no active peers, or
	// 3. restarted single-node with no snapshot
	// then, do nothing, because advancing ticks would have no effect
	waitTime := rafthttp.ConnReadTimeout
	itv := 50 * time.Millisecond
	for i := int64(0); i < int64(waitTime/itv); i++ {
		select {
		case <-time.After(itv):
		case <-s.stopping:
			return
		}

		peerN := s.r.transport.ActivePeers()
		if peerN > 1 {
			// multi-node received peer connection reports
			// adjust ticks, in case slow leader message receive
			ticks := s.Cfg.ElectionTicks - 2

			lg.Info(
				"initialized peer connections; fast-forwarding election ticks",
				zap.String("local-member-id", s.MemberID().String()),
				zap.Int("forward-ticks", ticks),
				zap.String("forward-duration", tickToDur(ticks, s.Cfg.TickMs)),
				zap.Int("election-ticks", s.Cfg.ElectionTicks),
				zap.String("election-timeout", tickToDur(s.Cfg.ElectionTicks, s.Cfg.TickMs)),
				zap.Int("active-remote-members", peerN),
			)

			s.r.advanceTicks(ticks)
			return
		}
	}
}

func tickToDur(ticks int, tickMs uint) string {
	return fmt.Sprintf("%v", time.Duration(ticks)*time.Duration(tickMs)*time.Millisecond)
}

func (s *EtcdServer) purgeFile() {
	lg := s.Logger()
	var dberrc, serrc, werrc <-chan error
	var dbdonec, sdonec, wdonec <-chan struct{}
	if s.Cfg.MaxSnapFiles > 0 {
		dbdonec, dberrc = fileutil.PurgeFileWithoutFlock(lg, s.Cfg.SnapDir(), "snap.db", s.Cfg.MaxSnapFiles, purgeFileInterval, s.stopping)
		sdonec, serrc = fileutil.PurgeFileWithoutFlock(lg, s.Cfg.SnapDir(), "snap", s.Cfg.MaxSnapFiles, purgeFileInterval, s.stopping)
	}
	if s.Cfg.MaxWALFiles > 0 {
		wdonec, werrc = fileutil.PurgeFileWithDoneNotify(lg, s.Cfg.WALDir(), "wal", s.Cfg.MaxWALFiles, purgeFileInterval, s.stopping)
	}

	select {
	case e := <-dberrc:
		lg.Fatal("failed to purge snap db file", zap.Error(e))
	case e := <-serrc:
		lg.Fatal("failed to purge snap file", zap.Error(e))
	case e := <-werrc:
		lg.Fatal("failed to purge wal file", zap.Error(e))
	case <-s.stopping:
		if dbdonec != nil {
			<-dbdonec
		}
		if sdonec != nil {
			<-sdonec
		}
		if wdonec != nil {
			<-wdonec
		}
		return
	}
}

func (s *EtcdServer) publishV3(timeout time.Duration) {
	req := &membershippb.ClusterMemberAttrSetRequest{
		Member_ID: uint64(s.MemberID()),
		MemberAttributes: &membershippb.Attributes{
			Name:       s.attributes.Name,
			ClientUrls: s.attributes.ClientURLs,
		},
	}
	// gofail: var beforePublishing struct{}
	lg := s.Logger()
	for {
		select {
		case <-s.stopping:
			lg.Warn(
				"stopped publish because server is stopping",
				zap.String("local-member-id", s.MemberID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.Duration("publish-timeout", timeout),
			)
			return

		default:
		}

		ctx, cancel := context.WithTimeout(s.ctx, timeout)
		_, err := s.raftRequest(ctx, pb.InternalRaftRequest{ClusterMemberAttrSet: req})
		cancel()
		switch err {
		case nil:
			close(s.readych)
			lg.Info(
				"published local member to cluster through raft",
				zap.String("local-member-id", s.MemberID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.String("cluster-id", s.cluster.ID().String()),
				zap.Duration("publish-timeout", timeout),
			)
			return

		default:
			lg.Warn(
				"failed to publish local member to cluster through raft",
				zap.String("local-member-id", s.MemberID().String()),
				zap.String("local-member-attributes", fmt.Sprintf("%+v", s.attributes)),
				zap.Duration("publish-timeout", timeout),
				zap.Error(err),
			)
		}
	}
}
