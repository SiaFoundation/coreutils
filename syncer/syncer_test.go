package syncer_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

// helper to wait for all provided chain managers to be synced
func synced(t *testing.T, cm ...*chain.Manager) {
	t.Helper()

	var heights []uint64
	for range 100 {
		heights = heights[:0]
		heights = append(heights, cm[0].Tip().Height)
		allEqual := true
		for _, c := range cm[1:] {
			heights = append(heights, c.Tip().Height)
			if c.Tip() != cm[0].Tip() {
				allEqual = false
			}
		}
		if allEqual {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("tips are not equal: %v", heights)
}

// helper to mine blocks on cm and broadcast to syncer s
func mineBlocks(t *testing.T, s *syncer.Syncer, cm *chain.Manager, n int) {
	t.Helper()
	for range n {
		b, ok := coreutils.MineBlock(cm, types.VoidAddress, time.Second)
		if !ok {
			t.Fatal("failed to mine block")
		} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
		if b.V2 != nil {
			// error is ignored, best effort relay
			s.BroadcastV2BlockOutline(gateway.OutlineBlock(b, cm.PoolTransactions(), cm.V2PoolTransactions()))
		}
	}
}

func newTestSyncer(t testing.TB, opts ...syncer.Option) (*syncer.Syncer, *chain.Manager) {
	n, genesis := testutil.Network()
	store, tipState1, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState1)

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		l.Close()
	})

	opts = append([]syncer.Option{syncer.WithSyncInterval(100 * time.Millisecond)}, opts...)
	s := syncer.New(l, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l.Addr().String(),
	}, opts...)
	go s.Run()
	return s, cm
}

func TestSyncer(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer2")))
	defer s2.Close()

	// mine enough blocks to test both v1 and v2 regimes
	testutil.MineBlocks(t, cm1, types.VoidAddress, int(cm1.TipState().Network.HardforkV2.RequireHeight+100))

	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}
	b, ok := cm1.Block(cm1.Tip().ID)
	if !ok {
		t.Fatal("failed to get block")
	}
	s1.BroadcastV2Header(b.Header())

	for range 100 {
		if cm1.Tip() == cm2.Tip() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if cm1.Tip() != cm2.Tip() {
		t.Fatalf("tips are not equal: %v != %v", cm1.Tip(), cm2.Tip())
	}
}

type evilManager struct {
	*chain.Manager
}

func (es evilManager) BlocksForHistory(history []types.BlockID, maxBlocks uint64) ([]types.Block, uint64, error) {
	blocks, rem, err := es.Manager.BlocksForHistory(history, maxBlocks)
	if len(blocks) > 0 && blocks[len(blocks)-1].V2 != nil {
		blocks[len(blocks)-1].Transactions = []types.Transaction{{ArbitraryData: [][]byte{[]byte("oops")}}}
	}
	return blocks, rem, err
}

func TestSyncWithBadPeer(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer2")))
	defer s2.Close()

	// mine enough blocks to test both v1 and v2 regimes
	testutil.MineBlocks(t, cm1, types.VoidAddress, int(cm1.TipState().Network.HardforkV2.RequireHeight+100))

	// simulate another peer, one that returns invalid blocks
	_, genesis := testutil.Network()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	badID := gateway.GenerateUniqueID()
	s3 := syncer.New(l, evilManager{cm1}, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   badID,
		NetAddress: l.Addr().String(),
	})
	go s3.Run()
	defer s3.Close()

	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}
	if _, err := s3.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}

	// sync should (eventually) complete despite the bad peer
	for range 100 {
		if cm1.Tip() == cm2.Tip() {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if cm1.Tip() != cm2.Tip() {
		t.Fatalf("tips are not equal: %v != %v", cm1.Tip(), cm2.Tip())
	}
	// bad peer should be banned
	if peers := s2.Peers(); len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %v", peers)
	} else if peers[0].UniqueID() == badID {
		t.Fatalf("should not be connected to bad peer")
	}
}

func TestSyncerConnectAfterClose(t *testing.T) {
	log := zaptest.NewLogger(t)

	s, _ := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	} else if _, err := s.Connect(context.Background(), "localhost:1234"); !errors.Is(err, threadgroup.ErrClosed) {
		t.Fatal(err)
	}
}

func hashEq(a, b types.EncoderTo) bool {
	h := types.NewHasher()
	a.EncodeTo(h.E)
	h1 := h.Sum()
	h.Reset()
	b.EncodeTo(h.E)
	h2 := h.Sum()
	return h1 == h2
}

func TestSendCheckpoint(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()

	s2, _ := newTestSyncer(t, syncer.WithLogger(log.Named("syncer2")))
	defer s2.Close()

	// mine above v2 hardfork height
	testutil.MineBlocks(t, cm1, types.VoidAddress, int(cm1.TipState().Network.HardforkV2.AllowHeight)+1)

	// request a checkpoint
	p, err := s2.Connect(context.Background(), s1.Addr())
	if err != nil {
		t.Fatal(err)
	}
	cs, b, err := p.SendCheckpoint(cm1.Tip(), cm1.TipState().Network, time.Second)
	if err != nil {
		t.Fatal(err)
	} else if b1, _ := cm1.Block(cm1.Tip().ID); !hashEq(types.V2Block(b), types.V2Block(b1)) {
		t.Fatalf("expected block %v, got %v", b1, b)
	} else if cs1, _ := cm1.State(cs.Index.ID); !hashEq(cs, cs1) {
		t.Fatalf("expected checkpoint %v, got %v", cs1, cs)
	}
}

func TestInstantSync(t *testing.T) {
	n, genesis := testutil.Network()
	log := zap.NewNop()

	s, cm := newTestSyncer(t, syncer.WithLogger(log.Named("syncer")))
	defer s.Close()

	// mine a few blocks above v2 hardfork height
	testutil.MineBlocks(t, cm, types.VoidAddress, int(n.HardforkV2.AllowHeight+10))

	// instant sync to 5 blocks below the tip
	index, ok := cm.BestIndex(cm.Tip().Height - 5)
	if !ok {
		t.Fatal("failed to get index")
	}

	cs, b, err := syncer.RetrieveCheckpoint(context.Background(), []string{s.Addr()}, index, n, genesis.ID())
	if err != nil {
		t.Fatal(err)
	} else if cs.Index.ID != b.ParentID {
		t.Fatalf("expected checkpoint state %v, got %v", b.ParentID, cs.Index.ID)
	}
	// initialize new manager at synced checkpoint
	store, newTipState, err := chain.NewDBStoreAtCheckpoint(chain.NewMemDB(), cs, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(store, newTipState)

	if cm2.Tip() != index {
		t.Fatalf("expected tip %v, got %v", index, cm2.Tip())
	} else if b2, ok := cm2.Block(b.ID()); !ok {
		t.Fatal("checkpoint block not stored")
	} else if !hashEq(types.V2Block(b2), types.V2Block(b)) {
		t.Fatalf("checkpoint block mismatch")
	} else if cs2, ok := cm2.State(cs.Index.ID); !ok {
		t.Fatal("parent state not stored")
	} else if !hashEq(cs2, cs) {
		t.Fatalf("parent state mismatch")
	}

	// sync to tip
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	s2 := syncer.New(l, cm2, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l.Addr().String(),
	}, syncer.WithSyncInterval(100*time.Millisecond))
	defer s2.Close()
	if _, err := s2.Connect(context.Background(), s.Addr()); err != nil {
		t.Fatal(err)
	}
	go s2.Run()
	for range 100 {
		if cm.Tip() == cm2.Tip() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if cm.Tip() != cm2.Tip() {
		t.Fatalf("tips are not equal: %v != %v", cm.Tip(), cm2.Tip())
	}
}

func TestSendHeaders(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer2")))
	defer s2.Close()
	cs := cm2.TipState()

	testutil.MineBlocks(t, cm1, types.VoidAddress, 100)

	p, err := s2.Connect(context.Background(), s1.Addr())
	if err != nil {
		t.Fatal(err)
	}
	headers, rem, err := p.SendHeaders(cs, 90, time.Second)
	if err != nil {
		t.Fatal(err)
	} else if len(headers) != 90 {
		t.Fatalf("expected 90 headers, got %d", len(headers))
	} else if rem != 10 {
		t.Fatalf("expected 10 remaining headers, got %d", rem)
	}
}

func TestSyncerReorg(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()

	// s2 must only be able to sync from s1 to force reorg propagation
	s2, cm2 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer2")), syncer.WithSyncInterval(100*time.Millisecond), syncer.WithMaxInboundPeers(1), syncer.WithMaxOutboundPeers(0))
	defer s2.Close()

	s3, cm3 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer3")))
	defer s3.Close()

	// connect s1 and s2
	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}
	log.Debug("connected s1 and s2")

	// mine above the v2 require height
	mineBlocks(t, s1, cm1, int(cm1.TipState().Network.HardforkV2.RequireHeight+10))

	// apply cm1 blocks manually to cm3 to simulate a synced node
	_, applied, err := cm1.UpdatesSince(types.ChainIndex{}, 1000)
	if err != nil {
		t.Fatalf("failed to get updates since genesis: %v", err)
	}
	for _, cau := range applied {
		if err := cm3.AddBlocks([]types.Block{cau.Block}); err != nil {
			t.Fatalf("failed to apply block at height %d: %v", cau.Block.V2.Height, err)
		}
	}

	// check that all three nodes are at the same tip
	synced(t, cm1, cm2, cm3)

	// mine conflicting chains on cm1 and cm3
	mineBlocks(t, s1, cm1, 1)
	mineBlocks(t, s3, cm3, 5)

	// connect cm1 and cm3, triggering a reorg on cm1 and cm2
	if _, err := s1.Connect(context.Background(), s3.Addr()); err != nil {
		t.Fatal(err)
	}
	log.Debug("syncer peers", zap.Int("s1", len(s1.Peers())), zap.Int("s2", len(s2.Peers())), zap.Int("s3", len(s3.Peers())))
	synced(t, cm1, cm2, cm3)
}

func TestParallelSyncReorgSplit(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")), syncer.WithSyncInterval(100*time.Millisecond))
	defer s1.Close()

	// s2 and s3 should not be able to connect to each other
	s2, cm2 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer2")), syncer.WithMaxInboundPeers(1), syncer.WithSyncInterval(100*time.Millisecond))
	defer s2.Close()

	s3, cm3 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer3")), syncer.WithMaxOutboundPeers(1), syncer.WithSyncInterval(100*time.Millisecond))
	defer s3.Close()

	// mine after the v2 hardfork height
	testutil.MineBlocks(t, cm2, types.VoidAddress, int(cm2.TipState().Network.HardforkV2.RequireHeight+10))

	// apply cm1 blocks manually to cm3 to simulate a synced node
	_, applied, err := cm2.UpdatesSince(types.ChainIndex{}, 1000)
	if err != nil {
		t.Fatalf("failed to get updates: %v", err)
	}
	for _, cau := range applied {
		if err := cm3.AddBlocks([]types.Block{cau.Block}); err != nil {
			t.Fatalf("failed to apply block: %v", err)
		}
	}

	// create a split on cm2 and cm3
	testutil.MineBlocks(t, cm2, types.VoidAddress, 5)
	testutil.MineBlocks(t, cm3, types.VoidAddress, 6)

	// Verify they've diverged
	if cm2.Tip() == cm3.Tip() {
		t.Fatal("chains should have diverged")
	}

	// Connect s1 to both s2 and s3
	// s1 will get headers from s2 (longer chain) and may ask s3 for blocks
	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.Connect(context.Background(), s3.Addr()); err != nil {
		t.Fatal(err)
	}
	synced(t, cm1, cm2, cm3)
}

func TestForkPeerSynced(t *testing.T) {
	log := zaptest.NewLogger(t)

	// s1 has a longer chain (20 blocks)
	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()
	testutil.MineBlocks(t, cm1, types.VoidAddress, 20)

	// s2 has a shorter fork chain (10 blocks) with a very long sync
	// interval so it never adopts s1's chain during the test
	n, genesis := testutil.Network()
	store2, tipState2, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(store2, tipState2)
	testutil.MineBlocks(t, cm2, types.VoidAddress, 10)

	l2, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l2.Close() })

	s2 := syncer.New(l2, cm2, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l2.Addr().String(),
	}, syncer.WithSyncInterval(time.Hour)) // effectively disabled
	go s2.Run()
	defer s2.Close()

	s1Tip := cm1.Tip()

	// connect s1 to s2 - s1 should sync s2's fork blocks but stay on
	// its own longer chain, and mark s2 as synced
	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}

	// wait for s2 to be marked synced in s1's peer list
	var peers []*syncer.Peer
	for range 100 {
		peers = s1.Peers()
		if len(peers) == 1 && peers[0].Synced() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(peers) != 1 || !peers[0].Synced() {
		t.Fatal("fork peer should be marked as synced after downloading its chain")
	}

	// verify s1 stayed on its original chain
	if cm1.Tip() != s1Tip {
		t.Fatalf("s1 tip should not have changed: expected %v, got %v", s1Tip, cm1.Tip())
	}
}

// stallManager serves valid headers but always fails to serve blocks,
// simulating a peer that disconnects during block fetching.
type stallManager struct {
	n uint64
	*chain.Manager
}

// BlocksForHistory always returns an error, simulating a peer that fails to
// serve blocks.
func (sm *stallManager) BlocksForHistory(_ []types.BlockID, _ uint64) ([]types.Block, uint64, error) {
	atomic.AddUint64(&sm.n, 1)
	return nil, 0, errors.New("stall")
}

// TestParallelSyncStall verifies that parallelSync doesn't block indefinitely
// if all peers fail to serve blocks.
func TestParallelSyncStall(t *testing.T) {
	log := zaptest.NewLogger(t)

	// s1 starts without blocks
	s1, cm1 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()

	// s2 has blocks but BlocksForHistory always fails, so blocks
	// can never be served despite valid headers
	n, genesis := testutil.Network()
	store2, tipState2, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(store2, tipState2)
	testutil.MineBlocks(t, cm2, types.VoidAddress, 10)

	l2, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l2.Close() })

	stuckCM := &stallManager{Manager: cm2}
	s2 := syncer.New(l2, stuckCM, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l2.Addr().String(),
	}, syncer.WithSyncInterval(100*time.Millisecond))
	go s2.Run()
	defer s2.Close()

	// connect s1 to s2 - parallelSync should detect the stall and
	// return an error instead of blocking forever
	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}

	// wait for a few cycles
	for range 10 {
		if n := atomic.LoadUint64(&stuckCM.n); n >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// verify s1 hasn't synced (stallManager prevents block serving)
	if cm1.Tip().Height != 0 {
		t.Fatal("s1 should not have synced from stallManager peer")
	}

	// connect a normal peer which should be able to sync
	s3, cm3 := newTestSyncer(t, syncer.WithLogger(log.Named("syncer3")))
	defer s3.Close()
	testutil.MineBlocks(t, cm3, types.VoidAddress, 5)

	if _, err := s1.Connect(context.Background(), s3.Addr()); err != nil {
		t.Fatal(err)
	}

	// wait for s1 and s3 to sync, verifying that parallelSync is not blocked
	// syncing with s2
	synced(t, cm1, cm3)
}

func TestShareNodesMalformed(t *testing.T) {
	log := zaptest.NewLogger(t)

	// s1's peer store contains malformed addresses that should not
	// be served via ShareNodes or added to other peer stores
	malformed := map[string]bool{
		":1234":      true,
		"host:99999": true,
		"host:0":     true,
		"noport":     true,
		"":           true,
	}
	n, genesis := testutil.Network()

	ps1 := testutil.NewEphemeralPeerStore()
	for addr := range malformed {
		ps1.AddPeer(addr)
	}
	ps1.AddPeer("127.0.0.1:65535")
	store1, tipState1, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm1 := chain.NewManager(store1, tipState1)

	l1, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l1.Close() })

	s1 := syncer.New(l1, cm1, ps1, gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l1.Addr().String(),
	}, syncer.WithLogger(log.Named("syncer1")), syncer.WithSyncInterval(100*time.Millisecond))
	go s1.Run()
	defer s1.Close()

	// s2 connects to s1 and runs peer discovery
	ps2 := testutil.NewEphemeralPeerStore()
	store2, tipState2, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(store2, tipState2)

	l2, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l2.Close() })

	s2 := syncer.New(l2, cm2, ps2, gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l2.Addr().String(),
	}, syncer.WithLogger(log.Named("syncer2")), syncer.WithSyncInterval(100*time.Millisecond), syncer.WithPeerDiscoveryInterval(100*time.Millisecond))
	go s2.Run()
	defer s2.Close()

	if _, err := s2.Connect(context.Background(), s1.Addr()); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		peers, err := ps2.Peers()
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range peers {
			if malformed[p.Address] {
				t.Fatalf("malformed address %q should not have been added to peer store", p.Address)
			}
		}
		if len(peers) > 1 {
			break
		} else if time.Now().After(deadline) {
			t.Fatal("peer discovery did not add any peers")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// blockingManager wraps a chain.Manager and blocks every BlocksForHistory call
// until unblock is called, signalling on entered each time a call begins. This
// lets a test pin inbound RPC handlers in-flight to exercise the per-subnet cap.
type blockingManager struct {
	*chain.Manager
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingManager) BlocksForHistory(history []types.BlockID, maxBlocks uint64) ([]types.Block, uint64, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-b.release
	return b.Manager.BlocksForHistory(history, maxBlocks)
}

// unblock releases all blocked (and future) BlocksForHistory calls. It is safe
// to call multiple times.
func (b *blockingManager) unblock() { b.once.Do(func() { close(b.release) }) }

// newBlockingSyncer starts a syncer backed by a blockingManager, listening on
// loopback so that every peer maps to a single subnet. Periodic sync and peer
// discovery are disabled so the only RPCs the server handles are the ones the
// test issues explicitly.
func newBlockingSyncer(t *testing.T, log *zap.Logger, opts ...syncer.Option) (*syncer.Syncer, *blockingManager) {
	t.Helper()
	n, genesis := testutil.Network()
	store, ts, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	bm := &blockingManager{
		Manager: chain.NewManager(store, ts),
		entered: make(chan struct{}, 64),
		release: make(chan struct{}),
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	base := []syncer.Option{
		syncer.WithLogger(log),
		syncer.WithSyncInterval(time.Hour),
		syncer.WithPeerDiscoveryInterval(time.Hour),
	}
	s := syncer.New(l, bm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l.Addr().String(),
	}, append(base, opts...)...)
	go s.Run()
	t.Cleanup(func() { s.Close() })
	// unblock handlers before the syncer is closed so its threadgroup can drain;
	// runs first (cleanups are LIFO) since it is registered last.
	t.Cleanup(bm.unblock)
	return s, bm
}

// newQuietClient starts a client syncer with background sync and discovery
// disabled so it does not issue RPCs to the server on its own.
func newQuietClient(t *testing.T, log *zap.Logger) (*syncer.Syncer, *chain.Manager) {
	t.Helper()
	s, cm := newTestSyncer(t, syncer.WithLogger(log),
		syncer.WithSyncInterval(time.Hour),
		syncer.WithPeerDiscoveryInterval(time.Hour))
	t.Cleanup(func() { s.Close() })
	return s, cm
}

// waitEntered blocks until n RPC handlers have entered BlocksForHistory.
func waitEntered(t *testing.T, ch <-chan struct{}, n int, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("only %d/%d RPC handlers became in-flight", i, n)
		}
	}
}

// TestMaxInflightRPCsPerSubnetDropsOverflow verifies that once a subnet has
// filled its in-flight budget, a further RPC — even from a different peer in the
// same subnet — is dropped (the stream is closed) rather than backpressured.
func TestMaxInflightRPCsPerSubnetDropsOverflow(t *testing.T) {
	log := zaptest.NewLogger(t)
	srv, bm := newBlockingSyncer(t, log.Named("srv"),
		syncer.WithMaxInflightRPCsPerSubnet(2),
		syncer.WithMaxInflightRPCs(64))

	// two peers from the same subnet
	c1, cm1 := newQuietClient(t, log.Named("c1"))
	c2, _ := newQuietClient(t, log.Named("c2"))

	p1, err := c1.Connect(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	p2, err := c2.Connect(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	hist := []types.BlockID{cm1.Tip().ID}

	// fill the subnet's 2 slots with blocking RPCs from c1
	for range 2 {
		go p1.SendV2Blocks(context.Background(), hist, 10, 10*time.Second)
	}
	waitEntered(t, bm.entered, 2, 5*time.Second)

	// a 3rd RPC from a different peer in the same subnet must be dropped, so it
	// returns promptly with an error and never reaches the handler.
	errCh := make(chan error, 1)
	go func() {
		_, _, err := p2.SendV2Blocks(context.Background(), hist, 10, 10*time.Second)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("overflow RPC should have been dropped, got success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("overflow RPC hung; it was backpressured, not dropped")
	}
	select {
	case <-bm.entered:
		t.Fatal("dropped RPC reached the handler")
	default:
	}
}

// TestMaxInflightRPCsPerSubnetDisabled verifies that a value <= 0 disables the
// per-subnet cap: many concurrent RPCs that would exceed a finite cap (see
// TestMaxInflightRPCsPerSubnetDropsOverflow) all reach the handler.
func TestMaxInflightRPCsPerSubnetDisabled(t *testing.T) {
	log := zaptest.NewLogger(t)
	srv, bm := newBlockingSyncer(t, log.Named("srv"),
		syncer.WithMaxInflightRPCsPerSubnet(0),
		syncer.WithMaxInflightRPCs(64))

	c1, cm1 := newQuietClient(t, log.Named("c1"))
	p1, err := c1.Connect(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	hist := []types.BlockID{cm1.Tip().ID}

	const rpcs = 8
	for range rpcs {
		go p1.SendV2Blocks(context.Background(), hist, 10, 10*time.Second)
	}
	waitEntered(t, bm.entered, rpcs, 5*time.Second)
}

// TestMaxInflightRPCsBackpressureNotDropped verifies the per-peer limit
// backpressures rather than drops: with the subnet cap disabled and a per-peer
// limit of 1, a second RPC neither errors nor completes while the first holds
// the slot, then succeeds once the slot is freed.
func TestMaxInflightRPCsBackpressureNotDropped(t *testing.T) {
	log := zaptest.NewLogger(t)
	srv, bm := newBlockingSyncer(t, log.Named("srv"),
		syncer.WithMaxInflightRPCsPerSubnet(0),
		syncer.WithMaxInflightRPCs(1))

	c1, cm1 := newQuietClient(t, log.Named("c1"))
	p1, err := c1.Connect(context.Background(), srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	hist := []types.BlockID{cm1.Tip().ID}

	// RPC #1 occupies the single per-peer slot and blocks in the handler.
	go p1.SendV2Blocks(context.Background(), hist, 10, 30*time.Second)
	waitEntered(t, bm.entered, 1, 5*time.Second)

	// RPC #2 must be backpressured: while #1 holds the slot it neither errors
	// nor completes, and never reaches the handler.
	errCh := make(chan error, 1)
	go func() {
		_, _, err := p1.SendV2Blocks(context.Background(), hist, 10, 30*time.Second)
		errCh <- err
	}()
	select {
	case err := <-errCh:
		t.Fatalf("second RPC returned while the slot was held (err=%v); expected backpressure", err)
	case <-time.After(time.Second):
	}
	select {
	case <-bm.entered:
		t.Fatal("backpressured RPC reached the handler before the slot was freed")
	default:
	}

	// releasing #1 frees the slot; #2 then proceeds and completes successfully.
	bm.unblock()
	if err := <-errCh; err != nil {
		t.Fatalf("backpressured RPC failed after the slot was freed: %v", err)
	}
}
