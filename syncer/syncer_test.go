package syncer_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func newTestSyncer(t testing.TB, name string, log *zap.Logger) (*syncer.Syncer, *chain.Manager) {
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

	s := syncer.New(l, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l.Addr().String(),
	}, syncer.WithLogger(log.Named(name)), syncer.WithSyncInterval(100*time.Millisecond))
	go s.Run()
	return s, cm
}

func TestSyncer(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, "syncer1", log)
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, "syncer2", log)
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

	s1, cm1 := newTestSyncer(t, "syncer1", log)
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, "syncer2", log)
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

	s, _ := newTestSyncer(t, "syncer1", log)
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

	s1, cm1 := newTestSyncer(t, "syncer1", log)
	defer s1.Close()

	s2, _ := newTestSyncer(t, "syncer2", log)
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

	s, cm := newTestSyncer(t, "syncer", log)
	defer s.Close()

	// mine a few blocks above v2 hardfork height
	testutil.MineBlocks(t, cm, types.VoidAddress, int(n.HardforkV2.AllowHeight+10))

	// instant sync to 5 blocks below the tip
	index, ok := cm.BestIndex(cm.Tip().Height - 5)
	if !ok {
		t.Fatal("failed to get index")
	}
	cs, b, err := syncer.SendCheckpoint(context.Background(), s.Addr(), index, n, genesis.ID())
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

	s1, cm1 := newTestSyncer(t, "syncer1", log)
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, "syncer2", log)
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
