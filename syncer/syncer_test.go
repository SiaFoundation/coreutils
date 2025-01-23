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
	store, tipState1, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
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
	}, syncer.WithLogger(log.Named(name)))
	go s.Run()
	return s, cm
}

func TestSyncer(t *testing.T) {
	log := zaptest.NewLogger(t)

	s1, cm1 := newTestSyncer(t, "syncer1", log)
	defer s1.Close()

	s2, cm2 := newTestSyncer(t, "syncer2", log)
	defer s2.Close()

	// mine a few blocks on cm1
	testutil.MineBlocks(t, cm1, types.VoidAddress, 10)
	// mine less blocks on cm2
	testutil.MineBlocks(t, cm2, types.VoidAddress, 5)

	if cm1.Tip().Height != 10 {
		t.Fatalf("expected cm1 tip height to be 10, got %v", cm1.Tip().Height)
	} else if cm2.Tip().Height != 5 {
		t.Fatalf("expected cm2 tip height to be 5, got %v", cm2.Tip().Height)
	}

	// connect the syncers
	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}
	// broadcast blocks from s1
	b, ok := cm1.Block(cm1.Tip().ID)
	if !ok {
		t.Fatal("failed to get block")
	}

	// broadcast the tip from s1 to s2
	s1.BroadcastHeader(b.Header())

	for i := 0; i < 100; i++ {
		if cm1.Tip() == cm2.Tip() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if cm1.Tip() != cm2.Tip() {
		t.Fatalf("tips are not equal: %v != %v", cm1.Tip(), cm2.Tip())
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
