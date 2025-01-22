package syncer_test

import (
	"context"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap/zaptest"
)

func TestSyncer(t *testing.T) {
	log := zaptest.NewLogger(t)

	n, genesis := testutil.Network()
	store1, tipState1, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		t.Fatal(err)
	}
	cm1 := chain.NewManager(store1, tipState1)

	store2, tipState2, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(store2, tipState2)

	l1, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()

	l2, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	s1 := syncer.New(l1, cm1, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l1.Addr().String(),
	}, syncer.WithLogger(log.Named("syncer1")))
	defer s1.Close()
	go s1.Run(context.Background())

	s2 := syncer.New(l2, cm2, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: l2.Addr().String(),
	}, syncer.WithLogger(log.Named("syncer2")), syncer.WithSyncInterval(10*time.Millisecond))
	defer s2.Close()
	go s2.Run(context.Background())

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
	if _, err := s1.Connect(context.Background(), l2.Addr().String()); err != nil {
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

func TestSyncerChain(t *testing.T) {
	log := zaptest.NewLogger(t)

	newDuo := func() (*chain.Manager, *syncer.Syncer) {
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
		t.Cleanup(func() { l.Close() })

		s := syncer.New(l, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
			GenesisID:  genesis.ID(),
			UniqueID:   gateway.GenerateUniqueID(),
			NetAddress: l.Addr().String(),
		}, syncer.WithLogger(log.Named("syncer1")))
		t.Cleanup(func() { s.Close() })
		go s.Run(context.Background())

		return cm, s
	}

	cm1, s1 := newDuo()
	cm2, s2 := newDuo()
	cm3, s3 := newDuo()

	// cm1 should be on a 10 block chain
	testutil.MineBlocks(t, cm1, types.VoidAddress, 10)

	// cm2 should be on a 5 block chain
	testutil.MineBlocks(t, cm2, types.VoidAddress, 5)

	// cm3 is on a 1 block chain
	testutil.MineBlocks(t, cm3, types.VoidAddress, 1)

	// connect s1 <-> s2 <-> s3
	if _, err := s1.Connect(context.Background(), s2.Addr()); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Connect(context.Background(), s3.Addr()); err != nil {
		t.Fatal(err)
	}

	sync := func() {
		t.Helper()
		for i := 0; i < 1000; i++ {
			if cm1.Tip() == cm2.Tip() && cm2.Tip() == cm3.Tip() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for sync: cm1: %v cm2: %v cm3: %v", cm1.Tip().Height, cm2.Tip().Height, cm3.Tip().Height)
	}
	sync()

	testutil.MineBlocks(t, cm2, types.VoidAddress, 20)
	sync()
}
