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
