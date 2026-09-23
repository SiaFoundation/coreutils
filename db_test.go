package coreutils_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
)

func TestPruneBoltChainDB(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	path := filepath.Join(t.TempDir(), "chain.db")

	bdb, err := coreutils.OpenBoltChainDB(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := chain.NewDBStore(bdb, n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store)
	testutil.MineBlocks(t, cm, types.VoidAddress, 100)
	tip := cm.Tip()

	// the database must not be pruned while it is open
	if err := coreutils.PruneBoltChainDB(path, n, 50, nil); err == nil {
		t.Fatal("expected error pruning open database")
	} else if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expected no temporary database")
	} else if err := bdb.Close(); err != nil {
		t.Fatal(err)
	}

	if err := coreutils.PruneBoltChainDB(path, n, 50, nil); err != nil {
		t.Fatal(err)
	} else if _, err := os.Stat(path + ".tmp"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("expected temporary database to be removed")
	}

	bdb, err = coreutils.OpenBoltChainDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	store, err = chain.NewDBStore(bdb, n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm = chain.NewManager(store)
	if cm.Tip() != tip {
		t.Fatalf("expected tip %v, got %v", tip, cm.Tip())
	}
	for height := range tip.Height + 1 {
		if index, ok := cm.BestIndex(height); ok != (height >= 50) {
			t.Fatalf("unexpected index presence (%t) at height %d", ok, height)
		} else if _, ok := cm.Block(index.ID); ok != (height >= 50) {
			t.Fatalf("unexpected block presence (%t) at height %d", ok, height)
		}
	}
	testutil.MineBlocks(t, cm, types.VoidAddress, 1)
}
