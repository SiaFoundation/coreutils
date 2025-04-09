package chain_test

import (
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"lukechampine.com/frand"
)

func TestGetEmptyBlockID(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)
	_, _ = cm.Block(types.BlockID{})
}

func TestExpiringFileContracts(t *testing.T) {
	n, genesisBlock := chain.TestnetZen()
	store, cs, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}

	// create two file contracts with the same expiration height
	b := types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
		Transactions: []types.Transaction{{
			FileContracts: []types.FileContract{
				{FileMerkleRoot: frand.Entropy256(), WindowEnd: 2},
				{FileMerkleRoot: frand.Entropy256(), WindowEnd: 2},
			},
		}},
	}
	bs := store.SupplementTipBlock(b)
	var cau consensus.ApplyUpdate
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// apply another block, causing the expired contracts to be removed
	b = types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
	}
	bs = store.SupplementTipBlock(b)
	if len(bs.ExpiringFileContracts) != 2 {
		t.Fatalf("expected 2 file contracts, got %d", len(bs.ExpiringFileContracts))
	}
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// revert the block, causing the expired contracts to be re-created
	prev, _ := store.State(b.ParentID)
	cru := consensus.RevertBlock(prev, b, bs)
	store.RevertBlock(prev, cru)

	// the supplement for the next block should contain the same expiring
	// contracts as before, in the same order
	bs2 := store.SupplementTipBlock(types.Block{ParentID: cs.Index.ID})
	if !reflect.DeepEqual(bs, bs2) {
		t.Fatalf("expected supplements to be the same")
	}
}
