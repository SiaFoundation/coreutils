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

func TestReorgExpiringFileContractOrder(t *testing.T) {
	n, genesis := chain.TestnetZen()
	store, cs, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Block 1: three contracts that all expire at height 3 (A, B, C).
	b := types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
		Transactions: []types.Transaction{{
			FileContracts: []types.FileContract{
				{FileMerkleRoot: frand.Entropy256(), WindowStart: 1, WindowEnd: 4}, // A
				{FileMerkleRoot: frand.Entropy256(), WindowStart: 1, WindowEnd: 4}, // B
				{FileMerkleRoot: frand.Entropy256(), WindowStart: 1, WindowEnd: 4}, // C
			},
		}},
	}
	contractB := b.Transactions[0].FileContractID(1)
	contractC := b.Transactions[0].FileContractID(2)
	bs := store.SupplementTipBlock(b)
	var cau consensus.ApplyUpdate
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// Block 2: one extra contract (D) with the same WindowEnd.
	// Its sole purpose is to sit at the tail of the slice.
	b = types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
		Transactions: []types.Transaction{{
			FileContracts: []types.FileContract{
				{FileMerkleRoot: frand.Entropy256(), WindowStart: 1, WindowEnd: 4}, // D
			},
		}},
	}
	bs = store.SupplementTipBlock(b)
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// Block 3: Submit storage proofs contracts B and C. They are now in the
	// middle of the slice (A B C D) when the deletions occur.
	b = types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
		Transactions: []types.Transaction{{
			// Missed‑proof outputs spend the contracts without needing
			// a real storage proof.
			StorageProofs: []types.StorageProof{
				{ParentID: contractB},
				{ParentID: contractC},
			},
		}},
	}
	bs = store.SupplementTipBlock(b)
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// Record the order of expiring contracts seen by the next block.
	before := store.SupplementTipBlock(types.Block{}).ExpiringFileContracts
	if len(before) != 2 {
		t.Fatalf("expected 2 expiring contracts, got %d", len(before))
	}

	// Reorg: revert and immediately re‑apply the spend block.
	prev, _ := store.State(b.ParentID)
	cru := consensus.RevertBlock(prev, b, bs)
	store.RevertBlock(prev, cru)
	cs = prev
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	after := store.SupplementTipBlock(types.Block{}).ExpiringFileContracts
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("expiring-contract order changed after reorg:\n%v\n%v", before, after)
	}
}
