package chain_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/bits"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"lukechampine.com/frand"
)

func taxAdjustedPayout(target types.Currency) types.Currency {
	guess := target.Mul64(1000).Div64(961)
	mod64 := func(c types.Currency, v uint64) types.Currency {
		var r uint64
		if c.Hi < v {
			_, r = bits.Div64(c.Hi, c.Lo, v)
		} else {
			_, r = bits.Div64(0, c.Hi, v)
			_, r = bits.Div64(r, c.Lo, v)
		}
		return types.NewCurrency64(r)
	}
	sfc := (consensus.State{}).SiafundCount()
	tm := mod64(target, sfc)
	gm := mod64(guess, sfc)
	if gm.Cmp(tm) < 0 {
		guess = guess.Sub(types.NewCurrency64(sfc))
	}
	return guess.Add(tm).Sub(gm)
}

func TestGetEmptyBlockID(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	store, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store)
	_, _ = cm.Block(types.BlockID{})
}

func TestExpiringFileContracts(t *testing.T) {
	n, genesisBlock := chain.TestnetZen()
	store, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	sp := store.Scratchpad()
	cs := sp.TipState()

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
	bs := sp.SupplementTipBlock(b)
	var cau consensus.ApplyUpdate
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	sp.AddState(cs)
	sp.AddBlock(b, &bs)
	sp.ApplyBlock(cs, cau)

	// apply another block, causing the expired contracts to be removed
	b = types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
	}
	bs = sp.SupplementTipBlock(b)
	if len(bs.ExpiringFileContracts) != 2 {
		t.Fatalf("expected 2 file contracts, got %d", len(bs.ExpiringFileContracts))
	}
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	sp.AddState(cs)
	sp.AddBlock(b, &bs)
	sp.ApplyBlock(cs, cau)

	// revert the block, causing the expired contracts to be re-created
	prev, _ := sp.State(b.ParentID)
	cru := consensus.RevertBlock(prev, b, bs)
	sp.RevertBlock(prev, cru)

	// the supplement for the next block should contain the same expiring
	// contracts as before, in the same order
	bs2 := sp.SupplementTipBlock(types.Block{ParentID: cs.Index.ID})
	if !reflect.DeepEqual(bs, bs2) {
		t.Fatalf("expected supplements to be the same")
	}
}

func TestReorgExpiringFileContractOrder(t *testing.T) {
	n, genesis := testutil.Network()
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 100

	sk := types.GeneratePrivateKey()
	uc := types.StandardUnlockConditions(sk.PublicKey())
	addr := uc.UnlockHash()

	genesis.Transactions[0].SiacoinOutputs = []types.SiacoinOutput{
		{Address: addr, Value: types.Siacoins(1000)},
	}
	giftAmount := types.Siacoins(1000)
	giftUTXOID := genesis.Transactions[0].SiacoinOutputID(0)

	db1, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm1 := chain.NewManager(db1)

	newFileContract := func() types.FileContract {
		return types.FileContract{
			Payout:         taxAdjustedPayout(types.Siacoins(1)),
			FileMerkleRoot: frand.Entropy256(),
			WindowStart:    3,
			WindowEnd:      6,
			ValidProofOutputs: []types.SiacoinOutput{
				{Value: types.Siacoins(1), Address: addr},
			},
			MissedProofOutputs: []types.SiacoinOutput{
				{Value: types.Siacoins(1), Address: addr},
			},
			UnlockHash: uc.UnlockHash(),
		}
	}

	cost := taxAdjustedPayout(types.Siacoins(1)).Mul64(3) // 3 contracts, each with 1 SC proof output
	formationTxn := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{
			{ParentID: giftUTXOID, UnlockConditions: uc},
		},
		SiacoinOutputs: []types.SiacoinOutput{
			{Value: giftAmount.Sub(cost), Address: addr}, // Refund output
		},
		FileContracts: []types.FileContract{
			newFileContract(), // A
			newFileContract(), // B
			newFileContract(), // C
		},
		Signatures: []types.TransactionSignature{
			{ParentID: types.Hash256(giftUTXOID), CoveredFields: types.CoveredFields{WholeTransaction: true}},
		},
	}
	cs := cm1.TipState()
	sigHash := cs.WholeSigHash(formationTxn, types.Hash256(giftUTXOID), 0, 0, nil)
	sig := sk.SignHash(sigHash)
	formationTxn.Signatures[0].Signature = sig[:]

	_, err = cm1.AddPoolTransactions([]types.Transaction{formationTxn})
	if err != nil {
		t.Fatal(err)
	}
	// Block 1: three contracts that all expire at height 3 (A, B, C).
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)
	// update the gift UTXO ID and amount for the next transaction
	giftUTXOID = formationTxn.SiacoinOutputID(0)
	giftAmount = formationTxn.SiacoinOutputs[0].Value

	contractA := formationTxn.FileContractID(0)
	contractB := formationTxn.FileContractID(1)
	contractC := formationTxn.FileContractID(2)

	cs = cm1.TipState()
	cost = taxAdjustedPayout(types.Siacoins(1)) // 1 contract with 1 SC proof output
	formationTxn2 := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{
			{ParentID: giftUTXOID, UnlockConditions: uc},
		},
		SiacoinOutputs: []types.SiacoinOutput{
			{Value: giftAmount.Sub(cost), Address: addr}, // Refund output
		},
		FileContracts: []types.FileContract{
			newFileContract(), // D
		},
		Signatures: []types.TransactionSignature{
			{ParentID: types.Hash256(giftUTXOID), CoveredFields: types.CoveredFields{WholeTransaction: true}},
		},
	}
	sigHash = cs.WholeSigHash(formationTxn2, types.Hash256(giftUTXOID), 0, 0, nil)
	sig = sk.SignHash(sigHash)
	formationTxn2.Signatures[0].Signature = sig[:]

	_, err = cm1.AddPoolTransactions([]types.Transaction{formationTxn2})
	if err != nil {
		t.Fatal(err)
	}
	// Block 2: one extra contract (D) with the same WindowEnd.
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)
	contractD := formationTxn2.FileContractID(0)

	proofTxn := types.Transaction{
		// Missed‑proof outputs spend the contracts without needing
		// a real storage proof.
		StorageProofs: []types.StorageProof{
			{ParentID: contractB},
			{ParentID: contractC},
		},
	}
	if _, err = cm1.AddPoolTransactions([]types.Transaction{proofTxn}); err != nil {
		t.Fatal(err)
	}
	// Block 3: Submit storage proofs contracts B and C. The remaining
	// contracts are (A D).
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)

	// revert and reapply the block
	if err := cm1.ForceRevertTip(); err != nil {
		t.Fatal(err)
	}
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)

	// mine one block past contract expiration
	for i := cm1.Tip().Height; i <= 6+1; i++ {
		testutil.MineBlocks(t, cm1, types.VoidAddress, 1)
	}

	expirationIndex, ok := cm1.BestIndex(6)
	if !ok {
		t.Fatal("expected to find index at height 6")
	}

	_, applied, err := cm1.UpdatesSince(types.ChainIndex{}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	blocks := make([]types.Block, 0, len(applied))
	for _, cau := range applied {
		blocks = append(blocks, cau.Block)
	}

	db2, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(db2)

	// applying the blocks should fail because the expiring file contract
	// order is not the same because of the missing revert.
	if err := cm2.AddBlocks(blocks); !errors.Is(err, consensus.ErrCommitmentMismatch) {
		t.Fatalf("expected %q, got %q", consensus.ErrCommitmentMismatch, err)
	}

	// reinit the chain manager with the correct expiring contract order
	cm2 = chain.NewManager(db2, chain.WithExpiringContractOrder(map[types.BlockID][]types.FileContractID{
		expirationIndex.ID: {
			contractD,
			contractA,
		},
	}))
	// applying the blocks should succeed because the ordering is overwritten
	if err := cm2.AddBlocks(blocks); err != nil {
		t.Fatal(err)
	}

	// init a fresh chain manager with the correct expiring contract order
	db3, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm3 := chain.NewManager(db3, chain.WithExpiringContractOrder(map[types.BlockID][]types.FileContractID{
		expirationIndex.ID: {
			contractD,
			contractA,
		},
	}))
	// applying the blocks should succeed because the ordering is overwritten
	if err := cm3.AddBlocks(blocks); err != nil {
		t.Fatal(err)
	}

	cs1 := cm1.TipState()
	json1, err := json.Marshal(cs1)
	if err != nil {
		t.Fatal(err)
	}
	cs2 := cm2.TipState()
	json2, err := json.Marshal(cs2)
	if err != nil {
		t.Fatal(err)
	}
	cs3 := cm3.TipState()
	json3, err := json.Marshal(cs3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(json1, json2) {
		t.Log(string(json1))
		t.Log(string(json2))
		t.Fatal("expected the chain states to be equal after reorg")
	} else if !bytes.Equal(json1, json3) {
		t.Log(string(json1))
		t.Log(string(json3))
		t.Fatal("expected the chain states to be equal after reorg with new manager")
	}
}

func TestPruneBlocks(t *testing.T) {
	n, genesisBlock := testutil.V2Network()

	store, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store)

	// mine a bunch of blocks
	testutil.MineBlocks(t, cm, types.VoidAddress, 100)

	// prune up to height 50
	cm.PruneBlocks(50)
	// mine another block; pruned blocks are only guaranteed to disappear from
	// read-only methods at the next commit
	testutil.MineBlocks(t, cm, types.VoidAddress, 1)

	// ensure blocks < 50 are pruned
	for height := range uint64(50) {
		if index, ok := cm.BestIndex(height); !ok {
			t.Fatalf("expected header at height %d to exist", height)
		} else if _, exists := cm.Block(index.ID); exists {
			t.Fatalf("expected block at height %d to not exist", height)
		}
	}

	// ensure blocks >= 50 exist
	for height := uint64(50); height <= 100; height++ {
		index, ok := cm.BestIndex(height)
		if !ok {
			t.Fatalf("expected block at height %d to exist", height)
		} else if block, exists := cm.Block(index.ID); !exists {
			t.Fatalf("expected block at height %d to exist", height)
		} else if block.ID() != index.ID {
			t.Fatalf("block ID mismatch at height %d: expected %s, got %s", height, index.ID, block.ID())
		}
	}
}

func testDBSnapshot(t *testing.T, db chain.DB) {
	t.Helper()

	sp := db.Scratchpad()
	b, err := sp.CreateBucket([]byte("test"))
	if err != nil {
		t.Fatal(err)
	} else if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
		t.Fatal(err)
	} else if err := sp.Flush(); err != nil {
		t.Fatal(err)
	}

	// unflushed writes should be visible via the scratchpad, but not via
	// Snapshot
	b = sp.Bucket([]byte("test"))
	if err := b.Put([]byte("baz"), []byte("quux")); err != nil {
		t.Fatal(err)
	} else if err := b.Delete([]byte("foo")); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b.Get([]byte("baz")), []byte("quux")) {
		t.Fatal("expected unflushed write to be visible via the scratchpad")
	}
	var iterated int
	for k, v := range b.Iter() {
		if !bytes.Equal(k, []byte("baz")) || !bytes.Equal(v, []byte("quux")) {
			t.Fatal("unexpected key-value pair:", string(k), string(v))
		}
		iterated++
	}
	if iterated != 1 {
		t.Fatal("expected unflushed write to be visible via scratchpad iteration")
	}
	snapshot, release := db.Snapshot()
	if vb := snapshot.Bucket([]byte("nonexistent")); vb != nil {
		t.Fatal("expected nil bucket for nonexistent bucket")
	}
	vb := snapshot.Bucket([]byte("test"))
	if vb == nil {
		t.Fatal("expected snapshot bucket to exist")
	} else if vb.Get([]byte("baz")) != nil {
		t.Fatal("expected unflushed write to not be visible via Snapshot")
	} else if !bytes.Equal(vb.Get([]byte("foo")), []byte("bar")) {
		t.Fatal("expected flushed write to be visible via Snapshot")
	}

	// open snapshots are unaffected by Flush
	if err := sp.Flush(); err != nil {
		t.Fatal(err)
	} else if vb.Get([]byte("baz")) != nil {
		t.Fatal("expected open snapshot to be unaffected by Flush")
	} else if !bytes.Equal(vb.Get([]byte("foo")), []byte("bar")) {
		t.Fatal("expected open snapshot to be unaffected by Flush")
	}
	release()

	// a new snapshot should see the flushed writes
	snapshot, release = db.Snapshot()
	defer release()
	vb = snapshot.Bucket([]byte("test"))
	if !bytes.Equal(vb.Get([]byte("baz")), []byte("quux")) {
		t.Fatal("expected flushed write to be visible via Snapshot")
	} else if vb.Get([]byte("foo")) != nil {
		t.Fatal("expected flushed delete to be visible via Snapshot")
	}
	for k, v := range vb.Iter() {
		if !bytes.Equal(k, []byte("baz")) || !bytes.Equal(v, []byte("quux")) {
			t.Fatal("unexpected key-value pair:", string(k), string(v))
		}
	}
}

func TestMemDBSnapshot(t *testing.T) {
	testDBSnapshot(t, chain.NewMemDB())
}

func TestCacheDBSnapshot(t *testing.T) {
	testDBSnapshot(t, chain.NewCacheDB(chain.NewMemDB()))
}

func TestBoltDBSnapshot(t *testing.T) {
	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()
	testDBSnapshot(t, bdb)
}

func TestCopyPrunedDB(t *testing.T) {
	// checkPruned verifies that dst matches the checkpoint layout: nothing
	// below pruneHeight except the checkpoint's parent state, and the full
	// main chain, headers, states, and blocks from pruneHeight through the tip
	checkPruned := func(t *testing.T, srcStore, dstStore *chain.DBStore, dstDB chain.DB, pruneHeight uint64) {
		t.Helper()
		src, releaseSrc := srcStore.Snapshot()
		defer releaseSrc()
		dst, releaseDst := dstStore.Snapshot()
		defer releaseDst()

		if !reflect.DeepEqual(src.TipState(), dst.TipState()) {
			t.Fatal("tip state mismatch")
		}
		for height := range src.TipState().Index.Height + 1 {
			index, ok := src.BestIndex(height)
			if !ok {
				t.Fatalf("missing source index at height %d", height)
			}
			if height < pruneHeight {
				if _, ok := dst.BestIndex(height); ok {
					t.Fatalf("expected index at height %d to be pruned", height)
				} else if _, ok := dst.Header(index.ID); ok {
					t.Fatalf("expected header at height %d to be pruned", height)
				} else if _, _, ok := dst.Block(index.ID); ok {
					t.Fatalf("expected block at height %d to be pruned", height)
				} else if _, ok := dst.State(index.ID); ok != (height == pruneHeight-1) {
					t.Fatalf("unexpected state presence (%t) at height %d", ok, height)
				}
				continue
			}
			if dstIndex, ok := dst.BestIndex(height); !ok || dstIndex != index {
				t.Fatalf("index mismatch at height %d: expected %v, got %v", height, index, dstIndex)
			} else if srcState, ok := src.State(index.ID); !ok {
				t.Fatalf("missing source state at height %d", height)
			} else if dstState, ok := dst.State(index.ID); !ok {
				t.Fatalf("missing state at height %d", height)
			} else if !reflect.DeepEqual(srcState, dstState) {
				t.Fatalf("state mismatch at height %d", height)
			} else if srcBlock, _, ok := src.Block(index.ID); !ok {
				t.Fatalf("missing source block at height %d", height)
			} else if dstBlock, _, ok := dst.Block(index.ID); !ok {
				t.Fatalf("missing block at height %d", height)
			} else if dstBlock.ID() != srcBlock.ID() {
				t.Fatalf("block mismatch at height %d", height)
			}
		}

		// the v1 element buckets must exist, but be empty
		ss, release := dstDB.Snapshot()
		defer release()
		for _, name := range []string{"Tree", "SiacoinElements", "SiafundElements", "FileContracts"} {
			b := ss.Bucket([]byte(name))
			if b == nil {
				t.Fatalf("expected bucket %q to exist", name)
			}
			for k := range b.Iter() {
				t.Fatalf("expected bucket %q to be empty, found key %x", name, k)
			}
		}
	}

	n, genesisBlock := testutil.V2Network()
	srcDB := chain.NewMemDB()
	srcStore, err := chain.NewDBStore(srcDB, n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	srcCM := chain.NewManager(srcStore)
	testutil.MineBlocks(t, srcCM, types.VoidAddress, 100)

	t.Run("errors", func(t *testing.T) {
		if err := chain.CopyPrunedDB(srcDB, chain.NewMemDB(), n, 101, nil); err == nil {
			t.Fatal("expected error for prune height above tip")
		} else if err := chain.CopyPrunedDB(srcDB, chain.NewMemDB(), n, n.HardforkV2.RequireHeight+1, nil); err == nil {
			t.Fatal("expected error for prune height within reach of v1 element state")
		} else if err := chain.CopyPrunedDB(chain.NewMemDB(), chain.NewMemDB(), n, 50, nil); err == nil {
			t.Fatal("expected error for uninitialized source")
		} else if err := chain.CopyPrunedDB(srcDB, srcDB, n, 50, nil); err == nil {
			t.Fatal("expected error for non-empty destination")
		}
		mainnet, _ := chain.Mainnet()
		if err := chain.CopyPrunedDB(srcDB, chain.NewMemDB(), mainnet, 50, nil); err == nil {
			t.Fatal("expected error for network mismatch")
		}

		// a source that has already pruned a block at or above the prune
		// height has nothing left to prune there
		prunedDB := chain.NewMemDB()
		prunedStore, err := chain.NewDBStore(prunedDB, n, genesisBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		prunedCM := chain.NewManager(prunedStore)
		testutil.MineBlocks(t, prunedCM, types.VoidAddress, 10)
		prunedCM.PruneBlocks(6)
		testutil.MineBlocks(t, prunedCM, types.VoidAddress, 1) // flushes the prune
		if err := chain.CopyPrunedDB(prunedDB, chain.NewMemDB(), n, 5, nil); err == nil {
			t.Fatal("expected error for prune height below the source's retained blocks")
		} else if err := chain.CopyPrunedDB(prunedDB, chain.NewMemDB(), n, 6, nil); err != nil {
			t.Fatal(err)
		}
	})

	dstDB := chain.NewMemDB()
	if err := chain.CopyPrunedDB(srcDB, dstDB, n, 50, nil); err != nil {
		t.Fatal(err)
	}
	dstStore, err := chain.NewDBStore(dstDB, n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkPruned(t, srcStore, dstStore, dstDB, 50)

	// a pruned source must be prunable again
	dst2DB := chain.NewMemDB()
	if err := chain.CopyPrunedDB(dstDB, dst2DB, n, 60, nil); err != nil {
		t.Fatal(err)
	} else if dst2Store, err := chain.NewDBStore(dst2DB, n, genesisBlock, nil); err != nil {
		t.Fatal(err)
	} else {
		checkPruned(t, srcStore, dst2Store, dst2DB, 60)
	}

	dstCM := chain.NewManager(dstStore)
	if dstCM.MinReorgIndex().Height != 50 {
		t.Fatalf("expected min reorg height 50, got %d", dstCM.MinReorgIndex().Height)
	}

	// the syncer looks up the state of every non-empty history entry
	hist, err := dstCM.History()
	if err != nil {
		t.Fatal(err)
	}
	var seenEmpty bool
	for _, id := range hist {
		if id == (types.BlockID{}) {
			seenEmpty = true
			continue
		} else if seenEmpty {
			t.Fatal("expected history to end after first empty entry")
		} else if cs, ok := dstCM.State(id); !ok {
			t.Fatalf("missing state for history entry %v", id)
		} else if cs.Index.Height < 50 {
			t.Fatalf("history entry %v is below prune height", cs.Index)
		}
	}
	if !seenEmpty {
		t.Fatal("expected history to be truncated")
	}

	testutil.MineBlocks(t, dstCM, types.VoidAddress, 10)
	if retained, _ := dstCM.BestIndex(50); retained.Height != 50 {
		t.Fatal("missing index at height 50")
	} else if _, aus, err := dstCM.UpdatesSince(retained, 100); err != nil {
		t.Fatal(err)
	} else if len(aus) != 60 {
		t.Fatalf("expected 60 updates, got %d", len(aus))
	}
	if pruned, _ := srcCM.BestIndex(49); pruned.Height != 49 {
		t.Fatal("missing index at height 49")
	} else if _, _, err := dstCM.UpdatesSince(pruned, 100); !errors.Is(err, chain.ErrMissingBlock) {
		t.Fatalf("expected %v, got %v", chain.ErrMissingBlock, err)
	}

	// a reorg that replaces the checkpoint block itself must succeed, since
	// its parent state is retained
	forkStore, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	forkCM := chain.NewManager(forkStore)
	for height := uint64(1); height < 50; height++ {
		index, _ := srcCM.BestIndex(height)
		b, ok := srcCM.Block(index.ID)
		if !ok {
			t.Fatalf("missing source block at height %d", height)
		} else if err := forkCM.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
	}
	testutil.MineBlocks(t, forkCM, types.Address{1}, 65)
	var forkBlocks []types.Block
	for height := uint64(50); height <= forkCM.Tip().Height; height++ {
		index, _ := forkCM.BestIndex(height)
		b, _ := forkCM.Block(index.ID)
		forkBlocks = append(forkBlocks, b)
	}
	if err := dstCM.AddBlocks(forkBlocks); err != nil {
		t.Fatal(err)
	} else if dstCM.Tip() != forkCM.Tip() {
		t.Fatalf("expected tip %v, got %v", forkCM.Tip(), dstCM.Tip())
	}
	testutil.MineBlocks(t, dstCM, types.VoidAddress, 1)
}
