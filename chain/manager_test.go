package chain

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

// ForceRevertTip forces the manager to revert the tip block, regardless of whether
// the tip is valid or not. This is useful for testing purposes, where we want to
// simulate a reorganization of the blockchain.
func (m *Manager) ForceRevertTip() error {
	sp, unlock := m.scratchpad()
	defer unlock()

	if err := m.revertTip(sp); err != nil {
		return err
	}
	return sp.Flush()
}

func findBlockNonce(cs consensus.State, b *types.Block) {
	for b.ID().CmpWork(cs.PoWTarget()) < 0 {
		b.Nonce += cs.NonceFactor()
		// ensure nonce meets factor requirement
		for b.Nonce%cs.NonceFactor() != 0 {
			b.Nonce++
		}
	}
}

func TestManager(t *testing.T) {
	n, genesisBlock := TestnetZen()

	n.InitialTarget = types.BlockID{0xFF}

	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)

	sp := store.Scratchpad()
	mine := func(cs consensus.State, n int) (blocks []types.Block) {
		for i := 0; i < n; i++ {
			b := types.Block{
				ParentID:  cs.Index.ID,
				Timestamp: types.CurrentTimestamp(),
				MinerPayouts: []types.SiacoinOutput{{
					Value:   cs.BlockReward(),
					Address: types.Address(frand.Entropy256()),
				}},
			}
			findBlockNonce(cs, &b)
			ancestorTimestamp, _ := sp.AncestorTimestamp(b.ParentID)
			cs, _ = consensus.ApplyBlock(cs, b, sp.SupplementTipBlock(b), ancestorTimestamp)
			blocks = append(blocks, b)
		}
		return
	}

	var reorgs []uint64
	cm.OnReorg(func(index types.ChainIndex) {
		reorgs = append(reorgs, index.Height)
	})

	// mine two chains
	chain1 := mine(cm.TipState(), 5)
	chain2 := mine(cm.TipState(), 7)

	// give the lighter chain to the manager, then the heavier chain
	if err := cm.AddBlocks(chain1); err != nil {
		t.Fatal(err)
	}
	if err := cm.AddBlocks(chain2); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reorgs, []uint64{5, 7}) {
		t.Error("wrong reorg history:", reorgs)
	}

	// get full update history
	rus, aus, err := cm.UpdatesSince(types.ChainIndex{}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(rus) != 0 && len(aus) != 8 {
		t.Fatal("wrong number of updates:", len(rus), len(aus))
	}
	var path []uint64
	for _, au := range aus {
		path = append(path, au.State.Index.Height)
	}
	if !reflect.DeepEqual(path, []uint64{0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Error("wrong update path:", path)
	}

	// get update history from the middle of the lighter chain
	rus, aus, err = cm.UpdatesSince(types.ChainIndex{Height: 3, ID: chain1[3].ParentID}, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(rus) != 3 && len(aus) != 7 {
		t.Fatal("wrong number of updates:", len(rus), len(aus))
	}
	path = nil
	for _, ru := range rus {
		path = append(path, ru.State.Index.Height)
	}
	for _, au := range aus {
		path = append(path, au.State.Index.Height)
	}
	if !reflect.DeepEqual(path, []uint64{2, 1, 0, 1, 2, 3, 4, 5, 6, 7}) {
		t.Error("wrong update path:", path)
	}
}

func TestTxPool(t *testing.T) {
	n, genesisBlock := TestnetZen()

	n.InitialTarget = types.BlockID{0xFF}

	giftPrivateKey := types.GeneratePrivateKey()
	giftPublicKey := giftPrivateKey.PublicKey()
	giftAddress := types.StandardUnlockHash(giftPublicKey)
	giftAmountSC := types.Siacoins(100)
	giftTxn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: giftAddress, Value: giftAmountSC},
		},
	}
	genesisBlock.Transactions = []types.Transaction{giftTxn}

	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)

	// add a listener
	var changeSets [][]types.TransactionID
	cm.OnPoolChange(func() {
		var ids []types.TransactionID
		for _, txn := range cm.PoolTransactions() {
			ids = append(ids, txn.ID())
		}
		changeSets = append(changeSets, ids)
	})

	signTxn := func(txn *types.Transaction) {
		for _, sci := range txn.SiacoinInputs {
			sig := giftPrivateKey.SignHash(cm.TipState().WholeSigHash(*txn, types.Hash256(sci.ParentID), 0, 0, nil))
			txn.Signatures = append(txn.Signatures, types.TransactionSignature{
				ParentID:       types.Hash256(sci.ParentID),
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				PublicKeyIndex: 0,
				Signature:      sig[:],
			})
		}
	}

	// add a transaction to the pool
	parentTxn := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{{
			ParentID:         giftTxn.SiacoinOutputID(0),
			UnlockConditions: types.StandardUnlockConditions(giftPublicKey),
		}},
		SiacoinOutputs: []types.SiacoinOutput{{
			Address: giftAddress,
			Value:   giftAmountSC,
		}},
	}
	signTxn(&parentTxn)
	if known, err := cm.AddPoolTransactions([]types.Transaction{parentTxn}); known || err != nil {
		t.Fatal(err)
	} else if _, ok := cm.PoolTransaction(parentTxn.ID()); !ok {
		t.Fatal("pool should contain parent transaction")
	} else if len(changeSets) != 1 || len(changeSets[0]) != 1 || changeSets[0][0] != parentTxn.ID() {
		t.Fatal("wrong change set:", changeSets)
	}

	// add another transaction, dependent on the first
	childTxn := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{{
			ParentID:         parentTxn.SiacoinOutputID(0),
			UnlockConditions: types.StandardUnlockConditions(giftPublicKey),
		}},
		MinerFees: []types.Currency{giftAmountSC},
	}
	signTxn(&childTxn)
	// submitted alone, it should be rejected
	if known, err := cm.AddPoolTransactions([]types.Transaction{childTxn}); known || err == nil {
		t.Fatal("child transaction without parent should be rejected")
	} else if _, ok := cm.PoolTransaction(childTxn.ID()); ok {
		t.Fatal("pool should not contain child transaction")
	} else if len(changeSets) != 1 {
		t.Fatal("wrong change set:", changeSets)
	}
	// the pool should identify the parent
	if parents := cm.UnconfirmedParents(childTxn); len(parents) != 1 || parents[0].ID() != parentTxn.ID() {
		t.Fatal("pool should identify parent of child transaction")
	}
	// submitted together, the set should be accepted
	if known, err := cm.AddPoolTransactions([]types.Transaction{parentTxn, childTxn}); known || err != nil {
		t.Fatal(err)
	} else if _, ok := cm.PoolTransaction(childTxn.ID()); !ok {
		t.Fatal("pool should contain child transaction")
	} else if len(cm.PoolTransactions()) != 2 {
		t.Fatal("pool should contain both transactions")
	} else if len(changeSets) != 2 || len(changeSets[1]) != 2 || changeSets[1][0] != parentTxn.ID() || changeSets[1][1] != childTxn.ID() {
		t.Fatal("wrong change set:", changeSets)
	}

	// mine a block containing the transactions
	b := types.Block{
		ParentID:  cm.TipState().Index.ID,
		Timestamp: types.CurrentTimestamp(),
		MinerPayouts: []types.SiacoinOutput{{
			Value:   cm.TipState().BlockReward().Add(giftAmountSC),
			Address: types.Address(frand.Entropy256()),
		}},
		Transactions: cm.PoolTransactions(),
	}
	findBlockNonce(cm.TipState(), &b)
	if err := cm.AddBlocks([]types.Block{b}); err != nil {
		t.Fatal(err)
	}

	// the pool should be empty
	if len(cm.PoolTransactions()) != 0 {
		t.Fatal("pool should be empty after mining")
	} else if len(changeSets) != 3 || len(changeSets[2]) != 0 {
		t.Fatal("wrong change set:", changeSets)
	}
}

func TestUpdateV2TransactionSet(t *testing.T) {
	n, genesisBlock := TestnetZen()

	n.InitialTarget = types.BlockID{0xFF}
	n.HardforkDevAddr.Height = 0
	n.HardforkTax.Height = 0
	n.HardforkStorageProof.Height = 0
	n.HardforkOak.Height = 0
	n.HardforkOak.FixHeight = 0
	n.HardforkASIC.Height = 0
	n.HardforkFoundation.Height = 0
	n.HardforkV2.AllowHeight = 0

	giftPrivateKey := types.GeneratePrivateKey()
	giftPublicKey := giftPrivateKey.PublicKey()
	giftAddress := types.StandardAddress(giftPublicKey)
	giftAmountSC := types.Siacoins(100)
	giftTxn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: giftAddress, Value: giftAmountSC},
		},
	}
	genesisBlock.Transactions = []types.Transaction{giftTxn}

	// construct a transaction that's valid at genesis
	bs := consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, 1)}
	cs, cau := consensus.ApplyBlock(n.GenesisState(), genesisBlock, bs, time.Time{})
	txn := types.V2Transaction{
		SiacoinInputs: []types.V2SiacoinInput{{
			Parent: cau.SiacoinElementDiffs()[0].SiacoinElement.Copy(),
			SatisfiedPolicy: types.SatisfiedPolicy{
				Policy:     types.PolicyPublicKey(giftPublicKey),
				Signatures: []types.Signature{},
			},
		}},
		MinerFee: giftAmountSC,
	}
	txn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{giftPrivateKey.SignHash(cs.InputSigHash(txn))}

	ms := consensus.NewMidState(cs)
	if err := consensus.ValidateV2Transaction(ms, txn); err != nil {
		t.Fatal(err)
	}

	// initialize chain manager and mine a mix of v1 and v2 blocks
	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)
	genesisState := cm.TipState()
	for range 10 {
		cs := cm.TipState()
		b := types.Block{
			ParentID:  cs.Index.ID,
			Timestamp: types.CurrentTimestamp(),
			MinerPayouts: []types.SiacoinOutput{{
				Value:   cs.BlockReward(),
				Address: frand.Entropy256(),
			}},
		}
		if cs.Index.Height%2 == 0 {
			b.V2 = &types.V2BlockData{
				Height:     cs.Index.Height + 1,
				Commitment: cs.Commitment(b.MinerPayouts[0].Address, b.Transactions, nil),
			}
		}

		findBlockNonce(cs, &b)
		if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
	}

	// transaction should not be valid at tip
	ms = consensus.NewMidState(cm.TipState())
	if err := consensus.ValidateV2Transaction(ms, txn); err == nil {
		t.Fatal("transaction should not be valid at tip")
	}

	// update the transaction
	txns := []types.V2Transaction{txn}
	txns, err = cm.UpdateV2TransactionSet(txns, genesisState.Index, cm.Tip())
	if err != nil {
		t.Fatal(err)
	}
	// the transaction should now be valid
	ms = consensus.NewMidState(cm.TipState())
	if err := consensus.ValidateV2Transaction(ms, txns[0]); err != nil {
		t.Fatal(err)
	}
}

func TestFullTxPool(t *testing.T) {
	n, genesisBlock := TestnetZen()

	n.InitialTarget = types.BlockID{0xFF}

	giftPrivateKey := types.GeneratePrivateKey()
	giftPublicKey := giftPrivateKey.PublicKey()
	giftAddress := types.StandardUnlockHash(giftPublicKey)
	giftTxn := types.Transaction{
		SiacoinOutputs: make([]types.SiacoinOutput, 99),
	}
	for i := range giftTxn.SiacoinOutputs {
		giftTxn.SiacoinOutputs[i].Address = giftAddress
		giftTxn.SiacoinOutputs[i].Value = types.Siacoins(100)
	}
	genesisBlock.Transactions = []types.Transaction{giftTxn}

	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)

	signTxn := func(txn *types.Transaction) {
		for _, sci := range txn.SiacoinInputs {
			sig := giftPrivateKey.SignHash(cm.TipState().WholeSigHash(*txn, types.Hash256(sci.ParentID), 0, 0, nil))
			txn.Signatures = append(txn.Signatures, types.TransactionSignature{
				ParentID:       types.Hash256(sci.ParentID),
				CoveredFields:  types.CoveredFields{WholeTransaction: true},
				PublicKeyIndex: 0,
				Signature:      sig[:],
			})
		}
	}

	// add transactions (with decreasing fees) to the pool
	var ids []types.TransactionID
	for i := range giftTxn.SiacoinOutputs {
		txn := types.Transaction{
			SiacoinInputs: []types.SiacoinInput{{
				ParentID:         giftTxn.SiacoinOutputID(i),
				UnlockConditions: types.StandardUnlockConditions(giftPublicKey),
			}},
			SiacoinOutputs: []types.SiacoinOutput{{
				Address: types.VoidAddress,
				Value:   types.Siacoins(uint32(i + 1)), // can't create zero-valued outputs
			}},
			MinerFees:     []types.Currency{types.Siacoins(uint32(100 - (i + 1)))},
			ArbitraryData: [][]byte{make([]byte, 500e3)},
		}
		signTxn(&txn)
		ids = append(ids, txn.ID())
		if known, err := cm.AddPoolTransactions([]types.Transaction{txn}); known || err != nil {
			t.Fatal(err)
		} else if _, ok := cm.PoolTransaction(txn.ID()); !ok {
			break
		}
	}
	// the highest-fee transaction should be in the pool, but not the lowest-fee
	if _, ok := cm.PoolTransaction(ids[0]); !ok {
		t.Fatal("high-fee transaction should be in the pool")
	} else if _, ok := cm.PoolTransaction(ids[len(ids)-1]); ok {
		t.Fatal("low-fee transaction should not be in the pool")
	}
}

func TestNewDBStoreAtCheckpoint(t *testing.T) {
	n, genesisBlock := TestnetZen()
	genesisBlock.Transactions = nil
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	n.HardforkV2.FinalCutHeight = 1

	// mine to checkpoint
	checkpointParent := n.GenesisState()
	checkpointBlock := genesisBlock
	for range 20 {
		checkpointParent, _ = consensus.ApplyBlock(checkpointParent, checkpointBlock, consensus.V1BlockSupplement{}, time.Time{})
		checkpointBlock = types.Block{
			ParentID:  checkpointParent.Index.ID,
			Timestamp: types.CurrentTimestamp(),
			MinerPayouts: []types.SiacoinOutput{{
				Value:   checkpointParent.BlockReward(),
				Address: types.VoidAddress,
			}},
		}
		findBlockNonce(checkpointParent, &checkpointBlock)
	}
	checkpointState, _ := consensus.ApplyBlock(checkpointParent, checkpointBlock, consensus.V1BlockSupplement{}, time.Time{})

	t.Run("DBThenCheckpoint", func(t *testing.T) {
		db := NewMemDB()
		_, _ = NewDBStore(db, n, genesisBlock, nil)
		db.Flush()
		store, err := NewDBStoreAtCheckpoint(db, checkpointParent, checkpointBlock, nil)
		if err != nil {
			t.Fatal(err)
		} else if store.Scratchpad().TipState().Index != checkpointState.Index {
			t.Fatal("DB should be initialized at checkpoint")
		}
	})

	t.Run("CheckpointThenDB", func(t *testing.T) {
		db := NewMemDB()
		_, err := NewDBStoreAtCheckpoint(db, checkpointParent, checkpointBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		db.Flush()
		store, err := NewDBStore(db, n, genesisBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		if store.Scratchpad().TipState().Index != checkpointState.Index {
			t.Fatal("DB should remain at checkpoint")
		}
	})

	t.Run("CheckpointTwice", func(t *testing.T) {
		db := NewMemDB()
		_, err := NewDBStoreAtCheckpoint(db, checkpointParent, checkpointBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		db.Flush()
		store, err := NewDBStoreAtCheckpoint(db, checkpointParent, checkpointBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		if store.Scratchpad().TipState().Index != checkpointState.Index {
			t.Fatal("DB should remain at checkpoint")
		}
	})

	t.Run("DifferentNetwork", func(t *testing.T) {
		mainnet, mainnetGenesis := Mainnet()
		db := NewMemDB()
		_, err := NewDBStore(db, mainnet, mainnetGenesis, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = NewDBStoreAtCheckpoint(db, checkpointParent, checkpointBlock, nil)
		if err == nil {
			t.Fatal("expected error when initializing with different network")
		}
	})
}

func TestMinReorgIndex(t *testing.T) {
	n, genesisBlock := TestnetZen()
	genesisBlock.Transactions = nil
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	n.HardforkV2.FinalCutHeight = 1

	// mine to checkpoint
	cs := n.GenesisState()
	b := genesisBlock
	var blocks []types.Block
	for range 20 {
		cs, _ = consensus.ApplyBlock(cs, b, consensus.V1BlockSupplement{}, time.Time{})
		b = types.Block{
			ParentID:  cs.Index.ID,
			Timestamp: types.CurrentTimestamp(),
			MinerPayouts: []types.SiacoinOutput{{
				Value:   cs.BlockReward(),
				Address: types.VoidAddress,
			}},
		}
		findBlockNonce(cs, &b)
		blocks = append(blocks, b)
	}
	checkpointState, _ := consensus.ApplyBlock(cs, b, consensus.V1BlockSupplement{}, time.Time{})

	// initialize manager at checkpoint
	store, err := NewDBStoreAtCheckpoint(NewMemDB(), cs, b, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)
	// min reorg index should be at checkpoint
	if minReorg := cm.MinReorgIndex(); minReorg != checkpointState.Index {
		t.Fatal("unexpected min reorg index:", minReorg, "expected:", checkpointState.Index)
	}

	// reinitialize at genesis
	store, err = NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm = NewManager(store)
	if minReorg := cm.MinReorgIndex(); minReorg.ID != genesisBlock.ID() {
		t.Fatal("unexpected min reorg index:", minReorg, "expected:", genesisBlock.ID())
	}
	// add intervening blocks
	if err := cm.AddBlocks(blocks); err != nil {
		t.Fatal(err)
	} else if cm.Tip() != checkpointState.Index {
		t.Fatal("tip mismatch:", cm.Tip(), "expected:", checkpointState.Index)
	}
	// min reorg index should still be at genesis
	if minReorg := cm.MinReorgIndex(); minReorg.ID != genesisBlock.ID() {
		t.Fatal("unexpected min reorg index:", minReorg, "expected:", genesisBlock.ID())
	}
}

// mineEmptyBlocks mines n blocks containing only a miner payout. Returns the
// chain manager's tip after mining.
func mineEmptyBlocks(t *testing.T, cm *Manager, n int) types.ChainIndex {
	t.Helper()
	for range n {
		cs := cm.TipState()
		b := types.Block{
			ParentID:  cs.Index.ID,
			Timestamp: types.CurrentTimestamp(),
			MinerPayouts: []types.SiacoinOutput{{
				Value:   cs.BlockReward(),
				Address: types.Address(frand.Entropy256()),
			}},
		}
		findBlockNonce(cs, &b)
		if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
		println("mined block at height", cs.Index.Height+1)
	}
	return cm.Tip()
}

// TestReorgPathMaxLen exercises the maxLen parameter of reorgPath: paths
// at or below the limit return cleanly, paths exceeding it return an error
// containing "too long".
func TestReorgPathMaxLen(t *testing.T) {
	n, genesisBlock := TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	n.HardforkV2.FinalCutHeight = 1
	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)

	const chainLen = 200
	tip := mineEmptyBlocks(t, cm, chainLen)
	genesisIdx := types.ChainIndex{Height: 0, ID: genesisBlock.ID()}

	cases := []struct {
		name      string
		maxLen    int
		shouldErr bool
	}{
		{"large maxLen", chainLen * 5, false},
		{"exactly path length", chainLen, false},
		{"one below path length", chainLen - 1, true},
		{"well below path length", chainLen / 2, true},
		{"zero maxLen", 0, true},
		{"math.MaxInt", math.MaxInt, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sp, unlock := cm.scratchpad()
			defer unlock()
			_, _, err := cm.reorgPath(sp, genesisIdx, tip, tc.maxLen)
			if tc.shouldErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "too long") {
					t.Fatalf("expected 'too long' error, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestReorgPathBogusBasisBailsFast verifies that reorgPath rejects a bogus
// basis cheaply via its in-loop maxLen guard.
func TestReorgPathBogusBasisBailsFast(t *testing.T) {
	n, genesisBlock := TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	n.HardforkV2.FinalCutHeight = 1
	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)

	// mine enough real blocks that the in-loop cap fires before the rewind
	// reaches genesis (and would otherwise error with ErrMissingBlock).
	tip := mineEmptyBlocks(t, cm, 200)

	// real tip ID, but height far past tip
	bogus := types.ChainIndex{Height: math.MaxUint64, ID: tip.ID}

	const maxLen = 144
	sp, unlock := cm.scratchpad()
	defer unlock()
	revert, apply, err := cm.reorgPath(sp, bogus, tip, maxLen)
	if err == nil {
		t.Fatal("expected error for bogus basis, got nil")
	}
	if !strings.Contains(err.Error(), "too long") {
		t.Fatalf("expected 'too long' error, got %v", err)
	}
	// Cap should fire one iteration after exceeding maxLen, so revert+apply
	// is bounded near maxLen+1. Anything beyond that means the in-loop check
	// is missing or has regressed.
	if pathLen := len(revert) + len(apply); pathLen > maxLen+2 {
		t.Fatalf("revert+apply grew to %d, expected ≤ %d (maxLen+2)", pathLen, maxLen+2)
	}
}

func TestReadsDoNotBlockOnWriters(t *testing.T) {
	n, genesisBlock := TestnetZen()
	genesisBlock.Transactions = nil
	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)
	tipState := cm.TipState()

	// simulate a long-running writer by holding the write lock; readers
	// should still be able to observe the snapshot
	_, unlock := cm.scratchpad()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if cm.TipState().Index != tipState.Index {
			t.Error("unexpected tip state")
		}
		if _, ok := cm.Block(genesisBlock.ID()); !ok {
			t.Error("missing genesis block")
		}
		if _, ok := cm.BestIndex(0); !ok {
			t.Error("missing genesis index")
		}
		if _, _, err := cm.UpdatesSince(types.ChainIndex{}, 10); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reads blocked while writer held the write lock")
	}
	unlock()
}

// TestManagerConcurrentReads exercises the Manager's read methods from many
// goroutines while blocks are concurrently added, verifying (particularly
// under the race detector) that readers observe consistent snapshots without
// blocking on writers.
func TestManagerConcurrentReads(t *testing.T) {
	n, genesisBlock := TestnetZen()
	genesisBlock.Transactions = nil
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	n.HardforkV2.FinalCutHeight = 1
	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				// each of these should observe a consistent snapshot; since
				// the chain is extended linearly, blocks and best indices
				// visible in one call must remain visible in the next
				tip := cm.Tip()
				if cs := cm.TipState(); cs.Index.Height < tip.Height {
					t.Error("tip height regressed:", cs.Index, tip)
				}
				if index, ok := cm.BestIndex(tip.Height); !ok || index != tip {
					t.Error("tip not on best chain:", tip, index)
				}
				if b, ok := cm.Block(tip.ID); !ok {
					t.Error("missing tip block:", tip)
				} else if b.ID() != tip.ID {
					t.Error("block ID mismatch:", b.ID(), tip.ID)
				}
				if _, ok := cm.State(tip.ID); !ok {
					t.Error("missing tip state:", tip)
				}
				if _, err := cm.History(); err != nil {
					t.Error(err)
				}
				if _, _, err := cm.UpdatesSince(tip, 10); err != nil {
					t.Error(err)
				}
				if _, _, err := cm.Headers(tip, 10); err != nil {
					t.Error(err)
				}
				if _, _, err := cm.BlocksForHistory([]types.BlockID{tip.ID}, 10); err != nil {
					t.Error(err)
				}
				cm.MinReorgIndex()
				cm.PoolTransactions()
				cm.RecommendedFee()
			}
		}()
	}

	mineEmptyBlocks(t, cm, 25)
	close(stop)
	wg.Wait()

	if cm.Tip().Height != 25 {
		t.Fatal("expected tip height 25, got", cm.Tip().Height)
	}
}

// TestSnapshotDoesNotBlockWrites verifies that a long-lived snapshot neither
// stalls writers nor observes their effects.
func TestSnapshotDoesNotBlockWrites(t *testing.T) {
	n, genesisBlock := TestnetZen()
	genesisBlock.Transactions = nil
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	n.HardforkV2.FinalCutHeight = 1
	store, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store)
	tipState := cm.TipState()

	// hold a snapshot across several AddBlocks calls; if snapshots excluded
	// writers, this would deadlock
	snap, release := store.Snapshot()
	defer release()
	mineEmptyBlocks(t, cm, 5)

	// the Manager should have advanced...
	if cm.Tip().Height != tipState.Index.Height+5 {
		t.Fatal("expected tip to advance to height 5, got", cm.Tip().Height)
	}
	// ...while the held snapshot still reflects the old tip
	if snap.TipState().Index != tipState.Index {
		t.Fatal("snapshot tip changed:", snap.TipState().Index)
	} else if _, ok := snap.BestIndex(tipState.Index.Height + 1); ok {
		t.Fatal("snapshot should not see new blocks")
	}

	// a snapshot taken now should see the new tip
	snap2, release2 := store.Snapshot()
	defer release2()
	if snap2.TipState().Index != cm.Tip() {
		t.Fatal("new snapshot tip mismatch:", snap2.TipState().Index, cm.Tip())
	}
}
