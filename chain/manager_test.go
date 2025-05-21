package chain

import (
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"lukechampine.com/frand"
)

func findBlockNonce(cs consensus.State, b *types.Block) {
	for b.ID().CmpWork(cs.ChildTarget) < 0 {
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

	store, tipState, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store, tipState)

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
			ancestorTimestamp, _ := store.AncestorTimestamp(b.ParentID)
			cs, _ = consensus.ApplyBlock(cs, b, store.SupplementTipBlock(b), ancestorTimestamp)
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

	store, tipState, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store, tipState)

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
	store, genesisState, err := NewDBStore(NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := NewManager(store, genesisState)
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
