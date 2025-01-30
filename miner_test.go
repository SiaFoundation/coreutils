package coreutils_test

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
)

func TestMiner(t *testing.T) {
	n, genesisBlock := testutil.Network()

	sk := types.GeneratePrivateKey()
	genesisBlock.Transactions = []types.Transaction{{
		SiacoinOutputs: []types.SiacoinOutput{
			{
				Address: types.StandardUnlockHash(sk.PublicKey()),
				Value:   types.Siacoins(10),
			},
		},
	}}

	store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)

	// create a transaction
	txn := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{{
			ParentID:         genesisBlock.Transactions[0].SiacoinOutputID(0),
			UnlockConditions: types.StandardUnlockConditions(sk.PublicKey()),
		}},
		SiacoinOutputs: []types.SiacoinOutput{{
			Address: types.StandardUnlockHash(sk.PublicKey()),
			Value:   types.Siacoins(9),
		}},
		MinerFees: []types.Currency{types.Siacoins(1)},
	}

	// sign the inputs
	for _, sci := range txn.SiacoinInputs {
		sig := sk.SignHash(cm.TipState().WholeSigHash(txn, types.Hash256(sci.ParentID), 0, 0, nil))
		txn.Signatures = append(txn.Signatures, types.TransactionSignature{
			ParentID:       types.Hash256(sci.ParentID),
			CoveredFields:  types.CoveredFields{WholeTransaction: true},
			PublicKeyIndex: 0,
			Signature:      sig[:],
		})
	}

	// add the transaction to the pool
	_, err = cm.AddPoolTransactions([]types.Transaction{txn})
	if err != nil {
		t.Fatal(err)
	}

	// assert the minerpayout includes the txn fee
	b, found := coreutils.MineBlock(cm, types.VoidAddress, time.Second)
	if !found {
		t.Fatal("PoW failed")
	} else if len(b.MinerPayouts) != 1 {
		t.Fatal("expected one miner payout")
	} else if b.MinerPayouts[0].Value.Cmp(types.Siacoins(1).Add(cm.TipState().BlockReward())) != 0 {
		t.Fatal("unexpected miner payout", b.MinerPayouts[0].Value.ExactString())
	}
}

func TestV2MineBlocks(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	n.HardforkV2.AllowHeight = 5
	n.HardforkV2.RequireHeight = 10
	n.InitialTarget = types.BlockID{0xFF}

	genesisBlock.Transactions = []types.Transaction{{
		SiacoinOutputs: []types.SiacoinOutput{
			{
				Address: types.AnyoneCanSpend().Address(),
				Value:   types.Siacoins(10),
			},
		},
	}}

	store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)

	mineBlocks := func(t *testing.T, n int) {
		for ; n > 0; n-- {
			b, ok := coreutils.MineBlock(cm, types.VoidAddress, time.Second)
			if !ok {
				t.Fatal("failed to mine block")
			} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// mine until just before the allow height
	mineBlocks(t, 4)

	elements := make(map[types.SiacoinOutputID]types.SiacoinElement)
	_, applied, err := cm.UpdatesSince(types.ChainIndex{}, 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, cau := range applied {
		for _, sced := range cau.SiacoinElementDiffs() {
			sce := sced.SiacoinElement
			if sce.SiacoinOutput.Address == types.AnyoneCanSpend().Address() {
				if sced.Created {
					elements[sce.ID] = sce
				}
				if sced.Spent {
					delete(elements, sce.ID)
				}
			}
		}
		for k, v := range elements {
			cau.UpdateElementProof(&v.StateElement)
			elements[k] = v
		}
	}

	var se types.SiacoinElement
	for _, v := range elements {
		se = v
		break
	}

	txn := types.V2Transaction{
		MinerFee: se.SiacoinOutput.Value,
		SiacoinInputs: []types.V2SiacoinInput{{
			Parent:          se,
			SatisfiedPolicy: types.SatisfiedPolicy{Policy: types.AnyoneCanSpend()},
		}},
	}

	// add the transaction to the pool
	_, err = cm.AddV2PoolTransactions(cm.Tip(), []types.V2Transaction{txn})
	if err != nil {
		t.Fatal(err)
	}

	mineBlocks(t, 1)
}
