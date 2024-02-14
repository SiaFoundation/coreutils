package coreutils

import (
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

func TestMiner(t *testing.T) {
	n, genesisBlock := chain.TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}

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
	b, found := MineBlock(cm, types.VoidAddress, time.Second)
	if !found {
		t.Fatal("PoW failed")
	} else if len(b.MinerPayouts) != 1 {
		t.Fatal("expected one miner payout")
	} else if b.MinerPayouts[0].Value.Cmp(types.Siacoins(1).Add(cm.TipState().BlockReward())) != 0 {
		t.Fatal("unexpected miner payout", b.MinerPayouts[0].Value.ExactString())
	}
}
