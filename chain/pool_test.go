package chain_test

import (
	"slices"
	"strings"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
)

func TestAddV2PoolTransactionsRecover(t *testing.T) {
	n, genesisBlock := testutil.V2Network()

	sk := types.GeneratePrivateKey()
	sp := types.PolicyPublicKey(sk.PublicKey())
	addr := sp.Address()

	store, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store)
	es := testutil.NewElementStateStore(t, cm)

	testutil.MineBlocks(t, cm, addr, 20+int(n.MaturityDelay))
	es.Wait(t)

	cs := cm.TipState()
	basis, sces := es.SiacoinElements()

	// pick the earliest element to use for the corrupt transaction
	i := -1
	var selected types.SiacoinElement
	for j, sce := range sces {
		if sce.SiacoinOutput.Address != addr || sce.MaturityHeight > cs.Index.Height {
			continue
		}
		if i == -1 || sce.StateElement.LeafIndex < selected.StateElement.LeafIndex {
			i = j
			selected = sce
		}
	}
	if i == -1 {
		t.Fatal("no valid SiacoinElement found")
	}
	sces = slices.Delete(sces, i, i)

	// spend all the other utxos to create a large tree diff
	for _, sce := range sces {
		if sce.SiacoinOutput.Address != addr || sce.MaturityHeight > cs.Index.Height {
			continue
		}

		txn := types.V2Transaction{
			SiacoinInputs: []types.V2SiacoinInput{
				{
					Parent: sce,
					SatisfiedPolicy: types.SatisfiedPolicy{
						Policy: sp,
					},
				},
			},
			MinerFee: types.Siacoins(1),
			SiacoinOutputs: []types.SiacoinOutput{
				{
					Address: addr,
					Value:   sce.SiacoinOutput.Value.Sub(types.Siacoins(1)),
				},
			},
		}
		sigHash := cs.InputSigHash(txn)
		txn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{sk.SignHash(sigHash)}

		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		}
	}

	// create a transaction with the selected element but corrupt its proof
	selected.StateElement.MerkleProof = selected.StateElement.MerkleProof[1:]
	txn := types.V2Transaction{
		SiacoinInputs: []types.V2SiacoinInput{
			{
				Parent: selected,
				SatisfiedPolicy: types.SatisfiedPolicy{
					Policy: sp,
				},
			},
		},
		SiacoinOutputs: []types.SiacoinOutput{
			{
				Address: types.VoidAddress,
				Value:   selected.SiacoinOutput.Value,
			},
		},
	}
	sigHash := cs.InputSigHash(txn)
	txn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{sk.SignHash(sigHash)}

	// mine blocks to require an update
	testutil.MineBlocks(t, cm, addr, 20)
	es.Wait(t)

	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err == nil || !strings.Contains(err.Error(), "invalid Merkle proof") {
		t.Fatalf("expected invalid transaction, got %v", err)
	}
}

func TestAddV2PoolTransactionsEphemeralValue(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	if n.HardforkV2.EphemeralOutputHeight == 0 {
		t.Fatal("expected a non-zero EphemeralOutputHeight")
	}

	sk := types.GeneratePrivateKey()
	sp := types.PolicyPublicKey(sk.PublicKey())
	addr := sp.Address()

	store, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store)
	es := testutil.NewElementStateStore(t, cm)

	testutil.MineBlocks(t, cm, addr, 5+int(n.MaturityDelay))
	es.Wait(t)

	cs := cm.TipState()
	basis, sces := es.SiacoinElements()

	var parent types.SiacoinElement
	for _, sce := range sces {
		if sce.SiacoinOutput.Address == addr && sce.MaturityHeight <= cs.Index.Height {
			parent = sce
			break
		}
	}
	if parent.ID == (types.SiacoinOutputID{}) {
		t.Fatal("no valid SiacoinElement found")
	}

	fee := types.Siacoins(1)
	ephemeralValue := parent.SiacoinOutput.Value.Sub(fee)
	parentTxn := types.V2Transaction{
		SiacoinInputs: []types.V2SiacoinInput{{
			Parent:          parent,
			SatisfiedPolicy: types.SatisfiedPolicy{Policy: sp},
		}},
		MinerFee: fee,
		SiacoinOutputs: []types.SiacoinOutput{{
			Address: addr,
			Value:   ephemeralValue,
		}},
	}
	parentTxn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{sk.SignHash(cs.InputSigHash(parentTxn))}

	ephemeral := parentTxn.EphemeralSiacoinOutput(0)
	ephemeral.SiacoinOutput.Value = ephemeralValue.Add(types.Siacoins(1000))
	childTxn := types.V2Transaction{
		SiacoinInputs: []types.V2SiacoinInput{{
			Parent:          ephemeral,
			SatisfiedPolicy: types.SatisfiedPolicy{Policy: sp},
		}},
		MinerFee: fee,
		SiacoinOutputs: []types.SiacoinOutput{{
			Address: types.VoidAddress,
			Value:   ephemeral.SiacoinOutput.Value.Sub(fee),
		}},
	}
	childTxn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{sk.SignHash(cs.InputSigHash(childTxn))}

	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{parentTxn, childTxn}); err == nil || !strings.Contains(err.Error(), "claims incorrect value") {
		t.Fatalf("expected incorrect value error, got %v", err)
	}

	ephemeral = parentTxn.EphemeralSiacoinOutput(0)
	childTxn.SiacoinInputs[0].Parent = ephemeral
	childTxn.SiacoinOutputs[0].Value = ephemeral.SiacoinOutput.Value.Sub(fee)
	childTxn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{sk.SignHash(cs.InputSigHash(childTxn))}

	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{parentTxn, childTxn}); err != nil {
		t.Fatalf("expected honest set to be accepted, got %v", err)
	}
}
