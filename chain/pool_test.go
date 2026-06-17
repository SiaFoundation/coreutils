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

	store, genesisState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, genesisState)
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

func TestRemoveV2PoolTransactions(t *testing.T) {
	// setup returns a fresh manager along with a spendable siacoin element and
	// the key/policy needed to spend it.
	setup := func(t *testing.T) (*chain.Manager, types.ChainIndex, types.SiacoinElement, types.PrivateKey, types.SpendPolicy) {
		n, genesisBlock := testutil.V2Network()
		sk := types.GeneratePrivateKey()
		sp := types.PolicyPublicKey(sk.PublicKey())
		addr := sp.Address()

		store, genesisState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		cm := chain.NewManager(store, genesisState)
		es := testutil.NewElementStateStore(t, cm)

		testutil.MineBlocks(t, cm, addr, 20+int(n.MaturityDelay))
		es.Wait(t)

		cs := cm.TipState()
		basis, sces := es.SiacoinElements()
		for _, sce := range sces {
			if sce.SiacoinOutput.Address == addr && sce.MaturityHeight <= cs.Index.Height {
				return cm, basis, sce, sk, sp
			}
		}
		t.Fatal("no spendable element found")
		return nil, types.ChainIndex{}, types.SiacoinElement{}, nil, types.SpendPolicy{}
	}

	// spend builds and signs a transaction spending parent and sending the
	// remaining value (minus a fee) to dest.
	spend := func(cm *chain.Manager, sk types.PrivateKey, sp types.SpendPolicy, parent types.SiacoinElement, dest types.Address) types.V2Transaction {
		cs := cm.TipState()
		txn := types.V2Transaction{
			SiacoinInputs: []types.V2SiacoinInput{
				{Parent: parent, SatisfiedPolicy: types.SatisfiedPolicy{Policy: sp}},
			},
			MinerFee: types.Siacoins(1),
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: dest, Value: parent.SiacoinOutput.Value.Sub(types.Siacoins(1))},
			},
		}
		txn.SiacoinInputs[0].SatisfiedPolicy.Signatures = []types.Signature{sk.SignHash(cs.InputSigHash(txn))}
		return txn
	}

	t.Run("removes dependent transactions", func(t *testing.T) {
		cm, basis, parent, sk, sp := setup(t)
		parentTxn := spend(cm, sk, sp, parent, sp.Address())
		childTxn := spend(cm, sk, sp, parentTxn.EphemeralSiacoinOutput(0), types.VoidAddress)
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{parentTxn, childTxn}); err != nil {
			t.Fatal(err)
		} else if n := len(cm.V2PoolTransactions()); n != 2 {
			t.Fatalf("expected 2 pool transactions, got %d", n)
		}

		// removing the parent must also drop the child that depends on it
		cm.RemoveV2PoolTransactions([]types.TransactionID{parentTxn.ID()})
		if n := len(cm.V2PoolTransactions()); n != 0 {
			t.Fatalf("expected empty pool after removing parent, got %d", n)
		}
	})

	t.Run("keeps independent transactions", func(t *testing.T) {
		cm, basis, parent, sk, sp := setup(t)
		parentTxn := spend(cm, sk, sp, parent, sp.Address())
		childTxn := spend(cm, sk, sp, parentTxn.EphemeralSiacoinOutput(0), types.VoidAddress)
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{parentTxn, childTxn}); err != nil {
			t.Fatal(err)
		}

		// removing only the child must leave the parent in the pool
		cm.RemoveV2PoolTransactions([]types.TransactionID{childTxn.ID()})
		if _, ok := cm.V2PoolTransaction(parentTxn.ID()); !ok {
			t.Fatal("expected parent to remain in pool")
		} else if _, ok := cm.V2PoolTransaction(childTxn.ID()); ok {
			t.Fatal("expected child to be removed from pool")
		}
	})

	t.Run("no-op when transaction not in pool", func(t *testing.T) {
		cm, basis, parent, sk, sp := setup(t)
		txn := spend(cm, sk, sp, parent, types.VoidAddress)
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		}

		var notified int
		cancel := cm.OnPoolChange(func() { notified++ })
		defer cancel()

		// removing an unknown transaction must not change the pool or notify
		cm.RemoveV2PoolTransactions([]types.TransactionID{{1, 2, 3}})
		if n := len(cm.V2PoolTransactions()); n != 1 {
			t.Fatalf("expected pool to be unchanged, got %d", n)
		} else if notified != 0 {
			t.Fatalf("expected no notification for no-op removal, got %d", notified)
		}

		// removing a known transaction must notify listeners
		cm.RemoveV2PoolTransactions([]types.TransactionID{txn.ID()})
		if notified != 1 {
			t.Fatalf("expected 1 notification, got %d", notified)
		} else if n := len(cm.V2PoolTransactions()); n != 0 {
			t.Fatalf("expected empty pool, got %d", n)
		}
	})

	t.Run("frees inputs for reuse", func(t *testing.T) {
		cm, basis, parent, sk, sp := setup(t)
		first := spend(cm, sk, sp, parent, sp.Address())
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{first}); err != nil {
			t.Fatal(err)
		}

		// a different transaction reusing the same input conflicts while the
		// first transaction is still in the pool
		second := spend(cm, sk, sp, parent, types.VoidAddress)
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{second}); err == nil {
			t.Fatal("expected double-spend conflict while first transaction is in the pool")
		}

		// removing the first transaction frees the input so it can be reused
		cm.RemoveV2PoolTransactions([]types.TransactionID{first.ID()})
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{second}); err != nil {
			t.Fatalf("expected to reuse freed input after removal, got %v", err)
		}
	})
}
