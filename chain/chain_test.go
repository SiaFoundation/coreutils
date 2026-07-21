package chain_test

import (
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"lukechampine.com/frand"
)

type memState struct {
	index              types.ChainIndex
	utxos              map[types.SiacoinOutputID]types.SiacoinElement
	chainIndexElements []types.ChainIndexElement
}

// Sync updates the memState to match the current state of the chain manager.
func (ms *memState) Sync(t *testing.T, cm *chain.Manager) {
	for cm.Tip() != ms.index {
		reverted, applied, err := cm.UpdatesSince(ms.index, 100)
		if err != nil {
			t.Fatal(err)
		}

		for _, cru := range reverted {
			revertedIndex := types.ChainIndex{
				ID:     cru.Block.ID(),
				Height: cru.State.Index.Height + 1,
			}

			// remove chain index element
			if len(ms.chainIndexElements) > 0 {
				last := ms.chainIndexElements[len(ms.chainIndexElements)-1].Copy()
				if last.ChainIndex != revertedIndex {
					t.Fatalf("expected reverted index %v, got %v", last.ChainIndex, revertedIndex)
				}
				ms.chainIndexElements = ms.chainIndexElements[:len(ms.chainIndexElements)-1]
			}

			// revert utxos
			for _, sced := range cru.SiacoinElementDiffs() {
				sce := &sced.SiacoinElement
				if sce.SiacoinOutput.Address == types.AnyoneCanSpend().Address() {
					if sced.Spent {
						ms.utxos[sce.ID] = sce.Copy()
					}
					if sced.Created {
						delete(ms.utxos, sce.ID)
					}
				}
			}

			// update utxos proofs
			for key, se := range ms.utxos {
				cru.UpdateElementProof(&se.StateElement)
				ms.utxos[key] = se.Copy()
			}
			ms.index = cru.State.Index
		}

		for _, cau := range applied {
			// update chain index elements
			for i := range ms.chainIndexElements {
				cau.UpdateElementProof(&ms.chainIndexElements[i].StateElement)
			}
			// append new chain index element
			ms.chainIndexElements = append(ms.chainIndexElements, cau.ChainIndexElement())

			// apply utxos
			for _, sced := range cau.SiacoinElementDiffs() {
				sce := &sced.SiacoinElement
				if sce.SiacoinOutput.Address == types.AnyoneCanSpend().Address() {
					if sced.Created {
						ms.utxos[sce.ID] = sce.Copy()
					}
					if sced.Spent {
						delete(ms.utxos, sce.ID)
					}
				}
			}

			// update utxos proofs
			for key, se := range ms.utxos {
				cau.UpdateElementProof(&se.StateElement)
				ms.utxos[key] = se.Move()
			}
			ms.index = cau.State.Index
		}
	}
}

// SpendableElement returns the first spendable Siacoin utxo.
func (ms *memState) SpendableElement(t *testing.T) (se types.SiacoinElement) {
	for _, se = range ms.utxos {
		if se.MaturityHeight <= ms.index.Height {
			return
		}
	}
	t.Fatal("no spendable utxos")
	return
}

func newMemState() *memState {
	return &memState{
		utxos: make(map[types.SiacoinOutputID]types.SiacoinElement),
	}
}

func TestV2Attestations(t *testing.T) {
	n, genesisBlock := testutil.V2Network()

	policy := types.AnyoneCanSpend()
	addr := policy.Address()

	mineBlocks := func(t *testing.T, cm *chain.Manager, n int) {
		t.Helper()

		for i := 0; i < n; i++ {
			b, ok := coreutils.MineBlock(cm, addr, 5*time.Second)
			if !ok {
				t.Fatal("failed to mine block")
			} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
				t.Fatal(err)
			}
		}
	}

	sk := types.GeneratePrivateKey()
	ann := chain.V2HostAnnouncement{
		{Address: "foo.bar:1234", Protocol: "tcp"},
	}

	// A transaction that spends no consensus elements stays valid forever and
	// would be mined in every block, so the txpool must reject it regardless of
	// any attestations or arbitrary data it carries.
	t.Run("rejects transactions that spend no elements", func(t *testing.T) {
		store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		cm := chain.NewManager(store, tipState)

		cases := map[string]types.V2Transaction{
			"arbitrary data only": {
				ArbitraryData: frand.Bytes(16),
			},
			"attestation only": {
				Attestations: []types.Attestation{ann.ToAttestation(cm.TipState(), sk)},
			},
			"arbitrary data + attestation": {
				ArbitraryData: frand.Bytes(16),
				Attestations:  []types.Attestation{ann.ToAttestation(cm.TipState(), sk)},
			},
		}
		for name, txn := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := cm.AddV2PoolTransactions(cm.Tip(), []types.V2Transaction{txn})
				if err == nil || !strings.Contains(err.Error(), "does not spend any elements") {
					t.Fatalf("expected rejection for transaction that spends no elements, got %v", err)
				}
			})
		}
	})

	// A funded transaction carrying an attestation and arbitrary data (the host
	// announcement flow) must still be accepted, and because it spends an input
	// it must leave the pool once it is mined.
	t.Run("accepts funded announcement transaction", func(t *testing.T) {
		store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
		if err != nil {
			t.Fatal(err)
		}
		cm := chain.NewManager(store, tipState)
		ms := newMemState()

		// mine until a utxo is spendable
		mineBlocks(t, cm, int(n.MaturityDelay)+1)
		ms.Sync(t, cm)

		se := ms.SpendableElement(t)
		minerFee := types.Siacoins(1)
		txn := types.V2Transaction{
			SiacoinInputs: []types.V2SiacoinInput{
				{Parent: se.Copy(), SatisfiedPolicy: types.SatisfiedPolicy{Policy: policy}},
			},
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: addr, Value: se.SiacoinOutput.Value.Sub(minerFee)},
			},
			MinerFee:      minerFee,
			ArbitraryData: frand.Bytes(16),
			Attestations: []types.Attestation{
				ann.ToAttestation(cm.TipState(), sk),
			},
		}

		if _, err := cm.AddV2PoolTransactions(cm.Tip(), []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		} else if len(cm.V2PoolTransactions()) != 1 {
			t.Fatalf("expected 1 transaction in pool, got %v", len(cm.V2PoolTransactions()))
		}

		mineBlocks(t, cm, 1)
		ms.Sync(t, cm)

		if len(cm.V2PoolTransactions()) != 0 {
			t.Fatalf("expected pool to be empty after transaction was mined, got %v", len(cm.V2PoolTransactions()))
		}
	})
}
