package testutil

import (
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
)

// An EphemeralWalletStore is a Store that does not persist its state to disk. It is
// primarily useful for testing or as a reference implementation.
type EphemeralWalletStore struct {
	privateKey types.PrivateKey

	mu          sync.Mutex
	uncommitted []*chain.ApplyUpdate
	tip         types.ChainIndex
	utxos       map[types.Hash256]types.SiacoinElement

	immaturePayoutTransactions map[uint64][]wallet.Transaction // transactions are never removed to simplify reorg handling
	transactions               []wallet.Transaction
}

func (es *EphemeralWalletStore) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	// if the update is uncommitted, remove it from the uncommitted list
	if len(es.uncommitted) > 0 && es.uncommitted[len(es.uncommitted)-1].State.Index == cru.State.Index {
		es.uncommitted = es.uncommitted[:len(es.uncommitted)-1]
		return nil
	}

	walletAddress := types.StandardUnlockHash(es.privateKey.PublicKey())

	// revert any siacoin element changes
	cru.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
		if se.SiacoinOutput.Address != walletAddress {
			return
		}

		if spent {
			es.utxos[se.ID] = se
		} else {
			delete(es.utxos, se.ID)
		}
	})

	// remove any transactions that were added in the reverted block
	filtered := es.transactions[:0]
	for _, txn := range es.transactions {
		if txn.Index == cru.State.Index {
			continue
		}
		filtered = append(filtered, txn)
	}

	// update element proofs
	for id, se := range es.utxos {
		cru.UpdateElementProof(&se.StateElement)
		es.utxos[id] = se
	}
	return nil
}

func (es *EphemeralWalletStore) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, commit bool) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.uncommitted = append(es.uncommitted, cau)
	if !commit {
		return nil
	}

	walletAddress := types.StandardAddress(es.privateKey.PublicKey())

	for _, update := range es.uncommitted {
		// add matured transactions
		for _, txn := range es.immaturePayoutTransactions[update.State.Index.Height] {
			txn.Index = update.State.Index
			txn.Timestamp = update.Block.Timestamp
			es.transactions = append(es.transactions, txn)
		}

		// determine the source of new immature outputs
		siacoinOutputSources := make(map[types.SiacoinOutputID]wallet.TransactionSource)
		for i := range update.Block.MinerPayouts {
			siacoinOutputSources[update.Block.ParentID.MinerOutputID(i)] = wallet.TxnSourceMinerPayout
		}

		update.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved bool, valid bool) {
			if !resolved {
				return
			}

			if valid {
				for i := range fce.FileContract.ValidProofOutputs {
					siacoinOutputSources[types.FileContractID(fce.ID).ValidOutputID(i)] = wallet.TxnSourceContract
				}
			} else {
				for i := range fce.FileContract.MissedProofOutputs {
					siacoinOutputSources[types.FileContractID(fce.ID).MissedOutputID(i)] = wallet.TxnSourceContract
				}
			}
		})

		for _, txn := range update.Block.Transactions {
			for i := range txn.SiafundInputs {
				siacoinOutputSources[txn.SiafundInputs[i].ParentID.ClaimOutputID()] = wallet.TxnSourceSiafundClaim
			}
		}

		update.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
			if se.SiacoinOutput.Address != walletAddress {
				return
			}

			// update the utxo set
			if spent {
				delete(es.utxos, se.ID)
			} else {
				es.utxos[se.ID] = se
			}

			// create an immature payout transaction for any immature siacoin outputs
			if se.MaturityHeight == 0 || spent {
				return
			}

			source, ok := siacoinOutputSources[types.SiacoinOutputID(se.ID)]
			if !ok {
				source = wallet.TxnSourceFoundationPayout
			}

			es.immaturePayoutTransactions[se.MaturityHeight] = append(es.immaturePayoutTransactions[se.MaturityHeight], wallet.Transaction{
				ID:          types.TransactionID(se.ID),
				Transaction: types.Transaction{SiacoinOutputs: []types.SiacoinOutput{se.SiacoinOutput}},
				Inflow:      se.SiacoinOutput.Value,
				Source:      source,
				// Index and Timestamp will be filled in later
			})
		})

		// add the block transactions
		for _, txn := range update.Block.Transactions {
			if !wallet.IsRelevantTransaction(txn, walletAddress) {
				continue
			}

			wt := wallet.Transaction{
				ID:          txn.ID(),
				Index:       update.State.Index,
				Transaction: txn,
				Timestamp:   update.Block.Timestamp,
				Source:      wallet.TxnSourceTransaction,
			}

			for _, sci := range txn.SiacoinInputs {
				if sci.UnlockConditions.UnlockHash() != walletAddress {
					continue
				}

				sce, ok := es.utxos[types.Hash256(sci.ParentID)]
				if !ok {
					return fmt.Errorf("missing relevant siacoin element %q", sci.ParentID)
				}
				wt.Outflow = wt.Outflow.Add(sce.SiacoinOutput.Value)
			}

			for _, sco := range txn.SiacoinOutputs {
				if sco.Address != walletAddress {
					continue
				}

				wt.Inflow = wt.Inflow.Add(sco.Value)
			}
		}

		// update the element proofs
		for id, se := range es.utxos {
			cau.UpdateElementProof(&se.StateElement)
			es.utxos[id] = se
		}
	}

	es.uncommitted = es.uncommitted[:0]
	es.tip = cau.State.Index
	return nil
}

func (es *EphemeralWalletStore) Transactions(limit, offset int) ([]wallet.Transaction, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	if offset > len(es.transactions) {
		return nil, nil
	}

	end := offset + limit
	if end > len(es.transactions) {
		end = len(es.transactions)
	}
	return es.transactions[offset:end], nil
}

func (es *EphemeralWalletStore) TransactionCount() (uint64, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return uint64(len(es.transactions)), nil
}

func (es *EphemeralWalletStore) UnspentSiacoinElements() (utxos []types.SiacoinElement, _ error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for _, se := range es.utxos {
		utxos = append(utxos, se)
	}
	return utxos, nil
}

// LastIndexedTip returns the last indexed tip of the wallet.
func (es *EphemeralWalletStore) LastIndexedTip() (types.ChainIndex, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.tip, nil
}

// VerifyWalletKey checks that the wallet seed matches the existing seed
// hash. This detects if the user's recovery phrase has changed and the
// wallet needs to rescan.
func (es *EphemeralWalletStore) VerifyWalletKey(seedHash types.Hash256) error {
	return nil
}

// ResetWallet resets the wallet to its initial state. This is used when a
// consensus subscription error occurs.
func (es *EphemeralWalletStore) ResetWallet(seedHash types.Hash256) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.uncommitted = es.uncommitted[:0]
	es.tip = types.ChainIndex{}
	es.utxos = make(map[types.Hash256]types.SiacoinElement)
	es.immaturePayoutTransactions = make(map[uint64][]wallet.Transaction)
	es.transactions = nil
	return nil
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore(pk types.PrivateKey) *EphemeralWalletStore {
	return &EphemeralWalletStore{
		privateKey: pk,

		utxos:                      make(map[types.Hash256]types.SiacoinElement),
		immaturePayoutTransactions: make(map[uint64][]wallet.Transaction),
	}
}
