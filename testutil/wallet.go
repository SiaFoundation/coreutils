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

// ProcessChainRevertUpdate implements chain.Subscriber
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

// ProcessChainApplyUpdate implements chain.Subscriber
func (es *EphemeralWalletStore) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, commit bool) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.uncommitted = append(es.uncommitted, cau)
	if !commit {
		return nil
	}

	walletAddress := types.StandardUnlockHash(es.privateKey.PublicKey())

	for _, update := range es.uncommitted {
		// cache the source of new immature outputs to show payout transactions
		siacoinOutputSources := map[types.SiacoinOutputID]wallet.TransactionSource{
			update.Block.ID().FoundationOutputID(): wallet.TxnSourceFoundationPayout,
		}
		// add the miner payouts
		for i := range update.Block.MinerPayouts {
			siacoinOutputSources[update.Block.ID().MinerOutputID(i)] = wallet.TxnSourceMinerPayout
		}

		// add the file contract outputs
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

		// add matured transactions first since
		for _, txn := range es.immaturePayoutTransactions[update.State.Index.Height] {
			txn.Index = update.State.Index
			txn.Timestamp = update.Block.Timestamp
			// prepend the transaction to the wallet
			es.transactions = append([]wallet.Transaction{txn}, es.transactions...)
		}

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

			// prepend the transaction to the wallet
			es.transactions = append([]wallet.Transaction{wt}, es.transactions...)

			// add the siafund claim output IDs
			for i := range txn.SiafundInputs {
				siacoinOutputSources[txn.SiafundInputs[i].ParentID.ClaimOutputID()] = wallet.TxnSourceSiafundClaim
			}
		}

		// update the utxo set
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
				panic("missing siacoin source")
			}

			es.immaturePayoutTransactions[se.MaturityHeight] = append(es.immaturePayoutTransactions[se.MaturityHeight], wallet.Transaction{
				ID:          types.TransactionID(se.ID),
				Transaction: types.Transaction{SiacoinOutputs: []types.SiacoinOutput{se.SiacoinOutput}},
				Inflow:      se.SiacoinOutput.Value,
				Source:      source,
				// Index and Timestamp will be filled in later
			})
		})

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

// Transactions returns the wallet's transactions.
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

// TransactionCount returns the number of transactions in the wallet.
func (es *EphemeralWalletStore) TransactionCount() (uint64, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return uint64(len(es.transactions)), nil
}

// UnspentSiacoinElements returns the wallet's unspent siacoin outputs.
func (es *EphemeralWalletStore) UnspentSiacoinElements() (utxos []types.SiacoinElement, _ error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for _, se := range es.utxos {
		utxos = append(utxos, se)
	}
	return utxos, nil
}

// Tip returns the last indexed tip of the wallet.
func (es *EphemeralWalletStore) Tip() (types.ChainIndex, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.tip, nil
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore(pk types.PrivateKey) *EphemeralWalletStore {
	return &EphemeralWalletStore{
		privateKey: pk,

		utxos:                      make(map[types.Hash256]types.SiacoinElement),
		immaturePayoutTransactions: make(map[uint64][]wallet.Transaction),
	}
}
