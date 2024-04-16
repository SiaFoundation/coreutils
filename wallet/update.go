package wallet

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	// UpdateTx is an interface for atomically applying chain updates to a
	// single address wallet.
	UpdateTx interface {
		// WalletStateElements returns all state elements related to the wallet. It is used
		// to update the proofs of all state elements affected by the update.
		WalletStateElements() ([]types.StateElement, error)
		// UpdateStateElements updates the proofs of all state elements affected by the
		// update.
		UpdateStateElements([]types.StateElement) error

		// ApplyIndex is called with the chain index that is being applied.
		// Any transactions and siacoin elements that were created by the index
		// should be added and any siacoin elements that were spent should be
		// removed.
		ApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []Event) error
		// RevertIndex is called with the chain index that is being reverted.
		// Any transactions and siacoin elements that were created by the index
		// should be removed.
		RevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement) error
	}
)

// addressPayoutEvents is a helper to add all payout transactions from an
// apply update to a slice of transactions.
func addressPayoutEvents(addr types.Address, cau chain.ApplyUpdate) (events []Event) {
	index := cau.State.Index
	state := cau.State
	block := cau.Block

	// cache the source of new immature outputs to show payout transactions
	if state.FoundationPrimaryAddress == addr {
		events = append(events, Event{
			ID:    types.Hash256(index.ID.FoundationOutputID()),
			Index: index,
			Transaction: types.Transaction{
				SiacoinOutputs: []types.SiacoinOutput{
					state.FoundationSubsidy(),
				},
			},
			Inflow:    state.FoundationSubsidy().Value,
			Source:    EventSourceFoundationPayout,
			Timestamp: block.Timestamp,
		})
	}

	// add the miner payouts
	for i := range block.MinerPayouts {
		if block.MinerPayouts[i].Address != addr {
			continue
		}

		events = append(events, Event{
			ID:    types.Hash256(index.ID.MinerOutputID(i)),
			Index: index,
			Transaction: types.Transaction{
				SiacoinOutputs: []types.SiacoinOutput{
					block.MinerPayouts[i],
				},
			},
			Inflow:         block.MinerPayouts[i].Value,
			MaturityHeight: state.MaturityHeight(),
			Source:         EventSourceMinerPayout,
			Timestamp:      block.Timestamp,
		})
	}

	// add the file contract outputs
	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved bool, valid bool) {
		if !resolved {
			return
		}

		if valid {
			for i, output := range fce.FileContract.ValidProofOutputs {
				if output.Address != addr {
					continue
				}

				outputID := types.FileContractID(fce.ID).ValidOutputID(i)
				events = append(events, Event{
					ID:    types.Hash256(outputID),
					Index: index,
					Transaction: types.Transaction{
						SiacoinOutputs: []types.SiacoinOutput{output},
						FileContracts:  []types.FileContract{fce.FileContract},
					},
					Inflow:         fce.FileContract.ValidProofOutputs[i].Value,
					MaturityHeight: state.MaturityHeight(),
					Source:         EventSourceValidContract,
					Timestamp:      block.Timestamp,
				})
			}
		} else {
			for i, output := range fce.FileContract.MissedProofOutputs {
				if output.Address != addr {
					continue
				}

				outputID := types.FileContractID(fce.ID).MissedOutputID(i)
				events = append(events, Event{
					ID:    types.Hash256(outputID),
					Index: index,
					Transaction: types.Transaction{
						SiacoinOutputs: []types.SiacoinOutput{output},
						FileContracts:  []types.FileContract{fce.FileContract},
					},
					Inflow:         fce.FileContract.ValidProofOutputs[i].Value,
					MaturityHeight: state.MaturityHeight(),
					Source:         EventSourceMissedContract,
					Timestamp:      block.Timestamp,
				})
			}
		}
	})
	return
}

// applyChainState atomically applies a chain update
func applyChainState(tx UpdateTx, address types.Address, cau chain.ApplyUpdate) error {
	stateElements, err := tx.WalletStateElements()
	if err != nil {
		return fmt.Errorf("failed to get state elements: %w", err)
	}

	// determine which siacoin and siafund elements are ephemeral
	//
	// note: I thought we could use LeafIndex == EphemeralLeafIndex, but
	// it seems to be set before the subscriber is called.
	created := make(map[types.Hash256]bool)
	ephemeral := make(map[types.Hash256]bool)
	for _, txn := range cau.Block.Transactions {
		for i := range txn.SiacoinOutputs {
			created[types.Hash256(txn.SiacoinOutputID(i))] = true
		}
		for _, input := range txn.SiacoinInputs {
			ephemeral[types.Hash256(input.ParentID)] = created[types.Hash256(input.ParentID)]
		}
		for i := range txn.SiafundOutputs {
			created[types.Hash256(txn.SiafundOutputID(i))] = true
		}
		for _, input := range txn.SiafundInputs {
			ephemeral[types.Hash256(input.ParentID)] = created[types.Hash256(input.ParentID)]
		}
	}

	var createdUTXOs, spentUTXOs []types.SiacoinElement
	events := addressPayoutEvents(address, cau)
	utxoValues := make(map[types.SiacoinOutputID]types.Currency)

	cau.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
		if se.SiacoinOutput.Address != address {
			return
		}

		// cache the value of the utxo to use when calculating outflow
		utxoValues[types.SiacoinOutputID(se.ID)] = se.SiacoinOutput.Value

		// ignore ephemeral elements
		if ephemeral[types.Hash256(se.ID)] {
			return
		}

		if spent {
			spentUTXOs = append(spentUTXOs, se)
		} else {
			createdUTXOs = append(createdUTXOs, se)
		}
	})

	for _, txn := range cau.Block.Transactions {
		ev := Event{
			ID:             types.Hash256(txn.ID()),
			Index:          cau.State.Index,
			Transaction:    txn,
			Source:         EventSourceTransaction,
			MaturityHeight: cau.State.Index.Height, // regular transactions "mature" immediately
			Timestamp:      cau.Block.Timestamp,
		}

		for _, si := range txn.SiacoinInputs {
			if si.UnlockConditions.UnlockHash() == address {
				value, ok := utxoValues[si.ParentID]
				if !ok {
					panic("missing utxo") // this should never happen
				}
				ev.Inflow = ev.Inflow.Add(value)
			}
		}

		for _, so := range txn.SiacoinOutputs {
			if so.Address != address {
				continue
			}
			ev.Outflow = ev.Outflow.Add(so.Value)
		}

		// skip irrelevant transactions
		if ev.Inflow.IsZero() && ev.Outflow.IsZero() {
			continue
		}

		events = append(events, ev)
	}

	for i := range stateElements {
		cau.UpdateElementProof(&stateElements[i])
	}

	if err := tx.ApplyIndex(cau.State.Index, createdUTXOs, spentUTXOs, events); err != nil {
		return fmt.Errorf("failed to apply index: %w", err)
	} else if err := tx.UpdateStateElements(stateElements); err != nil {
		return fmt.Errorf("failed to update state elements: %w", err)
	}
	return nil
}

// revertChainUpdate atomically reverts a chain update from a wallet
func revertChainUpdate(tx UpdateTx, revertedIndex types.ChainIndex, address types.Address, cru chain.RevertUpdate) error {
	var removedUTXOs, unspentUTXOs []types.SiacoinElement
	cru.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
		if se.SiacoinOutput.Address != address {
			return
		}

		if spent {
			unspentUTXOs = append(unspentUTXOs, se)
		} else {
			removedUTXOs = append(removedUTXOs, se)
		}
	})

	// remove any existing events that were added in the reverted block
	if err := tx.RevertIndex(revertedIndex, removedUTXOs, unspentUTXOs); err != nil {
		return fmt.Errorf("failed to revert block: %w", err)
	}

	// update the remaining state elements
	stateElements, err := tx.WalletStateElements()
	if err != nil {
		return fmt.Errorf("failed to get state elements: %w", err)
	}

	for i := range stateElements {
		cru.UpdateElementProof(&stateElements[i])
	}

	if err := tx.UpdateStateElements(stateElements); err != nil {
		return fmt.Errorf("failed to update state elements: %w", err)
	}
	return nil
}

// UpdateChainState atomically applies and reverts chain updates to a single
// wallet store.
func UpdateChainState(tx UpdateTx, address types.Address, applied []chain.ApplyUpdate, reverted []chain.RevertUpdate) error {
	for _, cru := range reverted {
		revertedIndex := types.ChainIndex{
			ID:     cru.Block.ID(),
			Height: cru.State.Index.Height + 1,
		}
		if err := revertChainUpdate(tx, revertedIndex, address, cru); err != nil {
			return err
		}
	}

	for _, cau := range applied {
		if err := applyChainState(tx, address, cau); err != nil {
			return fmt.Errorf("failed to apply chain update %q: %w", cau.State.Index, err)
		}
	}

	return nil
}
