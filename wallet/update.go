package wallet

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	// ApplyTx is an interface for atomically applying a chain update to a
	// single address wallet.
	ApplyTx interface {
		// WalletStateElements returns all state elements related to the wallet. It is used
		// to update the proofs of all state elements affected by the update.
		WalletStateElements() ([]types.StateElement, error)
		// UpdateStateElements updates the proofs of all state elements affected by the
		// update.
		UpdateStateElements([]types.StateElement) error

		// AddEvents is called with all relevant events added in the update.
		AddEvents([]Event) error
		// AddSiacoinElements is called with all new siacoin elements in the
		// update. Ephemeral siacoin elements are not included.
		AddSiacoinElements([]SiacoinElement) error
		// RemoveSiacoinElements is called with all siacoin elements that were
		// spent in the update.
		RemoveSiacoinElements([]types.SiacoinOutputID) error
	}

	// RevertTx is an interface for atomically reverting a chain update from a
	// single address wallet.
	RevertTx interface {
		// WalletStateElements returns all state elements in the database. It is used
		// to update the proofs of all state elements affected by the update.
		WalletStateElements() ([]types.StateElement, error)
		// UpdateStateElements updates the proofs of all state elements affected by the
		// update.
		UpdateStateElements([]types.StateElement) error

		// RevertIndex is called with the chain index that is being reverted.
		// Any transactions and siacoin elements that were created by the index
		// should be removed.
		RevertIndex(types.ChainIndex) error
		// AddSiacoinElements is called with all siacoin elements that are
		// now unspent due to the revert.
		AddSiacoinElements([]SiacoinElement) error
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

// ApplyChainUpdates atomically applies a batch of wallet updates
func ApplyChainUpdates(tx ApplyTx, address types.Address, updates []chain.ApplyUpdate) error {
	stateElements, err := tx.WalletStateElements()
	if err != nil {
		return fmt.Errorf("failed to get state elements: %w", err)
	}

	var events []Event
	var spentUTXOs []types.SiacoinOutputID
	newUTXOs := make(map[types.Hash256]SiacoinElement)

	for _, cau := range updates {
		events = append(events, addressPayoutEvents(address, cau)...)
		utxoValues := make(map[types.SiacoinOutputID]types.Currency)

		cau.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
			if se.SiacoinOutput.Address != address {
				return
			}

			// cache the value of the utxo to use when calculating outflow
			utxoValues[types.SiacoinOutputID(se.ID)] = se.SiacoinOutput.Value
			if spent {
				// remove the utxo from the new utxos
				delete(newUTXOs, se.ID)

				// skip ephemeral outputs
				if se.StateElement.LeafIndex != types.EphemeralLeafIndex {
					spentUTXOs = append(spentUTXOs, types.SiacoinOutputID(se.ID))
				}
			} else {
				newUTXOs[se.ID] = SiacoinElement{
					SiacoinElement: se,
					Index:          cau.State.Index,
				}
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

		for _, se := range newUTXOs {
			cau.UpdateElementProof(&se.StateElement)
		}
	}

	createdUTXOs := make([]SiacoinElement, 0, len(newUTXOs))
	for _, se := range newUTXOs {
		createdUTXOs = append(createdUTXOs, se)
	}

	if err := tx.AddSiacoinElements(createdUTXOs); err != nil {
		return fmt.Errorf("failed to add siacoin elements: %w", err)
	} else if err := tx.RemoveSiacoinElements(spentUTXOs); err != nil {
		return fmt.Errorf("failed to remove siacoin elements: %w", err)
	} else if err := tx.AddEvents(events); err != nil {
		return fmt.Errorf("failed to add events: %w", err)
	} else if err := tx.UpdateStateElements(stateElements); err != nil {
		return fmt.Errorf("failed to update state elements: %w", err)
	}
	return nil
}

// RevertChainUpdate atomically reverts a chain update from a wallet
func RevertChainUpdate(tx RevertTx, address types.Address, cru chain.RevertUpdate) error {
	stateElements, err := tx.WalletStateElements()
	if err != nil {
		return fmt.Errorf("failed to get state elements: %w", err)
	}

	var readdedUTXOs []SiacoinElement

	cru.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
		if se.SiacoinOutput.Address != address {
			return
		}

		if !spent {
			readdedUTXOs = append(readdedUTXOs, SiacoinElement{
				SiacoinElement: se,
				Index:          cru.State.Index,
			})
		}
	})

	for i := range stateElements {
		cru.UpdateElementProof(&stateElements[i])
	}

	if err := tx.RevertIndex(cru.State.Index); err != nil {
		return fmt.Errorf("failed to revert block: %w", err)
	} else if err := tx.AddSiacoinElements(readdedUTXOs); err != nil {
		return fmt.Errorf("failed to add siacoin elements: %w", err)
	} else if err := tx.UpdateStateElements(stateElements); err != nil {
		return fmt.Errorf("failed to update state elements: %w", err)
	}
	return nil
}
