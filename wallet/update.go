package wallet

import (
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

type (
	// A ChainUpdate is an interface for iterating over the elements in a chain
	// update.
	ChainUpdate interface {
		ForEachSiacoinElement(func(sce types.SiacoinElement, spent bool))
		ForEachSiafundElement(func(sfe types.SiafundElement, spent bool))
		ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool))
		ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType))
	}

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
		// Any transactions that were added by the index should be removed
		//
		// removed contains the siacoin elements that were created by the index
		// and should be deleted.
		//
		// unspent contains the siacoin elements that were spent and should be
		// recreated. They are not necessarily created by the index and should
		// not be associated with it.
		RevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement) error
	}
)

// appliedEvents returns a slice of events that are relevant to the wallet
// in the chain update.
func appliedEvents(cau chain.ApplyUpdate, walletAddress types.Address) (events []Event) {
	cs := cau.State
	block := cau.Block
	index := cs.Index
	siacoinElements := make(map[types.SiacoinOutputID]types.SiacoinElement)

	// cache the value of spent siacoin elements to use when calculating outflow
	cau.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
		if spent {
			siacoinElements[types.SiacoinOutputID(se.ID)] = se
		}
	})

	addEvent := func(id types.Hash256, data any) {
		ev := Event{
			ID:        id,
			Index:     index,
			Data:      data,
			Timestamp: block.Timestamp,
		}

		switch data := data.(type) {
		case EventMinerPayout:
			ev.Inflow = data.SiacoinElement.SiacoinOutput.Value
			ev.Type = EventTypeMinerPayout
			ev.MaturityHeight = cs.MaturityHeight()
		case EventFoundationSubsidy:
			ev.Inflow = data.SiacoinElement.SiacoinOutput.Value
			ev.Type = EventTypeFoundationSubsidy
			ev.MaturityHeight = cs.MaturityHeight()
		case types.Transaction:
			for _, si := range data.SiacoinInputs {
				if si.UnlockConditions.UnlockHash() == walletAddress {
					ev.Outflow = ev.Outflow.Add(siacoinElements[si.ParentID].SiacoinOutput.Value)
				}
			}

			for _, so := range data.SiacoinOutputs {
				if so.Address == walletAddress {
					ev.Inflow = ev.Inflow.Add(so.Value)
				}
			}
			ev.MaturityHeight = index.Height
			ev.Type = EventTypeV1Transaction
		case EventV1ContractPayout:
			ev.Inflow = data.SiacoinElement.SiacoinOutput.Value
			ev.Type = EventTypeV1Contract
			ev.MaturityHeight = cs.MaturityHeight()
		case types.V2Transaction:
			for _, si := range data.SiacoinInputs {
				if si.SatisfiedPolicy.Policy.Address() == walletAddress {
					ev.Outflow = ev.Outflow.Add(siacoinElements[types.SiacoinOutputID(si.Parent.ID)].SiacoinOutput.Value)
				}
			}

			for _, so := range data.SiacoinOutputs {
				if so.Address == walletAddress {
					ev.Inflow = ev.Inflow.Add(so.Value)
				}
			}
			ev.Type = EventTypeV2Transaction
			ev.MaturityHeight = index.Height
		case EventV2ContractPayout:
			ev.Inflow = data.SiacoinElement.SiacoinOutput.Value
			ev.Type = EventTypeV2Contract
			ev.MaturityHeight = cs.MaturityHeight()
		}

		events = append(events, ev)
	}

	relevantV1Txn := func(txn types.Transaction) bool {
		for _, so := range txn.SiacoinOutputs {
			if so.Address == walletAddress {
				return true
			}
		}
		for _, si := range txn.SiacoinInputs {
			if si.UnlockConditions.UnlockHash() == walletAddress {
				return true
			}
		}
		return false
	}

	for _, txn := range block.Transactions {
		if !relevantV1Txn(txn) {
			continue
		}
		addEvent(types.Hash256(txn.ID()), txn)
	}

	relevantV2Txn := func(txn types.V2Transaction) bool {
		for _, so := range txn.SiacoinOutputs {
			if so.Address == walletAddress {
				return true
			}
		}
		for _, si := range txn.SiacoinInputs {
			if si.Parent.SiacoinOutput.Address == walletAddress {
				return true
			}
		}
		return false
	}

	for _, txn := range block.V2Transactions() {
		if !relevantV2Txn(txn) {
			continue
		}
		addEvent(types.Hash256(txn.ID()), txn)
	}

	cau.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved bool, valid bool) {
		if !resolved {
			return
		}

		if valid {
			for i, so := range fce.FileContract.ValidProofOutputs {
				if so.Address != walletAddress {
					continue
				}

				outputID := types.FileContractID(fce.ID).ValidOutputID(i)
				addEvent(types.Hash256(types.FileContractID(fce.ID).ValidOutputID(0)), EventV1ContractPayout{
					FileContract:   fce,
					SiacoinElement: siacoinElements[outputID],
					Missed:         false,
				})
			}
		} else {
			for i, so := range fce.FileContract.MissedProofOutputs {
				if so.Address != walletAddress {
					continue
				}

				outputID := types.FileContractID(fce.ID).MissedOutputID(i)
				addEvent(types.Hash256(types.FileContractID(fce.ID).MissedOutputID(0)), EventV1ContractPayout{
					FileContract:   fce,
					SiacoinElement: siacoinElements[outputID],
					Missed:         true,
				})
			}
		}
	})

	cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
		if res == nil {
			return
		}

		var missed bool
		if _, ok := res.(*types.V2FileContractExpiration); ok {
			missed = true
		}

		if fce.V2FileContract.HostOutput.Address == walletAddress {
			outputID := types.FileContractID(fce.ID).V2HostOutputID()
			addEvent(types.Hash256(outputID), EventV2ContractPayout{
				FileContract:   fce,
				Resolution:     res,
				SiacoinElement: siacoinElements[outputID],
				Missed:         missed,
			})
		}

		if fce.V2FileContract.RenterOutput.Address == walletAddress {
			outputID := types.FileContractID(fce.ID).V2RenterOutputID()
			addEvent(types.Hash256(outputID), EventV2ContractPayout{
				FileContract:   fce,
				Resolution:     res,
				SiacoinElement: siacoinElements[outputID],
				Missed:         missed,
			})
		}
	})

	blockID := block.ID()
	for i, so := range block.MinerPayouts {
		if so.Address != walletAddress {
			continue
		}

		outputID := blockID.MinerOutputID(i)
		addEvent(types.Hash256(outputID), EventMinerPayout{
			SiacoinElement: siacoinElements[outputID],
		})
	}

	outputID := blockID.FoundationOutputID()
	se, ok := siacoinElements[outputID]
	if !ok || se.SiacoinOutput.Address != walletAddress {
		return
	}
	addEvent(types.Hash256(outputID), EventFoundationSubsidy{
		SiacoinElement: se,
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
	utxoValues := make(map[types.SiacoinOutputID]types.Currency)

	cau.ForEachSiacoinElement(func(se types.SiacoinElement, spent bool) {
		if se.SiacoinOutput.Address != address {
			return
		}

		// cache the value of the utxo to use when calculating outflow
		utxoValues[types.SiacoinOutputID(se.ID)] = se.SiacoinOutput.Value

		// ignore ephemeral elements
		if ephemeral[se.ID] {
			return
		}

		if spent {
			spentUTXOs = append(spentUTXOs, se)
		} else {
			createdUTXOs = append(createdUTXOs, se)
		}
	})

	for i := range stateElements {
		cau.UpdateElementProof(&stateElements[i])
	}

	if err := tx.ApplyIndex(cau.State.Index, createdUTXOs, spentUTXOs, appliedEvents(cau, address)); err != nil {
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
