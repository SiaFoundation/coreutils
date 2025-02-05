package wallet

import (
	"fmt"
	"time"

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

	// A ProofUpdater is an interface for updating the proof of a state element.
	ProofUpdater interface {
		UpdateElementProof(e *types.StateElement)
	}

	// UpdateTx is an interface for atomically applying chain updates to a
	// single address wallet.
	UpdateTx interface {
		// UpdateWalletSiacoinElementProofs updates the proofs of all state elements
		// affected by the update. ProofUpdater.UpdateElementProof must be called
		// for each state element in the database.
		UpdateWalletSiacoinElementProofs(ProofUpdater) error

		// WalletApplyIndex is called with the chain index that is being applied.
		// Any transactions and siacoin elements that were created by the index
		// should be added and any siacoin elements that were spent should be
		// removed.
		//
		// timestamp is the timestamp of the block being applied.
		WalletApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []Event, timestamp time.Time) error
		// WalletRevertIndex is called with the chain index that is being reverted.
		// Any transactions that were added by the index should be removed
		//
		// removed contains the siacoin elements that were created by the index
		// and should be deleted.
		//
		// unspent contains the siacoin elements that were spent and should be
		// recreated. They are not necessarily created by the index and should
		// not be associated with it.
		//
		// timestamp is the timestamp of the block being reverted
		WalletRevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement, timestamp time.Time) error
	}
)

// relevantV1Txn returns true if the transaction is relevant to the provided address
func relevantV1Txn(txn types.Transaction, addr types.Address) bool {
	for _, so := range txn.SiacoinOutputs {
		if so.Address == addr {
			return true
		}
	}
	for _, si := range txn.SiacoinInputs {
		if si.UnlockConditions.UnlockHash() == addr {
			return true
		}
	}
	return false
}

func relevantV2Txn(txn types.V2Transaction, addr types.Address) bool {
	for _, so := range txn.SiacoinOutputs {
		if so.Address == addr {
			return true
		}
	}
	for _, si := range txn.SiacoinInputs {
		if si.Parent.SiacoinOutput.Address == addr {
			return true
		}
	}
	return false
}

// appliedEvents returns a slice of events that are relevant to the wallet
// in the chain update.
func appliedEvents(cau chain.ApplyUpdate, walletAddress types.Address) (events []Event) {
	cs := cau.State
	block := cau.Block
	index := cs.Index
	siacoinElements := make(map[types.SiacoinOutputID]types.SiacoinElement)

	// cache the value of siacoin elements to use when calculating v1 outflow
	for _, sced := range cau.SiacoinElementDiffs() {
		sced.SiacoinElement.StateElement.MerkleProof = nil // clear the proof to save space
		siacoinElements[sced.SiacoinElement.ID] = sced.SiacoinElement
	}

	addEvent := func(id types.Hash256, eventType string, data EventData, maturityHeight uint64) {
		ev := Event{
			ID:             id,
			Index:          index,
			Data:           data,
			Type:           eventType,
			Timestamp:      block.Timestamp,
			MaturityHeight: maturityHeight,
			Relevant:       []types.Address{walletAddress},
		}

		if ev.SiacoinInflow().Equals(ev.SiacoinOutflow()) {
			// skip events that don't affect the wallet
			return
		}
		events = append(events, ev)
	}

	for _, txn := range block.Transactions {
		if !relevantV1Txn(txn, walletAddress) {
			continue
		}
		for _, si := range txn.SiafundInputs {
			if si.UnlockConditions.UnlockHash() == walletAddress {
				outputID := si.ParentID.ClaimOutputID()
				sce, ok := siacoinElements[outputID]
				if !ok {
					panic("missing claim siacoin element")
				}

				addEvent(types.Hash256(outputID), EventTypeSiafundClaim, EventPayout{
					SiacoinElement: sce,
				}, sce.MaturityHeight)
			}
		}

		event := EventV1Transaction{
			Transaction: txn,
		}

		for _, si := range txn.SiacoinInputs {
			se, ok := siacoinElements[types.SiacoinOutputID(si.ParentID)]
			if !ok {
				panic("missing transaction siacoin element")
			} else if se.SiacoinOutput.Address != walletAddress {
				continue
			}
			event.SpentSiacoinElements = append(event.SpentSiacoinElements, se)
		}
		addEvent(types.Hash256(txn.ID()), EventTypeV1Transaction, event, index.Height)
	}

	for _, txn := range block.V2Transactions() {
		if !relevantV2Txn(txn, walletAddress) {
			continue
		}
		for _, si := range txn.SiafundInputs {
			if si.Parent.SiafundOutput.Address == walletAddress {
				outputID := types.SiafundOutputID(si.Parent.ID).V2ClaimOutputID()
				sce, ok := siacoinElements[outputID]
				if !ok {
					panic("missing claim siacoin element")
				}

				addEvent(types.Hash256(outputID), EventTypeSiafundClaim, EventPayout{
					SiacoinElement: sce,
				}, sce.MaturityHeight)
			}
		}

		addEvent(types.Hash256(txn.ID()), EventTypeV2Transaction, EventV2Transaction(txn), index.Height)
	}

	// add the file contract outputs
	for _, fced := range cau.FileContractElementDiffs() {
		if !fced.Resolved {
			continue
		}
		fce := fced.FileContractElement
		fce.StateElement.MerkleProof = nil // clear the proof to save space

		if fced.Valid {
			for i, so := range fce.FileContract.ValidProofOutputs {
				if so.Address != walletAddress {
					continue
				}

				outputID := fce.ID.ValidOutputID(i)
				sce, ok := siacoinElements[outputID]
				if !ok {
					panic("missing siacoin element")
				}

				addEvent(types.Hash256(fce.ID.ValidOutputID(0)), EventTypeV1ContractResolution, EventV1ContractResolution{
					Parent:         fce,
					SiacoinElement: sce,
					Missed:         false,
				}, sce.MaturityHeight)
			}
		} else {
			for i, so := range fce.FileContract.MissedProofOutputs {
				if so.Address != walletAddress {
					continue
				}

				outputID := fce.ID.MissedOutputID(i)
				sce, ok := siacoinElements[outputID]
				if !ok {
					panic("missing siacoin element")
				}

				addEvent(types.Hash256(fce.ID.MissedOutputID(0)), EventTypeV1ContractResolution, EventV1ContractResolution{
					Parent:         fce,
					SiacoinElement: sce,
					Missed:         true,
				}, sce.MaturityHeight)
			}
		}
	}

	for _, fced := range cau.V2FileContractElementDiffs() {
		if fced.Resolution == nil {
			continue
		}

		fce := fced.V2FileContractElement
		fce.StateElement.MerkleProof = nil // clear the proof to save space

		_, missed := fced.Resolution.(*types.V2FileContractExpiration)
		if fce.V2FileContract.HostOutput.Address == walletAddress {
			outputID := fce.ID.V2HostOutputID()
			sce, ok := siacoinElements[outputID]
			if !ok {
				panic("missing siacoin element")
			}

			addEvent(types.Hash256(outputID), EventTypeV2ContractResolution, EventV2ContractResolution{
				Resolution: types.V2FileContractResolution{
					Parent:     fce,
					Resolution: fced.Resolution,
				},
				SiacoinElement: sce,
				Missed:         missed,
			}, sce.MaturityHeight)
		}

		if fce.V2FileContract.RenterOutput.Address == walletAddress {
			outputID := fce.ID.V2RenterOutputID()
			sce, ok := siacoinElements[outputID]
			if !ok {
				panic("missing siacoin element")
			}

			addEvent(types.Hash256(outputID), EventTypeV2ContractResolution, EventV2ContractResolution{
				Resolution: types.V2FileContractResolution{
					Parent:     fce,
					Resolution: fced.Resolution,
				},
				SiacoinElement: sce,
				Missed:         missed,
			}, sce.MaturityHeight)
		}
	}

	blockID := block.ID()
	for i, so := range block.MinerPayouts {
		if so.Address != walletAddress {
			continue
		}

		outputID := blockID.MinerOutputID(i)
		sce, ok := siacoinElements[outputID]
		if !ok {
			panic("missing siacoin element")
		}
		addEvent(types.Hash256(outputID), EventTypeMinerPayout, EventPayout{
			SiacoinElement: sce,
		}, sce.MaturityHeight)
	}

	outputID := blockID.FoundationOutputID()
	if sce, ok := siacoinElements[outputID]; ok && sce.SiacoinOutput.Address == walletAddress {
		addEvent(types.Hash256(outputID), EventTypeFoundationSubsidy, EventPayout{
			SiacoinElement: sce,
		}, sce.MaturityHeight)
	}
	return
}

// applyChainUpdate atomically applies a chain update
func (sw *SingleAddressWallet) applyChainUpdate(tx UpdateTx, address types.Address, cau chain.ApplyUpdate) error {
	// update current state elements
	if err := tx.UpdateWalletSiacoinElementProofs(cau); err != nil {
		return fmt.Errorf("failed to update state elements: %w", err)
	}

	var createdUTXOs, spentUTXOs []types.SiacoinElement
	for _, sced := range cau.SiacoinElementDiffs() {
		switch {
		case sced.Created && sced.Spent:
			continue // ignore ephemeral elements
		case sced.SiacoinElement.SiacoinOutput.Address != address:
			continue // ignore elements that are not related to the wallet
		case sced.Created:
			createdUTXOs = append(createdUTXOs, sced.SiacoinElement)
		case sced.Spent:
			spentUTXOs = append(spentUTXOs, sced.SiacoinElement)
		default:
			panic("unexpected siacoin element") // developer error
		}
	}

	if err := tx.WalletApplyIndex(cau.State.Index, createdUTXOs, spentUTXOs, appliedEvents(cau, address), cau.Block.Timestamp); err != nil {
		return fmt.Errorf("failed to apply index: %w", err)
	}
	sw.mu.Lock()
	sw.tip = cau.State.Index
	sw.mu.Unlock()
	return nil
}

// revertChainUpdate atomically reverts a chain update from a wallet
func (sw *SingleAddressWallet) revertChainUpdate(tx UpdateTx, revertedIndex types.ChainIndex, address types.Address, cru chain.RevertUpdate) error {
	var removedUTXOs, unspentUTXOs []types.SiacoinElement
	for _, sced := range cru.SiacoinElementDiffs() {
		switch {
		case sced.Created && sced.Spent:
			continue // ignore ephemeral elements
		case sced.SiacoinElement.SiacoinOutput.Address != address:
			continue // ignore elements that are not related to the wallet
		case sced.Spent:
			unspentUTXOs = append(unspentUTXOs, sced.SiacoinElement)
		case sced.Created:
			removedUTXOs = append(removedUTXOs, sced.SiacoinElement)
		default:
			panic("unexpected siacoin element") // developer error
		}
	}

	// remove any existing events that were added in the reverted block
	if err := tx.WalletRevertIndex(revertedIndex, removedUTXOs, unspentUTXOs, cru.Block.Timestamp); err != nil {
		return fmt.Errorf("failed to revert block: %w", err)
	}

	// update the remaining state elements
	if err := tx.UpdateWalletSiacoinElementProofs(cru); err != nil {
		return fmt.Errorf("failed to update state elements: %w", err)
	}
	sw.mu.Lock()
	sw.tip = revertedIndex
	sw.mu.Unlock()
	return nil
}

// UpdateChainState atomically applies and reverts chain updates to a single
// wallet store.
func (sw *SingleAddressWallet) UpdateChainState(tx UpdateTx, reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	for _, cru := range reverted {
		revertedIndex := types.ChainIndex{
			ID:     cru.Block.ID(),
			Height: cru.State.Index.Height + 1,
		}
		err := sw.revertChainUpdate(tx, revertedIndex, sw.addr, cru)
		if err != nil {
			return fmt.Errorf("failed to revert chain update %q: %w", cru.State.Index, err)
		}
	}

	for _, cau := range applied {
		err := sw.applyChainUpdate(tx, sw.addr, cau)
		if err != nil {
			return fmt.Errorf("failed to apply chain update %q: %w", cau.State.Index, err)
		}
	}
	return nil
}
