package testutil

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

// An EphemeralWalletStore is a Store that does not persist its state to disk. It is
// primarily useful for testing or as a reference implementation.
type (
	EphemeralWalletStore struct {
		mu          sync.Mutex
		tip         types.ChainIndex
		utxos       map[types.SiacoinOutputID]types.SiacoinElement
		events      []wallet.Event
		broadcasted []wallet.BroadcastedSet
	}

	ephemeralWalletUpdateTxn struct {
		store *EphemeralWalletStore
	}
)

var _ wallet.SingleAddressStore = (*EphemeralWalletStore)(nil)

func (et *ephemeralWalletUpdateTxn) WalletStateElements() (elements []types.StateElement, _ error) {
	for _, se := range et.store.utxos {
		elements = append(elements, se.StateElement.Copy())
	}
	return
}

// UpdateWalletSiacoinElementProofs updates the proofs of all state elements
// affected by the update. ProofUpdater.UpdateElementProof must be called
// for each state element in the database.
func (et *ephemeralWalletUpdateTxn) UpdateWalletSiacoinElementProofs(pu wallet.ProofUpdater) error {
	for _, se := range et.store.utxos {
		pu.UpdateElementProof(&se.StateElement)
		et.store.utxos[se.ID] = se.Move()
	}
	return nil
}

func (et *ephemeralWalletUpdateTxn) WalletApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event, _ time.Time) error {
	for _, se := range spent {
		if _, ok := et.store.utxos[se.ID]; !ok {
			panic(fmt.Sprintf("siacoin element %q does not exist", se.ID))
		}
		delete(et.store.utxos, se.ID)
	}
	// add siacoin elements
	for _, se := range created {
		if _, ok := et.store.utxos[se.ID]; ok {
			panic("duplicate element")
		}
		et.store.utxos[se.ID] = se.Copy()
	}

	// add events
	et.store.events = append(et.store.events, events...)
	et.store.tip = index
	return nil
}

func (et *ephemeralWalletUpdateTxn) WalletRevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement, _ time.Time) error {
	// remove any events that were added in the reverted block
	filtered := et.store.events[:0]
	for i := range et.store.events {
		if et.store.events[i].Index == index {
			continue
		}
		filtered = append(filtered, et.store.events[i])
	}
	et.store.events = filtered

	// remove any siacoin elements that were added in the reverted block
	for _, se := range removed {
		delete(et.store.utxos, se.ID)
	}

	// readd any siacoin elements that were spent in the reverted block
	for _, se := range unspent {
		et.store.utxos[se.ID] = se.Copy()
	}
	et.store.tip = index
	return nil
}

// UpdateChainState applies and reverts chain updates to the wallet.
func (es *EphemeralWalletStore) UpdateChainState(fn func(ux wallet.UpdateTx) error) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	return fn(&ephemeralWalletUpdateTxn{store: es})
}

// WalletEvents returns the wallet's events.
func (es *EphemeralWalletStore) WalletEvents(offset, limit int) ([]wallet.Event, error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	n := len(es.events)
	start, end := offset, offset+limit
	if start > n {
		return nil, nil
	} else if end > n {
		end = n
	}
	// events are inserted in chronological order, reverse the slice to get the
	// correct display order then sort by maturity height, so
	// immature events are displayed first.
	events := append([]wallet.Event(nil), es.events...)
	slices.Reverse(events)
	sort.SliceStable(events, func(i, j int) bool {
		return events[i].MaturityHeight > events[j].MaturityHeight
	})
	return events[start:end], nil
}

// WalletEventCount returns the number of events relevant to the wallet.
func (es *EphemeralWalletStore) WalletEventCount() (uint64, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return uint64(len(es.events)), nil
}

// UnspentSiacoinElements returns the wallet's unspent siacoin outputs as well
// as the last indexed tip of the wallet.
func (es *EphemeralWalletStore) UnspentSiacoinElements() (tip types.ChainIndex, utxos []types.SiacoinElement, _ error) {
	es.mu.Lock()
	defer es.mu.Unlock()

	for _, se := range es.utxos {
		utxos = append(utxos, se.Copy())
	}
	return es.tip, utxos, nil
}

// Tip returns the last indexed tip of the wallet.
func (es *EphemeralWalletStore) Tip() (types.ChainIndex, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.tip, nil
}

// AddBroadcastedSet adds a set of broadcasted transactions. The wallet will
// periodically rebroadcast the transactions in this set until all transactions
// are gone from the transaction pool or one week has passed.
func (es *EphemeralWalletStore) AddBroadcastedSet(set wallet.BroadcastedSet) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	for _, existing := range es.broadcasted {
		if existing.ID() == set.ID() {
			return nil
		}
	}
	es.broadcasted = append(es.broadcasted, set)
	return nil
}

// BroadcastedSets returns recently broadcasted sets.
func (es *EphemeralWalletStore) BroadcastedSets() ([]wallet.BroadcastedSet, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	return es.broadcasted, nil
}

// RemoveBroadcastedSet removes a set so it's no longer rebroadcasted.
func (es *EphemeralWalletStore) RemoveBroadcastedSet(set wallet.BroadcastedSet) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	for i, existing := range es.broadcasted {
		if existing.ID() == set.ID() {
			es.broadcasted = append(es.broadcasted[:i], es.broadcasted[i+1:]...)
			return nil
		}
	}
	return errors.New("broadcasted set not found")
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore() *EphemeralWalletStore {
	return &EphemeralWalletStore{
		utxos: make(map[types.SiacoinOutputID]types.SiacoinElement),
	}
}
