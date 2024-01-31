package testutil

import (
	"fmt"
	"slices"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
)

// An EphemeralWalletStore is a Store that does not persist its state to disk. It is
// primarily useful for testing or as a reference implementation.
type (
	EphemeralWalletStore struct {
		privateKey types.PrivateKey

		mu          sync.Mutex
		uncommitted []*chain.ApplyUpdate

		tip          types.ChainIndex
		utxos        map[types.SiacoinOutputID]wallet.SiacoinElement
		transactions []wallet.Transaction
	}

	ephemeralWalletUpdateTxn struct {
		store *EphemeralWalletStore
	}
)

func (et *ephemeralWalletUpdateTxn) WalletStateElements() (eles []types.StateElement, _ error) {
	for _, se := range et.store.utxos {
		eles = append(eles, se.StateElement)
	}
	return
}

func (et *ephemeralWalletUpdateTxn) UpdateStateElements(elements []types.StateElement) error {
	for _, se := range elements {
		utxo := et.store.utxos[types.SiacoinOutputID(se.ID)]
		utxo.StateElement = se
		et.store.utxos[types.SiacoinOutputID(se.ID)] = utxo
	}
	return nil
}

func (et *ephemeralWalletUpdateTxn) AddTransactions(transactions []wallet.Transaction) error {
	// transactions are added in reverse order to make the most recent transaction the first
	slices.Reverse(transactions)
	et.store.transactions = append(transactions, et.store.transactions...)
	return nil
}

func (et *ephemeralWalletUpdateTxn) AddSiacoinElements(elements []wallet.SiacoinElement) error {
	for _, se := range elements {
		if _, ok := et.store.utxos[types.SiacoinOutputID(se.ID)]; ok {
			return fmt.Errorf("siacoin element %q already exists", se.ID)
		}
		et.store.utxos[types.SiacoinOutputID(se.ID)] = se
	}
	return nil
}

func (et *ephemeralWalletUpdateTxn) RemoveSiacoinElements(ids []types.SiacoinOutputID) error {
	for _, id := range ids {
		if _, ok := et.store.utxos[id]; !ok {
			return fmt.Errorf("siacoin element %q does not exist", id)
		}
		delete(et.store.utxos, id)
	}
	return nil
}

func (et *ephemeralWalletUpdateTxn) RevertIndex(index types.ChainIndex) error {
	// remove any transactions that were added in the reverted block
	filtered := et.store.transactions[:0]
	for i := range et.store.transactions {
		if et.store.transactions[i].Index == index {
			continue
		}
		filtered = append(filtered, et.store.transactions[i])
	}
	et.store.transactions = filtered

	// remove any siacoin elements that were added in the reverted block
	for id, se := range et.store.utxos {
		if se.Index == index {
			delete(et.store.utxos, id)
		}
	}
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
func (es *EphemeralWalletStore) UnspentSiacoinElements() (utxos []wallet.SiacoinElement, _ error) {
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

func (es *EphemeralWalletStore) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	es.uncommitted = append(es.uncommitted, cau)
	if !mayCommit {
		return nil
	}

	address := types.StandardUnlockHash(es.privateKey.PublicKey())
	ephemeralWalletUpdateTxn := &ephemeralWalletUpdateTxn{store: es}

	if err := wallet.ApplyChainUpdates(ephemeralWalletUpdateTxn, address, es.uncommitted); err != nil {
		return err
	}
	es.tip = cau.State.Index
	es.uncommitted = nil
	return nil
}

func (es *EphemeralWalletStore) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	if len(es.uncommitted) > 0 && es.uncommitted[len(es.uncommitted)-1].State.Index == cru.State.Index {
		es.uncommitted = es.uncommitted[:len(es.uncommitted)-1]
		return nil
	}

	address := types.StandardUnlockHash(es.privateKey.PublicKey())
	return wallet.RevertChainUpdate(&ephemeralWalletUpdateTxn{store: es}, address, cru)
}

// NewEphemeralWalletStore returns a new EphemeralWalletStore.
func NewEphemeralWalletStore(pk types.PrivateKey) *EphemeralWalletStore {
	return &EphemeralWalletStore{
		privateKey: pk,

		utxos:        make(map[types.SiacoinOutputID]wallet.SiacoinElement),
		transactions: make([]wallet.Transaction, 0),
	}
}
