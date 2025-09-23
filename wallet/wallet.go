package wallet

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
)

const (
	// bytesPerInput is the encoded size of a SiacoinInput and corresponding
	// TransactionSignature, assuming standard UnlockConditions.
	bytesPerInput = 241

	// redistributeBatchSize is the number of outputs to redistribute per txn to
	// avoid creating a txn that is too large.
	redistributeBatchSize = 10
)

var (
	// ErrNotEnoughFunds is returned when there are not enough unspent outputs
	// to fund a transaction.
	ErrNotEnoughFunds = errors.New("not enough funds")

	// maxFee is the maximum fee returned by RecommendedFee. It equals 1SC per
	// 2000 bytes which equals the size of a large transaction.
	maxFee = types.Siacoins(1).Div64(2000)
)

type (
	// Balance is the balance of a wallet.
	Balance struct {
		Spendable   types.Currency `json:"spendable"`
		Confirmed   types.Currency `json:"confirmed"`
		Unconfirmed types.Currency `json:"unconfirmed"`
		Immature    types.Currency `json:"immature"`
	}

	// A ChainManager manages the current state of the blockchain.
	ChainManager interface {
		AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, err error)
		TipState() consensus.State
		BestIndex(height uint64) (types.ChainIndex, bool)
		PoolTransactions() []types.Transaction
		RecommendedFee() types.Currency
		V2PoolTransactions() []types.V2Transaction
		UpdateV2TransactionSet(txns []types.V2Transaction, from, to types.ChainIndex) ([]types.V2Transaction, error)
		OnReorg(func(types.ChainIndex)) func()
	}

	// A SingleAddressStore stores the state of a single-address wallet.
	// Implementations are assumed to be thread safe.
	SingleAddressStore interface {
		// Tip returns the consensus change ID and block height of
		// the last wallet change.
		Tip() (types.ChainIndex, error)
		// UnspentSiacoinElements returns the current chain tip along with a
		// list of all unspent siacoin outputs, including immature ones.
		UnspentSiacoinElements() (types.ChainIndex, []types.SiacoinElement, error)
		// WalletEvent returns an individual event with a given ID.
		WalletEvent(id types.Hash256) (Event, error)
		// WalletEvents returns a paginated list of transactions ordered by
		// maturity height, descending. If no more transactions are available,
		// (nil, nil) should be returned.
		WalletEvents(offset, limit int) ([]Event, error)
		// WalletEventCount returns the total number of events relevant to the
		// wallet.
		WalletEventCount() (uint64, error)

		// AddBroadcastedSet adds a set of broadcasted transactions. The wallet
		// will periodically rebroadcast the transactions in this set until all
		// transactions are gone from the transaction pool or one week has
		// passed.
		AddBroadcastedSet(BroadcastedSet) error
		// BroadcastedSets returns recently broadcasted sets.
		BroadcastedSets() ([]BroadcastedSet, error)
		// RemoveBroadcastedSet removes a set so it's no longer rebroadcasted.
		RemoveBroadcastedSet(BroadcastedSet) error
	}

	// A BroadcastedSet is a transaction set that was successfully broadcasted.
	// This set will be periodically rebroadcasted for one week or until it's
	// removed from the store.
	BroadcastedSet struct {
		Basis         types.ChainIndex
		BroadcastedAt time.Time
		Transactions  []types.V2Transaction
	}

	// A SingleAddressWallet is a hot wallet that manages the outputs controlled
	// by a single address.
	SingleAddressWallet struct {
		priv types.PrivateKey
		addr types.Address

		cm     ChainManager
		store  SingleAddressStore
		syncer Syncer

		tg  *threadgroup.ThreadGroup
		log *zap.Logger

		cfg config

		mu sync.Mutex
		// locked is a set of siacoin output IDs locked by FundTransaction. They
		// will be released either by calling Release for unused transactions or
		// being confirmed in a block.
		locked map[types.SiacoinOutputID]time.Time
	}

	// A Syncer can connect to other peers and broadcast transactions to the
	// network.
	Syncer interface {
		BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error
	}
)

// ID returns a unique identifier for the BroadcastedSet.
func (set BroadcastedSet) ID() types.Hash256 {
	h := types.NewHasher()
	set.Basis.EncodeTo(h.E)
	for _, txn := range set.Transactions {
		txn.EncodeTo(h.E)
	}
	h.E.WriteTime(set.BroadcastedAt)
	return h.Sum()
}

var (
	// ErrDifferentSeed is returned when a different seed is provided to
	// NewSingleAddressWallet than was used to initialize the wallet
	ErrDifferentSeed = errors.New("seed differs from wallet seed")

	// ErrEventNotFound is returned when Event can not find an event with the
	// given ID.
	ErrEventNotFound = errors.New("event not found")
)

// Close closes the wallet
func (sw *SingleAddressWallet) Close() error {
	sw.tg.Stop()
	return nil
}

// Address returns the address of the wallet.
func (sw *SingleAddressWallet) Address() types.Address {
	return sw.addr
}

// UnlockConditions returns the unlock conditions of the wallet.
func (sw *SingleAddressWallet) UnlockConditions() types.UnlockConditions {
	return types.StandardUnlockConditions(sw.priv.PublicKey())
}

// Balance returns the balance of the wallet.
func (sw *SingleAddressWallet) Balance() (balance Balance, err error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	_, outputs, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return Balance{}, fmt.Errorf("failed to get unspent outputs: %w", err)
	}

	tpoolSpent := make(map[types.SiacoinOutputID]bool)
	tpoolUtxos := make(map[types.SiacoinOutputID]types.SiacoinElement)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			if sci.UnlockConditions.UnlockHash() != sw.addr {
				continue
			}
			tpoolSpent[sci.ParentID] = true
			delete(tpoolUtxos, sci.ParentID)
		}
		for i, sco := range txn.SiacoinOutputs {
			if sco.Address != sw.addr {
				continue
			}

			outputID := txn.SiacoinOutputID(i)
			tpoolUtxos[outputID] = types.SiacoinElement{
				ID:            types.SiacoinOutputID(outputID),
				StateElement:  types.StateElement{LeafIndex: types.UnassignedLeafIndex},
				SiacoinOutput: sco,
			}
		}
	}

	for _, txn := range sw.cm.V2PoolTransactions() {
		for _, si := range txn.SiacoinInputs {
			if si.Parent.SiacoinOutput.Address != sw.addr {
				continue
			}
			tpoolSpent[si.Parent.ID] = true
			delete(tpoolUtxos, si.Parent.ID)
		}
		for i, sco := range txn.SiacoinOutputs {
			if sco.Address != sw.addr {
				continue
			}
			sce := txn.EphemeralSiacoinOutput(i)
			tpoolUtxos[sce.ID] = sce.Move()
		}
	}

	bh := sw.cm.TipState().Index.Height
	for _, sco := range outputs {
		if sco.MaturityHeight > bh {
			balance.Immature = balance.Immature.Add(sco.SiacoinOutput.Value)
		} else {
			balance.Confirmed = balance.Confirmed.Add(sco.SiacoinOutput.Value)
			if !sw.isLocked(sco.ID) && !tpoolSpent[sco.ID] {
				balance.Spendable = balance.Spendable.Add(sco.SiacoinOutput.Value)
			}
		}
	}

	for _, sco := range tpoolUtxos {
		balance.Unconfirmed = balance.Unconfirmed.Add(sco.SiacoinOutput.Value)
	}
	return
}

// Event returns an individual event with a given ID.
func (sw *SingleAddressWallet) Event(id types.Hash256) (Event, error) {
	return sw.store.WalletEvent(id)
}

// Events returns a paginated list of events, ordered by maturity height, descending.
// If no more events are available, (nil, nil) is returned.
func (sw *SingleAddressWallet) Events(offset, limit int) ([]Event, error) {
	return sw.store.WalletEvents(offset, limit)
}

// EventCount returns the total number of events relevant to the wallet.
func (sw *SingleAddressWallet) EventCount() (uint64, error) {
	return sw.store.WalletEventCount()
}

// SpendableOutputs returns a list of spendable siacoin outputs, a spendable
// output is an unspent output that's not locked, not currently in the
// transaction pool and that has matured.
func (sw *SingleAddressWallet) SpendableOutputs() ([]types.SiacoinElement, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	// fetch outputs from the store
	tip, utxos, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, err
	}

	// fetch outputs currently in the pool
	inPool := make(map[types.SiacoinOutputID]bool)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			inPool[sci.ParentID] = true
		}
	}

	// filter outputs that are either locked, in the pool or have not yet matured
	unspent := utxos[:0]
	for _, sce := range utxos {
		if sw.isLocked(sce.ID) || inPool[sce.ID] || tip.Height < sce.MaturityHeight {
			continue
		}
		unspent = append(unspent, sce.Copy())
	}
	return unspent, nil
}

func (sw *SingleAddressWallet) selectUTXOs(amount types.Currency, inputs int, useUnconfirmed bool) (types.ChainIndex, []types.SiacoinElement, types.Currency, error) {
	tip, elements, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return types.ChainIndex{}, nil, types.ZeroCurrency, err
	}

	if amount.IsZero() {
		return tip, nil, types.ZeroCurrency, nil
	}

	tpoolSpent := make(map[types.SiacoinOutputID]bool)
	tpoolUtxos := make(map[types.SiacoinOutputID]types.SiacoinElement)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			tpoolSpent[sci.ParentID] = true
			delete(tpoolUtxos, sci.ParentID)
		}
		for i, sco := range txn.SiacoinOutputs {
			tpoolUtxos[txn.SiacoinOutputID(i)] = types.SiacoinElement{
				ID:            txn.SiacoinOutputID(i),
				StateElement:  types.StateElement{LeafIndex: types.UnassignedLeafIndex},
				SiacoinOutput: sco,
			}
		}
	}
	for _, txn := range sw.cm.V2PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			tpoolSpent[sci.Parent.ID] = true
			delete(tpoolUtxos, sci.Parent.ID)
		}
		for i := range txn.SiacoinOutputs {
			sce := txn.EphemeralSiacoinOutput(i)
			tpoolUtxos[sce.ID] = sce.Move()
		}
	}

	// remove immature, locked and spent outputs
	utxos := make([]types.SiacoinElement, 0, len(elements))
	var usedSum types.Currency
	var immatureSum types.Currency
	for _, sce := range elements {
		if used := sw.isLocked(sce.ID) || tpoolSpent[sce.ID]; used {
			usedSum = usedSum.Add(sce.SiacoinOutput.Value)
			continue
		} else if immature := tip.Height < sce.MaturityHeight; immature {
			immatureSum = immatureSum.Add(sce.SiacoinOutput.Value)
			continue
		}
		utxos = append(utxos, sce.Share())
	}

	// sort by value, descending
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].SiacoinOutput.Value.Cmp(utxos[j].SiacoinOutput.Value) > 0
	})

	var unconfirmedUTXOs []types.SiacoinElement
	var unconfirmedSum types.Currency
	if useUnconfirmed {
		for _, sce := range tpoolUtxos {
			if sce.SiacoinOutput.Address != sw.addr || sw.isLocked(sce.ID) {
				continue
			}
			unconfirmedUTXOs = append(unconfirmedUTXOs, sce.Share())
			unconfirmedSum = unconfirmedSum.Add(sce.SiacoinOutput.Value)
		}
	}

	// sort by value, descending
	sort.Slice(unconfirmedUTXOs, func(i, j int) bool {
		return unconfirmedUTXOs[i].SiacoinOutput.Value.Cmp(unconfirmedUTXOs[j].SiacoinOutput.Value) > 0
	})

	// fund the transaction using the largest utxos first
	var selected []types.SiacoinElement
	var inputSum types.Currency
	for i, sce := range utxos {
		if inputSum.Cmp(amount) >= 0 {
			utxos = utxos[i:]
			break
		}
		selected = append(selected, sce.Share())
		inputSum = inputSum.Add(sce.SiacoinOutput.Value)
	}

	if inputSum.Cmp(amount) < 0 && useUnconfirmed {
		// try adding unconfirmed utxos.
		for _, sce := range unconfirmedUTXOs {
			selected = append(selected, sce.Share())
			inputSum = inputSum.Add(sce.SiacoinOutput.Value)
			if inputSum.Cmp(amount) >= 0 {
				break
			}
		}

		if inputSum.Cmp(amount) < 0 {
			// still not enough funds
			return types.ChainIndex{}, nil, types.ZeroCurrency, fmt.Errorf("%w: inputs %v < needed %v (used: %v immature: %v unconfirmed: %v)", ErrNotEnoughFunds, inputSum.String(), amount.String(), usedSum.String(), immatureSum.String(), unconfirmedSum.String())
		}
	} else if inputSum.Cmp(amount) < 0 {
		return types.ChainIndex{}, nil, types.ZeroCurrency, fmt.Errorf("%w: inputs %v < needed %v (used: %v immature: %v", ErrNotEnoughFunds, inputSum.String(), amount.String(), usedSum.String(), immatureSum.String())
	}

	// check if remaining utxos should be defragged
	txnInputs := inputs + len(selected)
	if len(utxos) > sw.cfg.DefragThreshold && txnInputs < sw.cfg.MaxInputsForDefrag {
		// add the smallest utxos to the transaction
		defraggable := utxos
		if len(defraggable) > sw.cfg.MaxDefragUTXOs {
			defraggable = defraggable[len(defraggable)-sw.cfg.MaxDefragUTXOs:]
		}
		for i := len(defraggable) - 1; i >= 0; i-- {
			if txnInputs >= sw.cfg.MaxInputsForDefrag {
				break
			}

			sce := &defraggable[i]
			selected = append(selected, sce.Share())
			inputSum = inputSum.Add(sce.SiacoinOutput.Value)
			txnInputs++
		}
	}
	return tip, selected, inputSum, nil
}

// cleanLockedUTXOs cleans up the locked UTXOs map by removing entries
// that have expired.
// It is expected that the caller will hold sw.mu.
func (sw *SingleAddressWallet) cleanLockedUTXOs() {
	for id, expiration := range sw.locked {
		if time.Now().After(expiration) {
			delete(sw.locked, id)
		}
	}
}

// lockUTXOs locks the given siacoin output IDs for use in a transaction.
// The outputs will be unlocked after the given time.
// It is expected that the caller will hold sw.mu.
func (sw *SingleAddressWallet) lockUTXOs(ids []types.SiacoinOutputID) {
	sw.cleanLockedUTXOs()
	if len(ids) == 0 {
		return
	}
	expirationTimestamp := time.Now().Add(sw.cfg.ReservationDuration)
	for _, id := range ids {
		sw.locked[id] = expirationTimestamp
	}
}

// FundTransaction adds siacoin inputs worth at least amount to the provided
// transaction. If necessary, a change output will also be added. The inputs
// will not be available to future calls to FundTransaction unless ReleaseInputs
// is called.
func (sw *SingleAddressWallet) FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error) {
	if amount.IsZero() {
		return nil, nil
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	_, selected, inputSum, err := sw.selectUTXOs(amount, len(txn.SiacoinInputs), useUnconfirmed)
	if err != nil {
		return nil, err
	}

	// add a change output if necessary
	if inputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   inputSum.Sub(amount),
			Address: sw.addr,
		})
	}

	toLock := make([]types.SiacoinOutputID, 0, len(selected))
	toSign := make([]types.Hash256, 0, len(selected))
	for _, sce := range selected {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         sce.ID,
			UnlockConditions: types.StandardUnlockConditions(sw.priv.PublicKey()),
		})
		toSign = append(toSign, types.Hash256(sce.ID))
		toLock = append(toLock, sce.ID)
	}
	sw.lockUTXOs(toLock)
	return toSign, nil
}

// SignTransaction adds a signature to each of the specified inputs.
func (sw *SingleAddressWallet) SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) {
	state := sw.cm.TipState()

	for _, id := range toSign {
		var h types.Hash256
		if cf.WholeTransaction {
			h = state.WholeSigHash(*txn, id, 0, 0, cf.Signatures)
		} else {
			h = state.PartialSigHash(*txn, cf)
		}
		sig := sw.priv.SignHash(h)
		txn.Signatures = append(txn.Signatures, types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
			Signature:      sig[:],
		})
	}
}

// FundV2Transaction adds siacoin inputs worth at least amount to the provided
// transaction. If necessary, a change output will also be added. The inputs
// will not be available to future calls to FundTransaction unless ReleaseInputs
// is called.
//
// The returned index should be used as the basis for AddV2PoolTransactions.
func (sw *SingleAddressWallet) FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error) {
	if amount.IsZero() {
		tip, err := sw.store.Tip()
		return tip, nil, err
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	tip, selected, inputSum, err := sw.selectUTXOs(amount, len(txn.SiacoinInputs), useUnconfirmed)
	if err != nil {
		return types.ChainIndex{}, nil, err
	}

	// add a change output if necessary
	if inputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   inputSum.Sub(amount),
			Address: sw.addr,
		})
	}

	toLock := make([]types.SiacoinOutputID, 0, len(selected))
	toSign := make([]int, 0, len(selected))
	for _, sce := range selected {
		toSign = append(toSign, len(txn.SiacoinInputs))
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.V2SiacoinInput{
			Parent: sce.Copy(),
		})
		toLock = append(toLock, sce.ID)
	}

	sw.lockUTXOs(toLock)
	return tip, toSign, nil
}

// SignV2Inputs adds a signature to each of the specified siacoin inputs.
func (sw *SingleAddressWallet) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	if len(toSign) == 0 {
		return
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	policy := sw.SpendPolicy()
	sigHash := sw.cm.TipState().InputSigHash(*txn)
	for _, i := range toSign {
		txn.SiacoinInputs[i].SatisfiedPolicy = types.SatisfiedPolicy{
			Policy:     policy,
			Signatures: []types.Signature{sw.SignHash(sigHash)},
		}
	}
}

// Tip returns the block height the wallet has scanned to.
func (sw *SingleAddressWallet) Tip() (types.ChainIndex, error) {
	return sw.store.Tip()
}

// SpendPolicy returns the wallet's default spend policy.
func (sw *SingleAddressWallet) SpendPolicy() types.SpendPolicy {
	return types.SpendPolicy{Type: types.PolicyTypeUnlockConditions(sw.UnlockConditions())}
}

// SignHash signs the hash with the wallet's private key.
func (sw *SingleAddressWallet) SignHash(h types.Hash256) types.Signature {
	return sw.priv.SignHash(h)
}

// UnconfirmedEvents returns all unconfirmed transactions relevant to the
// wallet.
func (sw *SingleAddressWallet) UnconfirmedEvents() (annotated []Event, err error) {
	tip, confirmed, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, fmt.Errorf("failed to get unspent outputs: %w", err)
	}

	utxos := make(map[types.SiacoinOutputID]types.SiacoinElement)
	for _, se := range confirmed {
		utxos[se.ID] = se.Share()
	}

	index := types.ChainIndex{Height: tip.Height + 1}
	timestamp := time.Now().Truncate(time.Second)

	addEvent := func(id types.Hash256, eventType string, data EventData) {
		ev := Event{
			ID:             id,
			Index:          index,
			MaturityHeight: index.Height,
			Timestamp:      timestamp,
			Type:           eventType,
			Data:           data,
			Relevant:       []types.Address{sw.addr},
		}

		if ev.SiacoinInflow().Equals(ev.SiacoinOutflow()) {
			// ignore events that don't affect the wallet
			return
		}
		annotated = append(annotated, ev)
	}

	for _, txn := range sw.cm.PoolTransactions() {
		event := EventV1Transaction{
			Transaction: txn,
		}

		var outflow types.Currency
		for _, sci := range txn.SiacoinInputs {
			sce, ok := utxos[sci.ParentID]
			if !ok {
				// ignore inputs that don't belong to the wallet
				continue
			}
			outflow = outflow.Add(sce.SiacoinOutput.Value)
			event.SpentSiacoinElements = append(event.SpentSiacoinElements, sce.Share())
		}

		var inflow types.Currency
		for i, so := range txn.SiacoinOutputs {
			if so.Address == sw.addr {
				inflow = inflow.Add(so.Value)
				utxos[txn.SiacoinOutputID(i)] = types.SiacoinElement{
					ID:            txn.SiacoinOutputID(i),
					StateElement:  types.StateElement{LeafIndex: types.UnassignedLeafIndex},
					SiacoinOutput: so,
				}
			}
		}

		// skip transactions that don't affect the wallet
		if inflow.IsZero() && outflow.IsZero() {
			continue
		}
		addEvent(types.Hash256(txn.ID()), EventTypeV1Transaction, event)
	}

	for _, txn := range sw.cm.V2PoolTransactions() {
		var inflow, outflow types.Currency
		for _, sci := range txn.SiacoinInputs {
			if sci.Parent.SiacoinOutput.Address != sw.addr {
				continue
			}
			outflow = outflow.Add(sci.Parent.SiacoinOutput.Value)
		}

		for _, sco := range txn.SiacoinOutputs {
			if sco.Address != sw.addr {
				continue
			}
			inflow = inflow.Add(sco.Value)
		}

		// skip transactions that don't affect the wallet
		if inflow.IsZero() && outflow.IsZero() {
			continue
		}

		addEvent(types.Hash256(txn.ID()), EventTypeV2Transaction, EventV2Transaction(txn))
	}
	return annotated, nil
}

func (sw *SingleAddressWallet) selectRedistributeUTXOs(bh uint64, outputs int, amount types.Currency, elements []types.SiacoinElement) ([]types.SiacoinElement, int, error) {
	// fetch outputs currently in the pool
	inPool := make(map[types.SiacoinOutputID]bool)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			inPool[sci.ParentID] = true
		}
	}
	for _, txn := range sw.cm.V2PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			inPool[sci.Parent.ID] = true
		}
	}

	// adjust the number of desired outputs for any output we encounter that is
	// unused, matured and has the same value
	utxos := make([]types.SiacoinElement, 0, len(elements))
	for _, sce := range elements {
		inUse := sw.isLocked(sce.ID) || inPool[sce.ID]
		matured := bh >= sce.MaturityHeight
		sameValue := sce.SiacoinOutput.Value.Equals(amount)

		// adjust number of desired outputs
		if !inUse && matured && sameValue {
			outputs--
		}

		// collect usable outputs for defragging
		if !inUse && matured && !sameValue {
			utxos = append(utxos, sce.Share())
		}
	}
	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].SiacoinOutput.Value.Cmp(utxos[j].SiacoinOutput.Value) > 0
	})
	return utxos, outputs, nil
}

// Redistribute returns a set of transactions that redistribute money in the
// wallet by selecting a minimal set of inputs to cover the creation of the
// requested outputs. It also returns a list of output IDs that need to be
// signed. The returned index should be used as the basis when broadcasting the
// transactions.
func (sw *SingleAddressWallet) Redistribute(outputs int, amount, feePerByte types.Currency) (types.ChainIndex, []types.V2Transaction, [][]int, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	tip, elements, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return types.ChainIndex{}, nil, nil, err
	}

	var toLock []types.SiacoinOutputID
	utxos, outputs, err := sw.selectRedistributeUTXOs(tip.Height, outputs, amount, elements)
	if err != nil {
		return types.ChainIndex{}, nil, nil, err
	}

	// return early if we don't have to defrag at all
	if outputs <= 0 {
		return types.ChainIndex{}, nil, nil, nil
	}

	var txns []types.V2Transaction
	var toSign [][]int

	// prepare defrag transactions
	state := sw.cm.TipState()
	for outputs > 0 {
		var txn types.V2Transaction
		for i := 0; i < outputs && i < redistributeBatchSize; i++ {
			txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
				Value:   amount,
				Address: sw.addr,
			})
		}
		outputs -= len(txn.SiacoinOutputs)

		// estimate the fees
		outputFees := feePerByte.Mul64(state.V2TransactionWeight(txn))
		feePerInput := feePerByte.Mul64(bytesPerInput)

		// collect outputs that cover the total amount
		var inputs []types.SiacoinElement
		want := amount.Mul64(uint64(len(txn.SiacoinOutputs)))
		for _, sce := range utxos {
			inputs = append(inputs, sce.Copy())
			fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
			if SumOutputs(inputs).Cmp(want.Add(fee)) > 0 {
				break
			}
		}

		// remove used inputs from utxos
		utxos = utxos[len(inputs):]

		// not enough outputs found
		fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
		if sumOut := SumOutputs(inputs); sumOut.Cmp(want.Add(fee)) < 0 {
			if len(txns) > 0 {
				// consider redistributing successful if we could generate at least one txn
				break
			}
			return types.ChainIndex{}, nil, nil, fmt.Errorf("%w: inputs %v < needed %v + txnFee %v", ErrNotEnoughFunds, sumOut.String(), want.String(), fee.String())
		}

		// set the miner fee
		if !fee.IsZero() {
			txn.MinerFee = fee
		}

		// add the change output
		change := SumOutputs(inputs).Sub(want.Add(fee))
		if !change.IsZero() {
			txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
				Value:   change,
				Address: sw.addr,
			})
		}

		// add the inputs
		toSignTxn := make([]int, 0, len(inputs))
		for _, sce := range inputs {
			toSignTxn = append(toSignTxn, len(txn.SiacoinInputs))
			txn.SiacoinInputs = append(txn.SiacoinInputs, types.V2SiacoinInput{
				Parent: sce.Move(),
			})
			toLock = append(toLock, sce.ID)
		}
		txns = append(txns, txn)
		toSign = append(toSign, toSignTxn)
	}

	sw.lockUTXOs(toLock)
	return tip, txns, toSign, nil
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (sw *SingleAddressWallet) ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	for _, txn := range txns {
		for _, in := range txn.SiacoinInputs {
			delete(sw.locked, in.ParentID)
		}
	}
	for _, txn := range v2txns {
		for _, in := range txn.SiacoinInputs {
			delete(sw.locked, in.Parent.ID)
		}
	}
	sw.cleanLockedUTXOs()
}

// isLocked returns true if the siacoin output with given id is locked, this
// method must be called whilst holding the mutex lock.
func (sw *SingleAddressWallet) isLocked(id types.SiacoinOutputID) bool {
	return time.Now().Before(sw.locked[id])
}

// BroadcastV2TransactionSet broadcasts a set of v2 transactions to the network
// after adding them to the transaction pool to make sure they are valid.
func (sw *SingleAddressWallet) BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error {
	if _, err := sw.cm.AddV2PoolTransactions(index, txns); err != nil {
		return fmt.Errorf("failed to broadcast v2 transaction set: %w", err)
	}
	set := BroadcastedSet{Basis: index, Transactions: txns, BroadcastedAt: time.Now()}
	if err := sw.store.AddBroadcastedSet(set); err != nil {
		sw.log.Debug("failed to store broadcasted v2 transaction set", zap.Error(err))
	}
	if err := sw.syncer.BroadcastV2TransactionSet(index, txns); err != nil {
		return fmt.Errorf("failed to broadcast v2 transaction set to syncer: %w", err)
	}
	return nil
}

// RecommendedFee returns the recommended fee for a transaction, capped at
// 'maxFee'.
func (sw *SingleAddressWallet) RecommendedFee() types.Currency {
	fee := sw.cm.RecommendedFee()
	if fee.Cmp(maxFee) > 0 {
		fee = maxFee
	}
	return fee
}

// IsRelevantTransaction returns true if the v1 transaction is relevant to the
// address
func IsRelevantTransaction(txn types.Transaction, addr types.Address) bool {
	for _, sci := range txn.SiacoinInputs {
		if sci.UnlockConditions.UnlockHash() == addr {
			return true
		}
	}

	for _, sco := range txn.SiacoinOutputs {
		if sco.Address == addr {
			return true
		}
	}

	for _, sci := range txn.SiafundInputs {
		if sci.UnlockConditions.UnlockHash() == addr {
			return true
		}
	}

	for _, sfo := range txn.SiafundOutputs {
		if sfo.Address == addr {
			return true
		}
	}
	return false
}

// ExplicitCoveredFields returns a CoveredFields that covers all elements
// present in txn.
func ExplicitCoveredFields(txn types.Transaction) (cf types.CoveredFields) {
	for i := range txn.SiacoinInputs {
		cf.SiacoinInputs = append(cf.SiacoinInputs, uint64(i))
	}
	for i := range txn.SiacoinOutputs {
		cf.SiacoinOutputs = append(cf.SiacoinOutputs, uint64(i))
	}
	for i := range txn.FileContracts {
		cf.FileContracts = append(cf.FileContracts, uint64(i))
	}
	for i := range txn.FileContractRevisions {
		cf.FileContractRevisions = append(cf.FileContractRevisions, uint64(i))
	}
	for i := range txn.StorageProofs {
		cf.StorageProofs = append(cf.StorageProofs, uint64(i))
	}
	for i := range txn.SiafundInputs {
		cf.SiafundInputs = append(cf.SiafundInputs, uint64(i))
	}
	for i := range txn.SiafundOutputs {
		cf.SiafundOutputs = append(cf.SiafundOutputs, uint64(i))
	}
	for i := range txn.MinerFees {
		cf.MinerFees = append(cf.MinerFees, uint64(i))
	}
	for i := range txn.ArbitraryData {
		cf.ArbitraryData = append(cf.ArbitraryData, uint64(i))
	}
	for i := range txn.Signatures {
		cf.Signatures = append(cf.Signatures, uint64(i))
	}
	return
}

// SumOutputs returns the total value of the supplied outputs.
func SumOutputs(outputs []types.SiacoinElement) (sum types.Currency) {
	for _, o := range outputs {
		sum = sum.Add(o.SiacoinOutput.Value)
	}
	return
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided
// private key and store.
func NewSingleAddressWallet(priv types.PrivateKey, cm ChainManager, store SingleAddressStore, syncer Syncer, opts ...Option) (*SingleAddressWallet, error) {
	cfg := config{
		DefragThreshold:      30,
		MaxInputsForDefrag:   30,
		MaxDefragUTXOs:       10,
		ReservationDuration:  3 * time.Hour,
		MaxRebroadcastPeriod: 48 * time.Hour,
		// Minimum time between reorgs before transactions
		// are rebroadcast
		RebroadcastDebounceInterval: 15 * time.Second,
		Log:                         zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	sw := &SingleAddressWallet{
		priv: priv,

		store:  store,
		cm:     cm,
		syncer: syncer,

		cfg: cfg,
		log: cfg.Log,
		tg:  threadgroup.New(),

		addr: types.StandardUnlockHash(priv.PublicKey()),

		locked: make(map[types.SiacoinOutputID]time.Time),
	}

	// load the broadcasted transactions and add them to the pool, we don't
	// bother checking whether the transactions are in the pool already because
	// the transaction pool is not persisted
	sets, err := store.BroadcastedSets()
	if err != nil {
		return nil, fmt.Errorf("failed to get broadcasted sets: %w", err)
	}
	for _, set := range sets {
		if time.Since(set.BroadcastedAt) < cfg.MaxRebroadcastPeriod {
			if _, err := cm.AddV2PoolTransactions(set.Basis, set.Transactions); err != nil {
				sw.log.Debug("failed to add broadcasted transactions to pool", zap.Error(err))
			}
			// NOTE: purposefully don't broadcast these to the network
		}
	}

	// subscribe to reorg events to rebroadcast transactions
	reorgCh := make(chan struct{}, 1)
	reorgCh <- struct{}{}
	stop := cm.OnReorg(func(_ types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})

	// rebroadcast transactions in a separate goroutine
	go func() {
		defer stop()

		ctx, cancel, err := sw.tg.AddContext(context.Background())
		if err != nil {
			sw.log.Error("failed to add context", zap.Error(err))
			return
		}
		defer cancel()

		// debounce rebroadcasting during heavy syncing
		debounce := time.NewTimer(cfg.RebroadcastDebounceInterval)
		debounce.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reorgCh:
				debounce.Reset(cfg.RebroadcastDebounceInterval)
			case <-debounce.C:
				if err := sw.rebroadcastTransactions(); err != nil && !errors.Is(err, context.Canceled) {
					sw.log.Debug("failed to rebroadcast transactions", zap.Error(err))
				}
			}
		}
	}()

	return sw, nil
}

func (sw *SingleAddressWallet) rebroadcastTransactions() error {
	sets, err := sw.store.BroadcastedSets()
	if err != nil {
		return fmt.Errorf("failed to get transactions to rebroadcast: %w", err)
	}

	basis := sw.cm.TipState().Index
	for _, set := range sets {
		if time.Since(set.BroadcastedAt) > sw.cfg.MaxRebroadcastPeriod {
			sw.log.Debug("removing broadcasted set", zap.Time("broadcastedAt", set.BroadcastedAt))
			if err := sw.store.RemoveBroadcastedSet(set); err != nil {
				sw.log.Debug("failed to remove broadcasted set", zap.Error(err))
			}
			continue
		}

		// update the transaction set to reflect the current chain state
		txns, err := sw.cm.UpdateV2TransactionSet(set.Transactions, set.Basis, basis)
		if err != nil || len(txns) == 0 {
			// remove the broadcasted set if it can no longer be updated
			// or if all transactions in the set have been confirmed.
			if err := sw.store.RemoveBroadcastedSet(set); err != nil {
				sw.log.Debug("failed to remove broadcasted set", zap.Error(err))
			}
			if err != nil {
				sw.log.Debug("failed to update transaction set", zap.Error(err))
			}
			continue
		}
		sw.log.Debug("updated transaction set for rebroadcast", zap.Int("txns", len(txns)), zap.Stringer("broadcastBasis", set.Basis), zap.Stringer("newBasis", basis))
		if _, err := sw.cm.AddV2PoolTransactions(basis, txns); err != nil {
			sw.log.Debug("failed to add transactions to pool", zap.Error(err))
		} else if err := sw.syncer.BroadcastV2TransactionSet(basis, txns); err != nil {
			sw.log.Debug("failed to broadcast transaction set", zap.Error(err))
		}
	}
	return nil
}
