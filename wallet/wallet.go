package wallet

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
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
		TipState() consensus.State
		BestIndex(height uint64) (types.ChainIndex, bool)
		PoolTransactions() []types.Transaction
		V2PoolTransactions() []types.V2Transaction
	}

	// A SingleAddressStore stores the state of a single-address wallet.
	// Implementations are assumed to be thread safe.
	SingleAddressStore interface {
		// Tip returns the consensus change ID and block height of
		// the last wallet change.
		Tip() (types.ChainIndex, error)
		// UnspentSiacoinElements returns a list of all unspent siacoin outputs
		// including immature outputs.
		UnspentSiacoinElements() ([]types.SiacoinElement, error)
		// WalletEvents returns a paginated list of transactions ordered by
		// maturity height, descending. If no more transactions are available,
		// (nil, nil) should be returned.
		WalletEvents(offset, limit int) ([]Event, error)
		// WalletEventCount returns the total number of events relevant to the
		// wallet.
		WalletEventCount() (uint64, error)

		// UpdateChainState applies and reverts chain updates to the wallet.
		UpdateChainState([]chain.RevertUpdate, []chain.ApplyUpdate) error
	}

	// A SingleAddressWallet is a hot wallet that manages the outputs controlled
	// by a single address.
	SingleAddressWallet struct {
		priv types.PrivateKey
		addr types.Address

		cm    ChainManager
		store SingleAddressStore
		log   *zap.Logger

		cfg config

		mu sync.Mutex // protects the following fields
		// locked is a set of siacoin output IDs locked by FundTransaction. They
		// will be released either by calling Release for unused transactions or
		// being confirmed in a block.
		locked map[types.Hash256]time.Time
	}
)

// ErrDifferentSeed is returned when a different seed is provided to
// NewSingleAddressWallet than was used to initialize the wallet
var ErrDifferentSeed = errors.New("seed differs from wallet seed")

// Close closes the wallet
func (sw *SingleAddressWallet) Close() error {
	// TODO: remove subscription??
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
	outputs, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return Balance{}, fmt.Errorf("failed to get unspent outputs: %w", err)
	}

	tpoolSpent := make(map[types.Hash256]bool)
	tpoolUtxos := make(map[types.Hash256]types.SiacoinElement)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			tpoolSpent[types.Hash256(sci.ParentID)] = true
			delete(tpoolUtxos, types.Hash256(sci.ParentID))
		}
		for i, sco := range txn.SiacoinOutputs {
			if sco.Address != sw.addr {
				continue
			}

			outputID := txn.SiacoinOutputID(i)
			tpoolUtxos[types.Hash256(outputID)] = types.SiacoinElement{
				StateElement: types.StateElement{
					ID: types.Hash256(types.SiacoinOutputID(outputID)),
				},
				SiacoinOutput: sco,
			}
		}
	}

	for _, txn := range sw.cm.V2PoolTransactions() {
		for _, si := range txn.SiacoinInputs {
			tpoolSpent[types.Hash256(si.Parent.ID)] = true
			delete(tpoolUtxos, types.Hash256(si.Parent.ID))
		}
		txnID := txn.ID()
		for i, sco := range txn.SiacoinOutputs {
			if sco.Address != sw.addr {
				continue
			}

			outputID := txn.SiacoinOutputID(txnID, i)
			tpoolUtxos[types.Hash256(outputID)] = types.SiacoinElement{
				StateElement: types.StateElement{
					ID: types.Hash256(types.SiacoinOutputID(outputID)),
				},
				SiacoinOutput: sco,
			}
		}
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()
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
	// fetch outputs from the store
	utxos, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, err
	}

	// fetch outputs currently in the pool
	inPool := make(map[types.Hash256]bool)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			inPool[types.Hash256(sci.ParentID)] = true
		}
	}

	// grab current height
	state := sw.cm.TipState()
	bh := state.Index.Height

	sw.mu.Lock()
	defer sw.mu.Unlock()

	// filter outputs that are either locked, in the pool or have not yet matured
	unspent := utxos[:0]
	for _, sce := range utxos {
		if sw.isLocked(sce.ID) || inPool[sce.ID] || bh < sce.MaturityHeight {
			continue
		}
		unspent = append(unspent, sce)
	}
	return unspent, nil
}

func (sw *SingleAddressWallet) selectUTXOs(amount types.Currency, inputs int, useUnconfirmed bool) ([]types.SiacoinElement, types.Currency, error) {
	if amount.IsZero() {
		return nil, types.ZeroCurrency, nil
	}

	elements, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, types.ZeroCurrency, err
	}

	tpoolSpent := make(map[types.Hash256]bool)
	tpoolUtxos := make(map[types.Hash256]types.SiacoinElement)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			tpoolSpent[types.Hash256(sci.ParentID)] = true
			delete(tpoolUtxos, types.Hash256(sci.ParentID))
		}
		for i, sco := range txn.SiacoinOutputs {
			tpoolUtxos[types.Hash256(txn.SiacoinOutputID(i))] = types.SiacoinElement{
				StateElement: types.StateElement{
					ID: types.Hash256(types.SiacoinOutputID(txn.SiacoinOutputID(i))),
				},
				SiacoinOutput: sco,
			}
		}
	}

	// remove immature, locked and spent outputs
	cs := sw.cm.TipState()
	utxos := make([]types.SiacoinElement, 0, len(elements))
	var usedSum types.Currency
	var immatureSum types.Currency
	for _, sce := range elements {
		if used := sw.isLocked(sce.ID) || tpoolSpent[sce.ID]; used {
			usedSum = usedSum.Add(sce.SiacoinOutput.Value)
			continue
		} else if immature := cs.Index.Height < sce.MaturityHeight; immature {
			immatureSum = immatureSum.Add(sce.SiacoinOutput.Value)
			continue
		}
		utxos = append(utxos, sce)
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
			unconfirmedUTXOs = append(unconfirmedUTXOs, sce)
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
		selected = append(selected, sce)
		inputSum = inputSum.Add(sce.SiacoinOutput.Value)
	}

	if inputSum.Cmp(amount) < 0 && useUnconfirmed {
		// try adding unconfirmed utxos.
		for _, sce := range unconfirmedUTXOs {
			selected = append(selected, sce)
			inputSum = inputSum.Add(sce.SiacoinOutput.Value)
			if inputSum.Cmp(amount) >= 0 {
				break
			}
		}

		if inputSum.Cmp(amount) < 0 {
			// still not enough funds
			return nil, types.ZeroCurrency, fmt.Errorf("%w: inputs %v < needed %v (used: %v immature: %v unconfirmed: %v)", ErrNotEnoughFunds, inputSum.String(), amount.String(), usedSum.String(), immatureSum.String(), unconfirmedSum.String())
		}
	} else if inputSum.Cmp(amount) < 0 {
		return nil, types.ZeroCurrency, fmt.Errorf("%w: inputs %v < needed %v (used: %v immature: %v", ErrNotEnoughFunds, inputSum.String(), amount.String(), usedSum.String(), immatureSum.String())
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

			sce := defraggable[i]
			selected = append(selected, sce)
			inputSum = inputSum.Add(sce.SiacoinOutput.Value)
			txnInputs++
		}
	}
	return selected, inputSum, nil
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

	selected, inputSum, err := sw.selectUTXOs(amount, len(txn.SiacoinInputs), useUnconfirmed)
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

	toSign := make([]types.Hash256, len(selected))
	for i, sce := range selected {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: types.StandardUnlockConditions(sw.priv.PublicKey()),
		})
		toSign[i] = types.Hash256(sce.ID)
		sw.locked[sce.ID] = time.Now().Add(sw.cfg.ReservationDuration)
	}

	return toSign, nil
}

// SignTransaction adds a signature to each of the specified inputs.
func (sw *SingleAddressWallet) SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

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
// The returned consensus state should be used to calculate the input signature
// hash and as the basis for AddV2PoolTransactions.
func (sw *SingleAddressWallet) FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (consensus.State, []int, error) {
	if amount.IsZero() {
		return sw.cm.TipState(), nil, nil
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	selected, inputSum, err := sw.selectUTXOs(amount, len(txn.SiacoinInputs), useUnconfirmed)
	if err != nil {
		return consensus.State{}, nil, err
	}

	// add a change output if necessary
	if inputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   inputSum.Sub(amount),
			Address: sw.addr,
		})
	}

	toSign := make([]int, 0, len(selected))
	for _, sce := range selected {
		toSign = append(toSign, len(txn.SiacoinInputs))
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.V2SiacoinInput{
			Parent: sce,
		})
		sw.locked[sce.ID] = time.Now().Add(sw.cfg.ReservationDuration)
	}

	return sw.cm.TipState(), toSign, nil
}

// SignV2Inputs adds a signature to each of the specified siacoin inputs.
func (sw *SingleAddressWallet) SignV2Inputs(state consensus.State, txn *types.V2Transaction, toSign []int) {
	if len(toSign) == 0 {
		return
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	policy := sw.SpendPolicy()
	sigHash := state.InputSigHash(*txn)
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

// UnconfirmedTransactions returns all unconfirmed transactions relevant to the
// wallet.
func (sw *SingleAddressWallet) UnconfirmedTransactions() (annotated []Event, err error) {
	confirmed, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, fmt.Errorf("failed to get unspent outputs: %w", err)
	}

	utxos := make(map[types.Hash256]types.SiacoinOutput)
	for _, se := range confirmed {
		utxos[types.Hash256(se.ID)] = se.SiacoinOutput
	}

	index := types.ChainIndex{
		Height: sw.cm.TipState().Index.Height + 1,
	}
	timestamp := time.Now().Truncate(time.Second)

	addEvent := func(id types.Hash256, eventType string, data EventData, inflow, outflow types.Currency) {
		ev := Event{
			ID:             id,
			Index:          index,
			MaturityHeight: index.Height,
			Timestamp:      timestamp,
			Inflow:         inflow,
			Outflow:        outflow,
			Type:           eventType,
			Data:           data,
		}
		annotated = append(annotated, ev)
	}

	for _, txn := range sw.cm.PoolTransactions() {
		var inflow, outflow types.Currency
		for _, sci := range txn.SiacoinInputs {
			if sco, ok := utxos[types.Hash256(sci.ParentID)]; ok {
				outflow = outflow.Add(sco.Value)
			}
		}

		for i, sco := range txn.SiacoinOutputs {
			if sco.Address == sw.addr {
				inflow = inflow.Add(sco.Value)
				utxos[types.Hash256(txn.SiacoinOutputID(i))] = sco
			}
		}

		// skip transactions that don't affect the wallet
		if inflow.IsZero() && outflow.IsZero() {
			continue
		}

		addEvent(types.Hash256(txn.ID()), EventTypeV1Transaction, EventV1Transaction(txn), inflow, outflow)
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

		addEvent(types.Hash256(txn.ID()), EventTypeV2Transaction, EventV2Transaction(txn), inflow, outflow)
	}
	return annotated, nil
}

// Redistribute returns a transaction that redistributes money in the wallet by
// selecting a minimal set of inputs to cover the creation of the requested
// outputs. It also returns a list of output IDs that need to be signed.
func (sw *SingleAddressWallet) Redistribute(outputs int, amount, feePerByte types.Currency) (txns []types.Transaction, toSign []types.Hash256, err error) {
	// fetch outputs from the store
	elements, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, nil, err
	}

	// fetch outputs currently in the pool
	inPool := make(map[types.Hash256]bool)
	for _, txn := range sw.cm.PoolTransactions() {
		for _, sci := range txn.SiacoinInputs {
			inPool[types.Hash256(sci.ParentID)] = true
		}
	}

	// grab current height
	state := sw.cm.TipState()
	bh := state.Index.Height

	sw.mu.Lock()
	defer sw.mu.Unlock()

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
			utxos = append(utxos, sce)
		}
	}

	// return early if we don't have to defrag at all
	if outputs <= 0 {
		return nil, nil, nil
	}

	// in case of an error we need to free all inputs
	defer func() {
		if err != nil {
			for _, id := range toSign {
				delete(sw.locked, id)
			}
		}
	}()

	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].SiacoinOutput.Value.Cmp(utxos[j].SiacoinOutput.Value) > 0
	})

	// prepare defrag transactions
	for outputs > 0 {
		var txn types.Transaction
		for i := 0; i < outputs && i < redistributeBatchSize; i++ {
			txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
				Value:   amount,
				Address: sw.addr,
			})
		}
		outputs -= len(txn.SiacoinOutputs)

		// estimate the fees
		outputFees := feePerByte.Mul64(state.TransactionWeight(txn))
		feePerInput := feePerByte.Mul64(bytesPerInput)

		// collect outputs that cover the total amount
		var inputs []types.SiacoinElement
		want := amount.Mul64(uint64(len(txn.SiacoinOutputs)))
		for _, sce := range utxos {
			inputs = append(inputs, sce)
			fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
			if SumOutputs(inputs).Cmp(want.Add(fee)) > 0 {
				break
			}
		}

		// not enough outputs found
		fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
		if sumOut := SumOutputs(inputs); sumOut.Cmp(want.Add(fee)) < 0 {
			return nil, nil, fmt.Errorf("%w: inputs %v < needed %v + txnFee %v", ErrNotEnoughFunds, sumOut.String(), want.String(), fee.String())
		}

		// set the miner fee
		if !fee.IsZero() {
			txn.MinerFees = []types.Currency{fee}
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
		for _, sce := range inputs {
			txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
				ParentID:         types.SiacoinOutputID(sce.ID),
				UnlockConditions: types.StandardUnlockConditions(sw.priv.PublicKey()),
			})
			toSign = append(toSign, sce.ID)
			sw.locked[sce.ID] = time.Now().Add(sw.cfg.ReservationDuration)
		}
		txns = append(txns, txn)
	}

	return
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (sw *SingleAddressWallet) ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, txn := range txns {
		for _, in := range txn.SiacoinInputs {
			delete(sw.locked, types.Hash256(in.ParentID))
		}
	}
	for _, txn := range v2txns {
		for _, in := range txn.SiacoinInputs {
			delete(sw.locked, in.Parent.ID)
		}
	}
}

// isLocked returns true if the siacoin output with given id is locked, this
// method must be called whilst holding the mutex lock.
func (sw *SingleAddressWallet) isLocked(id types.Hash256) bool {
	return time.Now().Before(sw.locked[id])
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
func NewSingleAddressWallet(priv types.PrivateKey, cm ChainManager, store SingleAddressStore, opts ...Option) (*SingleAddressWallet, error) {
	cfg := config{
		DefragThreshold:     30,
		MaxInputsForDefrag:  30,
		MaxDefragUTXOs:      10,
		ReservationDuration: 15 * time.Minute,
		Log:                 zap.NewNop(),
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	sw := &SingleAddressWallet{
		priv: priv,

		store: store,
		cm:    cm,

		cfg: cfg,
		log: cfg.Log,

		addr:   types.StandardUnlockHash(priv.PublicKey()),
		locked: make(map[types.Hash256]time.Time),
	}
	return sw, nil
}
