package wallet

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/chain"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	// BytesPerInput is the encoded size of a SiacoinInput and corresponding
	// TransactionSignature, assuming standard UnlockConditions.
	BytesPerInput = 241

	// redistributeBatchSize is the number of outputs to redistribute per txn to
	// avoid creating a txn that is too large.
	redistributeBatchSize = 10

	// maxInputsForDefrag is the maximum number of inputs a transaction can
	// have before the wallet will stop adding inputs
	maxInputsForDefrag = 30

	// maxDefragUTXOs is the maximum number of utxos that will be added to a
	// transaction when defragging
	maxDefragUTXOs = 10
)

// ErrInsufficientBalance is returned when there aren't enough unused outputs to
// cover the requested amount.
var ErrInsufficientBalance = errors.New("insufficient balance")

// StandardUnlockConditions returns the standard unlock conditions for a single
// Ed25519 key.
func StandardUnlockConditions(pk types.PublicKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys: []types.UnlockKey{{
			Algorithm: types.SpecifierEd25519,
			Key:       pk[:],
		}},
		SignaturesRequired: 1,
	}
}

// StandardAddress returns the standard address for an Ed25519 key.
func StandardAddress(pk types.PublicKey) types.Address {
	return StandardUnlockConditions(pk).UnlockHash()
}

// StandardTransactionSignature returns the standard signature object for a
// siacoin or siafund input.
func StandardTransactionSignature(id types.Hash256) types.TransactionSignature {
	return types.TransactionSignature{
		ParentID:       id,
		CoveredFields:  types.CoveredFields{WholeTransaction: true},
		PublicKeyIndex: 0,
	}
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

// A SiacoinElement is a SiacoinOutput along with its ID.
type SiacoinElement struct {
	types.SiacoinOutput
	ID             types.Hash256 `json:"id"`
	MaturityHeight uint64        `json:"maturityHeight"`
}

// A Transaction is an on-chain transaction relevant to a particular wallet,
// paired with useful metadata.
type Transaction struct {
	Raw       types.Transaction   `json:"raw,omitempty"`
	Index     types.ChainIndex    `json:"index"`
	ID        types.TransactionID `json:"id"`
	Inflow    types.Currency      `json:"inflow"`
	Outflow   types.Currency      `json:"outflow"`
	Timestamp time.Time           `json:"timestamp"`
}

// A SingleAddressStore stores the state of a single-address wallet.
// Implementations are assumed to be thread safe.
type SingleAddressStore interface {
	Height() uint64
	UnspentSiacoinElements(matured bool) ([]SiacoinElement, error)
	Transactions(before, since time.Time, offset, limit int) ([]Transaction, error)
}

// A TransactionPool contains transactions that have not yet been included in a
// block.
type TransactionPool interface {
	ContainsElement(id types.Hash256) bool
}

// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
// a single address.
type SingleAddressWallet struct {
	log             *zap.SugaredLogger
	priv            types.PrivateKey
	addr            types.Address
	defragThreshold uint // num utxos
	store           SingleAddressStore
	usedUTXOExpiry  time.Duration

	cm *chain.Manager

	// for building transactions
	mu       sync.Mutex
	lastUsed map[types.Hash256]time.Time
}

// PrivateKey returns the private key of the wallet.
func (w *SingleAddressWallet) PrivateKey() types.PrivateKey {
	return w.priv
}

// Address returns the address of the wallet.
func (w *SingleAddressWallet) Address() types.Address {
	return w.addr
}

// Balance returns the balance of the wallet.
func (w *SingleAddressWallet) Balance() (spendable, confirmed, unconfirmed types.Currency, _ error) {
	sces, err := w.store.UnspentSiacoinElements(true)
	if err != nil {
		return types.Currency{}, types.Currency{}, types.Currency{}, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, sce := range sces {
		if !w.isOutputUsed(sce.ID) {
			spendable = spendable.Add(sce.Value)
		}
		confirmed = confirmed.Add(sce.Value)
	}
	_, tpoolUtxos, _ := tpoolState(w.cm.PoolTransactions(), w.cm.V2PoolTransactions())
	for _, sco := range tpoolUtxos {
		if !w.isOutputUsed(sco.ID) {
			unconfirmed = unconfirmed.Add(sco.Value)
		}
	}
	return
}

func (w *SingleAddressWallet) Height() uint64 {
	return w.store.Height()
}

// UnspentOutputs returns the set of unspent Siacoin outputs controlled by the
// wallet.
func (w *SingleAddressWallet) UnspentOutputs() ([]SiacoinElement, error) {
	sces, err := w.store.UnspentSiacoinElements(false)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	filtered := sces[:0]
	for _, sce := range sces {
		if !w.isOutputUsed(sce.ID) {
			filtered = append(filtered, sce)
		}
	}
	return filtered, nil
}

// Transactions returns up to max transactions relevant to the wallet that have
// a timestamp later than since.
func (w *SingleAddressWallet) Transactions(before, since time.Time, offset, limit int) ([]Transaction, error) {
	return w.store.Transactions(before, since, offset, limit)
}

// FundTransaction adds siacoin inputs worth at least the requested amount to
// the provided transaction. A change output is also added, if necessary. The
// inputs will not be available to future calls to FundTransaction unless
// ReleaseInputs is called or enough time has passed.
func (w *SingleAddressWallet) FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, useUnconfirmedTxns bool) ([]types.Hash256, error) {
	if amount.IsZero() {
		return nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	// fetch all unspent siacoin elements
	utxos, err := w.store.UnspentSiacoinElements(false)
	if err != nil {
		return nil, err
	}

	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value.Cmp(utxos[j].Value) > 0
	})

	// add all unconfirmed outputs to the end of the slice as a last resort
	if useUnconfirmedTxns {
		var tpUtxos []SiacoinElement
		_, tpoolUtxos, _ := tpoolState(w.cm.PoolTransactions(), w.cm.V2PoolTransactions())
		for _, sco := range tpoolUtxos {
			tpUtxos = append(tpUtxos, sco)
		}
		// desc sort
		sort.Slice(tpUtxos, func(i, j int) bool {
			return tpUtxos[i].Value.Cmp(tpUtxos[j].Value) > 0
		})
		utxos = append(utxos, tpUtxos...)
	}

	// remove locked and spent outputs
	usableUTXOs := utxos[:0]
	for _, sce := range utxos {
		if w.isOutputUsed(sce.ID) {
			continue
		}
		usableUTXOs = append(usableUTXOs, sce)
	}

	// fund the transaction using the largest utxos first
	var selected []SiacoinElement
	var inputSum types.Currency
	for i, sce := range usableUTXOs {
		if inputSum.Cmp(amount) >= 0 {
			usableUTXOs = usableUTXOs[i:]
			break
		}
		selected = append(selected, sce)
		inputSum = inputSum.Add(sce.Value)
	}

	// if the transaction can't be funded, return an error
	if inputSum.Cmp(amount) < 0 {
		return nil, fmt.Errorf("%w: inputSum: %v, amount: %v", ErrInsufficientBalance, inputSum.String(), amount.String())
	}

	// check if remaining utxos should be defragged
	txnInputs := len(txn.SiacoinInputs) + len(selected)
	if uint(len(usableUTXOs)) > w.defragThreshold && txnInputs < maxInputsForDefrag {
		// add the smallest utxos to the transaction
		defraggable := usableUTXOs
		if len(defraggable) > maxDefragUTXOs {
			defraggable = defraggable[len(defraggable)-maxDefragUTXOs:]
		}
		for i := len(defraggable) - 1; i >= 0; i-- {
			if txnInputs >= maxInputsForDefrag {
				break
			}

			sce := defraggable[i]
			selected = append(selected, sce)
			inputSum = inputSum.Add(sce.Value)
			txnInputs++
		}
	}

	// add a change output if necessary
	if inputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   inputSum.Sub(amount),
			Address: w.addr,
		})
	}

	toSign := make([]types.Hash256, len(selected))
	for i, sce := range selected {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: types.StandardUnlockConditions(w.priv.PublicKey()),
		})
		toSign[i] = types.Hash256(sce.ID)
		w.lastUsed[sce.ID] = time.Now()
	}

	return toSign, nil
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (w *SingleAddressWallet) ReleaseInputs(txns ...types.Transaction) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.releaseInputs(txns...)
}

func (w *SingleAddressWallet) releaseInputs(txns ...types.Transaction) {
	for _, txn := range txns {
		for _, in := range txn.SiacoinInputs {
			delete(w.lastUsed, types.Hash256(in.ParentID))
		}
	}
}

// SignTransaction adds a signature to each of the specified inputs.
func (w *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
	for _, id := range toSign {
		ts := types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
		}
		var h types.Hash256
		if cf.WholeTransaction {
			h = cs.WholeSigHash(*txn, ts.ParentID, ts.PublicKeyIndex, ts.Timelock, cf.Signatures)
		} else {
			h = cs.PartialSigHash(*txn, cf)
		}
		sig := w.priv.SignHash(h)
		ts.Signature = sig[:]
		txn.Signatures = append(txn.Signatures, ts)
	}
	return nil
}

// Redistribute returns a transaction that redistributes money in the wallet by
// selecting a minimal set of inputs to cover the creation of the requested
// outputs. It also returns a list of output IDs that need to be signed.
func (w *SingleAddressWallet) Redistribute(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) ([]types.Transaction, []types.Hash256, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// build map of inputs currently in the tx pool
	inPool := make(map[types.Hash256]bool)
	for _, ptxn := range pool {
		for _, in := range ptxn.SiacoinInputs {
			inPool[types.Hash256(in.ParentID)] = true
		}
	}

	// fetch unspent transaction outputs
	utxos, err := w.store.UnspentSiacoinElements(false)
	if err != nil {
		return nil, nil, err
	}

	// check whether a redistribution is necessary, adjust number of desired
	// outputs accordingly
	for _, sce := range utxos {
		inUse := w.isOutputUsed(sce.ID) || inPool[sce.ID]
		matured := cs.Index.Height >= sce.MaturityHeight
		sameValue := sce.Value.Equals(amount)
		if !inUse && matured && sameValue {
			outputs--
		}
	}
	if outputs <= 0 {
		return nil, nil, nil
	}

	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value.Cmp(utxos[j].Value) > 0
	})

	// prepare all outputs
	var txns []types.Transaction
	var toSign []types.Hash256

	for outputs > 0 {
		var txn types.Transaction
		for i := 0; i < outputs && i < redistributeBatchSize; i++ {
			txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
				Value:   amount,
				Address: w.Address(),
			})
		}
		outputs -= len(txn.SiacoinOutputs)

		// estimate the fees
		buf := new(bytes.Buffer)
		enc := types.NewEncoder(buf)
		txn.EncodeTo(enc)
		if err := enc.Flush(); err != nil {
			return nil, nil, err
		}
		outputFees := feePerByte.Mul64(uint64(buf.Len()))
		feePerInput := feePerByte.Mul64(BytesPerInput)

		// collect outputs that cover the total amount
		var inputs []SiacoinElement
		want := amount.Mul64(uint64(len(txn.SiacoinOutputs)))
		var amtInUse, amtSameValue, amtNotMatured types.Currency
		for _, sce := range utxos {
			inUse := w.isOutputUsed(sce.ID) || inPool[sce.ID]
			matured := cs.Index.Height >= sce.MaturityHeight
			sameValue := sce.Value.Equals(amount)
			if inUse {
				amtInUse = amtInUse.Add(sce.Value)
				continue
			} else if sameValue {
				amtSameValue = amtSameValue.Add(sce.Value)
				continue
			} else if !matured {
				amtNotMatured = amtNotMatured.Add(sce.Value)
				continue
			}

			inputs = append(inputs, sce)
			fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
			if SumOutputs(inputs).Cmp(want.Add(fee)) > 0 {
				break
			}
		}

		// not enough outputs found
		fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
		if sumOut := SumOutputs(inputs); sumOut.Cmp(want.Add(fee)) < 0 {
			// in case of an error we need to free all inputs
			w.releaseInputs(txns...)
			return nil, nil, fmt.Errorf("%w: inputs %v < needed %v + txnFee %v (usable: %v, inUse: %v, sameValue: %v, notMatured: %v)",
				ErrInsufficientBalance, sumOut.String(), want.String(), fee.String(), sumOut.String(), amtInUse.String(), amtSameValue.String(), amtNotMatured.String())
		}

		// set the miner fee
		txn.MinerFees = []types.Currency{fee}

		// add the change output
		change := SumOutputs(inputs).Sub(want.Add(fee))
		if !change.IsZero() {
			txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
				Value:   change,
				Address: w.addr,
			})
		}

		// add the inputs
		for _, sce := range inputs {
			txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
				ParentID:         types.SiacoinOutputID(sce.ID),
				UnlockConditions: StandardUnlockConditions(w.priv.PublicKey()),
			})
			toSign = append(toSign, sce.ID)
			w.lastUsed[sce.ID] = time.Now()
		}

		txns = append(txns, txn)
	}

	return txns, toSign, nil
}

func (w *SingleAddressWallet) isOutputUsed(id types.Hash256) bool {
	_, _, tpoolSpent := tpoolState(w.cm.PoolTransactions(), w.cm.V2PoolTransactions())
	inPool := tpoolSpent[types.SiacoinOutputID(id)]
	lastUsed := w.lastUsed[id]
	if w.usedUTXOExpiry == 0 {
		return !lastUsed.IsZero() || inPool
	}
	return time.Since(lastUsed) <= w.usedUTXOExpiry || inPool
}

// SumOutputs returns the total value of the supplied outputs.
func SumOutputs(outputs []SiacoinElement) (sum types.Currency) {
	for _, o := range outputs {
		sum = sum.Add(o.Value)
	}
	return
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv types.PrivateKey, cm *chain.Manager, store SingleAddressStore, usedUTXOExpiry time.Duration, defragThreshold uint, log *zap.SugaredLogger) *SingleAddressWallet {
	return &SingleAddressWallet{
		cm:              cm,
		priv:            priv,
		addr:            StandardAddress(priv.PublicKey()),
		defragThreshold: defragThreshold,
		store:           store,
		lastUsed:        make(map[types.Hash256]time.Time),
		usedUTXOExpiry:  usedUTXOExpiry,
		log:             log.Named("wallet"),
	}
}

// tpoolState returns wallet related information about the transaction pool.
// tpoolTxns maps a transaction set ID to the transactions in that set
// tpoolUtxos maps a siacoin output ID to its corresponding siacoin
// the transaction pool.
// tpoolSpent is a set of siacoin output IDs that are currently in the
// transaction pool.
func tpoolState(txns []types.Transaction, v2Txns []types.V2Transaction) (map[types.Hash256][]Transaction, map[types.SiacoinOutputID]SiacoinElement, map[types.SiacoinOutputID]bool) {
	tpoolTxns := make(map[types.Hash256][]Transaction)
	tpoolUtxos := make(map[types.SiacoinOutputID]SiacoinElement)
	tpoolSpent := make(map[types.SiacoinOutputID]bool)

	panic("implement")

	return tpoolTxns, tpoolUtxos, tpoolSpent
}
