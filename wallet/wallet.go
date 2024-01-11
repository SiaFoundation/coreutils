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
	// transactionDefragThreshold is the number of utxos at which the wallet
	// will attempt to defrag itself by including small utxos in transactions.
	transactionDefragThreshold = 30
	// maxInputsForDefrag is the maximum number of inputs a transaction can
	// have before the wallet will stop adding inputs
	maxInputsForDefrag = 30
	// maxDefragUTXOs is the maximum number of utxos that will be added to a
	// transaction when defragging
	maxDefragUTXOs = 10
)

// transaction sources indicate the source of a transaction. Transactions can
// either be created by sending Siacoins between unlock hashes or they can be
// created by consensus (e.g. a miner payout, a siafund claim, or a contract).
const (
	TxnSourceTransaction      TransactionSource = "transaction"
	TxnSourceMinerPayout      TransactionSource = "miner"
	TxnSourceSiafundClaim     TransactionSource = "siafundClaim"
	TxnSourceContract         TransactionSource = "contract"
	TxnSourceFoundationPayout TransactionSource = "foundation"
)

var (
	// ErrNotEnoughFunds is returned when there are not enough unspent outputs
	// to fund a transaction.
	ErrNotEnoughFunds = errors.New("not enough funds")
)

type (
	// A TransactionSource is a string indicating the source of a transaction.
	TransactionSource string

	// A Transaction is a transaction relevant to a particular wallet, paired
	// with useful metadata.
	Transaction struct {
		ID          types.TransactionID `json:"id"`
		Index       types.ChainIndex    `json:"index"`
		Transaction types.Transaction   `json:"transaction"`
		Inflow      types.Currency      `json:"inflow"`
		Outflow     types.Currency      `json:"outflow"`
		Source      TransactionSource   `json:"source"`
		Timestamp   time.Time           `json:"timestamp"`
	}

	// A ChainManager manages the current state of the blockchain.
	ChainManager interface {
		BestIndex(height uint64) (types.ChainIndex, bool)

		PoolTransactions() []types.Transaction

		AddSubscriber(chain.Subscriber, types.ChainIndex) error
		RemoveSubscriber(chain.Subscriber)
	}

	// A SingleAddressStore stores the state of a single-address wallet.
	// Implementations are assumed to be thread safe.
	SingleAddressStore interface {
		chain.Subscriber

		// LastIndexedTip returns the consensus change ID and block height of
		// the last wallet change.
		LastIndexedTip() (types.ChainIndex, error)
		// UnspentSiacoinElements returns a list of all unspent siacoin outputs
		UnspentSiacoinElements() ([]types.SiacoinElement, error)
		// Transactions returns a paginated list of transactions ordered by
		// maturity height, descending. If no more transactions are available,
		// (nil, nil) should be returned.
		Transactions(limit, offset int) ([]Transaction, error)
		// TransactionCount returns the total number of transactions in the
		// wallet.
		TransactionCount() (uint64, error)
		// ResetWallet resets the wallet to its initial state. This is used when a
		// consensus subscription error occurs.
		ResetWallet(seedHash types.Hash256) error
		// VerifyWalletKey checks that the wallet seed matches the existing seed
		// hash. This detects if the user's recovery phrase has changed and the
		// wallet needs to rescan.
		VerifyWalletKey(seedHash types.Hash256) error
	}

	// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
	// a single address.
	SingleAddressWallet struct {
		priv types.PrivateKey
		addr types.Address

		cm    ChainManager
		store SingleAddressStore
		log   *zap.Logger

		mu  sync.Mutex // protects the following fields
		tip types.ChainIndex
		// locked is a set of siacoin output IDs locked by FundTransaction. They
		// will be released either by calling Release for unused transactions or
		// being confirmed in a block.
		locked map[types.Hash256]bool
	}
)

// ErrDifferentSeed is returned when a different seed is provided to
// NewSingleAddressWallet than was used to initialize the wallet
var ErrDifferentSeed = errors.New("seed differs from wallet seed")

// EncodeTo implements types.EncoderTo.
func (t Transaction) EncodeTo(e *types.Encoder) {
	t.ID.EncodeTo(e)
	t.Index.EncodeTo(e)
	t.Transaction.EncodeTo(e)
	t.Inflow.EncodeTo(e)
	t.Outflow.EncodeTo(e)
	e.WriteString(string(t.Source))
	e.WriteTime(t.Timestamp)
}

// DecodeFrom implements types.DecoderFrom.
func (t *Transaction) DecodeFrom(d *types.Decoder) {
	t.ID.DecodeFrom(d)
	t.Index.DecodeFrom(d)
	t.Transaction.DecodeFrom(d)
	t.Inflow.DecodeFrom(d)
	t.Outflow.DecodeFrom(d)
	t.Source = TransactionSource(d.ReadString())
	t.Timestamp = d.ReadTime()
}

func transactionIsRelevant(txn types.Transaction, addr types.Address) bool {
	for i := range txn.SiacoinInputs {
		if txn.SiacoinInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
	}
	for i := range txn.SiacoinOutputs {
		if txn.SiacoinOutputs[i].Address == addr {
			return true
		}
	}
	for i := range txn.SiafundInputs {
		if txn.SiafundInputs[i].UnlockConditions.UnlockHash() == addr {
			return true
		}
		if txn.SiafundInputs[i].ClaimAddress == addr {
			return true
		}
	}
	for i := range txn.SiafundOutputs {
		if txn.SiafundOutputs[i].Address == addr {
			return true
		}
	}
	for i := range txn.FileContracts {
		for _, sco := range txn.FileContracts[i].ValidProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
		for _, sco := range txn.FileContracts[i].MissedProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
	}
	for i := range txn.FileContractRevisions {
		for _, sco := range txn.FileContractRevisions[i].ValidProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
		for _, sco := range txn.FileContractRevisions[i].MissedProofOutputs {
			if sco.Address == addr {
				return true
			}
		}
	}
	return false
}

// Close closes the wallet
func (sw *SingleAddressWallet) Close() error {
	sw.cm.RemoveSubscriber(sw.store)
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
func (sw *SingleAddressWallet) Balance() (spendable, confirmed, unconfirmed types.Currency, err error) {
	outputs, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("failed to get unspent outputs: %w", err)
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

	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, sco := range outputs {
		confirmed = confirmed.Add(sco.SiacoinOutput.Value)
		if !sw.locked[sco.ID] && !tpoolSpent[sco.ID] {
			spendable = spendable.Add(sco.SiacoinOutput.Value)
		}
	}

	for _, sco := range tpoolUtxos {
		unconfirmed = unconfirmed.Add(sco.SiacoinOutput.Value)
	}
	return
}

// Transactions returns a paginated list of transactions, ordered by block
// height descending. If no more transactions are available, (nil, nil) is
// returned.
func (sw *SingleAddressWallet) Transactions(limit, offset int) ([]Transaction, error) {
	return sw.store.Transactions(limit, offset)
}

// TransactionCount returns the total number of transactions in the wallet.
func (sw *SingleAddressWallet) TransactionCount() (uint64, error) {
	return sw.store.TransactionCount()
}

// FundTransaction adds siacoin inputs worth at least amount to the provided
// transaction. If necessary, a change output will also be added. The inputs
// will not be available to future calls to FundTransaction unless ReleaseInputs
// is called.
func (sw *SingleAddressWallet) FundTransaction(txn *types.Transaction, amount types.Currency) ([]types.Hash256, func(), error) {
	if amount.IsZero() {
		return nil, func() {}, nil
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	utxos, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, nil, err
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

	// remove locked and spent outputs
	usableUTXOs := utxos[:0]
	for _, sce := range utxos {
		if sw.locked[sce.ID] || tpoolSpent[sce.ID] {
			continue
		}
		usableUTXOs = append(usableUTXOs, sce)
	}

	// sort by value, descending
	sort.Slice(usableUTXOs, func(i, j int) bool {
		return usableUTXOs[i].SiacoinOutput.Value.Cmp(usableUTXOs[j].SiacoinOutput.Value) > 0
	})

	// fund the transaction using the largest utxos first
	var selected []types.SiacoinElement
	var inputSum types.Currency
	for i, sce := range usableUTXOs {
		if inputSum.Cmp(amount) >= 0 {
			usableUTXOs = usableUTXOs[i:]
			break
		}
		selected = append(selected, sce)
		inputSum = inputSum.Add(sce.SiacoinOutput.Value)
	}

	// if the transaction can't be funded, return an error
	if inputSum.Cmp(amount) < 0 {
		return nil, nil, ErrNotEnoughFunds
	}

	// check if remaining utxos should be defragged
	txnInputs := len(txn.SiacoinInputs) + len(selected)
	if len(usableUTXOs) > transactionDefragThreshold && txnInputs < maxInputsForDefrag {
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
			inputSum = inputSum.Add(sce.SiacoinOutput.Value)
			txnInputs++
		}
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
		sw.locked[sce.ID] = true
	}

	release := func() {
		sw.mu.Lock()
		defer sw.mu.Unlock()
		for _, id := range toSign {
			delete(sw.locked, id)
		}
	}
	return toSign, release, nil
}

// SignTransaction adds a signature to each of the specified inputs.
func (sw *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
	for _, id := range toSign {
		var h types.Hash256
		if cf.WholeTransaction {
			h = cs.WholeSigHash(*txn, id, 0, 0, cf.Signatures)
		} else {
			h = cs.PartialSigHash(*txn, cf)
		}
		sig := sw.priv.SignHash(h)
		txn.Signatures = append(txn.Signatures, types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
			Signature:      sig[:],
		})
	}
	return nil
}

// Tip returns the block height the wallet has scanned to.
func (sw *SingleAddressWallet) Tip() types.ChainIndex {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.tip
}

// UnconfirmedTransactions returns all unconfirmed transactions relevant to the
// wallet.
func (sw *SingleAddressWallet) UnconfirmedTransactions() ([]Transaction, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	confirmed, err := sw.store.UnspentSiacoinElements()
	if err != nil {
		return nil, fmt.Errorf("failed to get unspent outputs: %w", err)
	}

	utxos := make(map[types.Hash256]types.SiacoinOutput)
	for _, se := range confirmed {
		utxos[types.Hash256(se.ID)] = se.SiacoinOutput
	}

	poolTxns := sw.cm.PoolTransactions()

	var annotated []Transaction
	for _, txn := range poolTxns {
		wt := Transaction{
			ID:          txn.ID(),
			Transaction: txn,
			Source:      TxnSourceTransaction,
			Timestamp:   time.Now(),
		}

		for _, sci := range txn.SiacoinInputs {
			if sco, ok := utxos[types.Hash256(sci.ParentID)]; ok {
				wt.Outflow = wt.Outflow.Add(sco.Value)
			}
		}

		for i, sco := range txn.SiacoinOutputs {
			if sco.Address == sw.addr {
				wt.Inflow = wt.Inflow.Add(sco.Value)
				utxos[types.Hash256(txn.SiacoinOutputID(i))] = sco
			}
		}

		if wt.Inflow.IsZero() && wt.Outflow.IsZero() {
			continue
		}

		annotated = append(annotated, wt)
	}
	return annotated, nil
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

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv types.PrivateKey, cm ChainManager, store SingleAddressStore, log *zap.Logger) (*SingleAddressWallet, error) {
	tip, err := store.LastIndexedTip()
	if err != nil {
		return nil, fmt.Errorf("failed to get last wallet change: %w", err)
	}

	seedHash := types.HashBytes(priv[:])
	if err := store.VerifyWalletKey(seedHash); errors.Is(err, ErrDifferentSeed) {
		tip = types.ChainIndex{}
		if err := store.ResetWallet(seedHash); err != nil {
			return nil, fmt.Errorf("failed to reset wallet: %w", err)
		}
		log.Info("wallet reset due to seed change")
	} else if err != nil {
		return nil, fmt.Errorf("failed to verify wallet key: %w", err)
	}

	sw := &SingleAddressWallet{
		priv: priv,
		tip:  tip,

		store: store,
		cm:    cm,
		log:   log,

		addr:   types.StandardUnlockHash(priv.PublicKey()),
		locked: make(map[types.Hash256]bool),
	}

	go func() {
		// note: start in goroutine to avoid blocking startup
		err := cm.AddSubscriber(store, tip)
		if err != nil { // no way to check for specific reorg error, so reset everything
			sw.log.Warn("rescanning blockchain due to unknown consensus change ID")
			// reset change ID and subscribe again
			if err := store.ResetWallet(seedHash); err != nil {
				sw.log.Fatal("failed to reset wallet", zap.Error(err))
			} else if err = cm.AddSubscriber(store, types.ChainIndex{}); err != nil {
				sw.log.Fatal("failed to reset consensus change subscription", zap.Error(err))
			}
		}
	}()
	return sw, nil
}
