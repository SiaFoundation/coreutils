package wallet_test

import (
	"errors"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap/zaptest"
)

type testWallet struct {
	t     *testing.T
	cm    *chain.Manager
	store wallet.SingleAddressStore

	*wallet.SingleAddressWallet
}

func newTestWallet(t *testing.T, funded bool) *testWallet {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore(pk)

	// create chain store
	network, genesis := testutil.Network()
	cs, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, tipState)
	err = cm.AddSubscriber(ws, types.ChainIndex{})
	if err != nil {
		t.Fatal(err)
	}

	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}

	if funded {
		// mine a block to fund the wallet
		b := testutil.MineBlock(cm, w.Address())
		if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}

		// mine until the payout matures
		tip := cm.TipState()
		target := tip.MaturityHeight() + 1
		for i := tip.Index.Height; i < target; i++ {
			b := testutil.MineBlock(cm, types.VoidAddress)
			if err := cm.AddBlocks([]types.Block{b}); err != nil {
				t.Fatal(err)
			}
		}
	}

	return &testWallet{t, cm, ws, w}
}

// redistribute creates a transaction that redistributes the wallet's balance
// into n outputs of amount, and mines a block to confirm the transaction.
func (w *testWallet) redistribute(n int, amount types.Currency) error {
	// redistribute & sign
	txns, toSign, err := w.Redistribute(n, amount, types.NewCurrency64(1))
	if err != nil {
		return err
	} else {
		for i := 0; i < len(txns); i++ {
			w.SignTransaction(&txns[i], toSign, types.CoveredFields{WholeTransaction: true})
		}
	}

	// add txn to the pool
	_, err = w.cm.AddPoolTransactions(txns)
	if err != nil {
		return err
	}

	// mine a block
	b := testutil.MineBlock(w.cm, w.Address())
	return w.cm.AddBlocks([]types.Block{b})
}

// assertBalance compares the wallet's balance to the expected values.
func (w *testWallet) assertBalance(spendable, confirmed, immature, unconfirmed types.Currency) {
	balance, err := w.Balance()
	if err != nil {
		w.t.Fatalf("failed to get balance: %v", err)
	} else if !balance.Confirmed.Equals(confirmed) {
		w.t.Fatalf("expected %v confirmed balance, got %v", confirmed, balance.Confirmed)
	} else if !balance.Spendable.Equals(spendable) {
		w.t.Fatalf("expected %v spendable balance, got %v", spendable, balance.Spendable)
	} else if !balance.Unconfirmed.Equals(unconfirmed) {
		w.t.Fatalf("expected %v unconfirmed balance, got %v", unconfirmed, balance.Unconfirmed)
	} else if !balance.Immature.Equals(immature) {
		w.t.Fatalf("expected %v immature balance, got %v", immature, balance.Immature)
	}
}

// assertOutputs checks that the wallet has the expected number of outputs with given value.
func (w *testWallet) assertOutputs(n int, amount types.Currency) {
	// assert outputs
	utxos, err := w.store.UnspentSiacoinElements()
	if err != nil {
		w.t.Fatal(err)
	}
	var cnt int
	for _, utxo := range utxos {
		if utxo.SiacoinOutput.Value.Equals(amount) {
			n--
		}
	}
	if cnt != n {
		w.t.Fatalf("expected %v outputs of %v, got %v", n, amount, cnt)
	}
}

func TestWallet(t *testing.T) {
	// create test wallet
	w := newTestWallet(t, false)
	defer w.Close()

	// convenience variables
	cm := w.cm

	// check balance
	w.assertBalance(types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// mine a block to fund the wallet
	b := testutil.MineBlock(cm, w.Address())
	if err := cm.AddBlocks([]types.Block{b}); err != nil {
		t.Fatal(err)
	}

	maturityHeight := cm.TipState().MaturityHeight()
	// check that the wallet has a single event
	if events, err := w.Events(0, 100); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 event, got %v", len(events))
	} else if events[0].Source != wallet.EventSourceMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Source)
	} else if events[0].MaturityHeight != maturityHeight {
		t.Fatalf("expected maturity height %v, got %v", maturityHeight, events[0].MaturityHeight)
	}

	// check that the wallet has an immature balance
	initialReward := cm.TipState().BlockReward()
	w.assertBalance(types.ZeroCurrency, types.ZeroCurrency, initialReward, types.ZeroCurrency)

	// create a transaction that splits the wallet's balance into 20 outputs
	txn := types.Transaction{
		SiacoinOutputs: make([]types.SiacoinOutput, 20),
	}
	for i := range txn.SiacoinOutputs {
		txn.SiacoinOutputs[i] = types.SiacoinOutput{
			Value:   initialReward.Div64(20),
			Address: w.Address(),
		}
	}

	// try funding the transaction, expect it to fail since the outputs are immature
	_, err := w.FundTransaction(&txn, initialReward, false)
	if err != wallet.ErrNotEnoughFunds {
		t.Fatal("expected ErrNotEnoughFunds, got", err)
	}

	// mine until the payout matures
	tip := cm.TipState()
	target := tip.MaturityHeight() + 1
	for i := tip.Index.Height; i < target; i++ {
		b := testutil.MineBlock(cm, types.VoidAddress)
		if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
	}

	// check that one payout has matured
	w.assertBalance(initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet has a single event
	count, err := w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transaction, got %v", count)
	}

	// check that the payout transaction was created
	events, err := w.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 transaction, got %v", len(events))
	} else if events[0].Source != wallet.EventSourceMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Source)
	}

	// fund and sign the transaction
	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	w.assertBalance(types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check the wallet has no unconfirmed transactions
	poolTxns, err := w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 0 {
		t.Fatalf("expected 0 unconfirmed transaction, got %v", len(poolTxns))
	}

	// add the transaction to the pool
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet has one unconfirmed transaction
	poolTxns, err = w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 1 {
		t.Fatalf("expected 1 unconfirmed transaction, got %v", len(poolTxns))
	} else if poolTxns[0].Transaction.ID() != txn.ID() {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), poolTxns[0].Transaction.ID())
	} else if poolTxns[0].Source != wallet.EventSourceTransaction {
		t.Fatalf("expected wallet source, got %v", poolTxns[0].Source)
	} else if !poolTxns[0].Inflow.Equals(initialReward) {
		t.Fatalf("expected %v inflow, got %v", initialReward, poolTxns[0].Inflow)
	} else if !poolTxns[0].Outflow.Equals(initialReward) {
		t.Fatalf("expected %v outflow, got %v", types.ZeroCurrency, poolTxns[0].Outflow)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	w.assertBalance(types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)

	// mine a block to confirm the transaction
	b = testutil.MineBlock(cm, types.VoidAddress)
	if err := cm.AddBlocks([]types.Block{b}); err != nil {
		t.Fatal(err)
	}

	// check that the balance was confirmed and the other values reset
	w.assertBalance(initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet has two events
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 2 transactions, got %v", count)
	}

	// check that the paginated transactions are in the proper order
	events, err = w.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 2 {
		t.Fatalf("expected 2 transactions, got %v", len(events))
	} else if events[0].ID != types.Hash256(txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), events[1].ID)
	} else if len(events[0].Transaction.SiacoinOutputs) != 20 {
		t.Fatalf("expected 20 outputs, got %v", len(events[1].Transaction.SiacoinOutputs))
	}

	// send all the outputs to the burn address individually
	sent := make([]types.Transaction, 20)
	sendAmount := initialReward.Div64(20)
	for i := range sent {
		sent[i].SiacoinOutputs = []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: sendAmount},
		}
		toSign, err := w.FundTransaction(&sent[i], sendAmount, false)
		if err != nil {
			t.Fatal(err)
		}
		w.SignTransaction(&sent[i], toSign, types.CoveredFields{WholeTransaction: true})
	}

	// add the transactions to the pool
	if _, err := cm.AddPoolTransactions(sent); err != nil {
		t.Fatal(err)
	}

	b = testutil.MineBlock(cm, types.VoidAddress)
	if err := cm.AddBlocks([]types.Block{b}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet now has 22 transactions, the initial payout
	// transaction, the split transaction, and 20 void transactions
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 22 {
		t.Fatalf("expected 22 transactions, got %v", count)
	}

	// check that all the wallet balances have reset
	w.assertBalance(types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// check that the paginated transactions are in the proper order
	events, err = w.Events(0, 20) // limit of 20 so the original two transactions are not included
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 20 {
		t.Fatalf("expected 20 transactions, got %v", len(events))
	}
	for i := range sent {
		// events should be chronologically ordered, reverse the order they
		// were added to the transaction pool
		j := len(events) - i - 1
		if events[j].ID != types.Hash256(sent[i].ID()) {
			t.Fatalf("expected transaction %v, got %v", sent[i].ID(), events[i].ID)
		}
	}
}

func TestWalletUnconfirmed(t *testing.T) {
	// create test wallet
	w := newTestWallet(t, true)
	defer w.Close()

	// convenience variables
	cm := w.cm

	// check that one payout has matured
	initialReward := cm.TipState().BlockReward()
	w.assertBalance(initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// fund and sign a transaction sending half the balance to the burn address
	txn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward.Div64(2)},
			{Address: w.Address(), Value: initialReward.Div64(2)},
		},
	}

	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	w.assertBalance(types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// add the transaction to the pool
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet has one unconfirmed transaction
	poolTxns, err := w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 1 {
		t.Fatal("expected 1 unconfirmed transaction")
	}

	txn2 := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward.Div64(2)},
		},
	}

	// try to send a new transaction without using the unconfirmed output
	_, err = w.FundTransaction(&txn2, initialReward.Div64(2), false)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatalf("expected funding error with no usable utxos, got %v", err)
	}

	toSign, err = w.FundTransaction(&txn2, initialReward.Div64(2), true)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn2, toSign, types.CoveredFields{WholeTransaction: true})

	// broadcast the transaction
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn, txn2}); err != nil {
		t.Fatal(err)
	}
}

func TestWalletRedistribute(t *testing.T) {
	// create test wallet
	w := newTestWallet(t, true)
	defer w.Close()

	// convenience variables
	ws := w.store

	// assert we have one output
	utxos, err := ws.UnspentSiacoinElements()
	if err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatalf("expected one output, got %v", len(utxos))
	}

	// redistribute the wallet into 3 outputs of 75KS
	amount := types.Siacoins(75e3)
	err = w.redistribute(3, amount)
	if err != nil {
		t.Fatal(err)
	}
	w.assertOutputs(3, amount)

	// redistribute the wallet into 4 outputs of 50KS
	amount = types.Siacoins(50e3)
	err = w.redistribute(4, amount)
	if err != nil {
		t.Fatal(err)
	}
	w.assertOutputs(4, amount)

	// redistribute the wallet into 3 outputs of 100KS - expect ErrNotEnoughFunds
	err = w.redistribute(3, types.Siacoins(100e3))
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal(err)
	}

	// redistribute the wallet into 3 outputs of 50KS - assert this is a no-op
	txns, toSign, err := w.Redistribute(3, amount, types.NewCurrency64(1))
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatalf("expected no transactions, got %v", len(txns))
	} else if len(toSign) != 0 {
		t.Fatalf("expected no ids, got %v", len(toSign))
	}
}
