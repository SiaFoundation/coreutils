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

func TestWallet(t *testing.T) {
	log := zaptest.NewLogger(t)

	network, genesis := testutil.Network()

	cs, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	cm := chain.NewManager(cs, tipState)

	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore(pk)

	if err := cm.AddSubscriber(ws, types.ChainIndex{}); err != nil {
		t.Fatal(err)
	}

	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(log.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	spendable, confirmed, unconfirmed, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero confirmed balance, got %v", confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero spendable balance, got %v", spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero unconfirmed balance, got %v", unconfirmed)
	}

	initialReward := cm.TipState().BlockReward()
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

	// check that one payout has matured
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(initialReward) {
		t.Fatalf("expected %v confirmed balance, got %v", initialReward, confirmed)
	} else if !spendable.Equals(initialReward) {
		t.Fatalf("expected %v spendable balance, got %v", initialReward, spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v unconfirmed balance, got %v", types.ZeroCurrency, unconfirmed)
	}

	// check that the wallet has a single transaction
	count, err := w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transaction, got %v", count)
	}

	// check that the payout transaction was created
	txns, err := w.Transactions(100, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %v", len(txns))
	} else if txns[0].Source != wallet.TxnSourceMinerPayout {
		t.Fatalf("expected miner payout, got %v", txns[0].Source)
	}

	// split the wallet's balance into 20 outputs
	txn := types.Transaction{
		SiacoinOutputs: make([]types.SiacoinOutput, 20),
	}
	for i := range txn.SiacoinOutputs {
		txn.SiacoinOutputs[i] = types.SiacoinOutput{
			Value:   initialReward.Div64(20),
			Address: w.Address(),
		}
	}

	// fund and sign the transaction
	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(initialReward) {
		t.Fatalf("expected %v confirmed balance, got %v", types.ZeroCurrency, confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v spendable balance, got %v", types.ZeroCurrency, spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v unconfirmed balance, got %v", types.ZeroCurrency, unconfirmed)
	}

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
	} else if poolTxns[0].Source != wallet.TxnSourceTransaction {
		t.Fatalf("expected wallet source, got %v", poolTxns[0].Source)
	} else if !poolTxns[0].Inflow.Equals(initialReward) {
		t.Fatalf("expected %v inflow, got %v", initialReward, poolTxns[0].Inflow)
	} else if !poolTxns[0].Outflow.Equals(initialReward) {
		t.Fatalf("expected %v outflow, got %v", types.ZeroCurrency, poolTxns[0].Outflow)
	}

	// check that the wallet has an unconfirmed balance
	// check that wallet now has no spendable balance
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(initialReward) {
		t.Fatalf("expected %v confirmed balance, got %v", types.ZeroCurrency, confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v spendable balance, got %v", types.ZeroCurrency, spendable)
	} else if !unconfirmed.Equals(initialReward) {
		t.Fatalf("expected %v unconfirmed balance, got %v", initialReward, unconfirmed)
	}

	// mine a block to confirm the transaction
	b = testutil.MineBlock(cm, types.VoidAddress)
	if err := cm.AddBlocks([]types.Block{b}); err != nil {
		t.Fatal(err)
	}

	// check that the balance was confirmed
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(initialReward) {
		t.Fatalf("expected %v confirmed balance, got %v", initialReward, confirmed)
	} else if !spendable.Equals(initialReward) {
		t.Fatalf("expected %v spendable balance, got %v", initialReward, spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v unconfirmed balance, got %v", types.ZeroCurrency, unconfirmed)
	}

	// check that the wallet has two transactions.
	count, err = w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 2 transactions, got %v", count)
	}

	// check that the paginated transactions are in the proper order
	txns, err = w.Transactions(100, 0)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 2 {
		t.Fatalf("expected 2 transactions, got %v", len(txns))
	} else if len(txns[0].Transaction.SiacoinOutputs) != 20 {
		t.Fatalf("expected 20 outputs, got %v", len(txns[0].Transaction.SiacoinOutputs))
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
	count, err = w.TransactionCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 22 {
		t.Fatalf("expected 22 transactions, got %v", count)
	}

	// check that the wallet's balance is 0
	// check that wallet now has no spendable balance
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v confirmed balance, got %v", types.ZeroCurrency, confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v spendable balance, got %v", types.ZeroCurrency, spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v unconfirmed balance, got %v", types.ZeroCurrency, unconfirmed)
	}

	// check that the paginated transactions are in the proper order
	txns, err = w.Transactions(20, 0) // limit of 20 so the original two transactions are not included
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 20 {
		t.Fatalf("expected 20 transactions, got %v", len(txns))
	}
	for i := range sent {
		j := len(txns) - i - 1 // transactions are received in reverse order
		if txns[j].ID != sent[i].ID() {
			t.Fatalf("expected transaction %v, got %v", sent[i].ID(), txns[i].ID)
		}
	}
}

func TestWalletUnconfirmed(t *testing.T) {
	log := zaptest.NewLogger(t)

	network, genesis := testutil.Network()

	cs, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	cm := chain.NewManager(cs, tipState)

	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore(pk)

	if err := cm.AddSubscriber(ws, types.ChainIndex{}); err != nil {
		t.Fatal(err)
	}

	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(log.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	spendable, confirmed, unconfirmed, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero confirmed balance, got %v", confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero spendable balance, got %v", spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected zero unconfirmed balance, got %v", unconfirmed)
	}

	initialReward := cm.TipState().BlockReward()
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

	// check that one payout has matured
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(initialReward) {
		t.Fatalf("expected %v confirmed balance, got %v", initialReward, confirmed)
	} else if !spendable.Equals(initialReward) {
		t.Fatalf("expected %v spendable balance, got %v", initialReward, spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v unconfirmed balance, got %v", types.ZeroCurrency, unconfirmed)
	}

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
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(initialReward) {
		t.Fatalf("expected %v confirmed balance, got %v", initialReward, confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v spendable balance, got %v", types.ZeroCurrency, spendable)
	} else if !unconfirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v unconfirmed balance, got %v", types.ZeroCurrency, unconfirmed)
	}

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
