package wallet_test

import (
	"testing"

	"go.sia.tech/core/chain"
	"go.sia.tech/core/types"
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

	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, log.Named("wallet"))
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
	tip := cm.TipState()
	// mine a block to fund the wallet
	b := testutil.MineBlock(tip, nil, w.Address())
	if err := cm.AddBlocks([]types.Block{b}); err != nil {
		t.Fatal(err)
	}
	tip = cm.TipState()

	// mine until the payout matures
	target := tip.MaturityHeight() + 1
	for i := tip.Index.Height; i < target; i++ {
		b := testutil.MineBlock(tip, nil, w.Address())
		if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
		tip = cm.TipState()
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

	toSign, release, err := w.FundTransaction(&txn, initialReward)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if err := w.SignTransaction(tip, &txn, toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
		t.Fatal(err)
	}

	// check that wallet now has no spendable balance
	spendable, confirmed, unconfirmed, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !confirmed.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v confirmed balance, got %v", types.ZeroCurrency, confirmed)
	} else if !spendable.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v spendable balance, got %v", types.ZeroCurrency, spendable)
	} else if !unconfirmed.Equals(initialReward) {
		t.Fatalf("expected %v unconfirmed balance, got %v", initialReward, unconfirmed)
	}

	// check the wallet's unconfirmed transactions
	poolTxns, err := w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 1 {
		t.Fatalf("expected 1 unconfirmed transaction, got %v", len(poolTxns))
	} else if poolTxns[0].Transaction.ID() != txn.ID() {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), poolTxns[0].Transaction.ID())
	}

	// mine a block to confirm the transaction
	b = testutil.MineBlock(tip, []types.Transaction{txn}, types.VoidAddress)
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
	for i := range txns {
		sent[i].SiacoinOutputs = []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward.Div64(20)},
		}

		toSign, release, err := w.FundTransaction(&sent[i], initialReward)
		if err != nil {
			t.Fatal(err)
		}
		defer release()

		if err := w.SignTransaction(tip, &sent[i], toSign, types.CoveredFields{WholeTransaction: true}); err != nil {
			t.Fatal(err)
		}
	}

	b = testutil.MineBlock(tip, sent, types.VoidAddress)
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
	} else if !unconfirmed.Equals(initialReward) {
		t.Fatalf("expected %v unconfirmed balance, got %v", initialReward, unconfirmed)
	}

	// check that the paginated transactions are in the proper order
	txns, err = w.Transactions(20, 0) // limit of 20 so the original two transactions are not included
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 20 {
		t.Fatalf("expected 20 transactions, got %v", len(txns))
	}
	for i := range sent {
		if txns[i].ID != sent[i].ID() {
			t.Fatalf("expected transaction %v, got %v", sent[i].ID(), txns[i].ID)
		}
	}
}
