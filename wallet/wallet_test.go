package wallet_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap/zaptest"
)

func syncDB(cm *chain.Manager, store wallet.SingleAddressStore) error {
	for {
		tip, err := store.Tip()
		if err != nil {
			return fmt.Errorf("failed to get tip: %w", err)
		} else if tip == cm.Tip() {
			return nil
		}

		reverted, applied, err := cm.UpdatesSince(tip, 1000)
		if err != nil {
			return fmt.Errorf("failed to get updates: %w", err)
		}

		if err := store.UpdateChainState(reverted, applied); err != nil {
			return fmt.Errorf("failed to update chain state: %w", err)
		}
	}
}

func mineAndSync(t *testing.T, cm *chain.Manager, ws wallet.SingleAddressStore, address types.Address, n uint64) {
	t.Helper()

	// mine n blocks
	for i := uint64(0); i < n; i++ {
		if block, found := coreutils.MineBlock(cm, address, 5*time.Second); !found {
			t.Fatal("failed to mine block")
		} else if err := cm.AddBlocks([]types.Block{block}); err != nil {
			t.Fatal(err)
		}
	}
	// wait for the wallet to sync
	if err := syncDB(cm, ws); err != nil {
		t.Fatal(err)
	}
}

// assertBalance compares the wallet's balance to the expected values.
func assertBalance(t *testing.T, w *wallet.SingleAddressWallet, spendable, confirmed, immature, unconfirmed types.Currency) {
	t.Helper()

	balance, err := w.Balance()
	if err != nil {
		t.Fatalf("failed to get balance: %v", err)
	} else if !balance.Confirmed.Equals(confirmed) {
		t.Fatalf("expected %v confirmed balance, got %v", confirmed, balance.Confirmed)
	} else if !balance.Spendable.Equals(spendable) {
		t.Fatalf("expected %v spendable balance, got %v", spendable, balance.Spendable)
	} else if !balance.Unconfirmed.Equals(unconfirmed) {
		t.Fatalf("expected %v unconfirmed balance, got %v", unconfirmed, balance.Unconfirmed)
	} else if !balance.Immature.Equals(immature) {
		t.Fatalf("expected %v immature balance, got %v", immature, balance.Immature)
	}
}

func TestWallet(t *testing.T) {
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
	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// check balance
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// mine a block to fund the wallet
	mineAndSync(t, cm, ws, w.Address(), 1)
	maturityHeight := cm.TipState().MaturityHeight()

	// check that the wallet has a single event
	if events, err := w.Events(0, 100); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 event, got %v", len(events))
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	} else if events[0].MaturityHeight != maturityHeight {
		t.Fatalf("expected maturity height %v, got %v", maturityHeight, events[0].MaturityHeight)
	}

	// check that the wallet has an immature balance
	initialReward := cm.TipState().BlockReward()
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, initialReward, types.ZeroCurrency)

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
	_, err = w.FundTransaction(&txn, initialReward, false)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal("expected ErrNotEnoughFunds, got", err)
	}

	// mine until the payout matures
	tip := cm.TipState()
	target := tip.MaturityHeight()
	mineAndSync(t, cm, ws, types.VoidAddress, target-tip.Index.Height)

	// check that one payout has matured
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet still has a single event
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
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	}

	// fund and sign the transaction
	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if poolTxns[0].ID != types.Hash256(txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), poolTxns[0].ID)
	} else if poolTxns[0].Type != wallet.EventTypeV1Transaction {
		t.Fatalf("expected wallet source, got %v", poolTxns[0].Type)
	} else if !poolTxns[0].Inflow.Equals(initialReward) {
		t.Fatalf("expected %v inflow, got %v", initialReward, poolTxns[0].Inflow)
	} else if !poolTxns[0].Outflow.Equals(initialReward) {
		t.Fatalf("expected %v outflow, got %v", types.ZeroCurrency, poolTxns[0].Outflow)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if n := len((events[0].Data.(types.Transaction)).SiacoinOutputs); n != 20 {
		t.Fatalf("expected 20 outputs, got %v", n)
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
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// check that the wallet now has 22 transactions, the initial payout
	// transaction, the split transaction, and 20 void transactions
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 22 {
		t.Fatalf("expected 22 transactions, got %v", count)
	}

	// check that all the wallet balances have reset
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

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
	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// fund the wallet
	mineAndSync(t, cm, ws, w.Address(), 1)
	mineAndSync(t, cm, ws, types.VoidAddress, cm.TipState().MaturityHeight()-1)

	// check that one payout has matured
	initialReward := cm.TipState().BlockReward()
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// fund the wallet
	mineAndSync(t, cm, ws, w.Address(), 1)
	mineAndSync(t, cm, ws, types.VoidAddress, cm.TipState().MaturityHeight()-1)

	redistribute := func(amount types.Currency, n int) error {
		txns, toSign, err := w.Redistribute(n, amount, types.ZeroCurrency)
		if err != nil {
			return fmt.Errorf("redistribute failed: %w", err)
		} else if len(txns) == 0 {
			return nil
		}

		for i := 0; i < len(txns); i++ {
			w.SignTransaction(&txns[i], toSign, types.CoveredFields{WholeTransaction: true})
		}
		if _, err := cm.AddPoolTransactions(txns); err != nil {
			return fmt.Errorf("failed to add transactions to pool: %w", err)
		}
		mineAndSync(t, cm, ws, types.VoidAddress, 1)
		return nil
	}

	assertOutputs := func(amount types.Currency, n int) error {
		utxos, err := w.SpendableOutputs()
		if err != nil {
			return fmt.Errorf("failed to get unspent outputs: %w", err)
		}
		var count int
		for _, utxo := range utxos {
			if utxo.SiacoinOutput.Value.Equals(amount) {
				count++
			}
		}
		if count != n {
			return fmt.Errorf("expected %v outputs of %v, got %v", n, amount, count)
		}
		return nil
	}

	// assert we have one output
	assertOutputs(tipState.BlockReward(), 1)

	// redistribute the wallet into 4 outputs of 75KS
	amount := types.Siacoins(75e3)
	if err := redistribute(amount, 4); err != nil {
		t.Fatal(err)
	}
	assertOutputs(amount, 4)

	// redistribute the wallet into 4 outputs of 50KS
	amount = types.Siacoins(50e3)
	if err := redistribute(amount, 4); err != nil {
		t.Fatal(err)
	}
	assertOutputs(amount, 4)

	// redistribute the wallet into 3 outputs of 101KS - expect ErrNotEnoughFunds
	if err := redistribute(types.Siacoins(101e3), 3); !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatalf("expected ErrNotEnoughFunds, got %v", err)
	}

	// redistribute the wallet into 3 outputs of 50KS - assert this is a no-op
	txns, toSign, err := w.Redistribute(3, amount, types.ZeroCurrency)
	if err != nil {
		t.Fatal(err)
	} else if len(txns) != 0 {
		t.Fatalf("expected no transactions, got %v", len(txns))
	} else if len(toSign) != 0 {
		t.Fatalf("expected no ids, got %v", len(toSign))
	}
}

func TestReorg(t *testing.T) {
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
	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// check balance
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// mine a block to fund the wallet
	mineAndSync(t, cm, ws, w.Address(), 1)
	maturityHeight := cm.TipState().MaturityHeight()

	// check that the wallet has a single event
	if events, err := w.Events(0, 100); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 event, got %v", len(events))
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	} else if events[0].MaturityHeight != maturityHeight {
		t.Fatalf("expected maturity height %v, got %v", maturityHeight, events[0].MaturityHeight)
	}

	// check that the wallet has an immature balance
	initialReward := cm.TipState().BlockReward()
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, initialReward, types.ZeroCurrency)

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
	_, err = w.FundTransaction(&txn, initialReward, false)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal("expected ErrNotEnoughFunds, got", err)
	}

	// mine until the payout matures
	tip := cm.TipState()
	target := tip.MaturityHeight()
	mineAndSync(t, cm, ws, types.VoidAddress, target-tip.Index.Height)

	// check that one payout has matured
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet still has a single event
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
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	}

	// fund and sign the transaction
	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if poolTxns[0].ID != types.Hash256(txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), poolTxns[0].ID)
	} else if poolTxns[0].Type != wallet.EventTypeV1Transaction {
		t.Fatalf("expected wallet source, got %v", poolTxns[0].Type)
	} else if !poolTxns[0].Inflow.Equals(initialReward) {
		t.Fatalf("expected %v inflow, got %v", initialReward, poolTxns[0].Inflow)
	} else if !poolTxns[0].Outflow.Equals(initialReward) {
		t.Fatalf("expected %v outflow, got %v", types.ZeroCurrency, poolTxns[0].Outflow)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, types.VoidAddress, 1)
	rollbackState := cm.TipState()

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if n := len((events[0].Data.(types.Transaction)).SiacoinOutputs); n != 20 {
		t.Fatalf("expected 20 outputs, got %v", n)
	}

	txn2 := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward},
		},
	}
	toSign, err = w.FundTransaction(&txn2, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn2, toSign, types.CoveredFields{WholeTransaction: true})

	// release the inputs to construct a double spend
	w.ReleaseInputs([]types.Transaction{txn2}, nil)

	txn1 := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward.Div64(2)},
		},
	}
	toSign, err = w.FundTransaction(&txn1, initialReward.Div64(2), false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn1, toSign, types.CoveredFields{WholeTransaction: true})

	// add the first transaction to the pool
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn1}); err != nil {
		t.Fatal(err)
	}
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// check that the wallet now has 3 transactions: the initial payout
	// transaction, the split transaction, and a void transaction
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 3 {
		t.Fatalf("expected 3 transactions, got %v", count)
	}

	events, err = w.Events(0, 1) // limit of 1 so the original two transactions are not included
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 transactions, got %v", len(events))
	} else if events[0].ID != types.Hash256(txn1.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn1.ID(), events[0].ID)
	}

	// check that all the wallet balance has half the initial reward
	assertBalance(t, w, initialReward.Div64(2), initialReward.Div64(2), types.ZeroCurrency, types.ZeroCurrency)

	var reorgBlocks []types.Block
	state := rollbackState
	for i := rollbackState.Index.Height; i < cm.Tip().Height+5; i++ {
		b := types.Block{
			ParentID:     state.Index.ID,
			Timestamp:    types.CurrentTimestamp(),
			MinerPayouts: []types.SiacoinOutput{{Address: types.VoidAddress, Value: state.BlockReward()}},
		}
		if !coreutils.FindBlockNonce(state, &b, time.Second) {
			t.Fatal("failed to find nonce")
		}
		reorgBlocks = append(reorgBlocks, b)
		state.Index.Height++
		state.Index.ID = b.ID()
	}
	b := types.Block{
		ParentID:     state.Index.ID,
		Timestamp:    types.CurrentTimestamp(),
		MinerPayouts: []types.SiacoinOutput{{Address: types.VoidAddress, Value: state.BlockReward()}},
		Transactions: []types.Transaction{txn2}, // spend the second transaction to invalidate the tpool transaction
	}
	if !coreutils.FindBlockNonce(state, &b, time.Second) {
		t.Fatal("failed to find nonce")
	}
	reorgBlocks = append(reorgBlocks, b)
	if err := cm.AddBlocks(reorgBlocks); err != nil {
		t.Fatal(err)
	} else if err := syncDB(cm, ws); err != nil {
		t.Fatal(err)
	}

	// all balances should now be zero
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet is back to two events
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 3 {
		t.Fatalf("expected 3 transactions, got %v", count)
	}

	events, err = w.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 3 {
		t.Fatalf("expected 3 transactions, got %v", len(events))
	} else if events[0].ID != types.Hash256(txn2.ID()) { // new transaction first
		t.Fatalf("expected transaction %v, got %v", txn2.ID(), events[0].ID)
	} else if events[1].ID != types.Hash256(txn.ID()) { // split transaction second
		t.Fatalf("expected transaction %v, got %v", txn.ID(), events[1].ID)
	} else if events[2].Type != wallet.EventTypeMinerPayout { // payout transaction last
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	}
}

func TestWalletV2(t *testing.T) {
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
	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// check balance
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// mine a block to fund the wallet
	mineAndSync(t, cm, ws, w.Address(), 1)
	maturityHeight := cm.TipState().MaturityHeight()

	// check that the wallet has a single event
	if events, err := w.Events(0, 100); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 event, got %v", len(events))
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	} else if events[0].MaturityHeight != maturityHeight {
		t.Fatalf("expected maturity height %v, got %v", maturityHeight, events[0].MaturityHeight)
	}

	// check that the wallet has an immature balance
	initialReward := cm.TipState().BlockReward()
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, initialReward, types.ZeroCurrency)

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
	_, err = w.FundTransaction(&txn, initialReward, false)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal("expected ErrNotEnoughFunds, got", err)
	}

	// mine until the payout matures
	tip := cm.TipState()
	target := tip.MaturityHeight()
	mineAndSync(t, cm, ws, types.VoidAddress, target-tip.Index.Height)

	// check that one payout has matured
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	}

	// fund and sign the transaction
	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if poolTxns[0].ID != types.Hash256(txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), poolTxns[0].ID)
	} else if poolTxns[0].Type != wallet.EventTypeV1Transaction {
		t.Fatalf("expected wallet source, got %v", poolTxns[0].Type)
	} else if !poolTxns[0].Inflow.Equals(initialReward) {
		t.Fatalf("expected %v inflow, got %v", initialReward, poolTxns[0].Inflow)
	} else if !poolTxns[0].Outflow.Equals(initialReward) {
		t.Fatalf("expected %v outflow, got %v", types.ZeroCurrency, poolTxns[0].Outflow)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if n := len((events[0].Data.(types.Transaction)).SiacoinOutputs); n != 20 {
		t.Fatalf("expected 20 outputs, got %v", n)
	}

	// mine until the v2 require height
	mineAndSync(t, cm, ws, types.VoidAddress, network.HardforkV2.RequireHeight-cm.Tip().Height)

	v2Txn := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: types.Siacoins(100)},
		},
	}

	// fund and sign the transaction
	state, toSignV2, err := w.FundV2Transaction(&v2Txn, types.Siacoins(100), false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(state, &v2Txn, toSignV2)

	// add the transaction to the pool
	if _, err := cm.AddV2PoolTransactions(state.Index, []types.V2Transaction{v2Txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet has one unconfirmed transaction
	poolTxns, err = w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 1 {
		t.Fatalf("expected 1 unconfirmed transaction, got %v", len(poolTxns))
	} else if poolTxns[0].ID != types.Hash256(v2Txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", v2Txn.ID(), poolTxns[0].ID)
	} else if poolTxns[0].Type != wallet.EventTypeV2Transaction {
		t.Fatalf("expected v2 transaction type, got %v", poolTxns[0].Type)
	}

	// confirm the transaction
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// check that the wallet has three events
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 3 {
		t.Fatalf("expected 3 events, got %v", count)
	}

	// check that the new transaction is the first event
	events, err = w.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 3 {
		t.Fatalf("expected 3 events, got %v", len(events))
	} else if events[0].ID != types.Hash256(v2Txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", v2Txn.ID(), events[0].ID)
	} else if events[0].Type != wallet.EventTypeV2Transaction {
		t.Fatalf("expected v2 transaction type, got %v", events[0].Type)
	}
}

func TestReorgV2(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore(pk)

	// create chain store
	network, genesis := testutil.Network()
	network.HardforkV2.AllowHeight = 10
	network.HardforkV2.RequireHeight = 20
	cs, tipState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, tipState)

	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// check balance
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// mine a block to fund the wallet
	mineAndSync(t, cm, ws, w.Address(), 1)
	maturityHeight := cm.TipState().MaturityHeight()
	// mine until the require height
	mineAndSync(t, cm, ws, types.VoidAddress, network.HardforkV2.RequireHeight-cm.Tip().Height)

	// check that the wallet has a single event
	if events, err := w.Events(0, 100); err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 event, got %v", len(events))
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	} else if events[0].MaturityHeight != maturityHeight {
		t.Fatalf("expected maturity height %v, got %v", maturityHeight, events[0].MaturityHeight)
	}

	// check that the wallet has an immature balance
	initialReward := cm.TipState().BlockReward()
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, initialReward, types.ZeroCurrency)

	// create a transaction that splits the wallet's balance into 20 outputs
	txn := types.V2Transaction{
		SiacoinOutputs: make([]types.SiacoinOutput, 20),
	}
	for i := range txn.SiacoinOutputs {
		txn.SiacoinOutputs[i] = types.SiacoinOutput{
			Value:   initialReward.Div64(20),
			Address: w.Address(),
		}
	}

	// try funding the transaction, expect it to fail since the outputs are immature
	_, _, err = w.FundV2Transaction(&txn, initialReward, false)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal("expected ErrNotEnoughFunds, got", err)
	}

	// mine until the payout matures
	tip := cm.TipState()
	target := tip.MaturityHeight()
	mineAndSync(t, cm, ws, types.VoidAddress, target-tip.Index.Height)

	// check that one payout has matured
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet still has a single event
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
	} else if events[0].Type != wallet.EventTypeMinerPayout {
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	}

	// fund and sign the transaction
	state, toSign, err := w.FundV2Transaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(state, &txn, toSign)

	// check that wallet now has no spendable balance
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check the wallet has no unconfirmed transactions
	poolTxns, err := w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 0 {
		t.Fatalf("expected 0 unconfirmed transaction, got %v", len(poolTxns))
	}

	// add the transaction to the pool
	if _, err := cm.AddV2PoolTransactions(state.Index, []types.V2Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet has one unconfirmed transaction
	poolTxns, err = w.UnconfirmedTransactions()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 1 {
		t.Fatalf("expected 1 unconfirmed transaction, got %v", len(poolTxns))
	} else if poolTxns[0].ID != types.Hash256(txn.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn.ID(), poolTxns[0].ID)
	} else if poolTxns[0].Type != wallet.EventTypeV2Transaction {
		t.Fatalf("expected v2 transaction type, got %v", poolTxns[0].Type)
	} else if !poolTxns[0].Inflow.Equals(initialReward) {
		t.Fatalf("expected %v inflow, got %v", initialReward, poolTxns[0].Inflow)
	} else if !poolTxns[0].Outflow.Equals(initialReward) {
		t.Fatalf("expected %v outflow, got %v", types.ZeroCurrency, poolTxns[0].Outflow)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// save a marker to this state to rollback to later
	rollbackState := cm.TipState()

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

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
	} else if n := len((events[0].Data.(types.V2Transaction)).SiacoinOutputs); n != 20 {
		t.Fatalf("expected 20 outputs, got %v", n)
	}

	txn2 := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward},
		},
	}
	state, toSign, err = w.FundV2Transaction(&txn2, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(state, &txn2, toSign)

	// release the inputs to construct a double spend
	w.ReleaseInputs(nil, []types.V2Transaction{txn2})

	txn1 := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward.Div64(2)},
		},
	}
	state, toSign, err = w.FundV2Transaction(&txn1, initialReward.Div64(2), false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(state, &txn1, toSign)

	// add the first transaction to the pool
	if _, err := cm.AddV2PoolTransactions(state.Index, []types.V2Transaction{txn1}); err != nil {
		t.Fatal(err)
	}
	mineAndSync(t, cm, ws, types.VoidAddress, 1)

	// check that the wallet now has 3 transactions: the initial payout
	// transaction, the split transaction, and a void transaction
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 3 {
		t.Fatalf("expected 3 transactions, got %v", count)
	}

	events, err = w.Events(0, 1) // limit of 1 so the original two transactions are not included
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 1 {
		t.Fatalf("expected 1 transactions, got %v", len(events))
	} else if events[0].ID != types.Hash256(txn1.ID()) {
		t.Fatalf("expected transaction %v, got %v", txn1.ID(), events[0].ID)
	}

	// check that the wallet balance has half the initial reward
	assertBalance(t, w, initialReward.Div64(2), initialReward.Div64(2), types.ZeroCurrency, types.ZeroCurrency)

	// spend the second transaction to invalidate the confirmed transaction
	state = rollbackState
	b := types.Block{
		ParentID:     state.Index.ID,
		Timestamp:    types.CurrentTimestamp(),
		MinerPayouts: []types.SiacoinOutput{{Address: types.VoidAddress, Value: state.BlockReward()}},
		V2: &types.V2BlockData{
			Height:       state.Index.Height + 1,
			Transactions: []types.V2Transaction{txn2},
		},
	}
	b.V2.Commitment = state.Commitment(state.TransactionsCommitment(b.Transactions, b.V2Transactions()), b.MinerPayouts[0].Address)
	if !coreutils.FindBlockNonce(state, &b, time.Second) {
		t.Fatal("failed to find nonce")
	}
	ancestorTimestamp, _ := cs.AncestorTimestamp(b.ParentID)
	state, _ = consensus.ApplyBlock(state, b, cs.SupplementTipBlock(b), ancestorTimestamp)
	reorgBlocks := []types.Block{b}
	for i := 0; i < 5; i++ {
		b := types.Block{
			ParentID:     state.Index.ID,
			Timestamp:    types.CurrentTimestamp(),
			MinerPayouts: []types.SiacoinOutput{{Address: types.VoidAddress, Value: state.BlockReward()}},
			V2: &types.V2BlockData{
				Height: state.Index.Height + 1,
			},
		}
		b.V2.Commitment = state.Commitment(state.TransactionsCommitment(b.Transactions, b.V2Transactions()), b.MinerPayouts[0].Address)
		if !coreutils.FindBlockNonce(state, &b, time.Second) {
			t.Fatal("failed to find nonce")
		}
		ancestorTimestamp, _ := cs.AncestorTimestamp(b.ParentID)
		state, _ = consensus.ApplyBlock(state, b, cs.SupplementTipBlock(b), ancestorTimestamp)
		reorgBlocks = append(reorgBlocks, b)
	}

	if err := cm.AddBlocks(reorgBlocks); err != nil {
		t.Fatal(err)
	} else if err := syncDB(cm, ws); err != nil {
		t.Fatal(err)
	} else if cm.Tip() != state.Index {
		t.Fatalf("expected tip %v, got %v", state.Index, cm.Tip())
	}

	// check that the original transaction is now invalid
	if _, err := cm.AddV2PoolTransactions(state.Index, []types.V2Transaction{txn1}); err == nil {
		t.Fatalf("expected double-spend error, got nil")
	}

	// all balances should now be zero
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet is back to two events
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 3 {
		t.Fatalf("expected 3 transactions, got %v", count)
	}

	events, err = w.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 3 {
		t.Fatalf("expected 3 transactions, got %v", len(events))
	} else if events[0].ID != types.Hash256(txn2.ID()) { // new transaction first
		t.Fatalf("expected transaction %v, got %v", txn2.ID(), events[0].ID)
	} else if events[1].ID != types.Hash256(txn.ID()) { // split transaction second
		t.Fatalf("expected transaction %v, got %v", txn.ID(), events[1].ID)
	} else if events[2].Type != wallet.EventTypeMinerPayout { // payout transaction last
		t.Fatalf("expected miner payout, got %v", events[0].Type)
	}
}
