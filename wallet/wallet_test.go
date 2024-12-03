package wallet_test

import (
	"errors"
	"fmt"
	"math/bits"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func syncDB(cm *chain.Manager, store *testutil.EphemeralWalletStore, w *wallet.SingleAddressWallet) error {
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

		err = store.UpdateChainState(func(tx wallet.UpdateTx) error {
			return w.UpdateChainState(tx, reverted, applied)
		})
		if err != nil {
			return fmt.Errorf("failed to update chain state: %w", err)
		}
	}
}

func mineAndSync(t *testing.T, cm *chain.Manager, ws *testutil.EphemeralWalletStore, w *wallet.SingleAddressWallet, address types.Address, n uint64) {
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
	if err := syncDB(cm, ws, w); err != nil {
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

func assertEvent(t *testing.T, wm *wallet.SingleAddressWallet, id types.Hash256, eventType string, inflow, outflow types.Currency, maturityHeight uint64) {
	t.Helper()

	events, err := wm.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	}

	for _, event := range events {
		if event.ID == id {
			if event.Type != eventType {
				t.Fatalf("expected %v event, got %v", eventType, event.Type)
			} else if event.MaturityHeight != maturityHeight {
				t.Fatalf("expected maturity height %v, got %v", maturityHeight, event.MaturityHeight)
			}

			if !event.SiacoinInflow().Equals(inflow) {
				t.Fatalf("expected inflow %v, got %v", inflow, event.SiacoinInflow())
			} else if !event.SiacoinOutflow().Equals(outflow) {
				t.Fatalf("expected outflow %v, got %v", outflow, event.SiacoinOutflow())
			}
			return
		}
	}
	t.Fatalf("event not found")
}

func transactionValues(t *testing.T, wm *wallet.SingleAddressWallet, txn types.Transaction, addr types.Address) (inflow, outflow types.Currency) {
	t.Helper()

	utxos, err := wm.UnspentSiacoinElements()
	if err != nil {
		t.Fatal("unspent siacoin elements", err)
	}

	elements := make(map[types.SiacoinOutputID]types.SiacoinElement)
	for _, se := range utxos {
		elements[types.SiacoinOutputID(se.ID)] = se
	}

	for _, si := range txn.SiacoinInputs {
		if si.UnlockConditions.UnlockHash() != addr {
			continue
		}
		sce, ok := elements[si.ParentID]
		if !ok {
			t.Fatalf("missing siacoin element %v", si.ParentID)
		}
		outflow = outflow.Add(sce.SiacoinOutput.Value)
	}

	for _, so := range txn.SiacoinOutputs {
		if so.Address == addr {
			inflow = inflow.Add(so.Value)
		}
	}
	return
}

func v2TransactionValues(t *testing.T, txn types.V2Transaction, addr types.Address) (inflow, outflow types.Currency) {
	t.Helper()

	for _, so := range txn.SiacoinOutputs {
		if so.Address == addr {
			inflow = inflow.Add(so.Value)
		}
	}

	for _, si := range txn.SiacoinInputs {
		if si.Parent.SiacoinOutput.Address == addr {
			outflow = outflow.Add(si.Parent.SiacoinOutput.Value)
		}
	}
	return
}

// NOTE: due to a bug in the transaction validation code, calculating payouts
// is way harder than it needs to be. Tax is calculated on the post-tax
// contract payout (instead of the sum of the renter and host payouts). So the
// equation for the payout is:
//
//	   payout = renterPayout + hostPayout + payout*tax
//	∴  payout = (renterPayout + hostPayout) / (1 - tax)
//
// This would work if 'tax' were a simple fraction, but because the tax must
// be evenly distributed among siafund holders, 'tax' is actually a function
// that multiplies by a fraction and then rounds down to the nearest multiple
// of the siafund count. Thus, when inverting the function, we have to make an
// initial guess and then fix the rounding error.
func taxAdjustedPayout(target types.Currency) types.Currency {
	// compute initial guess as target * (1 / 1-tax); since this does not take
	// the siafund rounding into account, the guess will be up to
	// types.SiafundCount greater than the actual payout value.
	guess := target.Mul64(1000).Div64(961)

	// now, adjust the guess to remove the rounding error. We know that:
	//
	//   (target % types.SiafundCount) == (payout % types.SiafundCount)
	//
	// therefore, we can simply adjust the guess to have this remainder as
	// well. The only wrinkle is that, since we know guess >= payout, if the
	// guess remainder is smaller than the target remainder, we must subtract
	// an extra types.SiafundCount.
	//
	// for example, if target = 87654321 and types.SiafundCount = 10000, then:
	//
	//   initial_guess  = 87654321 * (1 / (1 - tax))
	//                  = 91211572
	//   target % 10000 =     4321
	//   adjusted_guess = 91204321

	mod64 := func(c types.Currency, v uint64) types.Currency {
		var r uint64
		if c.Hi < v {
			_, r = bits.Div64(c.Hi, c.Lo, v)
		} else {
			_, r = bits.Div64(0, c.Hi, v)
			_, r = bits.Div64(r, c.Lo, v)
		}
		return types.NewCurrency64(r)
	}
	sfc := (consensus.State{}).SiafundCount()
	tm := mod64(target, sfc)
	gm := mod64(guess, sfc)
	if gm.Cmp(tm) < 0 {
		guess = guess.Sub(types.NewCurrency64(sfc))
	}
	return guess.Add(tm).Sub(gm)
}

func TestWallet(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

	// create chain store
	network, genesis := testutil.Network()
	cs, genesisState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, genesisState)
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
	mineAndSync(t, cm, ws, w, w.Address(), 1)

	// check that the wallet received the miner payout
	maturityHeight := genesisState.MaturityHeight()
	initialReward := genesisState.BlockReward()
	initialPayoutID := types.Hash256(cm.Tip().ID.MinerOutputID(0))
	assertEvent(t, w, initialPayoutID, wallet.EventTypeMinerPayout, initialReward, types.ZeroCurrency, maturityHeight)
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
	_, err = w.FundTransaction(&txn, initialReward, true)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal("expected ErrNotEnoughFunds, got", err)
	}

	// mine until the payout matures
	mineAndSync(t, cm, ws, w, types.VoidAddress, genesisState.MaturityHeight()-cm.Tip().Height)
	// check that one payout has matured
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet still has a single event
	count, err := w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transaction, got %v", count)
	}
	assertEvent(t, w, initialPayoutID, wallet.EventTypeMinerPayout, initialReward, types.ZeroCurrency, maturityHeight)

	// fund and sign the transaction
	toSign, err := w.FundTransaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

	// check that wallet now has no spendable balance
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check the wallet has no unconfirmed transactions
	poolTxns, err := w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 0 {
		t.Fatalf("expected 0 unconfirmed transaction, got %v", len(poolTxns))
	}

	// add the transaction to the pool
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)
	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the transaction event was not created since it has no
	// effect on the wallet's balance
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transactions, got %v", count)
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
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// check that the wallet now has 21 transactions: the initial payout
	// transaction and 20 void transactions
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 21 {
		t.Fatalf("expected 21 transactions, got %v", count)
	}

	// check that all the wallet balances have reset
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// check that the paginated transactions are in the proper order
	events, err := w.Events(0, 20) // limit of 20 to exclude the original payout
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
		assertEvent(t, w, events[j].ID, wallet.EventTypeV1Transaction, types.ZeroCurrency, sendAmount, cm.Tip().Height)
	}
}

func TestWalletUnconfirmed(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

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
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	mineAndSync(t, cm, ws, w, types.VoidAddress, cm.TipState().MaturityHeight()-1)

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
	poolTxns, err := w.UnconfirmedEvents()
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

	// check that the wallet has two unconfirmed events
	poolTxns, err = w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 2 {
		t.Fatal("expected 2 unconfirmed events")
	}

	if !poolTxns[0].SiacoinOutflow().Equals(initialReward) {
		t.Fatalf("expected outflow of %v, got %v", initialReward, poolTxns[0].SiacoinOutflow())
	} else if !poolTxns[0].SiacoinInflow().Equals(initialReward.Div64(2)) {
		t.Fatalf("expected inflow of %v, got %v", initialReward.Div64(2), poolTxns[0].SiacoinInflow())
	} else if !poolTxns[1].SiacoinOutflow().Equals(initialReward.Div64(2)) {
		t.Fatalf("expected outflow of %v, got %v", initialReward.Div64(2), poolTxns[1].SiacoinOutflow())
	} else if !poolTxns[1].SiacoinInflow().IsZero() {
		t.Fatalf("expected no inflow, got %v", poolTxns[1].SiacoinInflow())
	}
}

func TestWalletRedistribute(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

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
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	mineAndSync(t, cm, ws, w, types.VoidAddress, cm.TipState().MaturityHeight()-1)

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
		mineAndSync(t, cm, ws, w, types.VoidAddress, 1)
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

func TestWalletRedistributeV2(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

	// create chain store
	network, genesis := testutil.Network()
	network.HardforkV2.AllowHeight = 1 // allow V2 transactions from the start
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
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	mineAndSync(t, cm, ws, w, types.VoidAddress, cm.TipState().MaturityHeight()-1)

	redistribute := func(amount types.Currency, n int) error {
		txns, toSign, err := w.RedistributeV2(n, amount, types.ZeroCurrency)
		if err != nil {
			return fmt.Errorf("redistribute failed: %w", err)
		} else if len(txns) == 0 {
			return nil
		}

		for i := 0; i < len(txns); i++ {
			w.SignV2Inputs(&txns[i], toSign[i])
		}
		if _, err := cm.AddV2PoolTransactions(cm.Tip(), txns); err != nil {
			return fmt.Errorf("failed to add transactions to pool: %w", err)
		}
		mineAndSync(t, cm, ws, w, types.VoidAddress, 1)
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
	txns, toSign, err := w.RedistributeV2(3, amount, types.ZeroCurrency)
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
	ws := testutil.NewEphemeralWalletStore()

	// create chain store
	network, genesis := testutil.Network()
	cs, genesisState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, genesisState)
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
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	maturityHeight := genesisState.MaturityHeight()

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
	mineAndSync(t, cm, ws, w, types.VoidAddress, target-tip.Index.Height)

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
	poolTxns, err := w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 0 {
		t.Fatalf("expected 0 unconfirmed transaction, got %v", len(poolTxns))
	}

	// add the transaction to the pool
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)
	rollbackState := cm.TipState()

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet still has a single event
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transactions, got %v", count)
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
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// check that the wallet now has 2 transactions: the initial payout
	// and a void transaction
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 2 transactions, got %v", count)
	}
	assertEvent(t, w, types.Hash256(txn1.ID()), wallet.EventTypeV1Transaction, types.ZeroCurrency, initialReward.Div64(2), cm.Tip().Height)
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
	} else if err := syncDB(cm, ws, w); err != nil {
		t.Fatal(err)
	}

	// all balances should now be zero
	assertBalance(t, w, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency)

	// check that the second transaction was confirmed
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 1 transactions, got %v", count)
	}
	assertEvent(t, w, types.Hash256(txn2.ID()), wallet.EventTypeV1Transaction, types.ZeroCurrency, initialReward, cm.Tip().Height)
}

func TestWalletV2(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

	// create chain store
	network, genesis := testutil.Network()
	cs, genesisState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, genesisState)
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
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	maturityHeight := genesisState.MaturityHeight()

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
	mineAndSync(t, cm, ws, w, types.VoidAddress, target-tip.Index.Height)

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
	poolTxns, err := w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 0 {
		t.Fatalf("expected 0 unconfirmed transaction, got %v", len(poolTxns))
	}

	// add the transaction to the pool
	if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet still has a single event since the transaction
	// does not affect the wallet's balance
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transactions, got %v", count)
	}

	// mine until the v2 require height
	mineAndSync(t, cm, ws, w, types.VoidAddress, network.HardforkV2.RequireHeight-cm.Tip().Height)

	v2Txn := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: types.Siacoins(100)},
		},
	}

	// fund and sign the transaction
	basis, toSignV2, err := w.FundV2Transaction(&v2Txn, types.Siacoins(100), false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&v2Txn, toSignV2)

	// add the transaction to the pool
	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{v2Txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet has one unconfirmed transaction
	poolTxns, err = w.UnconfirmedEvents()
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
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// check that the wallet has three events
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 2 events, got %v", count)
	}

	inflow, outflow := v2TransactionValues(t, v2Txn, w.Address())
	assertEvent(t, w, types.Hash256(v2Txn.ID()), wallet.EventTypeV2Transaction, inflow, outflow, cm.Tip().Height)
}

func TestReorgV2(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

	// create chain store
	network, genesis := testutil.V2Network()
	cs, genesisState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, genesisState)

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
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	maturityHeight := genesisState.MaturityHeight()

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
	mineAndSync(t, cm, ws, w, types.VoidAddress, target-tip.Index.Height)

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
	basis, toSign, err := w.FundV2Transaction(&txn, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&txn, toSign)

	// check that wallet now has no spendable balance
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check the wallet has no unconfirmed transactions
	poolTxns, err := w.UnconfirmedEvents()
	if err != nil {
		t.Fatal(err)
	} else if len(poolTxns) != 0 {
		t.Fatalf("expected 0 unconfirmed transaction, got %v", len(poolTxns))
	}

	// add the transaction to the pool
	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
		t.Fatal(err)
	}

	// check that the wallet now has an unconfirmed balance
	// note: the wallet should still have a "confirmed" balance since the pool
	// transaction is not yet confirmed.
	assertBalance(t, w, types.ZeroCurrency, initialReward, types.ZeroCurrency, initialReward)
	// mine a block to confirm the transaction
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// save a marker to this state to rollback to later
	rollbackState := cm.TipState()

	// check that the balance was confirmed and the other values reset
	assertBalance(t, w, initialReward, initialReward, types.ZeroCurrency, types.ZeroCurrency)

	// check that the wallet has a single event
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 1 {
		t.Fatalf("expected 1 transactions, got %v", count)
	}

	txn2 := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward},
		},
	}
	_, toSign, err = w.FundV2Transaction(&txn2, initialReward, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&txn2, toSign)

	// release the inputs to construct a double spend
	w.ReleaseInputs(nil, []types.V2Transaction{txn2})

	txn1 := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: initialReward.Div64(2)},
		},
	}
	basis, toSign, err = w.FundV2Transaction(&txn1, initialReward.Div64(2), false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&txn1, toSign)

	// add the first transaction to the pool
	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn1}); err != nil {
		t.Fatal(err)
	}
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// check that the wallet now has 2 transactions: the initial payout
	// transaction and a void transaction
	count, err = w.EventCount()
	if err != nil {
		t.Fatal(err)
	} else if count != 2 {
		t.Fatalf("expected 2 transactions, got %v", count)
	}
	assertEvent(t, w, types.Hash256(txn1.ID()), wallet.EventTypeV2Transaction, types.ZeroCurrency, initialReward.Div64(2), cm.Tip().Height)
	assertBalance(t, w, initialReward.Div64(2), initialReward.Div64(2), types.ZeroCurrency, types.ZeroCurrency)

	// spend the second transaction to invalidate the confirmed transaction
	state := rollbackState
	txn2Height := state.Index.Height + 1
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
	} else if err := syncDB(cm, ws, w); err != nil {
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
	} else if count != 2 {
		t.Fatalf("expected 2 transactions, got %v", count)
	}

	events, err = w.Events(0, 100)
	if err != nil {
		t.Fatal(err)
	} else if len(events) != 2 {
		t.Fatalf("expected 3 transactions, got %v", len(events))
	} else if events[0].ID != types.Hash256(txn2.ID()) { // new transaction first
		t.Fatalf("expected transaction %v, got %v", txn2.ID(), events[0].ID)
	}
	assertEvent(t, w, types.Hash256(txn2.ID()), wallet.EventTypeV2Transaction, types.ZeroCurrency, initialReward, txn2Height)
}

func TestFundTransaction(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

	// use a network that results in coins mined before and after the v2
	// hardfork
	network, genesis := testutil.Network()
	network.HardforkV2.AllowHeight = 2
	network.HardforkV2.RequireHeight = 3

	// create chain store
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
	mineAndSync(t, cm, ws, w, w.Address(), 3)
	mineAndSync(t, cm, ws, w, types.VoidAddress, 200)

	balance, err := w.Balance()
	if err != nil {
		t.Fatal(err)
	}
	sendAmt := balance.Confirmed

	txnV2 := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{
				Address: w.Address(),
				Value:   sendAmt,
			},
		},
	}

	// Send full confirmed balance to the wallet
	basis, toSignV2, err := w.FundV2Transaction(&txnV2, sendAmt, false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&txnV2, toSignV2)

	_, err = cm.AddV2PoolTransactions(basis, []types.V2Transaction{txnV2})
	if err != nil {
		t.Fatal(err)
	}

	balance, err = w.Balance()
	if err != nil {
		t.Fatal(err)
	} else if !balance.Unconfirmed.Equals(sendAmt) {
		t.Fatalf("expected %v unconfirmed balance, got %v", sendAmt, balance.Unconfirmed)
	}

	// try again, should fail since wallet is empty
	_, _, err = w.FundV2Transaction(&txnV2, sendAmt, false)
	if !errors.Is(err, wallet.ErrNotEnoughFunds) {
		t.Fatal(err)
	}

	// try again using unconfirmed balance, should work
	txnV3 := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{
				Address: w.Address(),
				Value:   sendAmt,
			},
		},
	}
	basis, toSignV2, err = w.FundV2Transaction(&txnV3, sendAmt, true)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&txnV3, toSignV2)
	basis, txnset, err := cm.V2TransactionSet(basis, txnV3)
	if err != nil {
		t.Fatal(err)
	}

	_, err = cm.AddV2PoolTransactions(basis, txnset)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSingleAddressWalletEventTypes(t *testing.T) {
	pk := types.GeneratePrivateKey()
	addr := types.StandardUnlockHash(pk.PublicKey())

	log := zap.NewNop()
	dir := t.TempDir()

	bdb, err := coreutils.OpenBoltChainDB(filepath.Join(dir, "consensus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()

	network, genesisBlock := testutil.V2Network()
	// raise the require height to test v1 events
	network.HardforkV2.RequireHeight = 100
	store, genesisState, err := chain.NewDBStore(bdb, network, genesisBlock)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, genesisState)

	ws := testutil.NewEphemeralWalletStore()
	wm, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	defer wm.Close()

	// miner payout event
	mineAndSync(t, cm, ws, wm, addr, 1)
	assertEvent(t, wm, types.Hash256(cm.Tip().ID.MinerOutputID(0)), wallet.EventTypeMinerPayout, genesisState.BlockReward(), types.ZeroCurrency, genesisState.MaturityHeight())

	// mine until the payout matures
	mineAndSync(t, cm, ws, wm, types.VoidAddress, genesisState.MaturityHeight()-cm.Tip().Height+1)

	// v1 transaction
	t.Run("v1 transaction", func(t *testing.T) {
		// fund and sign a v1 transaction
		txn := types.Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: types.VoidAddress, Value: types.Siacoins(1000)},
			},
		}
		toSign, err := wm.FundTransaction(&txn, types.Siacoins(1000), false)
		if err != nil {
			t.Fatal("fund transaction", err)
		}
		wm.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})
		// calculate inflow and outflow before broadcasting
		inflow, outflow := transactionValues(t, wm, txn, wm.Address())
		// broadcast the transaction
		if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
			t.Fatal(err)
		}
		// confirm the transaction
		mineAndSync(t, cm, ws, wm, types.VoidAddress, 1)
		assertEvent(t, wm, types.Hash256(txn.ID()), wallet.EventTypeV1Transaction, inflow, outflow, cm.Tip().Height)
	})

	t.Run("v1 contract resolution - missed", func(t *testing.T) {
		// v1 contract resolution - only one type of resolution is supported.
		// The only difference is `missed == true` or `missed == false`

		// create a storage contract
		contractPayout := types.Siacoins(10000)
		missedPayout := contractPayout.Sub(types.Siacoins(1000))
		fc := types.FileContract{
			WindowStart: cm.TipState().Index.Height + 10,
			WindowEnd:   cm.TipState().Index.Height + 20,
			Payout:      taxAdjustedPayout(contractPayout),
			ValidProofOutputs: []types.SiacoinOutput{
				{Address: addr, Value: contractPayout},
			},
			MissedProofOutputs: []types.SiacoinOutput{
				{Address: addr, Value: missedPayout},
				{Address: types.VoidAddress, Value: types.Siacoins(1000)},
			},
		}

		// create a transaction with the contract
		txn := types.Transaction{
			FileContracts: []types.FileContract{fc},
		}
		toSign, err := wm.FundTransaction(&txn, fc.Payout, false)
		if err != nil {
			t.Fatal(err)
		}
		wm.SignTransaction(&txn, toSign, types.CoveredFields{WholeTransaction: true})

		// broadcast the transaction
		if _, err := cm.AddPoolTransactions([]types.Transaction{txn}); err != nil {
			t.Fatal(err)
		}

		// mine until the contract expires to trigger the resolution event
		mineAndSync(t, cm, ws, wm, types.VoidAddress, fc.WindowEnd-cm.Tip().Height)
		assertEvent(t, wm, types.Hash256(txn.FileContractID(0).MissedOutputID(0)), wallet.EventTypeV1ContractResolution, missedPayout, types.ZeroCurrency, fc.WindowEnd+network.MaturityDelay)
	})

	t.Run("v2 transaction", func(t *testing.T) {
		txn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: types.VoidAddress, Value: types.Siacoins(1000)},
			},
		}
		basis, toSign, err := wm.FundV2Transaction(&txn, types.Siacoins(1000), false)
		if err != nil {
			t.Fatal(err)
		}
		wm.SignV2Inputs(&txn, toSign)

		// broadcast the transaction
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		}
		// mine a block to confirm the transaction
		mineAndSync(t, cm, ws, wm, types.VoidAddress, 1)
		inflow, outflow := v2TransactionValues(t, txn, wm.Address())
		assertEvent(t, wm, types.Hash256(txn.ID()), wallet.EventTypeV2Transaction, inflow, outflow, cm.Tip().Height)
	})

	t.Run("v2 contract resolution - expired", func(t *testing.T) {
		// create a storage contract
		renterPayout := types.Siacoins(10000)
		fc := types.V2FileContract{
			RenterOutput: types.SiacoinOutput{
				Address: addr,
				Value:   renterPayout,
			},
			HostOutput: types.SiacoinOutput{
				Address: types.VoidAddress,
				Value:   types.ZeroCurrency,
			},
			ProofHeight:      cm.TipState().Index.Height + 10,
			ExpirationHeight: cm.TipState().Index.Height + 20,

			RenterPublicKey: pk.PublicKey(),
			HostPublicKey:   pk.PublicKey(),
		}
		contractValue := renterPayout.Add(cm.TipState().V2FileContractTax(fc))
		sigHash := cm.TipState().ContractSigHash(fc)
		sig := pk.SignHash(sigHash)
		fc.RenterSignature = sig
		fc.HostSignature = sig

		// create a transaction with the contract
		txn := types.V2Transaction{
			FileContracts: []types.V2FileContract{fc},
		}
		basis, toSign, err := wm.FundV2Transaction(&txn, contractValue, false)
		if err != nil {
			t.Fatal(err)
		}
		wm.SignV2Inputs(&txn, toSign)

		// broadcast the transaction
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		}
		// current tip
		tip := cm.Tip()
		// mine until the contract expires
		mineAndSync(t, cm, ws, wm, types.VoidAddress, fc.ExpirationHeight-cm.Tip().Height)

		// this is kind of annoying because we have to keep the file contract
		// proof up to date.
		_, applied, err := cm.UpdatesSince(tip, 1000)
		if err != nil {
			t.Fatal(err)
		}

		// get the confirmed file contract element
		var fce types.V2FileContractElement
		applied[0].ForEachV2FileContractElement(func(ele types.V2FileContractElement, _ bool, _ *types.V2FileContractElement, _ types.V2FileContractResolutionType) {
			fce = ele
		})
		for _, cau := range applied {
			cau.UpdateElementProof(&fce.StateElement)
		}

		resolutionTxn := types.V2Transaction{
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &types.V2FileContractExpiration{},
				},
			},
		}
		// broadcast the expire resolution
		if _, err := cm.AddV2PoolTransactions(cm.Tip(), []types.V2Transaction{resolutionTxn}); err != nil {
			t.Fatal(err)
		}
		// mine a block to confirm the resolution
		mineAndSync(t, cm, ws, wm, types.VoidAddress, 1)
		assertEvent(t, wm, types.Hash256(types.FileContractID(fce.ID).V2RenterOutputID()), wallet.EventTypeV2ContractResolution, renterPayout, types.ZeroCurrency, cm.Tip().Height+network.MaturityDelay)
	})

	t.Run("v2 contract resolution - storage proof", func(t *testing.T) {
		// create a storage contract
		renterPayout := types.Siacoins(10000)
		fc := types.V2FileContract{
			RenterOutput: types.SiacoinOutput{
				Address: types.VoidAddress,
				Value:   types.ZeroCurrency,
			},
			HostOutput: types.SiacoinOutput{
				Address: addr,
				Value:   renterPayout,
			},
			ProofHeight:      cm.TipState().Index.Height + 10,
			ExpirationHeight: cm.TipState().Index.Height + 20,

			RenterPublicKey: pk.PublicKey(),
			HostPublicKey:   pk.PublicKey(),
		}
		contractValue := renterPayout.Add(cm.TipState().V2FileContractTax(fc))
		sigHash := cm.TipState().ContractSigHash(fc)
		sig := pk.SignHash(sigHash)
		fc.RenterSignature = sig
		fc.HostSignature = sig

		// create a transaction with the contract
		txn := types.V2Transaction{
			FileContracts: []types.V2FileContract{fc},
		}
		basis, toSign, err := wm.FundV2Transaction(&txn, contractValue, false)
		if err != nil {
			t.Fatal(err)
		}
		wm.SignV2Inputs(&txn, toSign)

		// broadcast the transaction
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		}
		// current tip
		tip := cm.Tip()
		// mine until the contract proof window
		mineAndSync(t, cm, ws, wm, types.VoidAddress, fc.ProofHeight-cm.Tip().Height)

		// this is even more annoying because we have to keep the file contract
		// proof and the chain index proof up to date.
		_, applied, err := cm.UpdatesSince(tip, 1000)
		if err != nil {
			t.Fatal(err)
		}

		// get the confirmed file contract element
		var fce types.V2FileContractElement
		applied[0].ForEachV2FileContractElement(func(ele types.V2FileContractElement, _ bool, _ *types.V2FileContractElement, _ types.V2FileContractResolutionType) {
			fce = ele
		})
		// update its proof
		for _, cau := range applied {
			cau.UpdateElementProof(&fce.StateElement)
		}
		// get the proof index element
		indexElement := applied[len(applied)-1].ChainIndexElement()

		resolutionTxn := types.V2Transaction{
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent: fce,
					Resolution: &types.V2StorageProof{
						ProofIndex: indexElement,
						// proof is nil since there's no data
					},
				},
			},
		}

		// broadcast the expire resolution
		if _, err := cm.AddV2PoolTransactions(cm.Tip(), []types.V2Transaction{resolutionTxn}); err != nil {
			t.Fatal(err)
		}
		// mine a block to confirm the resolution
		mineAndSync(t, cm, ws, wm, types.VoidAddress, 1)
		assertEvent(t, wm, types.Hash256(types.FileContractID(fce.ID).V2HostOutputID()), wallet.EventTypeV2ContractResolution, renterPayout, types.ZeroCurrency, cm.Tip().Height+network.MaturityDelay)
	})

	t.Run("v2 contract resolution - renewal", func(t *testing.T) {
		// create a storage contract
		renterPayout := types.Siacoins(10000)
		fc := types.V2FileContract{
			RenterOutput: types.SiacoinOutput{
				Address: addr,
				Value:   renterPayout,
			},
			HostOutput: types.SiacoinOutput{
				Address: types.VoidAddress,
				Value:   types.ZeroCurrency,
			},
			ProofHeight:      cm.TipState().Index.Height + 10,
			ExpirationHeight: cm.TipState().Index.Height + 20,

			RenterPublicKey: pk.PublicKey(),
			HostPublicKey:   pk.PublicKey(),
		}
		contractValue := renterPayout.Add(cm.TipState().V2FileContractTax(fc))
		sigHash := cm.TipState().ContractSigHash(fc)
		sig := pk.SignHash(sigHash)
		fc.RenterSignature = sig
		fc.HostSignature = sig

		// create a transaction with the contract
		txn := types.V2Transaction{
			FileContracts: []types.V2FileContract{fc},
		}
		basis, toSign, err := wm.FundV2Transaction(&txn, contractValue, false)
		if err != nil {
			t.Fatal(err)
		}
		wm.SignV2Inputs(&txn, toSign)

		// broadcast the transaction
		if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{txn}); err != nil {
			t.Fatal(err)
		}
		// current tip
		tip := cm.Tip()
		// mine a block to confirm the contract formation
		mineAndSync(t, cm, ws, wm, types.VoidAddress, 1)

		// this is annoying because we have to keep the file contract
		// proof
		_, applied, err := cm.UpdatesSince(tip, 1000)
		if err != nil {
			t.Fatal(err)
		}

		// get the confirmed file contract element
		var fce types.V2FileContractElement
		applied[0].ForEachV2FileContractElement(func(ele types.V2FileContractElement, _ bool, _ *types.V2FileContractElement, _ types.V2FileContractResolutionType) {
			fce = ele
		})
		for _, cau := range applied {
			cau.UpdateElementProof(&fce.StateElement)
		}

		// create a renewal
		renewal := types.V2FileContractRenewal{
			FinalRenterOutput: fce.V2FileContract.RenterOutput,
			FinalHostOutput:   fce.V2FileContract.HostOutput,
			NewContract: types.V2FileContract{
				RenterOutput:     fc.RenterOutput,
				ProofHeight:      fc.ProofHeight + 10,
				ExpirationHeight: fc.ExpirationHeight + 10,

				RenterPublicKey: fc.RenterPublicKey,
				HostPublicKey:   fc.HostPublicKey,
			},
		}

		renewalSigHash := cm.TipState().RenewalSigHash(renewal)
		renewalSig := pk.SignHash(renewalSigHash)
		renewal.RenterSignature = renewalSig
		renewal.HostSignature = renewalSig
		contractSigHash := cm.TipState().ContractSigHash(renewal.NewContract)
		contractSig := pk.SignHash(contractSigHash)
		renewal.NewContract.RenterSignature = contractSig
		renewal.NewContract.HostSignature = contractSig

		newContractValue := renterPayout.Add(cm.TipState().V2FileContractTax(renewal.NewContract))

		// renewals can't have change outputs
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: addr, Value: newContractValue},
			},
		}
		setupBasis, setupToSign, err := wm.FundV2Transaction(&setupTxn, newContractValue, false)
		if err != nil {
			t.Fatal(err)
		}
		wm.SignV2Inputs(&setupTxn, setupToSign)

		// create the renewal transaction
		resolutionTxn := types.V2Transaction{
			SiacoinInputs: []types.V2SiacoinInput{
				{
					Parent: setupTxn.EphemeralSiacoinOutput(0),
					SatisfiedPolicy: types.SatisfiedPolicy{
						Policy: wm.SpendPolicy(),
					},
				},
			},
			FileContractResolutions: []types.V2FileContractResolution{
				{
					Parent:     fce,
					Resolution: &renewal,
				},
			},
		}
		wm.SignV2Inputs(&resolutionTxn, []int{0})

		// broadcast the renewal
		if _, err := cm.AddV2PoolTransactions(setupBasis, []types.V2Transaction{setupTxn, resolutionTxn}); err != nil {
			t.Fatal(err)
		}
		// mine a block to confirm the renewal
		mineAndSync(t, cm, ws, wm, types.VoidAddress, 1)
		assertEvent(t, wm, types.Hash256(types.FileContractID(fce.ID).V2RenterOutputID()), wallet.EventTypeV2ContractResolution, renterPayout, types.ZeroCurrency, cm.Tip().Height+network.MaturityDelay)
	})
}

func TestV2TPoolRace(t *testing.T) {
	// create wallet store
	pk := types.GeneratePrivateKey()
	ws := testutil.NewEphemeralWalletStore()

	// create chain store
	network, genesis := testutil.V2Network()
	cs, genesisState, err := chain.NewDBStore(chain.NewMemDB(), network, genesis)
	if err != nil {
		t.Fatal(err)
	}

	// create chain manager and subscribe the wallet
	cm := chain.NewManager(cs, genesisState)
	// create wallet
	l := zaptest.NewLogger(t)
	w, err := wallet.NewSingleAddressWallet(pk, cm, ws, wallet.WithLogger(l.Named("wallet")))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// fund the wallet
	mineAndSync(t, cm, ws, w, w.Address(), 1)
	// mine until one utxo is mature
	mineAndSync(t, cm, ws, w, types.VoidAddress, network.MaturityDelay)

	// create a transaction that creates an ephemeral output with 1000 SC
	setupTxn := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: w.Address(), Value: types.Siacoins(1000)},
		},
	}
	basis, toSign, err := w.FundV2Transaction(&setupTxn, types.Siacoins(1000), false)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&setupTxn, toSign)

	// broadcast the setup transaction
	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{setupTxn}); err != nil {
		t.Fatal(err)
	}

	// create a transaction that spends the ephemeral output
	spendTxn := types.V2Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Address: types.VoidAddress, Value: types.Siacoins(1000)},
		},
	}

	// try to fund with non-ephemeral output, should fail
	if _, _, err = w.FundV2Transaction(&spendTxn, types.Siacoins(1000), false); err == nil {
		t.Fatal("expected funding error, got nil")
	}

	// fund with the tpool ephemeral output
	basis, toSign, err = w.FundV2Transaction(&spendTxn, types.Siacoins(1000), true)
	if err != nil {
		t.Fatal(err)
	}
	w.SignV2Inputs(&spendTxn, toSign)

	// mine to confirm the setup transaction. This will make the ephemeral
	// output in the spend transaction invalid unless it is updated.
	mineAndSync(t, cm, ws, w, types.VoidAddress, 1)

	// broadcast the transaction set including the already confirmed setup
	// transaction. This seems unnecessary, but it's a fairly common occurrence
	// when passing transaction sets using unconfirmed outputs between a renter
	// and host. If the transaction set is not updated correctly, it will fail.
	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{setupTxn, spendTxn}); err != nil {
		t.Fatal(err)
	}
}
