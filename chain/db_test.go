package chain_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/bits"
	"reflect"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
	"lukechampine.com/frand"
)

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

func TestGetEmptyBlockID(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)
	_, _ = cm.Block(types.BlockID{})
}

func TestExpiringFileContracts(t *testing.T) {
	n, genesisBlock := chain.TestnetZen()
	store, cs, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock, nil)
	if err != nil {
		t.Fatal(err)
	}

	// create two file contracts with the same expiration height
	b := types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
		Transactions: []types.Transaction{{
			FileContracts: []types.FileContract{
				{FileMerkleRoot: frand.Entropy256(), WindowEnd: 2},
				{FileMerkleRoot: frand.Entropy256(), WindowEnd: 2},
			},
		}},
	}
	bs := store.SupplementTipBlock(b)
	var cau consensus.ApplyUpdate
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// apply another block, causing the expired contracts to be removed
	b = types.Block{
		ParentID:     cs.Index.ID,
		MinerPayouts: []types.SiacoinOutput{{Value: cs.BlockReward()}},
	}
	bs = store.SupplementTipBlock(b)
	if len(bs.ExpiringFileContracts) != 2 {
		t.Fatalf("expected 2 file contracts, got %d", len(bs.ExpiringFileContracts))
	}
	cs, cau = consensus.ApplyBlock(cs, b, bs, time.Time{})
	store.AddState(cs)
	store.AddBlock(b, &bs)
	store.ApplyBlock(cs, cau)

	// revert the block, causing the expired contracts to be re-created
	prev, _ := store.State(b.ParentID)
	cru := consensus.RevertBlock(prev, b, bs)
	store.RevertBlock(prev, cru)

	// the supplement for the next block should contain the same expiring
	// contracts as before, in the same order
	bs2 := store.SupplementTipBlock(types.Block{ParentID: cs.Index.ID})
	if !reflect.DeepEqual(bs, bs2) {
		t.Fatalf("expected supplements to be the same")
	}
}

func TestReorgExpiringFileContractOrder(t *testing.T) {
	n, genesis := testutil.Network()
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 100

	sk := types.GeneratePrivateKey()
	uc := types.StandardUnlockConditions(sk.PublicKey())
	addr := uc.UnlockHash()

	genesis.Transactions[0].SiacoinOutputs = []types.SiacoinOutput{
		{Address: addr, Value: types.Siacoins(1000)},
	}
	giftAmount := types.Siacoins(1000)
	giftUTXOID := genesis.Transactions[0].SiacoinOutputID(0)

	db1, ts1, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm1 := chain.NewManager(db1, ts1)

	newFileContract := func() types.FileContract {
		return types.FileContract{
			Payout:         taxAdjustedPayout(types.Siacoins(1)),
			FileMerkleRoot: frand.Entropy256(),
			WindowStart:    3,
			WindowEnd:      6,
			ValidProofOutputs: []types.SiacoinOutput{
				{Value: types.Siacoins(1), Address: addr},
			},
			MissedProofOutputs: []types.SiacoinOutput{
				{Value: types.Siacoins(1), Address: addr},
			},
			UnlockHash: uc.UnlockHash(),
		}
	}

	cost := taxAdjustedPayout(types.Siacoins(1)).Mul64(3) // 3 contracts, each with 1 SC proof output
	formationTxn := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{
			{ParentID: giftUTXOID, UnlockConditions: uc},
		},
		SiacoinOutputs: []types.SiacoinOutput{
			{Value: giftAmount.Sub(cost), Address: addr}, // Refund output
		},
		FileContracts: []types.FileContract{
			newFileContract(), // A
			newFileContract(), // B
			newFileContract(), // C
		},
		Signatures: []types.TransactionSignature{
			{ParentID: types.Hash256(giftUTXOID), CoveredFields: types.CoveredFields{WholeTransaction: true}},
		},
	}
	cs := cm1.TipState()
	sigHash := cs.WholeSigHash(formationTxn, types.Hash256(giftUTXOID), 0, 0, nil)
	sig := sk.SignHash(sigHash)
	formationTxn.Signatures[0].Signature = sig[:]

	_, err = cm1.AddPoolTransactions([]types.Transaction{formationTxn})
	if err != nil {
		t.Fatal(err)
	}
	// Block 1: three contracts that all expire at height 3 (A, B, C).
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)
	// update the gift UTXO ID and amount for the next transaction
	giftUTXOID = formationTxn.SiacoinOutputID(0)
	giftAmount = formationTxn.SiacoinOutputs[0].Value

	contractA := formationTxn.FileContractID(0)
	contractB := formationTxn.FileContractID(1)
	contractC := formationTxn.FileContractID(2)

	cs = cm1.TipState()
	cost = taxAdjustedPayout(types.Siacoins(1)) // 1 contract with 1 SC proof output
	formationTxn2 := types.Transaction{
		SiacoinInputs: []types.SiacoinInput{
			{ParentID: giftUTXOID, UnlockConditions: uc},
		},
		SiacoinOutputs: []types.SiacoinOutput{
			{Value: giftAmount.Sub(cost), Address: addr}, // Refund output
		},
		FileContracts: []types.FileContract{
			newFileContract(), // D
		},
		Signatures: []types.TransactionSignature{
			{ParentID: types.Hash256(giftUTXOID), CoveredFields: types.CoveredFields{WholeTransaction: true}},
		},
	}
	sigHash = cs.WholeSigHash(formationTxn2, types.Hash256(giftUTXOID), 0, 0, nil)
	sig = sk.SignHash(sigHash)
	formationTxn2.Signatures[0].Signature = sig[:]

	_, err = cm1.AddPoolTransactions([]types.Transaction{formationTxn2})
	if err != nil {
		t.Fatal(err)
	}
	// Block 2: one extra contract (D) with the same WindowEnd.
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)
	contractD := formationTxn2.FileContractID(0)

	proofTxn := types.Transaction{
		// Missed‑proof outputs spend the contracts without needing
		// a real storage proof.
		StorageProofs: []types.StorageProof{
			{ParentID: contractB},
			{ParentID: contractC},
		},
	}
	if _, err = cm1.AddPoolTransactions([]types.Transaction{proofTxn}); err != nil {
		t.Fatal(err)
	}
	// Block 3: Submit storage proofs contracts B and C. The remaining
	// contracts are (A D).
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)

	// revert and reapply the block
	if err := cm1.ForceRevertTip(); err != nil {
		t.Fatal(err)
	}
	testutil.MineBlocks(t, cm1, types.VoidAddress, 1)

	// mine one block past contract expiration
	for i := cm1.Tip().Height; i <= 6+1; i++ {
		testutil.MineBlocks(t, cm1, types.VoidAddress, 1)
	}

	expirationIndex, ok := cm1.BestIndex(6)
	if !ok {
		t.Fatal("expected to find index at height 6")
	}
	_, bs, ok := db1.Block(expirationIndex.ID)
	if !ok {
		t.Fatal("expected to find block at height 6")
	}
	fces := bs.ExpiringFileContracts
	t.Log(fces)

	_, applied, err := cm1.UpdatesSince(types.ChainIndex{}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	blocks := make([]types.Block, 0, len(applied))
	for _, cau := range applied {
		blocks = append(blocks, cau.Block)
	}

	db2, ts2, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm2 := chain.NewManager(db2, ts2)

	// applying the blocks should fail because the expiring file contract
	// order is not the same because of the missing revert.
	if err := cm2.AddBlocks(blocks); !errors.Is(err, consensus.ErrCommitmentMismatch) {
		t.Fatalf("expected %q, got %q", consensus.ErrCommitmentMismatch, err)
	}

	// reinit the chain manager with the correct expiring contract order
	cm2 = chain.NewManager(db2, ts2, chain.WithExpiringContractOrder(map[types.BlockID][]types.FileContractID{
		expirationIndex.ID: {
			contractD,
			contractA,
		},
	}))
	// applying the blocks should succeed because the ordering is overwritten
	if err := cm2.AddBlocks(blocks); err != nil {
		t.Fatal(err)
	}

	// init a fresh chain manager with the correct expiring contract order
	db3, ts3, err := chain.NewDBStore(chain.NewMemDB(), n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm3 := chain.NewManager(db3, ts3, chain.WithExpiringContractOrder(map[types.BlockID][]types.FileContractID{
		expirationIndex.ID: {
			contractD,
			contractA,
		},
	}))
	// applying the blocks should succeed because the ordering is overwritten
	if err := cm3.AddBlocks(blocks); err != nil {
		t.Fatal(err)
	}

	cs1 := cm1.TipState()
	json1, err := json.Marshal(cs1)
	if err != nil {
		t.Fatal(err)
	}
	cs2 := cm2.TipState()
	json2, err := json.Marshal(cs2)
	if err != nil {
		t.Fatal(err)
	}
	cs3 := cm3.TipState()
	json3, err := json.Marshal(cs3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(json1, json2) {
		t.Log(string(json1))
		t.Log(string(json2))
		t.Fatal("expected the chain states to be equal after reorg")
	} else if !bytes.Equal(json1, json3) {
		t.Log(string(json1))
		t.Log(string(json3))
		t.Fatal("expected the chain states to be equal after reorg with new manager")
	}
}
