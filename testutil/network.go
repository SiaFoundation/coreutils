package testutil

import (
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
)

// Network returns a test network and genesis block.
func Network() (*consensus.Network, types.Block) {
	// use a modified version of Zen
	n, genesisBlock := chain.TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}
	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 200 // comfortably above MaturityHeight
	n.HardforkV2.RequireHeight = 250
	return n, genesisBlock
}

// MineBlock mines a block with the given transactions, transaction fees are
// added to the miner payout.
func MineBlock(cm *chain.Manager, minerAddress types.Address) types.Block {
	var minerFees types.Currency
	for _, txn := range cm.PoolTransactions() {
		minerFees = minerFees.Add(txn.TotalFees())
	}

	state := cm.TipState()
	b := types.Block{
		ParentID:     state.Index.ID,
		Timestamp:    types.CurrentTimestamp(),
		Transactions: cm.PoolTransactions(),
		MinerPayouts: []types.SiacoinOutput{{Address: minerAddress, Value: state.BlockReward().Add(minerFees)}},
	}
	if !coreutils.FindBlockNonce(state, &b, 5*time.Second) {
		panic("failed to find nonce")
	}
	return b
}
