package testutil

import (
	"testing"
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
	n.BlockInterval = time.Second
	n.MaturityDelay = 5

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

// V2Network returns a test network and genesis block with early V2 hardforks
func V2Network() (*consensus.Network, types.Block) {
	// use a modified version of Zen
	n, genesisBlock := chain.TestnetZen()
	n.InitialTarget = types.BlockID{0xFF}
	n.BlockInterval = time.Second
	n.MaturityDelay = 5

	n.HardforkDevAddr.Height = 1
	n.HardforkTax.Height = 1
	n.HardforkStorageProof.Height = 1
	n.HardforkOak.Height = 1
	n.HardforkASIC.Height = 1
	n.HardforkFoundation.Height = 1
	n.HardforkV2.AllowHeight = 1
	n.HardforkV2.RequireHeight = 1
	return n, genesisBlock
}

// MineBlocks mines n blocks with the reward going to the given address.
func MineBlocks(tb testing.TB, cm *chain.Manager, addr types.Address, n int) {
	tb.Helper()

	for ; n > 0; n-- {
		b, ok := coreutils.MineBlock(cm, addr, time.Second)
		if !ok {
			tb.Fatal("failed to mine block")
		} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
			tb.Fatal(err)
		}
	}
}
