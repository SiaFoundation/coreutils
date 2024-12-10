package chain_test

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
)

func TestGetEmptyBlockID(t *testing.T) {
	n, genesisBlock := testutil.V2Network()
	store, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesisBlock)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)
	_, _ = cm.Block(types.BlockID{})
}
