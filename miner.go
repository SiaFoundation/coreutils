package coreutils

import (
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

// FindBlockNonce attempts to find a nonce for b that meets the PoW target.
func FindBlockNonce(cs consensus.State, b *types.Block, timeout time.Duration) bool {
	bh := b.Header()
	bh.Nonce = 0
	factor := cs.NonceFactor()
	startBlock := time.Now()
	for bh.ID().CmpWork(cs.ChildTarget) < 0 {
		bh.Nonce += factor
		if time.Since(startBlock) > timeout {
			return false
		}
	}
	b.Nonce = bh.Nonce
	return true
}

// MineBlock constructs a block from the provided address and the transactions
// in the txpool, and attempts to find a nonce for it that meets the PoW target.
func MineBlock(cm *chain.Manager, addr types.Address, timeout time.Duration) (types.Block, bool) {
retry:
	cs := cm.TipState()
	txns := cm.PoolTransactions()
	v2Txns := cm.V2PoolTransactions()
	if cs.Index != cm.Tip() {
		goto retry
	}

	b := types.Block{
		ParentID:  cs.Index.ID,
		Timestamp: types.CurrentTimestamp(),
		MinerPayouts: []types.SiacoinOutput{{
			Value:   cs.BlockReward(),
			Address: addr,
		}},
	}

	childHeight := cs.Index.Height + 1
	if childHeight >= cs.Network.HardforkV2.AllowHeight {
		b.V2 = &types.V2BlockData{
			Height: childHeight,
		}
	}

	var weight uint64
	for _, txn := range txns {
		if weight += cs.TransactionWeight(txn); weight > cs.MaxBlockWeight() {
			break
		}
		b.Transactions = append(b.Transactions, txn)
		b.MinerPayouts[0].Value = b.MinerPayouts[0].Value.Add(txn.TotalFees())
	}
	if b.V2 != nil {
		for _, txn := range v2Txns {
			if weight += cs.V2TransactionWeight(txn); weight > cs.MaxBlockWeight() {
				break
			}
			b.V2.Transactions = append(b.V2.Transactions, txn)
			b.MinerPayouts[0].Value = b.MinerPayouts[0].Value.Add(txn.MinerFee)
		}
		b.V2.Commitment = cs.Commitment(cs.TransactionsCommitment(b.Transactions, b.V2Transactions()), addr)
	}
	found := FindBlockNonce(cs, &b, timeout)
	return b, found
}
