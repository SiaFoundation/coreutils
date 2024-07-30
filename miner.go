package coreutils

import (
	"encoding/binary"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

// FindBlockNonce attempts to find a nonce for b that meets the PoW target.
func FindBlockNonce(cs consensus.State, b *types.Block, timeout time.Duration) bool {
	b.Nonce = 0
	buf := make([]byte, 32+8+8+32)
	binary.LittleEndian.PutUint64(buf[32:], b.Nonce)
	binary.LittleEndian.PutUint64(buf[40:], uint64(b.Timestamp.Unix()))
	if b.V2 != nil {
		copy(buf[:32], "sia/id/block|")
		copy(buf[48:], b.V2.Commitment[:])
	} else {
		root := b.MerkleRoot()
		copy(buf[:32], b.ParentID[:])
		copy(buf[48:], root[:])
	}
	factor := cs.NonceFactor()
	startBlock := time.Now()
	for types.BlockID(types.HashBytes(buf)).CmpWork(cs.ChildTarget) < 0 {
		b.Nonce += factor
		binary.LittleEndian.PutUint64(buf[32:], b.Nonce)
		if time.Since(startBlock) > timeout {
			return false
		}
	}
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

	if cs.Index.Height >= cs.Network.HardforkV2.AllowHeight {
		b.V2 = &types.V2BlockData{
			Height: cs.Index.Height + 1,
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
	for _, txn := range v2Txns {
		if weight += cs.V2TransactionWeight(txn); weight > cs.MaxBlockWeight() {
			break
		}
		b.V2.Transactions = append(b.V2.Transactions, txn)
		b.MinerPayouts[0].Value = b.MinerPayouts[0].Value.Add(txn.MinerFee)
	}
	if b.V2 != nil {
		b.V2.Commitment = cs.Commitment(cs.TransactionsCommitment(b.Transactions, b.V2Transactions()), addr)
	}
	found := FindBlockNonce(cs, &b, timeout)
	return b, found
}
