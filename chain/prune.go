package chain

import (
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/consensus"
)

// CopyPrunedDB copies the blocks, states, and main chain indices from height
// through the tip of src into dst, along with the state at height-1. Nothing
// else is copied, matching the layout produced by NewDBStoreAtCheckpoint.
//
// dst must be empty. src must be a v4 database whose blocks at and above height
// have not been pruned. height must exceed the v2 require height by more than
// one. The provided logger may be nil.
func CopyPrunedDB(src, dst DB, n *consensus.Network, height uint64, logger MigrationLogger) (err error) {
	if logger == nil {
		logger = noopLogger{}
	}
	if err := sanityCheckNetwork(n); err != nil {
		return fmt.Errorf("invalid network: %w", err)
	}

	ss, release := src.Snapshot()
	defer release()

	if version := readBucket(ss, bVersion).getRaw(bVersion); len(version) != 1 {
		return errors.New("source database is not initialized")
	} else if version[0] != 4 {
		return fmt.Errorf("source database version (%d) must be migrated before pruning", version[0])
	} else if network := readBucket(ss, bNetwork).getRaw(bNetwork); string(network) != n.Name {
		return fmt.Errorf("source database initialized with different network (%s)", string(network))
	}
	tipHeight := getHeight(ss)
	if height > tipHeight {
		return fmt.Errorf("prune height (%d) exceeds tip height (%d)", height, tipHeight)
	} else if height == 0 || height-1 <= n.HardforkV2.RequireHeight {
		return fmt.Errorf("prune height (%d) must exceed the v2 require height (%d) by more than one", height, n.HardforkV2.RequireHeight)
	}

	sp := dst.Scratchpad()
	// the db helpers panic on failure; return an error instead
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic while copying database: %v", r)
		}
		if err != nil {
			sp.Cancel()
		}
	}()
	for _, bucket := range [][]byte{
		bVersion,
		bNetwork,
		bMainChain,
		bStates,
		bBlocks,
		bFileContractElements,
		bSiacoinElements,
		bSiafundElements,
		bTree,
	} {
		if _, err := sp.CreateBucket(bucket); err != nil {
			return fmt.Errorf("failed to create bucket %q: %w", bucket, err)
		}
	}
	scratch := &dbScratchpad{sp: sp, n: n, lastFlush: time.Now()}
	scratch.bucket(bVersion).putRaw(bVersion, []byte{4})
	scratch.bucket(bNetwork).putRaw(bNetwork, []byte(n.Name))
	scratch.bucket(bMainChain).putRaw(keyHeight, encHeight(tipHeight))

	// the parent state of the lowest retained block is needed to revert it
	index, ok := bestIndex(ss, height)
	if !ok {
		return fmt.Errorf("missing main chain index at height %d", height)
	}
	bh, ok := getBlockHeader(ss, index.ID)
	if !ok {
		return fmt.Errorf("missing block %v", index)
	}
	parentState := readBucket(ss, bStates).getRaw(bh.ParentID[:])
	if parentState == nil {
		return fmt.Errorf("missing state for block %v", bh.ParentID)
	}
	scratch.bucket(bStates).putRaw(bh.ParentID[:], parentState)

	logger.Printf("Copying blocks %d through %d", height, tipHeight)
	for h := height; h <= tipHeight; h++ {
		index, ok := bestIndex(ss, h)
		if !ok {
			return fmt.Errorf("missing main chain index at height %d", h)
		}
		state := readBucket(ss, bStates).getRaw(index.ID[:])
		if state == nil {
			return fmt.Errorf("missing state for block %v", index)
		}
		bh, b, bs, ok := getBlock(ss, index.ID)
		if !ok {
			return fmt.Errorf("missing block %v", index)
		} else if b == nil {
			return fmt.Errorf("block %v has already been pruned from the source", index)
		}
		scratch.bucket(bMainChain).put(encHeight(h), &index.ID)
		scratch.bucket(bStates).putRaw(index.ID[:], state)
		scratch.putBlock(bh, b, bs)
		if scratch.shouldFlush() {
			if err := scratch.Flush(); err != nil {
				return err
			}
		}
		logger.SetProgress(100 * float64(h-height+1) / float64(tipHeight-height+1))
	}
	if err := scratch.Flush(); err != nil {
		return err
	}
	logger.SetProgress(100)
	return nil
}
