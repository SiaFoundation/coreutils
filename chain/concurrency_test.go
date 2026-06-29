package chain_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/testutil"
)

// TestManagerConcurrentReadsWrites hammers the Manager's read methods from many
// goroutines while a writer continuously appends blocks and periodically forces
// reorgs. It exercises the RWMutex + per-reader snapshot design and, in
// particular, the flush-before-unlock invariant: once a tip is observable, the
// block and state behind it must be committed, so any later snapshot can read
// them. If a writer ever released the lock without flushing (m.mu.Unlock
// instead of m.writeUnlock), a reader could observe an advanced tip whose block
// is missing from the snapshot it reads, and the assertions below would fail.
//
// It runs against both store backends. The bbolt backend is the one that
// actually exercises the invariant: its snapshots are isolated read
// transactions that do not observe uncommitted writes, whereas MemDB has no
// MVCC and reads pending writes regardless of flushing. Run with -race to also
// catch any cross-goroutine sharing of store state.
func TestManagerConcurrentReadsWrites(t *testing.T) {
	for _, tc := range []struct {
		name   string
		makeDB func(testing.TB) chain.DB
	}{
		{"MemDB", func(testing.TB) chain.DB { return chain.NewMemDB() }},
		{"BoltDB", func(tb testing.TB) chain.DB {
			db, err := coreutils.OpenBoltChainDB(filepath.Join(tb.TempDir(), "consensus.db"))
			if err != nil {
				tb.Fatal(err)
			}
			tb.Cleanup(func() { db.Close() })
			return db
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runConcurrentReadsWrites(t, tc.makeDB(t))
		})
	}
}

func runConcurrentReadsWrites(t *testing.T, db chain.DB) {
	n, genesis := testutil.V2Network()
	store, tipState, err := chain.NewDBStore(db, n, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(store, tipState)

	// give readers some history to traverse
	for i := 0; i < 20; i++ {
		b, ok := coreutils.MineBlock(cm, types.VoidAddress, time.Second)
		if !ok {
			t.Fatal("PoW failed")
		} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
			t.Fatal(err)
		}
	}

	const (
		totalBlocks   = 200
		readers       = 8
		reorgInterval = 20 // force a reorg roughly every N writer iterations
		reorgDepth    = 3  // blocks reverted per forced reorg
	)

	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) }

	var failOnce sync.Once
	var failure error
	fail := func(e error) {
		failOnce.Do(func() { failure = e })
		closeDone()
	}

	// forkReorg builds a competing chain reorgDepth+2 blocks long from an
	// ancestor reorgDepth back and submits it, forcing the Manager to revert
	// and reapply. The fork is mined on an independent store, so building it
	// never touches cm's store; only the final AddBlocks does.
	forkReorg := func() bool {
		tip := cm.Tip()
		if tip.Height <= reorgDepth {
			return false
		}
		aidx, ok := cm.BestIndex(tip.Height - reorgDepth)
		if !ok {
			return false
		}
		ab, ok := cm.Block(aidx.ID)
		if !ok {
			return false
		}
		parentState, ok := cm.State(ab.ParentID)
		if !ok {
			return false
		}
		fdb, ftip, err := chain.NewDBStoreAtCheckpoint(chain.NewMemDB(), parentState, ab, nil)
		if err != nil {
			fail(fmt.Errorf("fork: checkpoint init: %w", err))
			return false
		}
		fcm := chain.NewManager(fdb, ftip)
		fork := make([]types.Block, 0, reorgDepth+2)
		for j := 0; j < reorgDepth+2; j++ {
			b, ok := coreutils.MineBlock(fcm, types.VoidAddress, 5*time.Second)
			if !ok {
				fail(fmt.Errorf("fork: PoW failed"))
				return false
			} else if err := fcm.AddBlocks([]types.Block{b}); err != nil {
				fail(fmt.Errorf("fork: AddBlocks: %w", err))
				return false
			}
			fork = append(fork, b)
		}
		// the fork is strictly heavier (cm cannot advance while this single
		// writer builds it), so this triggers a reorg.
		if err := cm.AddBlocks(fork); err != nil {
			fail(fmt.Errorf("writer: reorg AddBlocks: %w", err))
			return false
		}
		return true
	}

	var wg sync.WaitGroup

	// writer: extend the chain, periodically forcing a reorg.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer closeDone()
		for i := 0; i < totalBlocks; i++ {
			if i > 0 && i%reorgInterval == 0 {
				if forkReorg() {
					continue
				}
				select {
				case <-done:
					return // forkReorg failed
				default:
				}
			}
			b, ok := coreutils.MineBlock(cm, types.VoidAddress, 5*time.Second)
			if !ok {
				fail(fmt.Errorf("writer: PoW failed at block %d", i))
				return
			} else if err := cm.AddBlocks([]types.Block{b}); err != nil {
				fail(fmt.Errorf("writer: AddBlocks failed at block %d: %w", i, err))
				return
			}
		}
	}()

	genesisIndex := types.ChainIndex{Height: 0, ID: genesis.ID()}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}

				// The core invariant, which survives reorgs: whatever tip the
				// Manager reports, that tip's block and state must be committed,
				// so a fresh snapshot can read them. This is exactly what
				// flush-before-unlock guarantees. (We can't assert
				// BestIndex(tip.Height) == tip here, because a reorg may land
				// between observing the tip and the lookup.)
				tip := cm.Tip()
				if b, ok := cm.Block(tip.ID); !ok {
					fail(fmt.Errorf("observed tip %v but its block is absent from the read snapshot", tip))
					return
				} else if b.ID() != tip.ID {
					fail(fmt.Errorf("Block(%v) returned block %v", tip.ID, b.ID()))
					return
				}
				if _, ok := cm.State(tip.ID); !ok {
					fail(fmt.Errorf("observed tip %v but its state is absent from the read snapshot", tip))
					return
				}

				// multi-lookup reads (each runs in a single snapshot): must
				// remain internally consistent and never error on a live best
				// chain, even while reorgs are happening.
				hist, err := cm.History()
				if err != nil {
					fail(fmt.Errorf("History: %w", err))
					return
				}
				if _, _, err := cm.BlocksForHistory(hist[:], 10); err != nil {
					fail(fmt.Errorf("BlocksForHistory: %w", err))
					return
				}
				if _, _, err := cm.Headers(genesisIndex, 100); err != nil {
					fail(fmt.Errorf("Headers: %w", err))
					return
				}
				if _, _, err := cm.UpdatesSince(types.ChainIndex{}, 10); err != nil {
					fail(fmt.Errorf("UpdatesSince: %w", err))
					return
				}
			}
		}()
	}

	wg.Wait()
	if failure != nil {
		t.Fatal(failure)
	}
}
