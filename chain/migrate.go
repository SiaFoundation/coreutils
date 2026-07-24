package chain

import (
	"errors"
	"fmt"
	"math"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// A MigrationLogger logs the progress of a database migration.
type MigrationLogger interface {
	Printf(format string, v ...any)
	SetProgress(percentage float64)
}

type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}
func (noopLogger) SetProgress(float64)   {}

type zapMigrationLogger struct {
	lastProgressReport time.Time
	logger             *zap.Logger
}

// Printf logs a message with the current progress.
func (zl *zapMigrationLogger) Printf(format string, v ...any) {
	zl.logger.Info(fmt.Sprintf(format, v...))
}

// SetProgress updates the progress percentage and logs it if enough time has passed
// since the last report.
func (zl *zapMigrationLogger) SetProgress(percentage float64) {
	if time.Since(zl.lastProgressReport) < 30*time.Second {
		return
	}
	zl.logger.Info("migration progress", zap.Float64("progress", math.Round(percentage*10)/10))
	zl.lastProgressReport = time.Now()
}

// NewZapMigrationLogger creates a new MigrationLogger that uses zap for logging
// progress.
func NewZapMigrationLogger(log *zap.Logger) MigrationLogger {
	return &zapMigrationLogger{logger: log.Named("chainMigration")}
}

func migrateDB(dbs *DBStore, l MigrationLogger) error {
	scratch := dbs.scratchpad
	sp := scratch.sp
	version := readBucket(sp, bVersion).getRaw(bVersion)
	switch version[0] {
	case 1, 2, 3:
		l.Printf("Removing sidechain blocks")
		toDelete := make(map[types.BlockID]bool)
		for id := range sp.Bucket(bBlocks).Iter() {
			toDelete[(types.BlockID)(id)] = true
		}
		for _, id := range sp.Bucket(bMainChain).Iter() {
			if len(id) == 32 {
				delete(toDelete, (types.BlockID)(id))
			}
		}
		for id := range toDelete {
			scratch.bucket(bBlocks).delete(id[:])
			scratch.bucket(bStates).delete(id[:])
		}
		l.Printf("Removing block supplement data")
		for id := range sp.Bucket(bFileContractElements).Iter() {
			scratch.bucket(bFileContractElements).delete(id)
		}
		for id := range sp.Bucket(bSiacoinElements).Iter() {
			scratch.bucket(bSiacoinElements).delete(id)
		}
		for id := range sp.Bucket(bSiafundElements).Iter() {
			scratch.bucket(bSiafundElements).delete(id)
		}
		if scratch.shouldFlush() {
			if err := scratch.Flush(); err != nil {
				return err
			}
		}

		l.Printf("Recomputing main chain")
		n := dbs.n
		v1Blocks := min(getHeight(sp), n.HardforkV2.RequireHeight) + 1
		cs := n.GenesisState()
		for height := range v1Blocks {
			index, _ := bestIndex(sp, height)
			_, b, _, _ := getBlock(sp, index.ID)
			if b == nil {
				return errors.New("missing block needed for migration")
			}
			bs := supplementTipBlock(sp, n, *b)
			scratch.AddBlock(*b, &bs)
			// v2 blocks may be invalid
			if height >= n.HardforkV2.AllowHeight {
				if err := consensus.ValidateBlock(cs, *b, bs); err != nil && index.Height > 0 {
					l.Printf("Block %v is invalid (%v), removing it and all subsequent blocks", index, err)
					for ; height < v1Blocks; height++ {
						if index, ok := bestIndex(sp, height); ok {
							scratch.bucket(bBlocks).delete(index.ID[:])
							scratch.bucket(bStates).delete(index.ID[:])
							if scratch.shouldFlush() {
								if err := scratch.Flush(); err != nil {
									return err
								}
							}
						}
					}
					break
				}
			}
			var cau consensus.ApplyUpdate
			ancestorTimestamp, _ := ancestorTimestamp(sp, n, b.ParentID)
			cs, cau = consensus.ApplyBlock(cs, *b, bs, ancestorTimestamp)
			scratch.AddState(cs)
			scratch.ApplyBlock(cs, cau) // flushes as necessary
			l.SetProgress(99.9 * float64(height) / float64(v1Blocks))
		}
		if err := scratch.Flush(); err != nil {
			return err
		}
		scratch.bucket(bVersion).putRaw(bVersion, []byte{4})
		if err := scratch.Flush(); err != nil {
			return err
		}
		l.SetProgress(100)
		fallthrough
	case 4:
		// up-to-date
		return nil
	default:
		return fmt.Errorf("unrecognized version (%d)", version[0])
	}
}
