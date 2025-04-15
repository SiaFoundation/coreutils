package chain

import (
	"errors"
	"fmt"
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
	zl.logger.Info("migration progress", zap.Float64("progress", percentage))
	zl.lastProgressReport = time.Now()
}

// NewZapMigrationLogger creates a new MigrationLogger that uses zap for logging progress.
func NewZapMigrationLogger(log *zap.Logger) MigrationLogger {
	return &zapMigrationLogger{logger: log.Named("chainMigration")}
}

func migrateDB(dbs *DBStore, n *consensus.Network, l MigrationLogger) error {
	version := dbs.bucket(bVersion).getRaw(bVersion)
	switch version[0] {
	case 1, 2, 3:
		l.Printf("Removing sidechain blocks")
		toDelete := make(map[types.BlockID]bool)
		for id := range dbs.db.Bucket(bBlocks).Iter() {
			toDelete[(types.BlockID)(id)] = true
		}
		for _, id := range dbs.db.Bucket(bMainChain).Iter() {
			if len(id) == 32 {
				delete(toDelete, (types.BlockID)(id))
			}
		}
		for id := range toDelete {
			dbs.bucket(bBlocks).delete(id[:])
			dbs.bucket(bStates).delete(id[:])
		}
		l.Printf("Removing block supplement data")
		for id := range dbs.db.Bucket(bFileContractElements).Iter() {
			dbs.bucket(bFileContractElements).delete(id)
		}
		for id := range dbs.db.Bucket(bSiacoinElements).Iter() {
			dbs.bucket(bSiacoinElements).delete(id)
		}
		for id := range dbs.db.Bucket(bSiafundElements).Iter() {
			dbs.bucket(bSiafundElements).delete(id)
		}
		l.Printf("Removing tree data")
		for k := range dbs.db.Bucket(bTree).Iter() {
			dbs.bucket(bTree).delete(k)
		}

		l.Printf("Recomputing main chain")
		v1Blocks := min(dbs.getHeight(), n.HardforkV2.RequireHeight) + 1
		cs := n.GenesisState()
		lastPrint := time.Now()
		for height := range v1Blocks {
			index, _ := dbs.BestIndex(height)
			_, b, _, _ := dbs.getBlock(index.ID)
			if b == nil {
				return errors.New("missing block needed for migration")
			}
			bs := dbs.SupplementTipBlock(*b)
			dbs.putBlock(b.Header(), b, &bs)
			if err := consensus.ValidateBlock(cs, *b, bs); err != nil && index.Height > 0 {
				l.Printf("Block %v is invalid (%v), removing it and all subsequent blocks", index, err)
				for ; height < v1Blocks; height++ {
					if index, ok := dbs.BestIndex(height); ok {
						dbs.bucket(bBlocks).delete(index.ID[:])
						dbs.bucket(bStates).delete(index.ID[:])
					}
					// deletes need to be flushed too
					if height%500 == 0 {
						if err := dbs.Flush(); err != nil {
							return err
						}
					}
				}
				break
			}
			var cau consensus.ApplyUpdate
			ancestorTimestamp, _ := dbs.AncestorTimestamp(b.ParentID)
			cs, cau = consensus.ApplyBlock(cs, *b, bs, ancestorTimestamp)
			dbs.putState(cs)
			dbs.ApplyBlock(cs, cau)
			if dbs.shouldFlush() {
				if err := dbs.Flush(); err != nil {
					return err
				}
			}
			if time.Since(lastPrint) > 100*time.Millisecond {
				l.SetProgress(99.9 * float64(height) / float64(v1Blocks))
				lastPrint = time.Now()
			}
		}
		dbs.bucket(bVersion).putRaw(bVersion, []byte{4})
		if err := dbs.Flush(); err != nil {
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
