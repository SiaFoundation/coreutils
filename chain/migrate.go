package chain

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
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
	case 1:
		l.Printf("Migrating database from version 1 to 2")
		l.Printf("Computing new element tree")
		cs := n.GenesisState()
		v1Blocks := min(dbs.getHeight(), n.HardforkV2.RequireHeight)
		seen := make(map[[2]uint64]types.Hash256)
		var tree [][2][]byte
		flush := func() error {
			sort.Slice(tree, func(i, j int) bool {
				return bytes.Compare(tree[i][0], tree[j][0]) < 0
			})
			bucket := dbs.bucket(bTree)
			for _, kv := range tree {
				bucket.putRaw(kv[0], kv[1])
			}
			clear(seen)
			tree = tree[:0]
			return dbs.Flush()
		}
		lastPrint := time.Now()
		for height := range v1Blocks {
			index, ok0 := dbs.BestIndex(height)
			_, b, bs, ok1 := dbs.getBlock(index.ID)
			ancestorTimestamp, ok2 := dbs.AncestorTimestamp(b.ParentID)
			if !ok0 || !ok1 || !ok2 {
				return errors.New("database is corrupt")
			} else if b == nil || bs == nil {
				return errors.New("missing block needed for migration")
			}
			var cau consensus.ApplyUpdate
			cs, cau = consensus.ApplyBlock(cs, *b, *bs, ancestorTimestamp)
			cau.ForEachTreeNode(func(row, col uint64, h types.Hash256) {
				if _, ok := seen[[2]uint64{row, col}]; !ok {
					seen[[2]uint64{row, col}] = h
					tree = append(tree, [2][]byte{dbs.treeKey(row, col), h[:]})
				}
			})
			const maxTreeSize = 100e6 // use up to 100 MB of memory
			if len(tree)*(16+32) >= maxTreeSize {
				if err := flush(); err != nil {
					return err
				}
			}
			if time.Since(lastPrint) > 100*time.Millisecond {
				l.SetProgress(99.9 * float64(height) / float64(v1Blocks))
				lastPrint = time.Now()
			}
		}
		// final flush
		if err := flush(); err != nil {
			return err
		}
		dbs.bucket(bVersion).putRaw(bVersion, []byte{2})
		dbs.Flush()
		l.SetProgress(100)
		fallthrough
	case 2:
		l.Printf("Migrating database from version 2 to 3")
		l.Printf("Recomputing expiring file contracts")
		cs := n.GenesisState()
		v1Blocks := min(dbs.getHeight(), n.HardforkV2.RequireHeight)
		lastPrint := time.Now()
		seen := make(map[uint64]bool)
		for height := range v1Blocks {
			index, ok0 := dbs.BestIndex(height)
			_, b, bs, ok1 := dbs.getBlock(index.ID)
			ancestorTimestamp, ok2 := dbs.AncestorTimestamp(b.ParentID)
			if !ok0 || !ok1 || !ok2 {
				return errors.New("database is corrupt")
			} else if b == nil || bs == nil {
				return errors.New("missing block needed for migration")
			}
			var cau consensus.ApplyUpdate
			cs, cau = consensus.ApplyBlock(cs, *b, *bs, ancestorTimestamp)

			for _, fced := range cau.FileContractElementDiffs() {
				fce := &fced.FileContractElement
				// if this is the first time we've seen this expiration height,
				// clear the existing contents
				if windowEnd := fce.FileContract.WindowEnd; !seen[windowEnd] {
					dbs.bucket(bFileContractElements).putRaw(dbs.encHeight(windowEnd), nil)
					seen[windowEnd] = true
				}
				if fced.Created && fced.Resolved {
					continue
				} else if fced.Resolved {
					dbs.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
				} else if fced.Revision != nil {
					if fced.Revision.WindowEnd != fce.FileContract.WindowEnd {
						dbs.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
						dbs.putFileContractExpiration(fce.ID, fced.Revision.WindowEnd, true)
					}
				} else {
					dbs.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, true)
				}
			}

			if time.Since(lastPrint) > 100*time.Millisecond {
				l.SetProgress(99.9 * float64(height) / float64(v1Blocks))
				lastPrint = time.Now()
			}
		}
		dbs.bucket(bVersion).putRaw(bVersion, []byte{3})
		dbs.Flush()
		l.SetProgress(100)
		fallthrough
	case 3:
		// up-to-date
		return nil
	default:
		return fmt.Errorf("unrecognized version (%d)", version[0])
	}
}
