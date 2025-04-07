package chain

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

// A MigrationLogger logs the progress of a database migration.
type MigrationLogger interface {
	Printf(format string, v ...any)
	SetProgress(percentage float64)
}

type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}
func (noopLogger) SetProgress(float64)   {}

// MigrateDB upgrades the database to the latest version.
func MigrateDB(db DB, n *consensus.Network, l MigrationLogger) error {
	if db.Bucket(bVersion) == nil {
		return nil // nothing to migrate
	}
	dbs := &DBStore{
		db: db,
		n:  n,
	}

	version := dbs.bucket(bVersion).getRaw(bVersion)
	if version == nil {
		version = []byte{1}
	}
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
			return db.Flush()
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
		// up-to-date
		return nil
	default:
		return fmt.Errorf("unrecognized version (%d)", version[0])
	}
}
