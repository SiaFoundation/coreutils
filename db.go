package coreutils

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"time"

	"go.etcd.io/bbolt"
	"go.sia.tech/core/consensus"
	"go.sia.tech/coreutils/chain"
)

type boltBucket struct {
	*bbolt.Bucket
}

func (b boltBucket) Iter() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if !yield(k, v) {
				return
			}
		}
	}
}

// BoltChainDB implements chain.DB with a BoltDB database.
type BoltChainDB struct {
	db         *bbolt.DB
	scratchpad *boltDBScratchpad
}

// boltDBScratchpad implements chain.DBScratchpad. It lazily opens a bbolt
// write transaction, which accumulates writes until Flush commits it.
type boltDBScratchpad struct {
	db *bbolt.DB
	tx *bbolt.Tx
}

func (s *boltDBScratchpad) newTx() (err error) {
	if s.tx == nil {
		s.tx, err = s.db.Begin(true)
	}
	return
}

// Bucket implements chain.DBScratchpad.
func (s *boltDBScratchpad) Bucket(name []byte) chain.DBBucket {
	if err := s.newTx(); err != nil {
		panic(err)
	}
	b := s.tx.Bucket(name)
	if b == nil {
		return nil
	}
	return boltBucket{b}
}

// CreateBucket implements chain.DBScratchpad.
func (s *boltDBScratchpad) CreateBucket(name []byte) (chain.DBBucket, error) {
	if err := s.newTx(); err != nil {
		return nil, err
	}
	b, err := s.tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return boltBucket{b}, nil
}

// Flush implements chain.DBScratchpad.
func (s *boltDBScratchpad) Flush() error {
	if s.tx == nil {
		return nil
	}
	err := s.tx.Commit()
	s.tx = nil
	return err
}

// Cancel implements chain.DBScratchpad.
func (s *boltDBScratchpad) Cancel() {
	if s.tx == nil {
		return
	}
	s.tx.Rollback()
	s.tx = nil
}

// Scratchpad implements chain.DB.
func (db *BoltChainDB) Scratchpad() chain.DBScratchpad { return db.scratchpad }

type boltDBSnapshot struct {
	tx *bbolt.Tx
}

// Bucket implements chain.DBSnapshot.
func (v boltDBSnapshot) Bucket(name []byte) chain.DBBucket {
	b := v.tx.Bucket(name)
	if b == nil {
		return nil
	}
	return boltBucket{b}
}

func (v boltDBSnapshot) release() {
	v.tx.Rollback()
}

// Snapshot implements chain.DB.
func (db *BoltChainDB) Snapshot() (chain.DBSnapshot, func()) {
	tx, err := db.db.Begin(false)
	if err != nil {
		panic(err)
	}
	s := boltDBSnapshot{tx}
	return s, s.release
}

// Close flushes any pending writes and closes the BoltDB database.
func (db *BoltChainDB) Close() error {
	if err := db.scratchpad.Flush(); err != nil {
		return err
	}
	return db.db.Close()
}

// NewBoltChainDB creates a new BoltChainDB.
func NewBoltChainDB(db *bbolt.DB) *BoltChainDB {
	return &BoltChainDB{
		db:         db,
		scratchpad: &boltDBScratchpad{db: db},
	}
}

// OpenBoltChainDB opens a BoltDB database.
func OpenBoltChainDB(path string) (*BoltChainDB, error) {
	db, err := bbolt.Open(path, 0600, nil)
	return NewBoltChainDB(db), err
}

// PruneBoltChainDB rewrites the BoltDB chain database at path, retaining only
// the state needed to operate a node at its current tip; see chain.CopyPrunedDB
// for what is retained. The pruned database is written to a temporary file
// alongside the original, which it atomically replaces on success. On failure,
// the original is left untouched and the temporary file is removed. The
// database must not be open elsewhere. The provided logger may be nil.
func PruneBoltChainDB(path string, n *consensus.Network, height uint64, logger chain.MigrationLogger) error {
	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat database: %w", err)
	}
	src, err := bbolt.Open(path, stat.Mode(), &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer src.Close()

	tmpPath := path + ".tmp"
	if err := os.Remove(tmpPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("failed to remove stale temporary database: %w", err)
	}
	dst, err := bbolt.Open(tmpPath, 0600, &bbolt.Options{NoSync: true, Timeout: time.Second})
	if err != nil {
		return fmt.Errorf("failed to create temporary database: %w", err)
	}
	defer func() {
		dst.Close()
		os.Remove(tmpPath) // no-op if the atomic rename was successful
	}()

	if err := chain.CopyPrunedDB(NewBoltChainDB(src), NewBoltChainDB(dst), n, height, logger); err != nil {
		return err
	} else if err := dst.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary database: %w", err)
	} else if err := dst.Close(); err != nil {
		return fmt.Errorf("failed to close temporary database: %w", err)
	} else if err := src.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	} else if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to replace database: %w", err)
	}
	return nil
}
