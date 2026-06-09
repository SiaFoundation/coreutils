package coreutils

import (
	"iter"

	"go.etcd.io/bbolt"
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
	tx *bbolt.Tx
	db *bbolt.DB
}

func (db *BoltChainDB) newTx() (err error) {
	if db.tx == nil {
		db.tx, err = db.db.Begin(true)
	}
	return
}

// Bucket implements chain.DB.
func (db *BoltChainDB) Bucket(name []byte) chain.DBBucket {
	if err := db.newTx(); err != nil {
		panic(err)
	}
	b := db.tx.Bucket(name)
	if b == nil {
		return nil
	}
	return boltBucket{b}
}

// CreateBucket implements chain.DB.
func (db *BoltChainDB) CreateBucket(name []byte) (chain.DBBucket, error) {
	if err := db.newTx(); err != nil {
		return nil, err
	}
	b, err := db.tx.CreateBucket(name)
	if err != nil {
		return nil, err
	}
	return boltBucket{b}, nil
}

// Flush implements chain.DB.
func (db *BoltChainDB) Flush() error {
	if db.tx == nil {
		return nil
	}
	err := db.tx.Commit()
	db.tx = nil
	return err
}

// Cancel implements chain.DB.
func (db *BoltChainDB) Cancel() {
	if db.tx == nil {
		return
	}
	db.tx.Rollback()
	db.tx = nil
}

// View implements chain.DB. It runs fn against a read-only bbolt transaction,
// which can execute concurrently with other View calls and with the open
// writer transaction. Writes performed via the DB passed to fn will fail.
func (db *BoltChainDB) View(fn func(chain.DB) error) error {
	return db.db.View(func(tx *bbolt.Tx) error {
		return fn(&boltViewDB{tx: tx})
	})
}

// boltViewDB is a read-only chain.DB backed by a bbolt read transaction.
type boltViewDB struct {
	tx *bbolt.Tx
}

func (db *boltViewDB) Bucket(name []byte) chain.DBBucket {
	b := db.tx.Bucket(name)
	if b == nil {
		return nil
	}
	return boltBucket{b}
}

func (db *boltViewDB) CreateBucket(_ []byte) (chain.DBBucket, error) {
	return nil, bbolt.ErrTxNotWritable
}

func (db *boltViewDB) Flush() error { return nil }
func (db *boltViewDB) Cancel()      {}

func (db *boltViewDB) View(fn func(chain.DB) error) error {
	return fn(db)
}

// Close closes the BoltDB database.
func (db *BoltChainDB) Close() error {
	db.Flush()
	return db.db.Close()
}

// NewBoltChainDB creates a new BoltChainDB.
func NewBoltChainDB(db *bbolt.DB) *BoltChainDB {
	return &BoltChainDB{db: db}
}

// OpenBoltChainDB opens a BoltDB database.
func OpenBoltChainDB(path string) (*BoltChainDB, error) {
	db, err := bbolt.Open(path, 0600, nil)
	return NewBoltChainDB(db), err
}
