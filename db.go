package coreutils

import (
	"go.etcd.io/bbolt"
	"go.sia.tech/core/chain"
)

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
	// NOTE: can't simply return db.tx.Bucket here, since it returns a concrete
	// type and we need a nil interface if the bucket does not exist
	b := db.tx.Bucket(name)
	if b == nil {
		return nil
	}
	return b
}

// CreateBucket implements chain.DB.
func (db *BoltChainDB) CreateBucket(name []byte) (chain.DBBucket, error) {
	if err := db.newTx(); err != nil {
		return nil, err
	}
	// NOTE: unlike Bucket, ok to return directly here, because caller should
	// always check err first
	return db.tx.CreateBucket(name)
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
