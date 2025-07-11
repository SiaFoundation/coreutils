package chain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"math/bits"
	"sort"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

type supplementedBlock struct {
	Header     *types.BlockHeader
	Block      *types.Block
	Supplement *consensus.V1BlockSupplement
}

func (sb supplementedBlock) EncodeTo(e *types.Encoder) {
	e.WriteUint8(3)
	types.EncodePtr(e, sb.Header)
	types.EncodePtr(e, (*types.V2Block)(sb.Block))
	types.EncodePtr(e, sb.Supplement)
}

func (sb *supplementedBlock) DecodeFrom(d *types.Decoder) {
	switch v := d.ReadUint8(); v {
	case 2:
		sb.Header = nil
		sb.Block = new(types.Block)
		(*types.V2Block)(sb.Block).DecodeFrom(d)
		types.DecodePtr(d, &sb.Supplement)
	case 3:
		types.DecodePtr(d, &sb.Header)
		types.DecodePtrCast[types.V2Block](d, &sb.Block)
		types.DecodePtr(d, &sb.Supplement)
	default:
		d.SetErr(fmt.Errorf("incompatible version (%d)", v))
	}
}

type versionedState struct {
	State consensus.State
}

func (vs versionedState) EncodeTo(e *types.Encoder) {
	e.WriteUint8(2)
	vs.State.EncodeTo(e)
}

func (vs *versionedState) DecodeFrom(d *types.Decoder) {
	if v := d.ReadUint8(); v != 2 {
		d.SetErr(fmt.Errorf("incompatible version (%d)", v))
	}
	vs.State.DecodeFrom(d)
}

// A DB is a generic key-value database.
type DB interface {
	Bucket(name []byte) DBBucket
	CreateBucket(name []byte) (DBBucket, error)
	Flush() error
	Cancel()
}

// A DBBucket is a set of key-value pairs.
type DBBucket interface {
	Get(key []byte) []byte
	Put(key, value []byte) error
	Delete(key []byte) error
	Iter() iter.Seq2[[]byte, []byte]
}

// MemDB implements DB with an in-memory map.
type MemDB struct {
	buckets map[string]map[string][]byte
	puts    map[string]map[string][]byte
	dels    map[string]map[string]struct{}
}

// Flush implements DB.
func (db *MemDB) Flush() error {
	for bucket, puts := range db.puts {
		if db.buckets[bucket] == nil {
			db.buckets[bucket] = make(map[string][]byte)
		}
		for key, val := range puts {
			db.buckets[bucket][key] = val
		}
		delete(db.puts, bucket)
	}
	for bucket, dels := range db.dels {
		if db.buckets[bucket] == nil {
			db.buckets[bucket] = make(map[string][]byte)
		}
		for key := range dels {
			delete(db.buckets[bucket], key)
		}
		delete(db.dels, bucket)
	}
	return nil
}

// Cancel implements DB.
func (db *MemDB) Cancel() {
	for k := range db.puts {
		delete(db.puts, k)
	}
	for k := range db.dels {
		delete(db.dels, k)
	}
}

func (db *MemDB) get(bucket string, key []byte) []byte {
	if val, ok := db.puts[bucket][string(key)]; ok {
		return val
	} else if _, ok := db.dels[bucket][string(key)]; ok {
		return nil
	}
	return db.buckets[bucket][string(key)]
}

func (db *MemDB) put(bucket string, key, value []byte) error {
	if db.puts[bucket] == nil {
		if db.buckets[bucket] == nil {
			return errors.New("bucket does not exist")
		}
		db.puts[bucket] = make(map[string][]byte)
	}
	db.puts[bucket][string(key)] = value
	delete(db.dels[bucket], string(key))
	return nil
}

func (db *MemDB) delete(bucket string, key []byte) error {
	if db.dels[bucket] == nil {
		if db.buckets[bucket] == nil {
			return errors.New("bucket does not exist")
		}
		db.dels[bucket] = make(map[string]struct{})
	}
	db.dels[bucket][string(key)] = struct{}{}
	delete(db.puts[bucket], string(key))
	return nil
}

// Bucket implements DB.
func (db *MemDB) Bucket(name []byte) DBBucket {
	if db.buckets[string(name)] == nil &&
		db.puts[string(name)] == nil &&
		db.dels[string(name)] == nil {
		return nil
	}
	return memBucket{string(name), db}
}

// CreateBucket implements DB.
func (db *MemDB) CreateBucket(name []byte) (DBBucket, error) {
	if db.buckets[string(name)] != nil {
		return nil, errors.New("bucket already exists")
	}
	db.puts[string(name)] = make(map[string][]byte)
	db.dels[string(name)] = make(map[string]struct{})
	return db.Bucket(name), nil
}

type memBucket struct {
	name string
	db   *MemDB
}

func (b memBucket) Get(key []byte) []byte       { return b.db.get(b.name, key) }
func (b memBucket) Put(key, value []byte) error { return b.db.put(b.name, key, value) }
func (b memBucket) Delete(key []byte) error     { return b.db.delete(b.name, key) }
func (b memBucket) Iter() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for key, val := range b.db.buckets[b.name] {
			if pval, ok := b.db.puts[b.name][string(key)]; ok {
				val = pval
			} else if _, ok := b.db.dels[b.name][string(key)]; ok {
				continue
			}
			if !yield([]byte(key), val) {
				return
			}
		}
	}
}

// NewMemDB returns an in-memory DB for use with DBStore.
func NewMemDB() *MemDB {
	return &MemDB{
		buckets: make(map[string]map[string][]byte),
		puts:    make(map[string]map[string][]byte),
		dels:    make(map[string]map[string]struct{}),
	}
}

type cacheBucket struct {
	mb memBucket
	db DBBucket
}

func (b cacheBucket) Get(key []byte) []byte {
	if val := b.mb.Get(key); val != nil {
		return val
	}
	return b.db.Get(key)
}

func (b cacheBucket) Put(key, value []byte) error {
	err := b.mb.Put(key, value)
	return err
}

func (b cacheBucket) Delete(key []byte) error {
	return b.mb.Delete(key)
}

func (b cacheBucket) Iter() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for k, v := range b.mb.Iter() {
			if !yield(k, v) {
				return
			}
		}
		for k, v := range b.db.Iter() {
			_, put := b.mb.db.puts[b.mb.name][string(k)]
			_, deleted := b.mb.db.dels[b.mb.name][string(k)]
			if put || deleted {
				continue
			} else if !yield(k, v) {
				return
			}
		}
	}
}

// A CacheDB caches DB writes in memory and sorts them before flushing to the
// underlying DB. This can greatly improve performance for certain databases.
type CacheDB struct {
	mem *MemDB
	db  DB
	kvs map[string][][2][]byte
}

// Bucket implements DB.
func (db *CacheDB) Bucket(name []byte) DBBucket {
	b := db.db.Bucket(name)
	if b == nil {
		return nil
	} else if db.mem.Bucket(name) == nil {
		db.mem.CreateBucket(name)
	}
	return cacheBucket{memBucket{string(name), db.mem}, b}
}

// CreateBucket implements DB.
func (db *CacheDB) CreateBucket(name []byte) (DBBucket, error) {
	if _, err := db.db.CreateBucket(name); err != nil {
		return nil, err
	}
	return db.mem.CreateBucket(name)
}

// Flush implements DB.
func (db *CacheDB) Flush() error {
	// puts
	for name, puts := range db.mem.puts {
		bucket := db.kvs[name]
		for key, val := range puts {
			if _, ok := db.mem.dels[name][key]; ok {
				continue
			}
			bucket = append(bucket, [2][]byte{[]byte(key), val})
		}
		db.kvs[name] = bucket
	}
	for bucket, kvs := range db.kvs {
		bucket := db.db.Bucket([]byte(bucket))
		sort.Slice(kvs, func(i, j int) bool {
			return bytes.Compare(kvs[i][0], kvs[j][0]) < 0
		})
		for _, kv := range kvs {
			bucket.Put(kv[0], kv[1])
		}
	}
	// remove references
	for name := range db.kvs {
		clear(db.kvs[name])
		db.kvs[name] = db.kvs[name][:0]
	}

	// dels
	for name, dels := range db.mem.dels {
		bucket := db.kvs[name]
		for key := range dels {
			bucket = append(bucket, [2][]byte{[]byte(key), nil})
		}
		db.kvs[name] = bucket
	}
	for name, kvs := range db.kvs {
		bucket := db.db.Bucket([]byte(name))
		sort.Slice(kvs, func(i, j int) bool {
			return bytes.Compare(kvs[i][0], kvs[j][0]) < 0
		})
		for _, kv := range kvs {
			bucket.Delete(kv[0])
		}
	}
	for name := range db.kvs {
		clear(db.kvs[name])
		db.kvs[name] = db.kvs[name][:0]
	}

	// clear MemDB
	for _, bucket := range db.mem.buckets {
		clear(bucket)
	}
	for _, bucket := range db.mem.puts {
		clear(bucket)
	}
	for _, bucket := range db.mem.dels {
		clear(bucket)
	}
	return db.db.Flush()
}

// Cancel implements DB.
func (db *CacheDB) Cancel() {
	db.mem.Cancel()
	db.db.Cancel()
}

// NewCacheDB returns a new CacheDB that wraps the given DB.
func NewCacheDB(db DB) DB {
	return &CacheDB{
		mem: NewMemDB(),
		db:  db,
		kvs: make(map[string][][2][]byte),
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// dbBucket is a helper type for implementing Store.
type dbBucket struct {
	b  DBBucket
	db *DBStore
}

func (b *dbBucket) getRaw(key []byte) []byte {
	if b.b == nil {
		return nil
	}
	return b.b.Get(key)
}

func (b *dbBucket) get(key []byte, v types.DecoderFrom) bool {
	val := b.getRaw(key)
	if val == nil {
		return false
	}
	d := types.NewBufDecoder(val)
	v.DecodeFrom(d)
	if d.Err() != nil {
		check(fmt.Errorf("error decoding %T: %w", v, d.Err()))
		return false
	}
	return true
}

func (b *dbBucket) putRaw(key, value []byte) {
	check(b.b.Put(key, value))
	b.db.unflushed += len(value)
}

func (b *dbBucket) put(key []byte, v types.EncoderTo) {
	var buf bytes.Buffer
	b.db.enc.Reset(&buf)
	v.EncodeTo(&b.db.enc)
	b.db.enc.Flush()
	b.putRaw(key, buf.Bytes())
}

func (b *dbBucket) delete(key []byte) {
	check(b.b.Delete(key))
	b.db.unflushed += len(key)
}

var (
	bVersion              = []byte("Version")
	bMainChain            = []byte("MainChain")
	bStates               = []byte("States")
	bBlocks               = []byte("Blocks")
	bFileContractElements = []byte("FileContracts")
	bSiacoinElements      = []byte("SiacoinElements")
	bSiafundElements      = []byte("SiafundElements")
	bTree                 = []byte("Tree")

	keyHeight = []byte("Height")
)

// DBStore implements Store using a key-value database.
type DBStore struct {
	db  DB
	n   *consensus.Network // for getState
	enc types.Encoder

	unflushed int
	lastFlush time.Time
}

func (db *DBStore) bucket(name []byte) *dbBucket {
	return &dbBucket{db.db.Bucket(name), db}
}

func (db *DBStore) encHeight(height uint64) []byte {
	var buf [8]byte
	return binary.BigEndian.AppendUint64(buf[:0], height)
}

func (db *DBStore) putBestIndex(index types.ChainIndex) {
	db.bucket(bMainChain).put(db.encHeight(index.Height), &index.ID)
}

func (db *DBStore) deleteBestIndex(height uint64) {
	db.bucket(bMainChain).delete(db.encHeight(height))
}

func (db *DBStore) getHeight() (height uint64) {
	if val := db.bucket(bMainChain).getRaw(keyHeight); len(val) == 8 {
		height = binary.BigEndian.Uint64(val)
	}
	return
}

func (db *DBStore) putHeight(height uint64) {
	db.bucket(bMainChain).putRaw(keyHeight, db.encHeight(height))
}

func (db *DBStore) getState(id types.BlockID) (consensus.State, bool) {
	var vs versionedState
	ok := db.bucket(bStates).get(id[:], &vs)
	vs.State.Network = db.n
	return vs.State, ok
}

func (db *DBStore) putState(cs consensus.State) {
	db.bucket(bStates).put(cs.Index.ID[:], versionedState{cs})
}

func (db *DBStore) getBlock(id types.BlockID) (bh types.BlockHeader, b *types.Block, bs *consensus.V1BlockSupplement, _ bool) {
	var sb supplementedBlock
	if ok := db.bucket(bBlocks).get(id[:], &sb); !ok {
		return types.BlockHeader{}, nil, nil, false
	} else if sb.Header == nil {
		sb.Header = new(types.BlockHeader)
		*sb.Header = sb.Block.Header()
	}
	return *sb.Header, sb.Block, sb.Supplement, true
}

func (db *DBStore) putBlock(bh types.BlockHeader, b *types.Block, bs *consensus.V1BlockSupplement) {
	id := bh.ID()
	db.bucket(bBlocks).put(id[:], supplementedBlock{&bh, b, bs})
}

func (db *DBStore) getAncestorInfo(id types.BlockID) (parentID types.BlockID, timestamp time.Time, ok bool) {
	ok = db.bucket(bBlocks).get(id[:], types.DecoderFunc(func(d *types.Decoder) {
		v := d.ReadUint8()
		if v != 2 && v != 3 {
			d.SetErr(fmt.Errorf("incompatible version (%d)", v))
		}
		// kinda cursed; don't worry about it
		if v == 3 {
			if !d.ReadBool() {
				d.ReadBool()
			}
		}
		parentID.DecodeFrom(d)
		_ = d.ReadUint64() // nonce
		timestamp = d.ReadTime()
	}))
	return
}

func (db *DBStore) getBlockHeader(id types.BlockID) (bh types.BlockHeader, ok bool) {
	ok = db.bucket(bBlocks).get(id[:], types.DecoderFunc(func(d *types.Decoder) {
		v := d.ReadUint8()
		if v != 2 && v != 3 {
			d.SetErr(fmt.Errorf("incompatible version (%d)", v))
			return
		}
		if v == 3 {
			var bhp *types.BlockHeader
			types.DecodePtr(d, &bhp)
			if bhp != nil {
				bh = *bhp
				return
			} else if !d.ReadBool() {
				d.SetErr(errors.New("neither header nor block present"))
				return
			}
		}
		var b types.Block
		(*types.V2Block)(&b).DecodeFrom(d)
		bh = b.Header()
	}))
	return
}

func (db *DBStore) treeKey(row, col uint64) []byte {
	// If we assume that the total number of elements is less than 2^32, we can
	// pack row and col into one uint32 key. We do this by setting the top 'row'
	// bits of 'col' to 1. Since each successive row has half as many columns,
	// we never have to worry about clobbering any bits of 'col'.
	var buf [4]byte
	return binary.BigEndian.AppendUint32(buf[:0], uint32(((1<<row)-1)<<(32-row)|col))
}

func (db *DBStore) getElementProof(leafIndex, numLeaves uint64) (proof []types.Hash256) {
	if leafIndex >= numLeaves {
		panic(fmt.Sprintf("leafIndex %v exceeds accumulator size %v", leafIndex, numLeaves)) // should never happen
	}
	// The size of the proof is the mergeHeight of leafIndex and numLeaves. To
	// see why, imagine a tree large enough to contain both leafIndex and
	// numLeaves within the same subtree; the height at which the paths to those
	// leaves diverge must be the size of the subtree containing leafIndex in
	// the actual tree.
	proof = make([]types.Hash256, bits.Len64(leafIndex^numLeaves)-1)
	for i := range proof {
		row, col := uint64(i), (leafIndex>>i)^1
		if !db.bucket(bTree).get(db.treeKey(row, col), &proof[i]) {
			panic(fmt.Sprintf("missing proof element %v for leaf %v", i, leafIndex))
		}
	}
	return
}

func (db *DBStore) getSiacoinElement(id types.SiacoinOutputID, numLeaves uint64) (sce types.SiacoinElement, ok bool) {
	ok = db.bucket(bSiacoinElements).get(id[:], &sce)
	if ok {
		sce.StateElement.MerkleProof = db.getElementProof(sce.StateElement.LeafIndex, numLeaves)
	}
	return
}

func (db *DBStore) putSiacoinElement(sce types.SiacoinElement) {
	sce.StateElement.MerkleProof = nil
	db.bucket(bSiacoinElements).put(sce.ID[:], sce.Share())
}

func (db *DBStore) deleteSiacoinElement(id types.SiacoinOutputID) {
	db.bucket(bSiacoinElements).delete(id[:])
}

func (db *DBStore) getSiafundElement(id types.SiafundOutputID, numLeaves uint64) (sfe types.SiafundElement, ok bool) {
	ok = db.bucket(bSiafundElements).get(id[:], &sfe)
	if ok {
		sfe.StateElement.MerkleProof = db.getElementProof(sfe.StateElement.LeafIndex, numLeaves)
	}
	return
}

func (db *DBStore) putSiafundElement(sfe types.SiafundElement) {
	sfe.StateElement.MerkleProof = nil
	db.bucket(bSiafundElements).put(sfe.ID[:], sfe.Share())
}

func (db *DBStore) deleteSiafundElement(id types.SiafundOutputID) {
	db.bucket(bSiafundElements).delete(id[:])
}

func (db *DBStore) getFileContractElement(id types.FileContractID, numLeaves uint64) (fce types.FileContractElement, ok bool) {
	ok = db.bucket(bFileContractElements).get(id[:], &fce)
	if ok {
		fce.StateElement.MerkleProof = db.getElementProof(fce.StateElement.LeafIndex, numLeaves)
	}
	return
}

func (db *DBStore) putFileContractElement(fce types.FileContractElement) {
	fce.StateElement.MerkleProof = nil
	db.bucket(bFileContractElements).put(fce.ID[:], fce.Share())
}

func (db *DBStore) deleteFileContractElement(id types.FileContractID) {
	db.bucket(bFileContractElements).delete(id[:])
}

func (db *DBStore) putFileContractExpiration(id types.FileContractID, windowEnd uint64, apply bool) {
	b := db.bucket(bFileContractElements)
	key := db.encHeight(windowEnd)
	// When applying, we append; when reverting, we prepend. This ensures that
	// the order of the IDs -- and consequently, the ExpiringFileContracts in a
	// V1BlockSupplement -- remain stable across reorgs. Without this adjustment,
	// a reorg could change the order in which missed proof outputs are added to
	// the element tree, giving them different leaf indices.
	if apply {
		b.putRaw(key, append(b.getRaw(key), id[:]...))
	} else {
		b.putRaw(key, append(id[:], b.getRaw(key)...))
	}
}

// ExpiringFileContractIDs returns the expiring file contract IDs at the given height.
func (db *DBStore) ExpiringFileContractIDs(height uint64) []types.FileContractID {
	buf := db.bucket(bFileContractElements).getRaw(db.encHeight(height))
	ids := make([]types.FileContractID, 0, len(buf)/32)
	for i := 0; i < len(buf); i += 32 {
		ids = append(ids, (types.FileContractID)(buf[i:]))
	}
	return ids
}

// OverwriteExpiringFileContractIDs overwrites the expiring file contract IDs at the given height.
// This should not be called unless the IDs are known to be correct, as it will overwrite
// any existing IDs at that height.
func (db *DBStore) OverwriteExpiringFileContractIDs(height uint64, ids []types.FileContractID) {
	buf := make([]byte, len(ids)*32)
	for i, id := range ids {
		copy(buf[i*32:], id[:])
	}
	db.bucket(bFileContractElements).putRaw(db.encHeight(height), buf)
}

func (db *DBStore) deleteFileContractExpiration(id types.FileContractID, windowEnd uint64) {
	b := db.bucket(bFileContractElements)
	key := db.encHeight(windowEnd)
	val := append([]byte(nil), b.getRaw(key)...)
	for i := 0; i < len(val); i += 32 {
		if *(*types.FileContractID)(val[i:]) == id {
			copy(val[i:], val[len(val)-32:])
			val = val[:len(val)-32]
			i -= 32
			b.putRaw(key, val)
			return
		}
	}
	panic("missing file contract expiration")
}

func (db *DBStore) applyState(next consensus.State) {
	db.putBestIndex(next.Index)
	db.putHeight(next.Index.Height)
}

func (db *DBStore) revertState(prev consensus.State) {
	db.deleteBestIndex(prev.Index.Height + 1)
	db.putHeight(prev.Index.Height)
}

func (db *DBStore) applyElements(cau consensus.ApplyUpdate) {
	cau.ForEachTreeNode(func(row, col uint64, h types.Hash256) {
		db.bucket(bTree).putRaw(db.treeKey(row, col), h[:])
	})

	for _, sced := range cau.SiacoinElementDiffs() {
		if sced.Created && sced.Spent {
			continue // ephemeral
		} else if sced.Spent {
			db.deleteSiacoinElement(sced.SiacoinElement.ID)
		} else {
			db.putSiacoinElement(sced.SiacoinElement.Share())
		}
	}
	for _, sfed := range cau.SiafundElementDiffs() {
		if sfed.Created && sfed.Spent {
			continue // ephemeral
		} else if sfed.Spent {
			db.deleteSiafundElement(sfed.SiafundElement.ID)
		} else {
			db.putSiafundElement(sfed.SiafundElement.Share())
		}
	}
	for _, fced := range cau.FileContractElementDiffs() {
		fce := &fced.FileContractElement
		if fced.Created && fced.Resolved {
			continue
		} else if fced.Resolved {
			db.deleteFileContractElement(fce.ID)
			db.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
		} else if fced.Revision != nil {
			rev := fce.Share()
			rev.FileContract = *fced.Revision
			db.putFileContractElement(rev.Share())
			if rev.FileContract.WindowEnd != fce.FileContract.WindowEnd {
				db.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
				db.putFileContractExpiration(fce.ID, rev.FileContract.WindowEnd, true)
			}
		} else {
			db.putFileContractElement(fce.Share())
			db.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, true)
		}
	}
}

func (db *DBStore) revertElements(cru consensus.RevertUpdate) {
	for _, fced := range cru.FileContractElementDiffs() {
		fce := &fced.FileContractElement
		if fced.Created && fced.Resolved {
			continue
		} else if fced.Resolved {
			// contract no longer resolved; restore it
			db.putFileContractElement(fce.Share())
			db.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, false)
		} else if fced.Revision != nil {
			// contract no longer revised; restore prior revision
			rev := fce.Share()
			rev.FileContract = *fced.Revision
			db.putFileContractElement(fce.Share())
			if rev.FileContract.WindowEnd != fce.FileContract.WindowEnd {
				db.deleteFileContractExpiration(fce.ID, rev.FileContract.WindowEnd)
				db.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, false)
			}
		} else {
			// contract no longer exists; delete it
			db.deleteFileContractElement(fce.ID)
			db.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
		}
	}

	for _, sfed := range cru.SiafundElementDiffs() {
		if sfed.Created && sfed.Spent {
			continue // ephemeral
		} else if sfed.Spent {
			// output no longer spent; restore it
			db.putSiafundElement(sfed.SiafundElement.Share())
		} else {
			// output no longer exists; delete it
			db.deleteSiafundElement(sfed.SiafundElement.ID)
		}
	}
	for _, sced := range cru.SiacoinElementDiffs() {
		if sced.Created && sced.Spent {
			continue // ephemeral
		} else if sced.Spent {
			// output no longer spent; restore it
			db.putSiacoinElement(sced.SiacoinElement.Share())
		} else {
			// output no longer exists; delete it
			db.deleteSiacoinElement(sced.SiacoinElement.ID)
		}
	}

	cru.ForEachTreeNode(func(row, col uint64, h types.Hash256) {
		db.bucket(bTree).putRaw(db.treeKey(row, col), h[:])
	})

	// NOTE: Although the element tree has shrunk, we do not need to explicitly
	// delete any nodes; getElementProof always stops at the correct height for
	// the given tree size, so the no-longer-valid nodes are simply never
	// accessed. (They will continue to occupy storage, but this storage will
	// inevitably be overwritten by future nodes, so there is little reason to
	// reclaim it immediately.)
}

// BestIndex implements Store.
func (db *DBStore) BestIndex(height uint64) (index types.ChainIndex, ok bool) {
	index.Height = height
	ok = db.bucket(bMainChain).get(db.encHeight(height), &index.ID)
	return
}

// SupplementTipTransaction implements Store.
func (db *DBStore) SupplementTipTransaction(txn types.Transaction) (ts consensus.V1TransactionSupplement) {
	height := db.getHeight()
	if height >= db.n.HardforkV2.RequireHeight {
		return consensus.V1TransactionSupplement{}
	}
	// get tip state, for proof-trimming
	index, _ := db.BestIndex(height)
	cs, _ := db.State(index.ID)
	numLeaves := cs.Elements.NumLeaves

	for _, sci := range txn.SiacoinInputs {
		if sce, ok := db.getSiacoinElement(sci.ParentID, numLeaves); ok {
			ts.SiacoinInputs = append(ts.SiacoinInputs, sce.Move())
		}
	}
	for _, sfi := range txn.SiafundInputs {
		if sfe, ok := db.getSiafundElement(sfi.ParentID, numLeaves); ok {
			ts.SiafundInputs = append(ts.SiafundInputs, sfe.Move())
		}
	}
	for _, fcr := range txn.FileContractRevisions {
		if fce, ok := db.getFileContractElement(fcr.ParentID, numLeaves); ok {
			ts.RevisedFileContracts = append(ts.RevisedFileContracts, fce.Move())
		}
	}
	for _, sp := range txn.StorageProofs {
		if fce, ok := db.getFileContractElement(sp.ParentID, numLeaves); ok {
			if windowIndex, ok := db.BestIndex(fce.FileContract.WindowStart - 1); ok {
				ts.StorageProofs = append(ts.StorageProofs, consensus.V1StorageProofSupplement{
					FileContract: fce.Move(),
					WindowID:     windowIndex.ID,
				})
			}
		}
	}
	return
}

// SupplementTipBlock implements Store.
func (db *DBStore) SupplementTipBlock(b types.Block) (bs consensus.V1BlockSupplement) {
	height := db.getHeight()
	if height >= db.n.HardforkV2.RequireHeight {
		return consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, len(b.Transactions))}
	}

	// get tip state, for proof-trimming
	index, _ := db.BestIndex(height)
	cs, _ := db.State(index.ID)
	numLeaves := cs.Elements.NumLeaves

	bs = consensus.V1BlockSupplement{
		Transactions: make([]consensus.V1TransactionSupplement, len(b.Transactions)),
	}
	for i, txn := range b.Transactions {
		bs.Transactions[i] = db.SupplementTipTransaction(txn)
	}
	ids := db.bucket(bFileContractElements).getRaw(db.encHeight(db.getHeight() + 1))
	for i := 0; i < len(ids); i += 32 {
		fce, ok := db.getFileContractElement(*(*types.FileContractID)(ids[i:]), numLeaves)
		if !ok {
			panic("missing FileContractElement")
		}
		bs.ExpiringFileContracts = append(bs.ExpiringFileContracts, fce.Move())
	}
	return bs
}

// AncestorTimestamp implements Store.
func (db *DBStore) AncestorTimestamp(id types.BlockID) (t time.Time, ok bool) {
	cs, _ := db.State(id)
	if cs.Index.Height > db.n.HardforkOak.Height {
		return time.Time{}, true
	}

	getBestID := func(height uint64) (id types.BlockID) {
		db.bucket(bMainChain).get(db.encHeight(height), &id)
		return
	}
	ancestorID := id
	for i := uint64(0); i < cs.AncestorDepth() && i < cs.Index.Height; i++ {
		// if we're on the best path, we can jump to the n'th block directly
		if ancestorID == getBestID(cs.Index.Height-i) {
			ancestorID = getBestID(cs.Index.Height - cs.AncestorDepth())
			if cs.Index.Height < cs.AncestorDepth() {
				ancestorID = getBestID(0)
			}
			break
		}
		ancestorID, _, _ = db.getAncestorInfo(ancestorID)
	}
	_, t, ok = db.getAncestorInfo(ancestorID)
	return
}

// State implements Store.
func (db *DBStore) State(id types.BlockID) (consensus.State, bool) {
	return db.getState(id)
}

// AddState implements Store.
func (db *DBStore) AddState(cs consensus.State) {
	db.putState(cs)
}

// Block implements Store.
func (db *DBStore) Block(id types.BlockID) (types.Block, *consensus.V1BlockSupplement, bool) {
	_, b, bs, ok := db.getBlock(id)
	if !ok || b == nil {
		return types.Block{}, nil, false
	}
	return *b, bs, ok
}

// AddBlock implements Store.
func (db *DBStore) AddBlock(b types.Block, bs *consensus.V1BlockSupplement) {
	db.putBlock(b.Header(), &b, bs)
}

// PruneBlock implements Store.
func (db *DBStore) PruneBlock(id types.BlockID) {
	if bh, _, _, ok := db.getBlock(id); ok {
		db.putBlock(bh, nil, nil)
	}
}

// Header implements Store.
func (db *DBStore) Header(id types.BlockID) (bh types.BlockHeader, exists bool) {
	return db.getBlockHeader(id)
}

func (db *DBStore) shouldFlush() bool {
	// NOTE: these values were chosen empirically and should constitute a
	// sensible default; if necessary, we can make them configurable
	const flushSizeThreshold = 100e6
	const flushDurationThreshold = 5 * time.Second
	return db.unflushed >= flushSizeThreshold || time.Since(db.lastFlush) >= flushDurationThreshold
}

// ApplyBlock implements Store.
func (db *DBStore) ApplyBlock(s consensus.State, cau consensus.ApplyUpdate) {
	db.applyState(s)
	if s.Index.Height <= db.n.HardforkV2.RequireHeight {
		db.applyElements(cau)
	}
	if db.shouldFlush() {
		if err := db.Flush(); err != nil {
			panic(err)
		}
	}
}

// RevertBlock implements Store.
func (db *DBStore) RevertBlock(s consensus.State, cru consensus.RevertUpdate) {
	if s.Index.Height <= db.n.HardforkV2.RequireHeight {
		db.revertElements(cru)
	}
	db.revertState(s)
	if db.shouldFlush() {
		if err := db.Flush(); err != nil {
			panic(err)
		}
	}
}

// Flush flushes any uncommitted data to the underlying DB.
func (db *DBStore) Flush() error {
	if db.unflushed == 0 {
		return nil
	}
	err := db.db.Flush()
	db.unflushed = 0
	db.lastFlush = time.Now()
	return err
}

// NewDBStore creates a new DBStore using the provided database. The tip state
// is also returned. The DB will be automatically migrated if necessary. The
// provided logger may be nil.
func NewDBStore(db DB, n *consensus.Network, genesisBlock types.Block, logger MigrationLogger) (_ *DBStore, _ consensus.State, err error) {
	// during initialization, we should return an error instead of panicking
	defer func() {
		if r := recover(); r != nil {
			db.Cancel()
			err = fmt.Errorf("panic during database initialization: %v", r)
		}
	}()

	// don't accidentally overwrite a siad database
	if db.Bucket([]byte("ChangeLog")) != nil {
		return nil, consensus.State{}, errors.New("detected siad database, refusing to proceed")
	}

	dbs := &DBStore{
		db: db,
		n:  n,
	}

	// if the db is empty, initialize it
	if version := dbs.bucket(bVersion).getRaw(bVersion); len(version) != 1 {
		for _, bucket := range [][]byte{
			bVersion,
			bMainChain,
			bStates,
			bBlocks,
			bFileContractElements,
			bSiacoinElements,
			bSiafundElements,
			bTree,
		} {
			if _, err := db.CreateBucket(bucket); err != nil {
				panic(err)
			}
		}
		dbs.bucket(bVersion).putRaw(bVersion, []byte{4})

		// store genesis state and apply genesis block to it
		genesisState := n.GenesisState()
		dbs.putState(genesisState)
		bs := consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, len(genesisBlock.Transactions))}
		cs, cau := consensus.ApplyBlock(genesisState, genesisBlock, bs, time.Time{})
		dbs.putBlock(genesisBlock.Header(), &genesisBlock, &bs)
		dbs.putState(cs)
		dbs.ApplyBlock(cs, cau)
		if err := dbs.Flush(); err != nil {
			return nil, consensus.State{}, err
		}
	} else if version[0] != 4 {
		if logger == nil {
			logger = noopLogger{}
		}
		if err := migrateDB(dbs, n, logger); err != nil {
			return nil, consensus.State{}, fmt.Errorf("failed to migrate database: %w", err)
		}
	}

	// check that we have the correct genesis block for this network
	if dbGenesis, ok := dbs.BestIndex(0); !ok || dbGenesis.ID != genesisBlock.ID() {
		// try to detect network so we can provide a more helpful error message
		_, mainnetGenesis := Mainnet()
		_, zenGenesis := TestnetZen()
		if genesisBlock.ID() == mainnetGenesis.ID() && dbGenesis.ID == zenGenesis.ID() {
			return nil, consensus.State{}, errors.New("cannot use Zen testnet database on mainnet")
		} else if genesisBlock.ID() == zenGenesis.ID() && dbGenesis.ID == mainnetGenesis.ID() {
			return nil, consensus.State{}, errors.New("cannot use mainnet database on Zen testnet")
		} else {
			return nil, consensus.State{}, errors.New("database previously initialized with different genesis block")
		}
	}

	// load tip state
	index, _ := dbs.BestIndex(dbs.getHeight())
	cs, _ := dbs.State(index.ID)
	return dbs, cs, err
}
