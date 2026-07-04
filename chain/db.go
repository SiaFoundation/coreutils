package chain

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math/bits"
	"sort"
	"sync"
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

// A DB is a generic key-value database, comprising a mutable "scratchpad" and
// read-only snapshots of its committed state.
type DB interface {
	// Snapshot returns a read-only snapshot of the DB's data as of the most
	// recent Flush. Snapshots are unaffected by concurrent scratchpad writes
	// and Flushes. The release function must be called when the snapshot is
	// no longer needed.
	Snapshot() (dbs DBSnapshot, release func())
	// Scratchpad returns a handle to the current scratchpad, which observes
	// the DB's uncommitted data.
	Scratchpad() DBScratchpad
}

// A DBSnapshot is a read-only snapshot of a DB.
type DBSnapshot interface {
	Bucket(name []byte) DBBucket
}

// A DBScratchpad accumulates writes to a DB, which become durable when Flush
// is called. Its methods observe the accumulated writes. It is not safe for
// concurrent use.
//
// Unlike a StoreScratchpad, a DBScratchpad must not flush autonomously:
// callers batch writes across multiple method calls, so only they know when
// the accumulated writes form a consistent snapshot.
type DBScratchpad interface {
	Bucket(name []byte) DBBucket
	CreateBucket(name []byte) (DBBucket, error)
	Flush() error
	Cancel()
}

// A DBBucket is a set of key-value pairs.
type DBBucket interface {
	Get(key []byte) []byte
	Iter() iter.Seq2[[]byte, []byte]
	// these methods MAY return errors if the bucket is read-only
	Put(key, value []byte) error
	Delete(key []byte) error
}

// a memGen is a "generation" of committed MemDB state. Each open snapshot pins
// the generation it was created from; Flush mutates the current generation in
// place if it is unpinned, and otherwise leaves it frozen, applying writes to
// a copy instead.
type memGen struct {
	buckets map[string]map[string][]byte
	pins    int
}

// MemDB implements DB with an in-memory map.
type MemDB struct {
	mu   sync.Mutex // guards gen pointer and pin counts
	gen  *memGen
	puts map[string]map[string][]byte
	dels map[string]map[string]struct{}
}

// Flush implements DBScratchpad.
func (db *MemDB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(db.puts) == 0 && len(db.dels) == 0 {
		return nil
	} else if db.gen.pins > 0 {
		// the current generation is pinned by one or more snapshots; leave it
		// frozen, applying our writes to a copy
		//
		// NOTE: bucket maps must not be shared between generations, as a
		// later Flush may mutate buckets that this Flush did not
		buckets := make(map[string]map[string][]byte, len(db.gen.buckets))
		for name, kvs := range db.gen.buckets {
			buckets[name] = maps.Clone(kvs)
		}
		db.gen = &memGen{buckets: buckets}
	}
	for bucket, puts := range db.puts {
		if db.gen.buckets[bucket] == nil {
			db.gen.buckets[bucket] = make(map[string][]byte)
		}
		for key, val := range puts {
			db.gen.buckets[bucket][key] = val
		}
		delete(db.puts, bucket)
	}
	for bucket, dels := range db.dels {
		if db.gen.buckets[bucket] == nil {
			db.gen.buckets[bucket] = make(map[string][]byte)
		}
		for key := range dels {
			delete(db.gen.buckets[bucket], key)
		}
		delete(db.dels, bucket)
	}
	return nil
}

// Cancel implements DBScratchpad.
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
	return db.gen.buckets[bucket][string(key)]
}

func (db *MemDB) put(bucket string, key, value []byte) error {
	if db.puts[bucket] == nil {
		if db.gen.buckets[bucket] == nil {
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
		if db.gen.buckets[bucket] == nil {
			return errors.New("bucket does not exist")
		}
		db.dels[bucket] = make(map[string]struct{})
	}
	db.dels[bucket][string(key)] = struct{}{}
	delete(db.puts[bucket], string(key))
	return nil
}

// Scratchpad implements DB.
func (db *MemDB) Scratchpad() DBScratchpad { return db }

// Bucket implements DBScratchpad.
func (db *MemDB) Bucket(name []byte) DBBucket {
	if db.gen.buckets[string(name)] == nil &&
		db.puts[string(name)] == nil &&
		db.dels[string(name)] == nil {
		return nil
	}
	return memBucket{string(name), db}
}

// CreateBucket implements DBScratchpad.
func (db *MemDB) CreateBucket(name []byte) (DBBucket, error) {
	if db.gen.buckets[string(name)] != nil {
		return nil, errors.New("bucket already exists")
	}
	db.puts[string(name)] = make(map[string][]byte)
	db.dels[string(name)] = make(map[string]struct{})
	return db.Bucket(name), nil
}

type memSnapshotBucket struct {
	kvs map[string][]byte
}

func (b memSnapshotBucket) Get(key []byte) []byte { return b.kvs[string(key)] }
func (b memSnapshotBucket) Iter() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		for key, val := range b.kvs {
			if !yield([]byte(key), val) {
				return
			}
		}
	}
}
func (b memSnapshotBucket) Put(_, _ []byte) error { return errors.New("bucket is read-only") }
func (b memSnapshotBucket) Delete(_ []byte) error { return errors.New("bucket is read-only") }

type memDBSnapshot struct {
	db       *MemDB
	gen      *memGen
	released bool
}

func (v *memDBSnapshot) Bucket(name []byte) DBBucket {
	kvs, ok := v.gen.buckets[string(name)]
	if !ok {
		return nil
	}
	return memSnapshotBucket{kvs}
}

func (v *memDBSnapshot) release() {
	v.db.mu.Lock()
	defer v.db.mu.Unlock()
	if !v.released {
		v.released = true
		v.gen.pins--
	}
}

// Snapshot implements DB.
func (db *MemDB) Snapshot() (DBSnapshot, func()) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.gen.pins++
	s := &memDBSnapshot{db: db, gen: db.gen}
	return s, s.release
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
		for key, val := range b.db.gen.buckets[b.name] {
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
		gen:  &memGen{buckets: make(map[string]map[string][]byte)},
		puts: make(map[string]map[string][]byte),
		dels: make(map[string]map[string]struct{}),
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

// Scratchpad implements DB.
func (db *CacheDB) Scratchpad() DBScratchpad { return db }

// Bucket implements DBScratchpad.
func (db *CacheDB) Bucket(name []byte) DBBucket {
	b := db.db.Scratchpad().Bucket(name)
	if b == nil {
		return nil
	} else if db.mem.Bucket(name) == nil {
		db.mem.CreateBucket(name)
	}
	return cacheBucket{memBucket{string(name), db.mem}, b}
}

// CreateBucket implements DBScratchpad.
func (db *CacheDB) CreateBucket(name []byte) (DBBucket, error) {
	if _, err := db.db.Scratchpad().CreateBucket(name); err != nil {
		return nil, err
	}
	return db.mem.CreateBucket(name)
}

// Flush implements DBScratchpad.
func (db *CacheDB) Flush() error {
	sp := db.db.Scratchpad()
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
		bucket := sp.Bucket([]byte(bucket))
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
		bucket := sp.Bucket([]byte(name))
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
	//
	// NOTE: the MemDB is internal to the CacheDB and never has open snapshots, so
	// its current generation can be mutated freely
	for _, bucket := range db.mem.gen.buckets {
		clear(bucket)
	}
	for _, bucket := range db.mem.puts {
		clear(bucket)
	}
	for _, bucket := range db.mem.dels {
		clear(bucket)
	}
	return sp.Flush()
}

// Cancel implements DBScratchpad.
func (db *CacheDB) Cancel() {
	db.mem.Cancel()
	db.db.Scratchpad().Cancel()
}

// Snapshot implements DB.
func (db *CacheDB) Snapshot() (DBSnapshot, func()) {
	// unflushed writes are cached in memory, so the underlying DB always
	// reflects the state as of the last Flush
	return db.db.Snapshot()
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

// dbBucketReader is a helper type for implementing the read half of Store.
type dbBucketReader struct {
	b DBBucket
}

func (b dbBucketReader) getRaw(key []byte) []byte {
	if b.b == nil {
		return nil
	}
	return b.b.Get(key)
}

func (b dbBucketReader) get(key []byte, v types.DecoderFrom) bool {
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

// dbBucket is a helper type for implementing the write half of Store.
type dbBucket struct {
	b  DBBucket
	sp *dbScratchpad
}

func (b dbBucket) getRaw(key []byte) []byte                 { return dbBucketReader{b.b}.getRaw(key) }
func (b dbBucket) get(key []byte, v types.DecoderFrom) bool { return dbBucketReader{b.b}.get(key, v) }

func (b dbBucket) putRaw(key, value []byte) {
	check(b.b.Put(key, value))
	b.sp.unflushed += len(value)
}

func (b dbBucket) put(key []byte, v types.EncoderTo) {
	var buf bytes.Buffer
	b.sp.enc.Reset(&buf)
	v.EncodeTo(&b.sp.enc)
	b.sp.enc.Flush()
	b.putRaw(key, buf.Bytes())
}

func (b dbBucket) delete(key []byte) {
	check(b.b.Delete(key))
	b.sp.unflushed += len(key)
}

var (
	bVersion              = []byte("Version")
	bNetwork              = []byte("Network")
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
	db         DB
	n          *consensus.Network
	scratchpad *dbScratchpad
}

// bucket returns a writeable handle for the named bucket.
func (s *dbScratchpad) bucket(name []byte) dbBucket {
	return dbBucket{s.sp.Bucket(name), s}
}

func readBucket(ss DBSnapshot, name []byte) dbBucketReader {
	return dbBucketReader{ss.Bucket(name)}
}

func encHeight(height uint64) []byte {
	var buf [8]byte
	return binary.BigEndian.AppendUint64(buf[:0], height)
}

func getHeight(ss DBSnapshot) (height uint64) {
	if val := readBucket(ss, bMainChain).getRaw(keyHeight); len(val) == 8 {
		height = binary.BigEndian.Uint64(val)
	}
	return
}

func getState(ss DBSnapshot, n *consensus.Network, id types.BlockID) (consensus.State, bool) {
	var vs versionedState
	ok := readBucket(ss, bStates).get(id[:], &vs)
	vs.State.Network = n
	return vs.State, ok
}

// tipState returns the consensus state of the best chain.s tip.
func tipState(ss DBSnapshot, n *consensus.Network) consensus.State {
	index, _ := bestIndex(ss, getHeight(ss))
	cs, _ := getState(ss, n, index.ID)
	return cs
}

func getBlock(ss DBSnapshot, id types.BlockID) (bh types.BlockHeader, b *types.Block, bs *consensus.V1BlockSupplement, _ bool) {
	var sb supplementedBlock
	if ok := readBucket(ss, bBlocks).get(id[:], &sb); !ok {
		return types.BlockHeader{}, nil, nil, false
	} else if sb.Header == nil {
		sb.Header = new(types.BlockHeader)
		*sb.Header = sb.Block.Header()
	}
	return *sb.Header, sb.Block, sb.Supplement, true
}

func (s *dbScratchpad) putBlock(bh types.BlockHeader, b *types.Block, bs *consensus.V1BlockSupplement) {
	id := bh.ID()
	s.bucket(bBlocks).put(id[:], supplementedBlock{&bh, b, bs})
}

func getAncestorInfo(ss DBSnapshot, id types.BlockID) (parentID types.BlockID, timestamp time.Time, ok bool) {
	ok = readBucket(ss, bBlocks).get(id[:], types.DecoderFunc(func(d *types.Decoder) {
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

func getBlockHeader(ss DBSnapshot, id types.BlockID) (bh types.BlockHeader, ok bool) {
	ok = readBucket(ss, bBlocks).get(id[:], types.DecoderFunc(func(d *types.Decoder) {
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

func treeKey(row, col uint64) []byte {
	// If we assume that the total number of elements is less than 2^32, we can
	// pack row and col into one uint32 key. We do this by setting the top 'row'
	// bits of 'col' to 1. Since each successive row has half as many columns,
	// we never have to worry about clobbering any bits of 'col'.
	var buf [4]byte
	return binary.BigEndian.AppendUint32(buf[:0], uint32(((1<<row)-1)<<(32-row)|col))
}

func getElementProof(ss DBSnapshot, leafIndex, numLeaves uint64) (proof []types.Hash256) {
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
		if !readBucket(ss, bTree).get(treeKey(row, col), &proof[i]) {
			panic(fmt.Sprintf("missing proof element %v for leaf %v", i, leafIndex))
		}
	}
	return
}

func getSiacoinElement(ss DBSnapshot, id types.SiacoinOutputID, numLeaves uint64) (sce types.SiacoinElement, ok bool) {
	ok = readBucket(ss, bSiacoinElements).get(id[:], &sce)
	if ok {
		sce.StateElement.MerkleProof = getElementProof(ss, sce.StateElement.LeafIndex, numLeaves)
	}
	return
}

func (s *dbScratchpad) putSiacoinElement(sce types.SiacoinElement) {
	sce.StateElement.MerkleProof = nil
	s.bucket(bSiacoinElements).put(sce.ID[:], sce.Share())
}

func (s *dbScratchpad) deleteSiacoinElement(id types.SiacoinOutputID) {
	s.bucket(bSiacoinElements).delete(id[:])
}

func getSiafundElement(ss DBSnapshot, id types.SiafundOutputID, numLeaves uint64) (sfe types.SiafundElement, ok bool) {
	ok = readBucket(ss, bSiafundElements).get(id[:], &sfe)
	if ok {
		sfe.StateElement.MerkleProof = getElementProof(ss, sfe.StateElement.LeafIndex, numLeaves)
	}
	return
}

func (s *dbScratchpad) putSiafundElement(sfe types.SiafundElement) {
	sfe.StateElement.MerkleProof = nil
	s.bucket(bSiafundElements).put(sfe.ID[:], sfe.Share())
}

func (s *dbScratchpad) deleteSiafundElement(id types.SiafundOutputID) {
	s.bucket(bSiafundElements).delete(id[:])
}

func getFileContractElement(ss DBSnapshot, id types.FileContractID, numLeaves uint64) (fce types.FileContractElement, ok bool) {
	ok = readBucket(ss, bFileContractElements).get(id[:], &fce)
	if ok {
		fce.StateElement.MerkleProof = getElementProof(ss, fce.StateElement.LeafIndex, numLeaves)
	}
	return
}

func (s *dbScratchpad) putFileContractElement(fce types.FileContractElement) {
	fce.StateElement.MerkleProof = nil
	s.bucket(bFileContractElements).put(fce.ID[:], fce.Share())
}

func (s *dbScratchpad) deleteFileContractElement(id types.FileContractID) {
	s.bucket(bFileContractElements).delete(id[:])
}

func (s *dbScratchpad) putFileContractExpiration(id types.FileContractID, windowEnd uint64, apply bool) {
	b := s.bucket(bFileContractElements)
	key := encHeight(windowEnd)
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

func expiringFileContractIDs(ss DBSnapshot, height uint64) []types.FileContractID {
	buf := readBucket(ss, bFileContractElements).getRaw(encHeight(height))
	ids := make([]types.FileContractID, 0, len(buf)/32)
	for i := 0; i < len(buf); i += 32 {
		ids = append(ids, (types.FileContractID)(buf[i:]))
	}
	return ids
}

func (s *dbScratchpad) deleteFileContractExpiration(id types.FileContractID, windowEnd uint64) {
	b := s.bucket(bFileContractElements)
	key := encHeight(windowEnd)
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

func (s *dbScratchpad) applyElements(cau consensus.ApplyUpdate) {
	cau.ForEachTreeNode(func(row, col uint64, h types.Hash256) {
		s.bucket(bTree).putRaw(treeKey(row, col), h[:])
	})

	for _, sced := range cau.SiacoinElementDiffs() {
		if sced.Created && sced.Spent {
			continue // ephemeral
		} else if sced.Spent {
			s.deleteSiacoinElement(sced.SiacoinElement.ID)
		} else {
			s.putSiacoinElement(sced.SiacoinElement.Share())
		}
	}
	for _, sfed := range cau.SiafundElementDiffs() {
		if sfed.Created && sfed.Spent {
			continue // ephemeral
		} else if sfed.Spent {
			s.deleteSiafundElement(sfed.SiafundElement.ID)
		} else {
			s.putSiafundElement(sfed.SiafundElement.Share())
		}
	}
	for _, fced := range cau.FileContractElementDiffs() {
		fce := &fced.FileContractElement
		if fced.Created && fced.Resolved {
			continue
		} else if fced.Resolved {
			s.deleteFileContractElement(fce.ID)
			s.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
		} else if fced.Revision != nil {
			rev := fce.Share()
			rev.FileContract = *fced.Revision
			s.putFileContractElement(rev.Share())
			if rev.FileContract.WindowEnd != fce.FileContract.WindowEnd {
				s.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
				s.putFileContractExpiration(fce.ID, rev.FileContract.WindowEnd, true)
			}
		} else {
			s.putFileContractElement(fce.Share())
			s.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, true)
		}
	}
}

func (s *dbScratchpad) revertElements(cru consensus.RevertUpdate) {
	for _, fced := range cru.FileContractElementDiffs() {
		fce := &fced.FileContractElement
		if fced.Created && fced.Resolved {
			continue
		} else if fced.Resolved {
			// contract no longer resolved; restore it
			s.putFileContractElement(fce.Share())
			s.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, false)
		} else if fced.Revision != nil {
			// contract no longer revised; restore prior revision
			rev := fce.Share()
			rev.FileContract = *fced.Revision
			s.putFileContractElement(fce.Share())
			if rev.FileContract.WindowEnd != fce.FileContract.WindowEnd {
				s.deleteFileContractExpiration(fce.ID, rev.FileContract.WindowEnd)
				s.putFileContractExpiration(fce.ID, fce.FileContract.WindowEnd, false)
			}
		} else {
			// contract no longer exists; delete it
			s.deleteFileContractElement(fce.ID)
			s.deleteFileContractExpiration(fce.ID, fce.FileContract.WindowEnd)
		}
	}

	for _, sfed := range cru.SiafundElementDiffs() {
		if sfed.Created && sfed.Spent {
			continue // ephemeral
		} else if sfed.Spent {
			// output no longer spent; restore it
			s.putSiafundElement(sfed.SiafundElement.Share())
		} else {
			// output no longer exists; delete it
			s.deleteSiafundElement(sfed.SiafundElement.ID)
		}
	}
	for _, sced := range cru.SiacoinElementDiffs() {
		if sced.Created && sced.Spent {
			continue // ephemeral
		} else if sced.Spent {
			// output no longer spent; restore it
			s.putSiacoinElement(sced.SiacoinElement.Share())
		} else {
			// output no longer exists; delete it
			s.deleteSiacoinElement(sced.SiacoinElement.ID)
		}
	}

	cru.ForEachTreeNode(func(row, col uint64, h types.Hash256) {
		s.bucket(bTree).putRaw(treeKey(row, col), h[:])
	})

	// NOTE: Although the element tree has shrunk, we do not need to explicitly
	// delete any nodes; getElementProof always stops at the correct height for
	// the given tree size, so the no-longer-valid nodes are simply never
	// accessed. (They will continue to occupy storage, but this storage will
	// inevitably be overwritten by future nodes, so there is little reason to
	// reclaim it immediately.)
}

func bestIndex(ss DBSnapshot, height uint64) (index types.ChainIndex, ok bool) {
	index.Height = height
	ok = readBucket(ss, bMainChain).get(encHeight(height), &index.ID)
	return
}

func supplementTipTransaction(ss DBSnapshot, n *consensus.Network, txn types.Transaction) (ts consensus.V1TransactionSupplement) {
	if getHeight(ss) >= n.HardforkV2.RequireHeight {
		return consensus.V1TransactionSupplement{}
	}
	// get tip state, for proof-trimming
	cs := tipState(ss, n)
	numLeaves := cs.Elements.NumLeaves

	for _, sci := range txn.SiacoinInputs {
		if sce, ok := getSiacoinElement(ss, sci.ParentID, numLeaves); ok {
			ts.SiacoinInputs = append(ts.SiacoinInputs, sce.Move())
		}
	}
	for _, sfi := range txn.SiafundInputs {
		if sfe, ok := getSiafundElement(ss, sfi.ParentID, numLeaves); ok {
			ts.SiafundInputs = append(ts.SiafundInputs, sfe.Move())
		}
	}
	for _, fcr := range txn.FileContractRevisions {
		if fce, ok := getFileContractElement(ss, fcr.ParentID, numLeaves); ok {
			ts.RevisedFileContracts = append(ts.RevisedFileContracts, fce.Move())
		}
	}
	for _, sp := range txn.StorageProofs {
		if fce, ok := getFileContractElement(ss, sp.ParentID, numLeaves); ok {
			if windowIndex, ok := bestIndex(ss, fce.FileContract.WindowStart-1); ok {
				ts.StorageProofs = append(ts.StorageProofs, consensus.V1StorageProofSupplement{
					FileContract: fce.Move(),
					WindowID:     windowIndex.ID,
				})
			}
		}
	}
	return
}

func supplementTipBlock(ss DBSnapshot, n *consensus.Network, b types.Block) (bs consensus.V1BlockSupplement) {
	if getHeight(ss) >= n.HardforkV2.RequireHeight {
		return consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, len(b.Transactions))}
	}

	// get tip state, for proof-trimming
	cs := tipState(ss, n)
	numLeaves := cs.Elements.NumLeaves

	bs = consensus.V1BlockSupplement{
		Transactions: make([]consensus.V1TransactionSupplement, len(b.Transactions)),
	}
	for i, txn := range b.Transactions {
		bs.Transactions[i] = supplementTipTransaction(ss, n, txn)
	}
	for _, id := range expiringFileContractIDs(ss, cs.Index.Height+1) {
		fce, ok := getFileContractElement(ss, id, numLeaves)
		if !ok {
			panic("missing FileContractElement")
		}
		bs.ExpiringFileContracts = append(bs.ExpiringFileContracts, fce.Move())
	}
	return bs
}

func ancestorTimestamp(ss DBSnapshot, n *consensus.Network, id types.BlockID) (t time.Time, ok bool) {
	cs, _ := getState(ss, n, id)
	if cs.Index.Height > n.HardforkOak.Height {
		return time.Time{}, true
	}

	getBestID := func(height uint64) types.BlockID {
		index, _ := bestIndex(ss, height)
		return index.ID
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
		ancestorID, _, _ = getAncestorInfo(ss, ancestorID)
	}
	_, t, ok = getAncestorInfo(ss, ancestorID)
	return
}

// NOTE: these values were chosen empirically and should constitute a
// sensible default; if necessary, we can make them configurable
const (
	flushSizeThreshold     = 100e6
	flushDurationThreshold = 5 * time.Second
)

func (s *dbScratchpad) shouldFlush() bool {
	return s.unflushed >= flushSizeThreshold || time.Since(s.lastFlush) >= flushDurationThreshold
}

// flushIfFull flushes the accumulated writes if they exceed the size
// threshold. Unlike shouldFlush, it ignores their age; it is used by methods
// for which an age-triggered flush would be undesirable, either because it
// would commit twice in quick succession (AddState/AddBlock, whose writes are
// shortly followed by an ApplyBlock or an explicit Flush) or because it would
// publish an intermediate reverted tip to snapshot readers (RevertBlock).
func (s *dbScratchpad) flushIfFull() {
	if s.unflushed >= flushSizeThreshold {
		s.mustFlush()
	}
}

func (s *dbScratchpad) mustFlush() { check(s.Flush()) }

// dbScratchpad implements StoreScratchpad. Its read methods observe the
// unflushed writes of the underlying DB scratchpad.
type dbScratchpad struct {
	sp  DBScratchpad
	n   *consensus.Network
	enc types.Encoder

	unflushed int
	lastFlush time.Time
	tip       consensus.State // updated by ApplyBlock/RevertBlock
}

// TipState implements StoreSnapshot.
func (s *dbScratchpad) TipState() consensus.State { return s.tip }

// BestIndex implements StoreSnapshot.
func (s *dbScratchpad) BestIndex(height uint64) (types.ChainIndex, bool) {
	return bestIndex(s.sp, height)
}

// Block implements StoreSnapshot.
func (s *dbScratchpad) Block(id types.BlockID) (types.Block, *consensus.V1BlockSupplement, bool) {
	_, b, bs, ok := getBlock(s.sp, id)
	if !ok || b == nil {
		return types.Block{}, nil, false
	}
	return *b, bs, ok
}

// Header implements StoreSnapshot.
func (s *dbScratchpad) Header(id types.BlockID) (types.BlockHeader, bool) {
	return getBlockHeader(s.sp, id)
}

// State implements StoreSnapshot.
func (s *dbScratchpad) State(id types.BlockID) (consensus.State, bool) {
	return getState(s.sp, s.n, id)
}

// ExpiringFileContractIDs implements StoreSnapshot.
func (s *dbScratchpad) ExpiringFileContractIDs(height uint64) []types.FileContractID {
	return expiringFileContractIDs(s.sp, height)
}

// AncestorTimestamp implements StoreSnapshot.
func (s *dbScratchpad) AncestorTimestamp(id types.BlockID) (time.Time, bool) {
	return ancestorTimestamp(s.sp, s.n, id)
}

// SupplementTipTransaction implements StoreSnapshot.
func (s *dbScratchpad) SupplementTipTransaction(txn types.Transaction) consensus.V1TransactionSupplement {
	return supplementTipTransaction(s.sp, s.n, txn)
}

// SupplementTipBlock implements StoreSnapshot.
func (s *dbScratchpad) SupplementTipBlock(b types.Block) consensus.V1BlockSupplement {
	return supplementTipBlock(s.sp, s.n, b)
}

// AddState implements StoreScratchpad.
func (s *dbScratchpad) AddState(cs consensus.State) {
	s.bucket(bStates).put(cs.Index.ID[:], versionedState{cs})
	s.flushIfFull()
}

// AddBlock implements StoreScratchpad.
func (s *dbScratchpad) AddBlock(b types.Block, bs *consensus.V1BlockSupplement) {
	s.putBlock(b.Header(), &b, bs)
	s.flushIfFull()
}

// PruneBlock implements StoreScratchpad.
func (s *dbScratchpad) PruneBlock(id types.BlockID) {
	if bh, _, _, ok := getBlock(s.sp, id); ok {
		s.putBlock(bh, nil, nil)
	}
	if s.shouldFlush() {
		s.mustFlush()
	}
}

// OverwriteExpiringFileContractIDs implements StoreScratchpad. This should
// not be called unless the IDs are known to be correct, as it will overwrite
// any existing IDs at that height.
func (s *dbScratchpad) OverwriteExpiringFileContractIDs(height uint64, ids []types.FileContractID) {
	buf := make([]byte, len(ids)*32)
	for i, id := range ids {
		copy(buf[i*32:], id[:])
	}
	s.bucket(bFileContractElements).putRaw(encHeight(height), buf)
}

// ApplyBlock implements StoreScratchpad.
func (s *dbScratchpad) ApplyBlock(cs consensus.State, cau consensus.ApplyUpdate) {
	s.bucket(bMainChain).put(encHeight(cs.Index.Height), &cs.Index.ID)
	s.bucket(bMainChain).putRaw(keyHeight, encHeight(cs.Index.Height))
	if cs.Index.Height <= s.n.HardforkV2.RequireHeight {
		s.applyElements(cau)
	}
	s.tip = cs
	if s.shouldFlush() {
		s.mustFlush()
	}
}

// RevertBlock implements StoreScratchpad.
func (s *dbScratchpad) RevertBlock(cs consensus.State, cru consensus.RevertUpdate) {
	if cs.Index.Height <= s.n.HardforkV2.RequireHeight {
		s.revertElements(cru)
	}
	s.bucket(bMainChain).delete(encHeight(cs.Index.Height + 1))
	s.bucket(bMainChain).putRaw(keyHeight, encHeight(cs.Index.Height))
	s.tip = cs
	s.flushIfFull()
}

// Flush implements StoreScratchpad.
func (s *dbScratchpad) Flush() error {
	if s.unflushed > 0 {
		if err := s.sp.Flush(); err != nil {
			return err
		}
		s.unflushed = 0
		s.lastFlush = time.Now()
	}
	return nil
}

// Scratchpad implements Store.
func (db *DBStore) Scratchpad() StoreScratchpad { return db.scratchpad }

// dbSnapshot implements StoreSnapshot.
type dbSnapshot struct {
	ss DBSnapshot
	n  *consensus.Network

	tipOnce sync.Once
	tip     consensus.State
}

// TipState implements StoreSnapshot.
func (s *dbSnapshot) TipState() consensus.State {
	// The tip is derived from the snapshot itself, so it is always consistent
	// with the snapshot's data; no synchronization with the scratchpad is
	// required.
	s.tipOnce.Do(func() {
		s.tip = tipState(s.ss, s.n)
	})
	return s.tip
}

// BestIndex implements StoreSnapshot.
func (s *dbSnapshot) BestIndex(height uint64) (types.ChainIndex, bool) {
	return bestIndex(s.ss, height)
}

// Block implements StoreSnapshot.
func (s *dbSnapshot) Block(id types.BlockID) (types.Block, *consensus.V1BlockSupplement, bool) {
	_, b, bs, ok := getBlock(s.ss, id)
	if !ok || b == nil {
		return types.Block{}, nil, false
	}
	return *b, bs, ok
}

// Header implements StoreSnapshot.
func (s *dbSnapshot) Header(id types.BlockID) (types.BlockHeader, bool) {
	return getBlockHeader(s.ss, id)
}

// State implements StoreSnapshot.
func (s *dbSnapshot) State(id types.BlockID) (consensus.State, bool) {
	return getState(s.ss, s.n, id)
}

// ExpiringFileContractIDs implements StoreSnapshot.
func (s *dbSnapshot) ExpiringFileContractIDs(height uint64) []types.FileContractID {
	return expiringFileContractIDs(s.ss, height)
}

// AncestorTimestamp implements StoreSnapshot.
func (s *dbSnapshot) AncestorTimestamp(id types.BlockID) (time.Time, bool) {
	return ancestorTimestamp(s.ss, s.n, id)
}

// SupplementTipTransaction implements StoreSnapshot.
func (s *dbSnapshot) SupplementTipTransaction(txn types.Transaction) consensus.V1TransactionSupplement {
	return supplementTipTransaction(s.ss, s.n, txn)
}

// SupplementTipBlock implements StoreSnapshot.
func (s *dbSnapshot) SupplementTipBlock(b types.Block) consensus.V1BlockSupplement {
	return supplementTipBlock(s.ss, s.n, b)
}

// Snapshot implements Store.
func (db *DBStore) Snapshot() (StoreSnapshot, func()) {
	ss, release := db.db.Snapshot()
	return &dbSnapshot{ss: ss, n: db.n}, release
}

// NewDBStore creates a new DBStore using the provided database. The DB will
// be automatically migrated if necessary. The provided logger may be nil.
func NewDBStore(db DB, n *consensus.Network, genesisBlock types.Block, logger MigrationLogger) (_ *DBStore, err error) {
	sp := db.Scratchpad()
	// during initialization, we should return an error instead of panicking
	defer func() {
		if r := recover(); r != nil {
			sp.Cancel()
			err = fmt.Errorf("panic during database initialization: %v", r)
		}
	}()

	if err := sanityCheckNetwork(n); err != nil {
		return nil, fmt.Errorf("invalid network: %w", err)
	}

	// don't accidentally overwrite a siad database
	if sp.Bucket([]byte("ChangeLog")) != nil {
		return nil, errors.New("detected siad database, refusing to proceed")
	}

	dbs := &DBStore{
		db:         db,
		n:          n,
		scratchpad: &dbScratchpad{sp: sp, n: n, lastFlush: time.Now()},
	}
	scratch := dbs.scratchpad

	// if the db is empty, initialize it
	if version := readBucket(sp, bVersion).getRaw(bVersion); len(version) != 1 {
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
				panic(err)
			}
		}
		scratch.bucket(bVersion).putRaw(bVersion, []byte{4})
		scratch.bucket(bNetwork).putRaw(bNetwork, []byte(n.Name))

		// store genesis state and apply genesis block to it
		genesisState := n.GenesisState()
		scratch.AddState(genesisState)
		bs := consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, len(genesisBlock.Transactions))}
		cs, cau := consensus.ApplyBlock(genesisState, genesisBlock, bs, time.Time{})
		scratch.AddBlock(genesisBlock, &bs)
		scratch.AddState(cs)
		scratch.ApplyBlock(cs, cau)
		if err := scratch.Flush(); err != nil {
			return nil, err
		}
	} else if version[0] != 4 {
		if logger == nil {
			logger = noopLogger{}
		}
		if err := migrateDB(dbs, logger); err != nil {
			return nil, fmt.Errorf("failed to migrate database: %w", err)
		}
	}
	if network := readBucket(sp, bNetwork).getRaw(bNetwork); len(network) != 0 && string(network) != n.Name {
		return nil, fmt.Errorf("database previously initialized with different network (%s)", string(network))
	}

	// load tip state
	dbs.scratchpad.tip = tipState(sp, n)
	return dbs, err
}

// NewDBStoreAtCheckpoint creates a DBStore initialized at the provided
// checkpoint. The checkpoint must be a v2 block. If the DB already exists, the
// checkpoint will be set as its new tip. The DB will be automatically migrated
// if necessary. The provided logger may be nil.
func NewDBStoreAtCheckpoint(db DB, cs consensus.State, b types.Block, logger MigrationLogger) (_ *DBStore, err error) {
	sp := db.Scratchpad()
	// during initialization, we should return an error instead of panicking
	defer func() {
		if r := recover(); r != nil {
			sp.Cancel()
			err = fmt.Errorf("panic during database initialization: %v", r)
		}
	}()

	if err := sanityCheckNetwork(cs.Network); err != nil {
		return nil, fmt.Errorf("invalid network: %w", err)
	}

	// don't accidentally overwrite a siad database
	if sp.Bucket([]byte("ChangeLog")) != nil {
		return nil, errors.New("detected siad database, refusing to proceed")
	}

	dbs := &DBStore{
		db:         db,
		n:          cs.Network,
		scratchpad: &dbScratchpad{sp: sp, n: cs.Network, lastFlush: time.Now()},
	}
	scratch := dbs.scratchpad

	// if the db is empty, initialize it
	if version := readBucket(sp, bVersion).getRaw(bVersion); len(version) != 1 {
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
				panic(err)
			}
		}
		scratch.bucket(bVersion).putRaw(bVersion, []byte{4})
		scratch.bucket(bNetwork).putRaw(bNetwork, []byte(cs.Network.Name))
	} else if version[0] != 4 {
		if logger == nil {
			logger = noopLogger{}
		}
		if err := migrateDB(dbs, logger); err != nil {
			return nil, fmt.Errorf("failed to migrate database: %w", err)
		}
	}
	if network := readBucket(sp, bNetwork).getRaw(bNetwork); len(network) != 0 && string(network) != cs.Network.Name {
		return nil, fmt.Errorf("database previously initialized with different network (%s)", string(network))
	}

	scratch.AddState(cs)
	bs := consensus.V1BlockSupplement{Transactions: make([]consensus.V1TransactionSupplement, len(b.Transactions))}
	cs, cau := consensus.ApplyBlock(cs, b, bs, time.Time{})
	scratch.AddBlock(b, &bs)
	scratch.AddState(cs)
	scratch.ApplyBlock(cs, cau)
	if err := scratch.Flush(); err != nil {
		return nil, err
	}
	return dbs, nil
}
