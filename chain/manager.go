package chain

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	// ErrFutureBlock is returned when a block's timestamp is too far in the future.
	ErrFutureBlock = errors.New("block's timestamp is too far in the future")
)

// An ApplyUpdate reflects the changes to the blockchain resulting from the
// addition of a block.
type ApplyUpdate struct {
	consensus.ApplyUpdate

	Block types.Block
	State consensus.State // post-application
}

// A RevertUpdate reflects the changes to the blockchain resulting from the
// removal of a block.
type RevertUpdate struct {
	consensus.RevertUpdate

	Block types.Block
	State consensus.State // post-reversion, i.e. pre-application
}

// A Store durably commits Manager-related data to storage. I/O errors must be
// handled internally, e.g. by panicking or calling os.Exit.
type Store interface {
	BestIndex(height uint64) (types.ChainIndex, bool)
	SupplementTipTransaction(txn types.Transaction) consensus.V1TransactionSupplement
	SupplementTipBlock(b types.Block) consensus.V1BlockSupplement

	Block(id types.BlockID) (types.Block, *consensus.V1BlockSupplement, bool)
	AddBlock(b types.Block, bs *consensus.V1BlockSupplement)
	PruneBlock(id types.BlockID)
	State(id types.BlockID) (consensus.State, bool)
	AddState(cs consensus.State)
	AncestorTimestamp(id types.BlockID) (time.Time, bool)

	// ApplyBlock and RevertBlock are free to commit whenever they see fit.
	ApplyBlock(s consensus.State, cau consensus.ApplyUpdate)
	RevertBlock(s consensus.State, cru consensus.RevertUpdate)
	Flush() error
}

// blockAndParent returns the block with the specified ID, along with its parent
// state.
func blockAndParent(s Store, id types.BlockID) (types.Block, *consensus.V1BlockSupplement, consensus.State, bool) {
	b, bs, ok := s.Block(id)
	cs, ok2 := s.State(b.ParentID)
	return b, bs, cs, ok && ok2
}

// blockAndChild returns the block with the specified ID, along with its child
// state.
func blockAndChild(s Store, id types.BlockID) (types.Block, *consensus.V1BlockSupplement, consensus.State, bool) {
	b, bs, ok := s.Block(id)
	cs, ok2 := s.State(id)
	return b, bs, cs, ok && ok2
}

// A Manager tracks multiple blockchains and identifies the best valid
// chain.
type Manager struct {
	store         Store
	tipState      consensus.State
	onReorg       map[[16]byte]func(types.ChainIndex)
	onPool        map[[16]byte]func()
	invalidBlocks map[types.BlockID]error

	// configuration options
	log         *zap.Logger
	pruneTarget uint64

	txpool struct {
		txns           []types.Transaction
		v2txns         []types.V2Transaction
		indices        map[types.TransactionID]int
		invalidTxnSets map[types.Hash256]error
		ms             *consensus.MidState
		weight         uint64
		medianFee      *types.Currency
		parentMap      map[types.Hash256]int
		lastReverted   []types.Transaction
		lastRevertedV2 []types.V2Transaction
	}

	mu sync.Mutex
}

// TipState returns the consensus state for the current tip.
func (m *Manager) TipState() consensus.State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tipState
}

// Tip returns the tip of the best known valid chain.
func (m *Manager) Tip() types.ChainIndex {
	return m.TipState().Index
}

// Block returns the block with the specified ID.
func (m *Manager) Block(id types.BlockID) (types.Block, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, _, ok := m.store.Block(id)
	return b, ok
}

// State returns the state with the specified ID.
func (m *Manager) State(id types.BlockID) (consensus.State, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.State(id)
}

// BestIndex returns the index of the block at the specified height within the
// best chain.
func (m *Manager) BestIndex(height uint64) (types.ChainIndex, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.store.BestIndex(height)
}

// History returns a set of block IDs that span the best chain, beginning with
// the 10 most-recent blocks, and subsequently spaced exponentionally farther
// apart until reaching the genesis block.
func (m *Manager) History() ([32]types.BlockID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	tipHeight := m.tipState.Index.Height
	histHeight := func(i int) uint64 {
		offset := uint64(i)
		if offset >= 10 {
			offset = 7 + 1<<(i-8) // strange, but it works
		}
		if offset > tipHeight {
			offset = tipHeight
		}
		return tipHeight - offset
	}
	var history [32]types.BlockID
	for i := range history {
		index, ok := m.store.BestIndex(histHeight(i))
		if !ok {
			return history, fmt.Errorf("missing best index at height %v", histHeight(i))
		}
		history[i] = index.ID
	}
	return history, nil
}

// BlocksForHistory returns up to max consecutive blocks from the best chain,
// starting from the "attach point" -- the first ID in the history that is
// present in the best chain (or, if no match is found, genesis). It also
// returns the number of blocks between the end of the returned slice and the
// current tip.
func (m *Manager) BlocksForHistory(history []types.BlockID, maxBlocks uint64) ([]types.Block, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var attachHeight uint64
	for _, id := range history {
		if cs, ok := m.store.State(id); !ok {
			continue
		} else if index, ok := m.store.BestIndex(cs.Index.Height); ok && index == cs.Index {
			attachHeight = cs.Index.Height
			break
		}
	}
	if maxBlocks > m.tipState.Index.Height-attachHeight {
		maxBlocks = m.tipState.Index.Height - attachHeight
	}
	blocks := make([]types.Block, maxBlocks)
	for i := range blocks {
		index, _ := m.store.BestIndex(attachHeight + uint64(i) + 1)
		b, _, ok := m.store.Block(index.ID)
		if !ok {
			return nil, 0, fmt.Errorf("missing block %v", index)
		}
		blocks[i] = b
	}
	return blocks, m.tipState.Index.Height - (attachHeight + maxBlocks), nil
}

// AddBlocks adds a sequence of blocks to a tracked chain. If the blocks are
// valid, the chain may become the new best chain, triggering a reorg.
func (m *Manager) AddBlocks(blocks []types.Block) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(blocks) == 0 {
		return nil
	}

	log := m.log.Named("AddBlocks")

	cs := m.tipState
	for _, b := range blocks {
		bid := b.ID()
		var ok bool
		if err := m.invalidBlocks[bid]; err != nil {
			return fmt.Errorf("block %v is invalid: %w", types.ChainIndex{Height: cs.Index.Height + 1, ID: bid}, err)
		} else if cs, ok = m.store.State(bid); ok {
			// already have this block
			continue
		} else if b.ParentID != cs.Index.ID {
			if cs, ok = m.store.State(b.ParentID); !ok {
				return fmt.Errorf("missing parent state for block %v", bid)
			}
		}
		if b.Timestamp.After(cs.MaxFutureTimestamp(time.Now())) {
			return ErrFutureBlock
		} else if err := consensus.ValidateOrphan(cs, b); err != nil {
			m.markBadBlock(bid, err)
			return fmt.Errorf("block %v is invalid: %w", types.ChainIndex{Height: cs.Index.Height + 1, ID: bid}, err)
		}
		ancestorTimestamp, ok := m.store.AncestorTimestamp(b.ParentID)
		if !ok {
			return fmt.Errorf("missing ancestor timestamp for block %v", b.ParentID)
		}
		cs = consensus.ApplyOrphan(cs, b, ancestorTimestamp)
		m.store.AddState(cs)
		m.store.AddBlock(b, nil)
		log.Debug("added block", zap.Uint64("height", cs.Index.Height), zap.Stringer("id", bid))
	}

	// if this chain is now the best chain, trigger a reorg
	if cs.SufficientlyHeavierThan(m.tipState) {
		oldTip := m.tipState.Index
		log.Debug("reorging to", zap.Stringer("current", oldTip), zap.Stringer("target", cs.Index))
		if err := m.reorgTo(cs.Index); err != nil {
			if err := m.reorgTo(oldTip); err != nil {
				return fmt.Errorf("failed to revert failed reorg: %w", err)
			}
			return fmt.Errorf("reorg failed: %w", err)
		}
		// release lock while notifying listeners
		tip := m.tipState.Index
		fns := make([]func(), 0, len(m.onReorg)+len(m.onPool))
		for _, fn := range m.onReorg {
			fns = append(fns, func() { fn(tip) })
		}
		for _, fn := range m.onPool {
			fns = append(fns, fn)
		}
		m.mu.Unlock()
		for _, fn := range fns {
			fn()
		}
		m.mu.Lock()
	}
	return nil
}

// markBadBlock marks a block as bad, so that we don't waste resources
// re-validating it if we see it again.
func (m *Manager) markBadBlock(bid types.BlockID, err error) {
	const maxInvalidBlocks = 1000
	if len(m.invalidBlocks) >= maxInvalidBlocks {
		// forget a random entry
		for bid := range m.invalidBlocks {
			delete(m.invalidBlocks, bid)
			break
		}
	}
	m.invalidBlocks[bid] = err
}

// revertTip reverts the current tip.
func (m *Manager) revertTip() error {
	b, bs, cs, ok := blockAndParent(m.store, m.tipState.Index.ID)
	if !ok {
		return fmt.Errorf("missing block at index %v", m.tipState.Index)
	}
	cru := consensus.RevertBlock(cs, b, *bs)
	m.store.RevertBlock(cs, cru)
	m.revertPoolUpdate(cru, cs)
	m.tipState = cs
	return nil
}

// applyTip adds a block to the current tip.
func (m *Manager) applyTip(index types.ChainIndex) error {
	var cau consensus.ApplyUpdate
	b, bs, cs, ok := blockAndChild(m.store, index.ID)
	if !ok {
		return fmt.Errorf("missing block at index %v", index)
	} else if b.ParentID != m.tipState.Index.ID {
		panic("applyTip called with non-attaching block")
	} else if bs == nil {
		bs = new(consensus.V1BlockSupplement)
		*bs = m.store.SupplementTipBlock(b)
		if err := consensus.ValidateBlock(m.tipState, b, *bs); err != nil {
			m.markBadBlock(index.ID, err)
			return fmt.Errorf("block %v is invalid: %w", index, err)
		}
		ancestorTimestamp, ok := m.store.AncestorTimestamp(b.ParentID)
		if !ok {
			return fmt.Errorf("missing ancestor timestamp for block %v", b.ParentID)
		}
		cs, cau = consensus.ApplyBlock(m.tipState, b, *bs, ancestorTimestamp)
		m.store.AddState(cs)
		m.store.AddBlock(b, bs)
	} else {
		ancestorTimestamp, ok := m.store.AncestorTimestamp(b.ParentID)
		if !ok {
			return fmt.Errorf("missing ancestor timestamp for block %v", b.ParentID)
		}
		_, cau = consensus.ApplyBlock(m.tipState, b, *bs, ancestorTimestamp)
	}

	m.store.ApplyBlock(cs, cau)
	m.applyPoolUpdate(cau, cs)
	m.tipState = cs

	if m.pruneTarget != 0 && cs.Index.Height > m.pruneTarget {
		if index, ok := m.store.BestIndex(cs.Index.Height - m.pruneTarget); ok {
			m.store.PruneBlock(index.ID)
		}
	}
	return nil
}

func (m *Manager) reorgPath(a, b types.ChainIndex) (revert, apply []types.ChainIndex, err error) {
	// helper function for "rewinding" to the parent index
	rewind := func(index *types.ChainIndex) (ok bool) {
		// if we're on the best chain, we can be a bit more efficient
		if bi, _ := m.store.BestIndex(index.Height); bi.ID == index.ID {
			*index, ok = m.store.BestIndex(index.Height - 1)
		} else {
			var b types.Block
			b, _, ok = m.store.Block(index.ID)
			*index = types.ChainIndex{Height: index.Height - 1, ID: b.ParentID}
		}
		return ok
	}

	// rewind a or b until their heights match
	for a.Height > b.Height {
		revert = append(revert, a)
		if !rewind(&a) {
			return
		}
	}
	for b.Height > a.Height {
		apply = append(apply, b)
		if !rewind(&b) {
			return
		}
	}

	// special case: if a is uninitialized, we're starting from genesis
	if a == (types.ChainIndex{}) {
		a, _ = m.store.BestIndex(0)
		apply = append(apply, a)
	}

	// now rewind both until we reach a common ancestor
	for a != b {
		revert = append(revert, a)
		apply = append(apply, b)
		if !rewind(&a) || !rewind(&b) {
			return
		}
	}

	// reverse the apply path
	for i := 0; i < len(apply)/2; i++ {
		j := len(apply) - i - 1
		apply[i], apply[j] = apply[j], apply[i]
	}
	return
}

func (m *Manager) reorgTo(index types.ChainIndex) error {
	revert, apply, err := m.reorgPath(m.tipState.Index, index)
	if err != nil {
		return err
	}
	for range revert {
		if err := m.revertTip(); err != nil {
			return fmt.Errorf("couldn't revert block %v: %w", m.tipState.Index, err)
		}
	}
	for _, index := range apply {
		if err := m.applyTip(index); err != nil {
			return fmt.Errorf("couldn't apply block %v: %w", index, err)
		}
	}
	if err := m.store.Flush(); err != nil {
		return err
	}

	// invalidate txpool caches
	m.txpool.ms = nil
	m.txpool.medianFee = nil
	m.txpool.parentMap = nil
	if len(revert) > 0 {
		b, _, _ := m.store.Block(revert[0].ID)
		m.txpool.lastReverted = b.Transactions
		m.txpool.lastRevertedV2 = b.V2Transactions()
	}
	return nil
}

// UpdatesSince returns at most max updates on the path between index and the
// Manager's current tip.
func (m *Manager) UpdatesSince(index types.ChainIndex, maxBlocks int) (rus []RevertUpdate, aus []ApplyUpdate, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	onBestChain := func(index types.ChainIndex) bool {
		bi, _ := m.store.BestIndex(index.Height)
		return bi.ID == index.ID || index == types.ChainIndex{}
	}

	for index != m.tipState.Index && len(rus)+len(aus) <= maxBlocks {
		// revert until we are on the best chain, then apply
		if !onBestChain(index) {
			b, bs, cs, ok := blockAndParent(m.store, index.ID)
			if !ok {
				return nil, nil, fmt.Errorf("missing block at index %v", index)
			} else if bs == nil {
				return nil, nil, fmt.Errorf("missing supplement for block %v", index)
			}
			cru := consensus.RevertBlock(cs, b, *bs)
			rus = append(rus, RevertUpdate{cru, b, cs})
			index = cs.Index
		} else {
			// special case: if index is uninitialized, we're starting from genesis
			if index == (types.ChainIndex{}) {
				index, _ = m.store.BestIndex(0)
			} else {
				index, _ = m.store.BestIndex(index.Height + 1)
			}
			b, bs, cs, ok := blockAndParent(m.store, index.ID)
			if !ok {
				return nil, nil, fmt.Errorf("missing block at index %v", index)
			} else if bs == nil {
				return nil, nil, fmt.Errorf("missing supplement for block %v", index)
			}
			ancestorTimestamp, ok := m.store.AncestorTimestamp(b.ParentID)
			if !ok && index.Height != 0 {
				return nil, nil, fmt.Errorf("missing ancestor timestamp for block %v", b.ParentID)
			}
			cs, cau := consensus.ApplyBlock(cs, b, *bs, ancestorTimestamp)
			aus = append(aus, ApplyUpdate{cau, b, cs})
		}
	}
	return
}

// OnReorg adds fn to the set of functions that are called whenever the best
// chain changes. The fn is called with the new tip. It returns a function that
// removes fn from the set.
func (m *Manager) OnReorg(fn func(types.ChainIndex)) (cancel func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := frand.Entropy128()
	m.onReorg[key] = fn
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.onReorg, key)
	}
}

// OnPoolChange adds fn to the set of functions that are called whenever the
// transaction pool may have changed. It returns a function that removes fn from
// the set.
func (m *Manager) OnPoolChange(fn func()) (cancel func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := frand.Entropy128()
	m.onPool[key] = fn
	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.onPool, key)
	}
}

func (m *Manager) revalidatePool() {
	log := m.log.Named("revalidatePool")
	txpoolMaxWeight := m.tipState.MaxBlockWeight() * 10
	if m.txpool.ms != nil && m.txpool.weight < txpoolMaxWeight {
		return
	}
	// if the pool is full, remove low-fee transactions until we are below 75%
	//
	// NOTE: ideally we would consider the total fees of each *set* of dependent
	// transactions, but that's expensive; this approach should work fine in
	// practice.
	if m.txpool.weight >= txpoolMaxWeight {
		// sort txns by fee without modifying the actual pool slice
		type feeTxn struct {
			index  int
			fees   types.Currency
			weight uint64
			v2     bool
		}
		txnFees := make([]feeTxn, 0, len(m.txpool.txns)+len(m.txpool.v2txns))
		for i, txn := range m.txpool.txns {
			txnFees = append(txnFees, feeTxn{
				index:  i,
				fees:   txn.TotalFees(),
				weight: m.tipState.TransactionWeight(txn),
			})
		}
		for i, txn := range m.txpool.v2txns {
			txnFees = append(txnFees, feeTxn{
				index:  i,
				fees:   txn.MinerFee,
				weight: m.tipState.V2TransactionWeight(txn),
				v2:     true,
			})
		}
		sort.Slice(txnFees, func(i, j int) bool {
			return txnFees[i].fees.Div64(txnFees[i].weight).Cmp(txnFees[j].fees.Div64(txnFees[j].weight)) < 0
		})
		for m.txpool.weight >= (txpoolMaxWeight*3)/4 && len(txnFees) > 0 {
			m.txpool.weight -= txnFees[0].weight
			txnFees = txnFees[1:]
		}
		sort.Slice(txnFees, func(i, j int) bool {
			return txnFees[i].index < txnFees[j].index
		})
		rem := m.txpool.txns[:0]
		v2rem := m.txpool.v2txns[:0]
		for _, ft := range txnFees {
			if !ft.v2 {
				rem = append(rem, m.txpool.txns[ft.index])
			} else {
				v2rem = append(v2rem, m.txpool.v2txns[ft.index])
			}
		}
		m.txpool.txns = rem
		m.txpool.v2txns = v2rem
	}

	// remove and re-add all transactions
	for txid := range m.txpool.indices {
		delete(m.txpool.indices, txid)
	}
	m.txpool.ms = consensus.NewMidState(m.tipState)
	m.txpool.txns = append(m.txpool.txns, m.txpool.lastReverted...)
	m.txpool.weight = 0
	filtered := m.txpool.txns[:0]
	for _, txn := range m.txpool.txns {
		ts := m.store.SupplementTipTransaction(txn)
		if err := consensus.ValidateTransaction(m.txpool.ms, txn, ts); err != nil {
			log.Debug("dropping invalid pool transaction", zap.Stringer("id", txn.ID()), zap.Error(err))
			continue
		}
		m.txpool.ms.ApplyTransaction(txn, ts)
		m.txpool.indices[txn.ID()] = len(m.txpool.txns)
		m.txpool.weight += m.tipState.TransactionWeight(txn)
		filtered = append(filtered, txn)
	}
	m.txpool.txns = filtered

	m.txpool.v2txns = append(m.txpool.v2txns, m.txpool.lastRevertedV2...)
	v2filtered := m.txpool.v2txns[:0]
	for _, txn := range m.txpool.v2txns {
		if err := consensus.ValidateV2Transaction(m.txpool.ms, txn); err != nil {
			log.Debug("dropping invalid pool v2 transaction", zap.Stringer("id", txn.ID()), zap.Error(err))
			continue
		}
		m.txpool.ms.ApplyV2Transaction(txn)
		m.txpool.indices[txn.ID()] = len(m.txpool.v2txns)
		m.txpool.weight += m.tipState.V2TransactionWeight(txn)
		v2filtered = append(v2filtered, txn)
	}
	m.txpool.v2txns = v2filtered
}

func (m *Manager) computeMedianFee() types.Currency {
	if m.txpool.medianFee != nil {
		return *m.txpool.medianFee
	}

	calculateBlockMedianFee := func(cs consensus.State, b types.Block) types.Currency {
		type weightedFee struct {
			weight uint64
			fee    types.Currency
		}
		var fees []weightedFee
		for _, txn := range b.Transactions {
			fees = append(fees, weightedFee{cs.TransactionWeight(txn), txn.TotalFees()})
		}
		for _, txn := range b.V2Transactions() {
			fees = append(fees, weightedFee{cs.V2TransactionWeight(txn), txn.MinerFee})
		}
		// account for the remaining space in the block, for which no fees were paid
		remaining := cs.MaxBlockWeight()
		for _, wf := range fees {
			remaining -= wf.weight
		}
		fees = append(fees, weightedFee{remaining, types.ZeroCurrency})
		sort.Slice(fees, func(i, j int) bool { return fees[i].fee.Cmp(fees[j].fee) < 0 })
		var progress uint64
		var i int
		for i = range fees {
			// use the 75th percentile
			if progress += fees[i].weight; progress > cs.MaxBlockWeight()/4 {
				break
			}
		}
		return fees[i].fee
	}
	prevFees := make([]types.Currency, 0, 10)
	for i := uint64(0); i < 10; i++ {
		index, ok1 := m.store.BestIndex(m.tipState.Index.Height - i)
		b, _, cs, ok2 := blockAndParent(m.store, index.ID)
		if ok1 && ok2 {
			prevFees = append(prevFees, calculateBlockMedianFee(cs, b))
		}
	}
	sort.Slice(prevFees, func(i, j int) bool { return prevFees[i].Cmp(prevFees[j]) < 0 })
	if len(prevFees) == 0 {
		return types.ZeroCurrency
	}
	m.txpool.medianFee = &prevFees[len(prevFees)/2]
	return *m.txpool.medianFee
}

func (m *Manager) computeParentMap() map[types.Hash256]int {
	if m.txpool.parentMap != nil {
		return m.txpool.parentMap
	}
	m.txpool.parentMap = make(map[types.Hash256]int)
	for index, txn := range m.txpool.txns {
		for i := range txn.SiacoinOutputs {
			m.txpool.parentMap[types.Hash256(txn.SiacoinOutputID(i))] = index
		}
		for i := range txn.SiafundInputs {
			m.txpool.parentMap[types.Hash256(txn.SiafundClaimOutputID(i))] = index
		}
		for i := range txn.SiafundOutputs {
			m.txpool.parentMap[types.Hash256(txn.SiafundOutputID(i))] = index
		}
		for i := range txn.FileContracts {
			m.txpool.parentMap[types.Hash256(txn.FileContractID(i))] = index
		}
	}
	for index, txn := range m.txpool.v2txns {
		txid := txn.ID()
		for i := range txn.SiacoinOutputs {
			m.txpool.parentMap[types.Hash256(txn.SiacoinOutputID(txid, i))] = index
		}
		for _, sfi := range txn.SiafundInputs {
			m.txpool.parentMap[types.Hash256(types.SiafundOutputID(sfi.Parent.ID).V2ClaimOutputID())] = index
		}
		for i := range txn.SiafundOutputs {
			m.txpool.parentMap[types.Hash256(txn.SiafundOutputID(txid, i))] = index
		}
		for i := range txn.FileContracts {
			m.txpool.parentMap[types.Hash256(txn.V2FileContractID(txid, i))] = index
		}
	}
	return m.txpool.parentMap
}

func updateTxnProofs(txn *types.V2Transaction, updateElementProof func(*types.StateElement), numLeaves uint64) (valid bool) {
	valid = true
	updateProof := func(e *types.StateElement) {
		valid = valid && e.LeafIndex < numLeaves
		if !valid || e.LeafIndex == types.UnassignedLeafIndex {
			return
		}
		*e = e.Copy()
		updateElementProof(e)
	}
	for i := range txn.SiacoinInputs {
		updateProof(&txn.SiacoinInputs[i].Parent.StateElement)
	}
	for i := range txn.SiafundInputs {
		updateProof(&txn.SiafundInputs[i].Parent.StateElement)
	}
	for i := range txn.FileContractRevisions {
		updateProof(&txn.FileContractRevisions[i].Parent.StateElement)
	}
	for i := range txn.FileContractResolutions {
		updateProof(&txn.FileContractResolutions[i].Parent.StateElement)
		if sp, ok := txn.FileContractResolutions[i].Resolution.(*types.V2StorageProof); ok {
			updateProof(&sp.ProofIndex.StateElement)
		}
	}
	return
}

func (m *Manager) revertPoolUpdate(cru consensus.RevertUpdate, cs consensus.State) {
	// applying a block can make ephemeral elements in the txpool non-ephemeral;
	// here, we undo that
	var uncreated map[types.Hash256]bool
	replaceEphemeral := func(id types.Hash256, e *types.StateElement) {
		if e.LeafIndex == types.UnassignedLeafIndex {
			return
		} else if uncreated == nil {
			uncreated = make(map[types.Hash256]bool)
			for _, sced := range cru.SiacoinElementDiffs() {
				if sced.Created {
					uncreated[types.Hash256(sced.SiacoinElement.ID)] = true
				}
			}
			for _, sfed := range cru.SiafundElementDiffs() {
				if sfed.Created {
					uncreated[types.Hash256(sfed.SiafundElement.ID)] = true
				}
			}
			for _, fced := range cru.FileContractElementDiffs() {
				if fced.Created {
					uncreated[types.Hash256(fced.FileContractElement.ID)] = true
				}
			}
			for _, v2fced := range cru.V2FileContractElementDiffs() {
				if v2fced.Created {
					uncreated[types.Hash256(v2fced.V2FileContractElement.ID)] = true
				}
			}
		}
		if uncreated[id] {
			*e = types.StateElement{LeafIndex: types.UnassignedLeafIndex}
		}
	}
	for _, txn := range m.txpool.v2txns {
		for i, si := range txn.SiacoinInputs {
			replaceEphemeral(types.Hash256(si.Parent.ID), &txn.SiacoinInputs[i].Parent.StateElement)
		}
		for i, si := range txn.SiafundInputs {
			replaceEphemeral(types.Hash256(si.Parent.ID), &txn.SiafundInputs[i].Parent.StateElement)
		}
		for i, fcr := range txn.FileContractRevisions {
			replaceEphemeral(types.Hash256(fcr.Parent.ID), &txn.FileContractRevisions[i].Parent.StateElement)
		}
		for i, fcr := range txn.FileContractResolutions {
			replaceEphemeral(types.Hash256(fcr.Parent.ID), &txn.FileContractResolutions[i].Parent.StateElement)
		}
	}

	rem := m.txpool.v2txns[:0]
	for _, txn := range m.txpool.v2txns {
		if updateTxnProofs(&txn, cru.UpdateElementProof, cs.Elements.NumLeaves) {
			rem = append(rem, txn)
		}
	}
	m.txpool.v2txns = rem
}

func (m *Manager) applyPoolUpdate(cau consensus.ApplyUpdate, cs consensus.State) {
	// applying a block can make ephemeral elements in the txpool non-ephemeral
	var newElements map[types.Hash256]types.StateElement
	replaceEphemeral := func(id types.Hash256, e *types.StateElement) {
		if e.LeafIndex != types.UnassignedLeafIndex {
			return
		} else if newElements == nil {
			newElements = make(map[types.Hash256]types.StateElement)

			for _, sced := range cau.SiacoinElementDiffs() {
				if sced.Created {
					newElements[types.Hash256(sced.SiacoinElement.ID)] = sced.SiacoinElement.StateElement.Share()
				}
			}
			for _, sfed := range cau.SiafundElementDiffs() {
				if sfed.Created {
					newElements[types.Hash256(sfed.SiafundElement.ID)] = sfed.SiafundElement.StateElement.Share()
				}
			}
			for _, fced := range cau.FileContractElementDiffs() {
				if fced.Created {
					newElements[types.Hash256(fced.FileContractElement.ID)] = fced.FileContractElement.StateElement.Share()
				}
			}
			for _, v2fced := range cau.V2FileContractElementDiffs() {
				if v2fced.Created {
					newElements[types.Hash256(v2fced.V2FileContractElement.ID)] = v2fced.V2FileContractElement.StateElement.Share()
				}
			}
		}
		if se, ok := newElements[id]; ok {
			*e = se.Share()
		}
	}
	for _, txn := range m.txpool.v2txns {
		for i, si := range txn.SiacoinInputs {
			replaceEphemeral(types.Hash256(si.Parent.ID), &txn.SiacoinInputs[i].Parent.StateElement)
		}
		for i, si := range txn.SiafundInputs {
			replaceEphemeral(types.Hash256(si.Parent.ID), &txn.SiafundInputs[i].Parent.StateElement)
		}
		for i, fcr := range txn.FileContractRevisions {
			replaceEphemeral(types.Hash256(fcr.Parent.ID), &txn.FileContractRevisions[i].Parent.StateElement)
		}
		for i, fcr := range txn.FileContractResolutions {
			replaceEphemeral(types.Hash256(fcr.Parent.ID), &txn.FileContractResolutions[i].Parent.StateElement)
		}
	}

	rem := m.txpool.v2txns[:0]
	for _, txn := range m.txpool.v2txns {
		if updateTxnProofs(&txn, cau.UpdateElementProof, cs.Elements.NumLeaves) {
			rem = append(rem, txn)
		}
	}
	m.txpool.v2txns = rem
}

// PoolTransaction returns the transaction with the specified ID, if it is
// currently in the pool.
func (m *Manager) PoolTransaction(id types.TransactionID) (types.Transaction, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()
	i, ok := m.txpool.indices[id]
	if !ok {
		return types.Transaction{}, false
	}
	return m.txpool.txns[i], ok
}

// PoolTransactions returns the transactions currently in the txpool. Any prefix
// of the returned slice constitutes a valid transaction set.
func (m *Manager) PoolTransactions() []types.Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()
	return append([]types.Transaction(nil), m.txpool.txns...)
}

// V2PoolTransaction returns the v2 transaction with the specified ID, if it is
// currently in the pool.
func (m *Manager) V2PoolTransaction(id types.TransactionID) (types.V2Transaction, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()
	i, ok := m.txpool.indices[id]
	if !ok {
		return types.V2Transaction{}, false
	}
	return m.txpool.v2txns[i].DeepCopy(), ok
}

// V2PoolTransactions returns the v2 transactions currently in the txpool. Any
// prefix of the returned slice constitutes a valid transaction set.
func (m *Manager) V2PoolTransactions() []types.V2Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()
	v2txns := make([]types.V2Transaction, len(m.txpool.v2txns))
	for i, txn := range m.txpool.v2txns {
		v2txns[i] = txn.DeepCopy()
	}
	return v2txns
}

// TransactionsForPartialBlock returns the transactions in the txpool with the
// specified hashes.
func (m *Manager) TransactionsForPartialBlock(missing []types.Hash256) (txns []types.Transaction, v2txns []types.V2Transaction) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()
	want := make(map[types.Hash256]bool)
	for _, h := range missing {
		want[h] = true
	}
	// TODO: might want to cache these
	for _, txn := range m.txpool.txns {
		if h := txn.FullHash(); want[h] {
			txns = append(txns, txn)
			if delete(want, h); len(want) == 0 {
				return
			}
		}
	}
	for _, txn := range m.txpool.v2txns {
		if h := txn.FullHash(); want[h] {
			v2txns = append(v2txns, txn)
			if delete(want, h); len(want) == 0 {
				return
			}
		}
	}
	return
}

// RecommendedFee returns the recommended fee (per weight unit) to ensure a high
// probability of inclusion in the next block.
func (m *Manager) RecommendedFee() types.Currency {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()

	medianFee := m.computeMedianFee()

	// calculate a fee relative to the total txpool weight
	//
	// NOTE: empirically, the average txn weight is ~1000
	estPoolWeight := m.txpool.weight + uint64(10e3)
	// the target weight of the pool is 3e6, with an average fee of 1 SC / 1e3;
	// compute targetFee * (estPoolWeight / targetWeight)^3
	//
	// NOTE: alternating the multiplications and divisions is crucial here to
	// prevent immediate values from overflowing
	const targetWeight = 3e6
	weightFee := types.Siacoins(1).Div64(1000).
		Mul64(estPoolWeight).Div64(targetWeight).Mul64(estPoolWeight).
		Div64(targetWeight).Mul64(estPoolWeight).Div64(targetWeight)

	// finally, an absolute minimum fee: 1 SC / 100 KB
	minFee := types.Siacoins(1).Div64(100e3)

	// use the largest of all calculated fees
	fee := medianFee
	if fee.Cmp(weightFee) < 0 {
		fee = weightFee
	}
	if fee.Cmp(minFee) < 0 {
		fee = minFee
	}
	return fee
}

// UnconfirmedParents returns the transactions in the txpool that are referenced
// by txn.
func (m *Manager) UnconfirmedParents(txn types.Transaction) []types.Transaction {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()

	parentMap := m.computeParentMap()
	var parents []types.Transaction
	seen := make(map[int]bool)
	check := func(id types.Hash256) {
		if index, ok := parentMap[id]; ok && !seen[index] {
			seen[index] = true
			parents = append(parents, m.txpool.txns[index])
		}
	}
	addParents := func(txn types.Transaction) {
		for _, sci := range txn.SiacoinInputs {
			check(types.Hash256(sci.ParentID))
		}
		for _, sfi := range txn.SiafundInputs {
			check(types.Hash256(sfi.ParentID))
		}
		for _, fcr := range txn.FileContractRevisions {
			check(types.Hash256(fcr.ParentID))
		}
		for _, sp := range txn.StorageProofs {
			check(types.Hash256(sp.ParentID))
		}
	}

	// check txn, then keep checking parents until done
	addParents(txn)
	for {
		n := len(parents)
		for _, txn := range parents {
			addParents(txn)
		}
		if len(parents) == n {
			break
		}
	}
	// reverse so that parents always come before children
	for i := 0; i < len(parents)/2; i++ {
		j := len(parents) - 1 - i
		parents[i], parents[j] = parents[j], parents[i]
	}
	return parents
}

// V2TransactionSet returns the full transaction set and basis necessary for
// broadcasting a transaction. If the provided basis does not match the current
// tip, the transaction will be updated. The transaction set includes the parents
// and the transaction itself in an order valid for broadcasting.
func (m *Manager) V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()

	// update the transaction's basis to match tip
	txns, err := m.updateV2TransactionProofs([]types.V2Transaction{txn}, basis, m.tipState.Index)
	if err != nil {
		return types.ChainIndex{}, nil, fmt.Errorf("failed to update transaction set basis: %w", err)
	} else if len(txns) == 0 {
		return types.ChainIndex{}, nil, errors.New("no transactions to broadcast")
	}
	txn = txns[0]

	// get the transaction's parents
	parentMap := m.computeParentMap()
	var parents []types.V2Transaction
	seen := make(map[int]bool)
	check := func(id types.Hash256) {
		if index, ok := parentMap[id]; ok && !seen[index] {
			seen[index] = true
			parents = append(parents, m.txpool.v2txns[index])
		}
	}
	addParents := func(txn types.V2Transaction) {
		for _, sci := range txn.SiacoinInputs {
			check(types.Hash256(sci.Parent.ID))
		}
		for _, sfi := range txn.SiafundInputs {
			check(types.Hash256(sfi.Parent.ID))
		}
		for _, fcr := range txn.FileContractRevisions {
			check(types.Hash256(fcr.Parent.ID))
		}
		for _, fcr := range txn.FileContractResolutions {
			check(types.Hash256(fcr.Parent.ID))
		}
	}

	// check txn, then keep checking parents until done
	addParents(txn)
	for {
		n := len(parents)
		for _, txn := range parents {
			addParents(txn)
		}
		if len(parents) == n {
			break
		}
	}
	// reverse so that parents always come before children
	for i := 0; i < len(parents)/2; i++ {
		j := len(parents) - 1 - i
		parents[i], parents[j] = parents[j], parents[i]
	}
	return m.tipState.Index, append(parents, txn), nil
}

func (m *Manager) checkTxnSet(txns []types.Transaction, v2txns []types.V2Transaction) (bool, error) {
	allInPool := true
	checkPool := func(txid types.TransactionID) types.TransactionID {
		if allInPool {
			if _, ok := m.txpool.indices[txid]; !ok {
				allInPool = false
			}
		}
		return txid
	}
	h := types.NewHasher()
	for _, txn := range txns {
		checkPool(txn.ID()).EncodeTo(h.E)
	}
	for _, txn := range v2txns {
		checkPool(txn.ID()).EncodeTo(h.E)
	}
	setID := h.Sum()
	if err := m.txpool.invalidTxnSets[setID]; allInPool || err != nil {
		return true, err
	}

	// validate
	markBadTxnSet := func(err error) error {
		const maxInvalidTxnSets = 1000
		if len(m.txpool.invalidTxnSets) >= maxInvalidTxnSets {
			// forget a random entry
			for id := range m.txpool.invalidTxnSets {
				delete(m.txpool.invalidTxnSets, id)
				break
			}
		}
		m.txpool.invalidTxnSets[setID] = err
		return err
	}
	ms := consensus.NewMidState(m.tipState)
	for _, txn := range txns {
		ts := m.store.SupplementTipTransaction(txn)
		if err := consensus.ValidateTransaction(ms, txn, ts); err != nil {
			return false, markBadTxnSet(fmt.Errorf("transaction %v is invalid: %w", txn.ID(), err))
		}
		ms.ApplyTransaction(txn, ts)
	}
	for _, txn := range v2txns {
		if err := consensus.ValidateV2Transaction(ms, txn); err != nil {
			return false, markBadTxnSet(fmt.Errorf("v2 transaction %v is invalid: %w", txn.ID(), err))
		}
		ms.ApplyV2Transaction(txn)
	}
	return false, nil
}

func (m *Manager) updateV2TransactionProofs(txns []types.V2Transaction, from, to types.ChainIndex) ([]types.V2Transaction, error) {
	revert, apply, err := m.reorgPath(from, to)
	if err != nil {
		return nil, fmt.Errorf("couldn't determine reorg path from %v to %v: %w", from, to, err)
	} else if len(revert)+len(apply) > 144 {
		return nil, fmt.Errorf("reorg path from %v to %v is too long (-%v +%v)", from, to, len(revert), len(apply))
	}
	for _, index := range revert {
		b, bs, cs, ok := blockAndParent(m.store, index.ID)
		if !ok {
			return nil, fmt.Errorf("missing reverted block at index %v", index)
		} else if bs == nil {
			bs = new(consensus.V1BlockSupplement)
		}
		cru := consensus.RevertBlock(cs, b, *bs)
		for i := range txns {
			if !updateTxnProofs(&txns[i], cru.UpdateElementProof, cs.Elements.NumLeaves) {
				return nil, fmt.Errorf("transaction %v references element that does not exist in our chain", txns[i].ID())
			}
		}
	}

	for _, index := range apply {
		b, bs, cs, ok := blockAndParent(m.store, index.ID)
		if !ok {
			return nil, fmt.Errorf("missing applied block at index %v", index)
		} else if bs == nil {
			bs = new(consensus.V1BlockSupplement)
		}
		ancestorTimestamp, _ := m.store.AncestorTimestamp(b.ParentID)
		cs, cau := consensus.ApplyBlock(cs, b, *bs, ancestorTimestamp)

		// get the transactions that were confirmed in this block
		confirmedTxns := make(map[types.TransactionID]bool)
		for _, txn := range b.V2Transactions() {
			confirmedTxns[txn.ID()] = true
		}
		confirmedStateElements := make(map[types.Hash256]types.StateElement)
		for _, sced := range cau.SiacoinElementDiffs() {
			if sced.Created {
				confirmedStateElements[types.Hash256(sced.SiacoinElement.ID)] = sced.SiacoinElement.StateElement.Share()
			}
		}
		for _, sfed := range cau.SiafundElementDiffs() {
			if sfed.Created {
				confirmedStateElements[types.Hash256(sfed.SiafundElement.ID)] = sfed.SiafundElement.StateElement.Share()
			}
		}

		rem := txns[:0]
		for i := range txns {
			if confirmedTxns[txns[i].ID()] {
				// remove any transactions that were confirmed in this block
				continue
			}

			// update the state elements for any confirmed ephemeral elements
			for j := range txns[i].SiacoinInputs {
				if txns[i].SiacoinInputs[j].Parent.StateElement.LeafIndex != types.UnassignedLeafIndex {
					continue
				}
				se, ok := confirmedStateElements[types.Hash256(txns[i].SiacoinInputs[j].Parent.ID)]
				if !ok {
					continue
				}
				txns[i].SiacoinInputs[j].Parent.StateElement = se.Share()
			}

			// update the state elements for any confirmed ephemeral elements
			for j := range txns[i].SiafundInputs {
				if txns[i].SiafundInputs[j].Parent.StateElement.LeafIndex != types.UnassignedLeafIndex {
					continue
				}
				se, ok := confirmedStateElements[types.Hash256(txns[i].SiafundInputs[j].Parent.ID)]
				if !ok {
					continue
				}
				txns[i].SiafundInputs[j].Parent.StateElement = se.Share()
			}

			// NOTE: all elements guaranteed to exist from here on, so no
			// need to check this return value
			updateTxnProofs(&txns[i], cau.UpdateElementProof, cs.Elements.NumLeaves)
			rem = append(rem, txns[i])
		}
		txns = rem
	}
	return txns, nil
}

// AddPoolTransactions validates a transaction set and adds it to the txpool. If
// any transaction references an element (SiacoinOutput, SiafundOutput, or
// FileContract) not present in the blockchain, that element must be created by
// a previous transaction in the set.
//
// If any transaction in the set is invalid, the entire set is rejected and none
// of the transactions are added to the pool. If all of the transactions are
// already known to the pool, AddPoolTransactions returns true.
func (m *Manager) AddPoolTransactions(txns []types.Transaction) (known bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()

	if known, err := m.checkTxnSet(txns, nil); known || err != nil {
		return known, err
	}

	for _, txn := range txns {
		txid := txn.ID()
		if _, ok := m.txpool.indices[txid]; ok {
			continue // skip transactions already in the pool
		}
		ts := m.store.SupplementTipTransaction(txn)
		if err := consensus.ValidateTransaction(m.txpool.ms, txn, ts); err != nil {
			m.txpool.ms = nil // force revalidation next time the pool is queried
			return false, fmt.Errorf("transaction %v conflicts with pool: %w", txid, err)
		}
		m.txpool.ms.ApplyTransaction(txn, ts)
		m.txpool.indices[txid] = len(m.txpool.txns)
		m.txpool.txns = append(m.txpool.txns, txn)
		m.txpool.weight += m.tipState.TransactionWeight(txn)
	}
	// invalidate caches
	m.txpool.medianFee = nil
	m.txpool.parentMap = nil

	// release lock while notifying listeners
	fns := make([]func(), 0, len(m.onPool))
	for _, fn := range m.onPool {
		fns = append(fns, fn)
	}
	m.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
	m.mu.Lock()

	return false, nil
}

// UpdateV2TransactionSet updates the basis of a transaction set from "from" to "to".
// If from and to are equal, the transaction set is returned as-is.
// Any transactions that were confirmed are removed from the set.
// Any ephemeral state elements that were created by an update are updated.
//
// If it is undesirable to modify the transaction set, deep-copy it
// before calling this method.
func (m *Manager) UpdateV2TransactionSet(txns []types.V2Transaction, from, to types.ChainIndex) ([]types.V2Transaction, error) {
	if from == to {
		return txns, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateV2TransactionProofs(txns, from, to)
}

// AddV2PoolTransactions validates a transaction set and adds it to the txpool.
// If any transaction references an element (SiacoinOutput, SiafundOutput, or
// FileContract) not present in the blockchain, that element must be created by
// a previous transaction in the set.
//
// If any transaction in the set is invalid, the entire set is rejected and none
// of the transactions are added to the pool. If all of the transactions are
// already known to the pool, AddV2PoolTransactions returns true.
//
// Since v2 transactions include Merkle proofs, AddV2PoolTransactions takes an
// index specifying the accumulator state for which those proofs are assumed to
// be valid. If that index differs from the Manager's current tip, the Merkle
// proofs will be updated accordingly. The original transactions are not
// modified and none of their memory is retained.
func (m *Manager) AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (known bool, _ error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revalidatePool()

	// take ownership of Merkle proofs, and update them to the current tip
	txns = append([]types.V2Transaction(nil), txns...)
	for i := range txns {
		txns[i] = txns[i].DeepCopy()
	}
	txns, err := m.updateV2TransactionProofs(txns, basis, m.tipState.Index)
	if err != nil {
		return false, fmt.Errorf("failed to update set basis: %w", err)
	}

	if known, err := m.checkTxnSet(nil, txns); known || err != nil {
		return known, err
	}

	for _, txn := range txns {
		txid := txn.ID()
		if _, ok := m.txpool.indices[txid]; ok {
			continue // skip transactions already in the pool
		}
		if err := consensus.ValidateV2Transaction(m.txpool.ms, txn); err != nil {
			m.txpool.ms = nil // force revalidation next time the pool is queried
			return false, fmt.Errorf("transaction %v conflicts with pool: %w", txid, err)
		}
		m.txpool.ms.ApplyV2Transaction(txn)
		m.txpool.indices[txid] = len(m.txpool.txns)
		m.txpool.v2txns = append(m.txpool.v2txns, txn)
		m.txpool.weight += m.tipState.V2TransactionWeight(txn)
	}
	// invalidate caches
	m.txpool.medianFee = nil
	m.txpool.parentMap = nil

	// release lock while notifying listeners
	fns := make([]func(), 0, len(m.onPool))
	for _, fn := range m.onPool {
		fns = append(fns, fn)
	}
	m.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
	m.mu.Lock()

	return false, nil
}

// NewManager returns a Manager initialized with the provided Store and State.
func NewManager(store Store, cs consensus.State, opts ...ManagerOption) *Manager {
	m := &Manager{
		log:           zap.NewNop(),
		store:         store,
		tipState:      cs,
		onReorg:       make(map[[16]byte]func(types.ChainIndex)),
		onPool:        make(map[[16]byte]func()),
		invalidBlocks: make(map[types.BlockID]error),
	}
	for _, opt := range opts {
		opt(m)
	}
	m.txpool.indices = make(map[types.TransactionID]int)
	m.txpool.invalidTxnSets = make(map[types.Hash256]error)
	return m
}
