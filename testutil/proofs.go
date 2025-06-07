package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
)

// ElementStateStore is a store that holds state elements in memory.
// It is primarily used for testing proof updaters and other components that
// need to access state elements without persisting them to disk.
type ElementStateStore struct {
	chain *chain.Manager

	mu                   sync.Mutex
	tip                  types.ChainIndex
	siacoinElements      map[types.SiacoinOutputID]types.SiacoinElement
	siafundElements      map[types.SiafundOutputID]types.SiafundElement
	fileContractElements map[types.FileContractID]types.FileContractElement
	chainIndexElements   map[types.ChainIndex]types.ChainIndexElement
}

// Sync triggers a sync of the ElementStateStore with the chain manager.
// It should not normally be called directly, as it is automatically
// triggered by the chain manager when a reorg occurs.
func (es *ElementStateStore) Sync() error {
	es.mu.Lock()
	defer es.mu.Unlock()

	for {
		reverted, applied, err := es.chain.UpdatesSince(es.tip, 1000)
		if err != nil {
			return fmt.Errorf("failed to get updates since %q: %w", es.tip, err)
		} else if len(reverted) == 0 && len(applied) == 0 {
			return nil
		}

		for _, cru := range reverted {
			revertedIndex := types.ChainIndex{
				Height: cru.State.Index.Height + 1,
				ID:     cru.Block.ID(),
			}
			for _, sce := range cru.SiacoinElementDiffs() {
				switch {
				case sce.Created && sce.Spent:
					continue
				case sce.Created:
					delete(es.siacoinElements, sce.SiacoinElement.ID)
				case sce.Spent:
					es.siacoinElements[sce.SiacoinElement.ID] = sce.SiacoinElement
				}
			}
			for _, sfe := range cru.SiafundElementDiffs() {
				switch {
				case sfe.Created && sfe.Spent:
					continue
				case sfe.Created:
					delete(es.siafundElements, sfe.SiafundElement.ID)
				case sfe.Spent:
					es.siafundElements[sfe.SiafundElement.ID] = sfe.SiafundElement
				}
			}
			for _, fce := range cru.FileContractElementDiffs() {
				switch {
				case fce.Resolved:
					es.fileContractElements[fce.FileContractElement.ID] = fce.FileContractElement
				case fce.Revision != nil:
					es.fileContractElements[fce.FileContractElement.ID] = fce.FileContractElement // revert the revision
				case fce.Created:
					delete(es.fileContractElements, fce.FileContractElement.ID)
				}
			}
			delete(es.chainIndexElements, revertedIndex)
			for id, sce := range es.siacoinElements {
				cru.UpdateElementProof(&sce.StateElement)
				es.siacoinElements[id] = sce.Copy()
			}
			for id, sfe := range es.siafundElements {
				cru.UpdateElementProof(&sfe.StateElement)
				es.siafundElements[id] = sfe.Copy()
			}
			for id, fce := range es.fileContractElements {
				cru.UpdateElementProof(&fce.StateElement)
				es.fileContractElements[id] = fce.Copy()
			}
			for id, cie := range es.chainIndexElements {
				cru.UpdateElementProof(&cie.StateElement)
				es.chainIndexElements[id] = cie.Copy()
			}
			es.tip = cru.State.Index
		}
		for _, cau := range applied {
			for _, sce := range cau.SiacoinElementDiffs() {
				switch {
				case sce.Created && sce.Spent:
					continue
				case sce.Created:
					es.siacoinElements[sce.SiacoinElement.ID] = sce.SiacoinElement
				case sce.Spent:
					delete(es.siacoinElements, sce.SiacoinElement.ID)
				}
			}
			for _, sfe := range cau.SiafundElementDiffs() {
				switch {
				case sfe.Created && sfe.Spent:
					continue
				case sfe.Created:
					es.siafundElements[sfe.SiafundElement.ID] = sfe.SiafundElement
				case sfe.Spent:
					delete(es.siafundElements, sfe.SiafundElement.ID)
				}
			}
			for _, fce := range cau.FileContractElementDiffs() {
				switch {
				case fce.Resolved:
					delete(es.fileContractElements, fce.FileContractElement.ID)
				case fce.Revision != nil:
					es.fileContractElements[fce.FileContractElement.ID], _ = fce.RevisionElement() // nil already checked
				case fce.Created:
					es.fileContractElements[fce.FileContractElement.ID] = fce.FileContractElement
				}
			}
			es.chainIndexElements[cau.State.Index] = cau.ChainIndexElement()
			for id, sce := range es.siacoinElements {
				cau.UpdateElementProof(&sce.StateElement)
				es.siacoinElements[id] = sce.Copy()
			}
			for id, sfe := range es.siafundElements {
				cau.UpdateElementProof(&sfe.StateElement)
				es.siafundElements[id] = sfe.Copy()
			}
			for id, fce := range es.fileContractElements {
				cau.UpdateElementProof(&fce.StateElement)
				es.fileContractElements[id] = fce.Copy()
			}
			for id, cie := range es.chainIndexElements {
				cau.UpdateElementProof(&cie.StateElement)
				es.chainIndexElements[id] = cie.Copy()
			}
			es.tip = cau.State.Index
		}
	}
}

// Wait blocks until the ElementStateStore is synced with the chain manager
// or the context cancelled.
func (es *ElementStateStore) Wait(tb testing.TB) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			tb.Fatal("store sync timed out")
		case <-time.After(time.Millisecond):
			es.mu.Lock()
			synced := es.tip == es.chain.Tip()
			es.mu.Unlock()
			if synced {
				return
			}
		}
	}
}

// SiacoinElements returns a copy of all SiacoinElements in the store.
func (es *ElementStateStore) SiacoinElements() (types.ChainIndex, []types.SiacoinElement) {
	es.mu.Lock()
	defer es.mu.Unlock()
	elements := make([]types.SiacoinElement, 0, len(es.siacoinElements))
	for _, sce := range es.siacoinElements {
		sce.StateElement = sce.StateElement.Copy()
		elements = append(elements, sce)
	}
	return es.tip, elements
}

// SiacoinElement returns the SiacoinElement with the given ID.
func (es *ElementStateStore) SiacoinElement(id types.SiacoinOutputID) (types.ChainIndex, types.SiacoinElement, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	sce, ok := es.siacoinElements[id]
	sce.StateElement = sce.StateElement.Copy()
	return es.tip, sce, ok
}

// SiafundElement returns the SiafundElement with the given ID.
func (es *ElementStateStore) SiafundElement(id types.SiafundOutputID) (types.ChainIndex, types.SiafundElement, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	sfe, ok := es.siafundElements[id]
	sfe.StateElement = sfe.StateElement.Copy()
	return es.tip, sfe, ok
}

// FileContractElement returns the FileContractElement with the given ID.
func (es *ElementStateStore) FileContractElement(id types.FileContractID) (types.ChainIndex, types.FileContractElement, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	fce, ok := es.fileContractElements[id]
	fce.StateElement = fce.StateElement.Copy()
	return es.tip, fce, ok
}

// ChainIndexElement returns the ChainIndexElement for the given chain index.
func (es *ElementStateStore) ChainIndexElement(index types.ChainIndex) (types.ChainIndexElement, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	cie, ok := es.chainIndexElements[index]
	cie.StateElement = cie.StateElement.Copy()
	return cie, ok
}

// NewElementStateStore creates a new ElementStateStore
func NewElementStateStore(tb testing.TB, cm *chain.Manager) *ElementStateStore {
	store := &ElementStateStore{
		chain:                cm,
		siacoinElements:      make(map[types.SiacoinOutputID]types.SiacoinElement),
		siafundElements:      make(map[types.SiafundOutputID]types.SiafundElement),
		fileContractElements: make(map[types.FileContractID]types.FileContractElement),
		chainIndexElements:   make(map[types.ChainIndex]types.ChainIndexElement),
	}

	reorgCh := make(chan struct{}, 1)
	cancel := cm.OnReorg(func(types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})

	ctx := tb.Context()
	go func() {
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reorgCh:
			}

			if err := store.Sync(); err != nil {
				panic(err)
			}
		}
	}()
	return store
}
