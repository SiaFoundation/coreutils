package host

import (
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
)

type (
	contract struct {
		mu sync.Mutex

		// the element proof must be updated every block
		element types.FileContractElement
		// the revision is updated during the revise contract RPC and broadcast
		// right before the proof window
		revision types.V2FileContract
		roots    []types.Hash256

		confirmationIndex *types.ChainIndex
		revisionIndex     *types.ChainIndex
		proofIndex        *types.ChainIndex

		confirmedRevisionNumber uint64
	}

	MemContractStore struct {
		log *zap.Logger

		updates []*chain.ApplyUpdate

		mu        sync.Mutex
		contracts map[types.Hash256]contract
	}
)

func (ms *MemContractStore) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	ms.updates = append(ms.updates, cau)

	if mayCommit {
		log := ms.log.Named("consensus.apply")
		// commit the updates
		ms.mu.Lock()
		defer ms.mu.Unlock()
		for _, update := range ms.updates {
			update.ForEachFileContractElement(func(fce types.FileContractElement, rev *types.FileContractElement, resolved, valid bool) {
				if resolved {
					log.Debug("resolved contract", zap.Stringer("id", fce.ID), zap.Bool("valid", valid))
					delete(ms.contracts, fce.ID)
					return
				}
			})
		}
	}

	return nil
}

func (ms *MemContractStore) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	if len(ms.updates) != 0 && ms.updates[len(ms.updates)-1].State.Index == cru.State.Index {
		ms.updates = ms.updates[:len(ms.updates)-1]
		return nil
	}

	panic("implement me")
}

// AddContract adds a contract to the store.
func (ms *MemContractStore) AddContract(fc types.V2FileContract) error {
	panic("implement me")
}
