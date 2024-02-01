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
		revision types.V2FileContractRevision
		roots    []types.Hash256

		revisionIndex *types.ChainIndex
		proofIndex    *types.ChainIndex

		confirmedRevisionNumber uint64
	}

	// A MemContractStore manages the state of file contracts in memory.
	MemContractStore struct {
		log *zap.Logger

		updates []*chain.ApplyUpdate

		mu        sync.Mutex
		contracts map[types.Hash256]*contract
	}
)

// ProcessChainApplyUpdate implements the chain.Subscriber interface
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

// ProcessChainRevertUpdate implements the chain.Subscriber interface
func (ms *MemContractStore) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error {
	if len(ms.updates) != 0 && ms.updates[len(ms.updates)-1].State.Index == cru.State.Index {
		ms.updates = ms.updates[:len(ms.updates)-1]
		return nil
	}

	panic("implement me")
}

// Revision returns the current revision of the contract.
func (ms *MemContractStore) Revision() types.V2FileContractRevision {
	panic("implement me")
}

// AddContract adds a contract to the store.
func (ms *MemContractStore) AddContract(fe types.V2FileContractElement) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, ok := ms.contracts[fe.ID]; ok {
		return ErrContractExists
	}

	ms.contracts[fe.ID] = &contract{
		revision: types.V2FileContractRevision{
			Parent:   fe,
			Revision: fe.V2FileContract,
		},
	}
	return nil
}
