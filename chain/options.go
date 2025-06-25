package chain

import (
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

// A ManagerOption sets an optional parameter on a Manager.
type ManagerOption func(*Manager)

// WithLog sets the logger used by the Manager.
func WithLog(l *zap.Logger) ManagerOption {
	return func(m *Manager) {
		m.log = l
	}
}

// WithPruneTarget sets the target number of blocks to store.
func WithPruneTarget(n uint64) ManagerOption {
	return func(m *Manager) {
		m.pruneTarget = n
	}
}

// WithExpiringContractOrder sets the order of file contracts that are expiring
// at a given height. This is used to work around a bug in the chain db
// where the order of expiring file contracts is not preserved across
// reorgs.
func WithExpiringContractOrder(overwriteIDs map[types.BlockID][]types.FileContractID) ManagerOption {
	return func(m *Manager) {
		m.expiringFileContractOrder = overwriteIDs
	}
}
