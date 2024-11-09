package chain

import "go.uber.org/zap"

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
