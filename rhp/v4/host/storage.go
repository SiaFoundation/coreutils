package host

import (
	"errors"
	"sync"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

// A MemSectorStore manages the state of sectors in memory.
type MemSectorStore struct {
	maxSectors uint64

	mu      sync.Mutex
	sectors map[types.Hash256][rhp.SectorSize]byte
}

// NewMemSectorStore creates a new MemSectorStore with a maximum number of
// sectors.
func NewMemSectorStore(maxSectors uint64) *MemSectorStore {
	return &MemSectorStore{
		maxSectors: maxSectors,
		sectors:    make(map[types.Hash256][rhp.SectorSize]byte),
	}
}

// Read retrieves a sector from the store.
func (m *MemSectorStore) Read(root types.Hash256) ([rhp.SectorSize]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sector, ok := m.sectors[root]
	return sector, ok
}

// Write stores a sector in the store.
func (m *MemSectorStore) Write(root types.Hash256, sector [rhp.SectorSize]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if uint64(len(m.sectors)) > m.maxSectors {
		return errors.New("not enough storage")
	}
	m.sectors[root] = sector
	return nil
}

// Delete removes a sector from the store.
func (m *MemSectorStore) Delete(root types.Hash256) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sectors[root]; !ok {
		return errors.New("sector not found")
	}
	delete(m.sectors, root)
	return nil
}
