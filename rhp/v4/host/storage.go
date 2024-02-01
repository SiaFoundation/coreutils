package host

import (
	"errors"
	"sync"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type MemSectorStore struct {
	maxSectors uint64

	mu      sync.Mutex
	sectors map[types.Hash256][rhp.SectorSize]byte
}

func NewMemSectorStore(maxSectors uint64) *MemSectorStore {
	return &MemSectorStore{
		maxSectors: maxSectors,
		sectors:    make(map[types.Hash256][rhp.SectorSize]byte),
	}
}

func (m *MemSectorStore) Read(root types.Hash256) ([rhp.SectorSize]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sector, ok := m.sectors[root]
	return sector, ok
}

func (m *MemSectorStore) Write(root types.Hash256, sector [rhp.SectorSize]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if uint64(len(m.sectors)) > m.maxSectors {
		return errors.New("not enough storage")
	}
	m.sectors[root] = sector
	return nil
}

func (m *MemSectorStore) Delete(root types.Hash256) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.sectors[root]; !ok {
		return errors.New("sector not found")
	}
	delete(m.sectors, root)
	return nil
}
