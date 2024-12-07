package testutil

import (
	"sync"
	"time"

	"go.sia.tech/coreutils/syncer"
)

// A EphemeralPeerStore is an in-memory implementation of a PeerStore.
type EphemeralPeerStore struct {
	mu    sync.Mutex
	peers map[string]syncer.PeerInfo
}

// AddPeer adds a peer to the store. If the peer already exists, nil should
// be returned.
func (ps *EphemeralPeerStore) AddPeer(addr string) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if _, ok := ps.peers[addr]; ok {
		return nil
	}
	ps.peers[addr] = syncer.PeerInfo{Address: addr}
	return nil
}

// Peers returns the set of known peers.
func (ps *EphemeralPeerStore) Peers() ([]syncer.PeerInfo, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	var peers []syncer.PeerInfo
	for _, p := range ps.peers {
		peers = append(peers, p)
	}
	return peers, nil
}

// PeerInfo returns the metadata for the specified peer or ErrPeerNotFound
// if the peer wasn't found in the store.
func (ps *EphemeralPeerStore) PeerInfo(addr string) (syncer.PeerInfo, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p, ok := ps.peers[addr]
	if !ok {
		return syncer.PeerInfo{}, syncer.ErrPeerNotFound
	}
	return p, nil
}

// UpdatePeerInfo updates the metadata for the specified peer. If the peer
// is not found, the error should be ErrPeerNotFound.
func (ps *EphemeralPeerStore) UpdatePeerInfo(addr string, fn func(*syncer.PeerInfo)) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	p := ps.peers[addr]
	fn(&p)
	ps.peers[addr] = p
	return nil
}

// Ban temporarily bans one or more IPs. The addr should either be a single
// IP with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
func (ps *EphemeralPeerStore) Ban(addr string, duration time.Duration, reason string) error {
	return nil
}

// Banned returns false
func (ps *EphemeralPeerStore) Banned(addr string) (bool, error) { return false, nil }

var _ syncer.PeerStore = (*EphemeralPeerStore)(nil)

// NewEphemeralPeerStore returns a new EphemeralPeerStore.
func NewEphemeralPeerStore() *EphemeralPeerStore {
	return &EphemeralPeerStore{
		peers: make(map[string]syncer.PeerInfo),
	}
}
