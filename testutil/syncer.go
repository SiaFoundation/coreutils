package testutil

import (
	"fmt"
	"net"
	"net/netip"
	"sync"
	"time"

	"go.sia.tech/coreutils/syncer"
)

// A EphemeralPeerStore is an in-memory implementation of a PeerStore.
type EphemeralPeerStore struct {
	mu    sync.Mutex
	peers map[string]syncer.PeerInfo
	bans  map[netip.Prefix]time.Time
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
func (ps *EphemeralPeerStore) Ban(addr string, duration time.Duration, _ string) error {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	subnet, err := netip.ParsePrefix(addr)
	if err != nil {
		ip, err := netip.ParseAddr(addr)
		if err != nil {
			return fmt.Errorf("invalid ban address: %w", err)
		}
		ip = ip.Unmap()
		subnet = netip.PrefixFrom(ip, ip.BitLen())
	} else if subnet.Addr().Is4In6() {
		const mappedBits = 96
		if subnet.Bits() >= mappedBits {
			subnet = netip.PrefixFrom(subnet.Addr().Unmap(), subnet.Bits()-mappedBits)
		}
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.bans[subnet.Masked()] = time.Now().Add(duration)
	return nil
}

// Banned returns true if the IP address is banned.
func (ps *EphemeralPeerStore) Banned(addr string) (bool, error) {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	ip, err := netip.ParseAddr(addr)
	if err != nil {
		return false, fmt.Errorf("invalid peer address: %w", err)
	}
	ip = ip.WithZone("").Unmap()

	ps.mu.Lock()
	defer ps.mu.Unlock()
	now := time.Now()
	for subnet, expiration := range ps.bans {
		if !now.Before(expiration) {
			delete(ps.bans, subnet)
		} else if subnet.Contains(ip) {
			return true, nil
		}
	}
	return false, nil
}

var _ syncer.PeerStore = (*EphemeralPeerStore)(nil)

// NewEphemeralPeerStore returns a new EphemeralPeerStore.
func NewEphemeralPeerStore() *EphemeralPeerStore {
	return &EphemeralPeerStore{
		peers: make(map[string]syncer.PeerInfo),
		bans:  make(map[netip.Prefix]time.Time),
	}
}
