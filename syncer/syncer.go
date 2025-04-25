package syncer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// ErrNoPeers is returned when there are no peers available to relay
// to
var ErrNoPeers = errors.New("no peers available")

// A ChainManager manages blockchain state.
type ChainManager interface {
	History() ([32]types.BlockID, error)
	BlocksForHistory(history []types.BlockID, max uint64) ([]types.Block, uint64, error)
	Block(id types.BlockID) (types.Block, bool)
	State(id types.BlockID) (consensus.State, bool)
	AddBlocks(blocks []types.Block) error
	Tip() types.ChainIndex
	TipState() consensus.State

	PoolTransaction(txid types.TransactionID) (types.Transaction, bool)
	AddPoolTransactions(txns []types.Transaction) (bool, error)
	V2PoolTransaction(txid types.TransactionID) (types.V2Transaction, bool)
	AddV2PoolTransactions(basis types.ChainIndex, txns []types.V2Transaction) (bool, error)
	TransactionsForPartialBlock(missing []types.Hash256) ([]types.Transaction, []types.V2Transaction)
}

// PeerInfo contains metadata about a peer.
type PeerInfo struct {
	Address      string        `json:"address"`
	FirstSeen    time.Time     `json:"firstSeen"`
	LastConnect  time.Time     `json:"lastConnect,omitempty"`
	SyncedBlocks uint64        `json:"syncedBlocks,omitempty"`
	SyncDuration time.Duration `json:"syncDuration,omitempty"`
}

// A PeerStore stores peers and bans.
type PeerStore interface {
	// AddPeer adds a peer to the store. If the peer already exists, nil should
	// be returned.
	AddPeer(addr string) error
	// Peers returns the set of known peers.
	Peers() ([]PeerInfo, error)
	// PeerInfo returns the metadata for the specified peer or ErrPeerNotFound
	// if the peer wasn't found in the store.
	PeerInfo(addr string) (PeerInfo, error)
	// UpdatePeerInfo updates the metadata for the specified peer. If the peer
	// is not found, the error should be ErrPeerNotFound.
	UpdatePeerInfo(addr string, fn func(*PeerInfo)) error
	// Ban temporarily bans one or more IPs. The addr should either be a single
	// IP with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
	Ban(addr string, duration time.Duration, reason string) error

	// Banned returns true, nil if the peer is banned.
	Banned(addr string) (bool, error)
}

var (
	// ErrPeerBanned is returned when a peer is banned.
	ErrPeerBanned = errors.New("peer is banned")
	// ErrPeerNotFound is returned when the peer is not found.
	ErrPeerNotFound = errors.New("peer not found")
)

// Subnet normalizes the provided CIDR subnet string.
func Subnet(addr, mask string) string {
	ip, ipnet, err := net.ParseCIDR(addr + mask)
	if err != nil {
		return "" // shouldn't happen
	}
	return ip.Mask(ipnet.Mask).String() + mask
}

type config struct {
	MaxInboundPeers            int
	MaxOutboundPeers           int
	MaxInflightRPCs            int
	RPCTimeout                 time.Duration
	ConnectTimeout             time.Duration
	ShareNodesTimeout          time.Duration
	SendBlockTimeout           time.Duration
	SendTransactionsTimeout    time.Duration
	RelayHeaderTimeout         time.Duration
	RelayBlockOutlineTimeout   time.Duration
	RelayTransactionSetTimeout time.Duration
	SendBlocksTimeout          time.Duration
	MaxSendBlocks              uint64
	PeerDiscoveryInterval      time.Duration
	SyncInterval               time.Duration
	BanDuration                time.Duration
	Logger                     *zap.Logger
}

// An Option modifies a Syncer's configuration.
type Option func(*config)

// WithMaxInboundPeers sets the maximum number of inbound connections. The
// default is 8.
func WithMaxInboundPeers(n int) Option {
	return func(c *config) { c.MaxInboundPeers = n }
}

// WithMaxOutboundPeers sets the maximum number of outbound connections. The
// default is 8.
func WithMaxOutboundPeers(n int) Option {
	return func(c *config) { c.MaxOutboundPeers = n }
}

// WithMaxInflightRPCs sets the maximum number of concurrent RPCs per peer. The
// default is 3.
func WithMaxInflightRPCs(n int) Option {
	return func(c *config) { c.MaxInflightRPCs = n }
}

// WithConnectTimeout sets the timeout when connecting to a peer. The default is
// 5 seconds.
func WithConnectTimeout(d time.Duration) Option {
	return func(c *config) { c.ConnectTimeout = d }
}

// WithRPCTimeout sets the timeout for all incoming RPCs. The default is 2 minutes.
func WithRPCTimeout(d time.Duration) Option {
	return func(c *config) { c.RPCTimeout = d }
}

// WithShareNodesTimeout sets the timeout for the ShareNodes RPC. The default is
// 5 seconds.
func WithShareNodesTimeout(d time.Duration) Option {
	return func(c *config) { c.ShareNodesTimeout = d }
}

// WithSendBlockTimeout sets the timeout for the SendBlock RPC. The default is
// 60 seconds.
func WithSendBlockTimeout(d time.Duration) Option {
	return func(c *config) { c.SendBlockTimeout = d }
}

// WithSendBlocksTimeout sets the timeout for the SendBlocks RPC. The default is
// 120 seconds.
func WithSendBlocksTimeout(d time.Duration) Option {
	return func(c *config) { c.SendBlocksTimeout = d }
}

// WithMaxSendBlocks sets the maximum number of blocks requested per SendBlocks
// RPC. The default is 10.
func WithMaxSendBlocks(n uint64) Option {
	return func(c *config) { c.MaxSendBlocks = n }
}

// WithSendTransactionsTimeout sets the timeout for the SendTransactions RPC.
// The default is 60 seconds.
func WithSendTransactionsTimeout(d time.Duration) Option {
	return func(c *config) { c.SendTransactionsTimeout = d }
}

// WithRelayHeaderTimeout sets the timeout for the RelayHeader and RelayV2Header
// RPCs. The default is 5 seconds.
func WithRelayHeaderTimeout(d time.Duration) Option {
	return func(c *config) { c.RelayHeaderTimeout = d }
}

// WithRelayBlockOutlineTimeout sets the timeout for the RelayV2BlockOutline
// RPC. The default is 60 seconds.
func WithRelayBlockOutlineTimeout(d time.Duration) Option {
	return func(c *config) { c.RelayBlockOutlineTimeout = d }
}

// WithRelayTransactionSetTimeout sets the timeout for the RelayTransactionSet
// RPC. The default is 60 seconds.
func WithRelayTransactionSetTimeout(d time.Duration) Option {
	return func(c *config) { c.RelayTransactionSetTimeout = d }
}

// WithPeerDiscoveryInterval sets the frequency at which the syncer attempts to
// discover and connect to new peers. The default is 5 seconds.
func WithPeerDiscoveryInterval(d time.Duration) Option {
	return func(c *config) { c.PeerDiscoveryInterval = d }
}

// WithSyncInterval sets the frequency at which the syncer attempts to sync with
// peers. The default is 5 seconds.
func WithSyncInterval(d time.Duration) Option {
	return func(c *config) { c.SyncInterval = d }
}

// WithBanDuration sets the duration for which a peer is banned when
// misbehaving.
func WithBanDuration(d time.Duration) Option {
	return func(c *config) { c.BanDuration = d }
}

// WithLogger sets the logger used by a Syncer. The default is a logger that
// outputs to io.Discard.
func WithLogger(l *zap.Logger) Option {
	return func(c *config) { c.Logger = l }
}

// A Syncer synchronizes blockchain data with peers.
type Syncer struct {
	l      net.Listener
	cm     ChainManager
	pm     PeerStore
	header gateway.Header
	config config
	log    *zap.Logger // redundant, but convenient

	tg *threadgroup.ThreadGroup

	mu          sync.Mutex
	peerRemoved sync.Cond // broadcasts when peer is removed from 'peers'
	peers       map[string]*Peer
	strikes     map[string]int
}

func (s *Syncer) resync(p *Peer, reason string) {
	if p.Synced() {
		p.setSynced(false)
		s.log.Debug("resync triggered", zap.String("peer", p.t.Addr), zap.String("reason", reason))
	}
}

func (s *Syncer) ban(p *Peer, err error) error {
	s.log.Debug("banning peer", zap.Stringer("peer", p), zap.Error(err))
	p.setErr(ErrPeerBanned)
	if err := s.pm.Ban(p.ConnAddr, s.config.BanDuration, err.Error()); err != nil {
		return fmt.Errorf("failed to ban peer: %w", err)
	}

	host, _, err := net.SplitHostPort(p.ConnAddr)
	if err != nil {
		return err
	}
	// add a strike to each subnet
	for subnet, maxStrikes := range map[string]int{
		Subnet(host, "/32"): 2,   // 1.2.3.4:*
		Subnet(host, "/24"): 8,   // 1.2.3.*
		Subnet(host, "/16"): 64,  // 1.2.*
		Subnet(host, "/8"):  512, // 1.*
	} {
		s.mu.Lock()
		ban := (s.strikes[subnet] + 1) >= maxStrikes
		if ban {
			delete(s.strikes, subnet)
		} else {
			s.strikes[subnet]++
		}
		s.mu.Unlock()
		if !ban {
			continue
		} else if err := s.pm.Ban(subnet, s.config.BanDuration, "too many strikes"); err != nil {
			return fmt.Errorf("failed to ban subnet %q: %w", subnet, err)
		}
	}
	return nil
}

func (s *Syncer) runPeer(p *Peer) error {
	// always try and remove the peer from the map when we're done, cause it
	// might already have been added by Connect()
	defer func() {
		s.mu.Lock()
		delete(s.peers, p.t.Addr)
		s.mu.Unlock()

		// notify goroutines of removed peer
		s.peerRemoved.Broadcast()
	}()

	if err := s.pm.AddPeer(p.t.Addr); err != nil {
		return fmt.Errorf("failed to add peer: %w", err)
	}
	err := s.pm.UpdatePeerInfo(p.t.Addr, func(info *PeerInfo) {
		info.LastConnect = time.Now()
	})
	if err != nil {
		return fmt.Errorf("failed to update peer info: %w", err)
	}
	s.mu.Lock()
	s.peers[p.t.Addr] = p
	s.mu.Unlock()

	inflight := make(chan struct{}, s.config.MaxInflightRPCs)

	for {
		if p.Err() != nil {
			return fmt.Errorf("peer error: %w", p.Err())
		}
		id, stream, err := p.acceptRPC()
		if err != nil {
			p.setErr(err)
			return fmt.Errorf("failed to accept rpc: %w", err)
		}
		select {
		case inflight <- struct{}{}:
		case <-s.tg.Done():
			return threadgroup.ErrClosed
		}

		go func() {
			defer func() { <-inflight }()

			done, err := s.tg.Add()
			if err != nil {
				return
			}
			defer done()
			defer stream.Close()
			if err := stream.SetDeadline(time.Now().Add(s.config.RPCTimeout)); err != nil {
				s.log.Debug("failed to set rpc deadline", zap.Error(err))
			} else if err := s.handleRPC(id, stream, p); err != nil {
				s.log.Debug("rpc failed", zap.Stringer("peer", p), zap.Stringer("rpc", id), zap.Error(err))
			}
		}()
	}
}

// withPeers is a helper function that calls fn concurrently for all connected peers
// except the origin peer. It returns nil if at least one call returns nil. If all calls
// fail, it returns the first error encountered. If there are no peers, it returns [ErrNoPeers].
//
// The Syncer mutex will be locked while calling fn
func (s *Syncer) withPeers(origin *Peer, fn func(p *Peer) error) error {
	s.mu.Lock()
	var inflight int
	errCh := make(chan error, len(s.peers))
	for _, p := range s.peers {
		if p == origin {
			continue
		}
		inflight++
		go func(p *Peer) {
			err := fn(p)
			errCh <- err
		}(p)
	}
	s.mu.Unlock()
	if inflight == 0 {
		return ErrNoPeers
	}
	var err error
	for ; inflight > 0; inflight-- {
		// block until at least one relay has succeeded
		// or all relays have failed
		peerErr := <-errCh
		if peerErr == nil {
			return nil
		} else if err == nil {
			err = peerErr // return the first error if all relays fail
		}
	}
	return err
}

// withV2Peers is a helper function that calls fn concurrently for all connected peers
// that support V2 except the origin peer. It returns nil if at least one call returns
// nil. If all calls fail, it returns the first error encountered. If there are no
// peers, it returns [ErrNoPeers].
//
// The Syncer mutex will be locked while calling fn
func (s *Syncer) withV2Peers(origin *Peer, fn func(p *Peer) error) error {
	s.mu.Lock()
	var inflight int
	errCh := make(chan error, len(s.peers))
	for _, p := range s.peers {
		if p == origin || !p.t.SupportsV2() {
			continue
		}
		inflight++
		go func(p *Peer) {
			err := fn(p)
			errCh <- err
		}(p)
	}
	s.mu.Unlock()
	if inflight == 0 {
		return ErrNoPeers
	}
	var err error
	for ; inflight > 0; inflight-- {
		// block until at least one relay has succeeded
		// or all relays have failed
		peerErr := <-errCh
		if peerErr == nil {
			return nil
		} else if err == nil {
			err = peerErr // return the first error if all relays fail
		}
	}
	return err
}

func (s *Syncer) relayHeader(h types.BlockHeader, origin *Peer) error {
	return s.withPeers(origin, func(p *Peer) error { p.RelayHeader(h, s.config.RelayHeaderTimeout); return nil })
}

func (s *Syncer) relayTransactionSet(txns []types.Transaction, origin *Peer) error {
	if len(txns) == 0 {
		return nil
	}
	return s.withPeers(origin, func(p *Peer) error { return p.RelayTransactionSet(txns, s.config.RelayTransactionSetTimeout) })
}

func (s *Syncer) relayV2Header(bh types.BlockHeader, origin *Peer) error {
	return s.withV2Peers(origin, func(p *Peer) error { p.RelayV2Header(bh, s.config.RelayHeaderTimeout); return nil })
}

func (s *Syncer) relayV2BlockOutline(pb gateway.V2BlockOutline, origin *Peer) error {
	return s.withV2Peers(origin, func(p *Peer) error { p.RelayV2BlockOutline(pb, s.config.RelayBlockOutlineTimeout); return nil })
}

func (s *Syncer) relayV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction, origin *Peer) error {
	if len(txns) == 0 {
		return nil
	}
	return s.withV2Peers(origin, func(p *Peer) error { return p.RelayV2TransactionSet(index, txns, s.config.RelayTransactionSetTimeout) })
}

func (s *Syncer) allowConnect(ctx context.Context, peer string, inbound bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	done, err := s.tg.Add()
	if err != nil {
		return err
	}
	defer done()

	var addrs []net.IPAddr
	if peerHost, _, err := net.SplitHostPort(peer); err != nil {
		return fmt.Errorf("failed to split peer host and port: %w", err)
	} else if addrs, err = (&net.Resolver{}).LookupIPAddr(ctx, peerHost); err != nil {
		return fmt.Errorf("failed to resolve peer address: %w", err)
	} else if len(addrs) == 0 {
		return fmt.Errorf("peer didn't resolve to any addresses")
	}
	for _, addr := range addrs {
		if banned, err := s.pm.Banned(addr.String()); err != nil {
			return err
		} else if banned {
			return ErrPeerBanned
		}
	}
	var in, out int
	for _, p := range s.peers {
		if p.Inbound {
			in++
		} else {
			out++
		}
	}
	// TODO: subnet-based limits
	if inbound && in >= s.config.MaxInboundPeers {
		return errors.New("too many inbound peers")
	} else if !inbound && out >= s.config.MaxOutboundPeers {
		return errors.New("too many outbound peers")
	}
	return nil
}

func (s *Syncer) alreadyConnected(id gateway.UniqueID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p.t.UniqueID == id {
			return true
		}
	}
	return false
}

func (s *Syncer) acceptLoop(ctx context.Context) error {
	ctx, done, err := s.tg.AddContext(ctx)
	if err != nil {
		return err
	}
	defer done()

	for {
		select {
		case <-ctx.Done():
			return threadgroup.ErrClosed
		default:
		}

		conn, err := s.l.Accept()
		if err != nil {
			return err
		}

		go func() {
			done, err := s.tg.Add()
			if err != nil {
				return
			}
			defer done()
			defer conn.Close()
			// set timeout for initial handshake
			conn.SetDeadline(time.Now().Add(s.config.ConnectTimeout))
			if err := s.allowConnect(ctx, conn.RemoteAddr().String(), true); err != nil {
				s.log.Debug("rejected inbound connection", zap.Stringer("remoteAddress", conn.RemoteAddr()), zap.Error(err))
			} else if t, err := gateway.Accept(conn, s.header); err != nil {
				s.log.Debug("failed to accept inbound connection", zap.Stringer("remoteAddress", conn.RemoteAddr()), zap.Error(err))
			} else if s.alreadyConnected(t.UniqueID) {
				s.log.Debug("already connected to peer", zap.Stringer("remoteAddress", conn.RemoteAddr()))
			} else {
				conn.SetDeadline(time.Time{})
				s.runPeer(&Peer{
					t:        t,
					ConnAddr: conn.RemoteAddr().String(),
					Inbound:  true,
				})
			}
		}()
	}
}

func (s *Syncer) peerLoop(ctx context.Context) error {
	log := s.log.Named("peerLoop")
	numOutbound := func() (n int) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, p := range s.peers {
			if !p.Inbound {
				n++
			}
		}
		return
	}

	lastTried := make(map[string]time.Time)
	peersForConnect := func() (peers []string) {
		p, err := s.pm.Peers()
		if err != nil {
			log.Error("failed to fetch peers", zap.Error(err))
			return
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		for _, p := range p {
			// TODO: don't include port in comparison
			if _, ok := s.peers[p.Address]; !ok && time.Since(lastTried[p.Address]) > 5*time.Minute {
				peers = append(peers, p.Address)
			}
		}
		// TODO: weighted random selection?
		frand.Shuffle(len(peers), func(i, j int) {
			peers[i], peers[j] = peers[j], peers[i]
		})
		return peers
	}
	discoverPeers := func() {
		// try up to three randomly-chosen peers
		var peers []*Peer
		s.mu.Lock()
		for _, p := range s.peers {
			if peers = append(peers, p); len(peers) >= 3 {
				break
			}
		}
		s.mu.Unlock()
		for _, p := range peers {
			nodes, err := p.ShareNodes(s.config.ShareNodesTimeout)
			if err != nil {
				continue
			}
			for _, n := range nodes {
				if err := s.pm.AddPeer(n); err != nil {
					log.Debug("failed to add peer", zap.String("peer", n), zap.Error(err))
				}
			}
		}
	}

	ticker := time.NewTicker(s.config.PeerDiscoveryInterval)
	defer ticker.Stop()
	sleep := func() bool {
		select {
		case <-ticker.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for fst := true; fst || sleep(); fst = false {
		select {
		case <-ctx.Done():
			return nil // avoid spamming "failed to connect" after context is cancelled
		default:
		}

		if numOutbound() >= s.config.MaxOutboundPeers {
			continue
		}
		candidates := peersForConnect()
		if len(candidates) == 0 {
			log.Debug("no peers to connect to")
			discoverPeers()
			continue
		}
		for _, p := range candidates {
			if numOutbound() >= s.config.MaxOutboundPeers {
				break
			} else if err := s.allowConnect(ctx, p, false); err != nil {
				log.Debug("rejected outbound peer", zap.String("peer", p), zap.Error(err))
				break
			}

			ctx, cancel := context.WithTimeout(ctx, s.config.ConnectTimeout)
			if _, err := s.Connect(ctx, p); err != nil {
				log.Debug("failed to connect to peer", zap.String("peer", p), zap.Error(err))
			} else {
				log.Debug("connected to peer", zap.String("peer", p))
			}
			cancel()
			lastTried[p] = time.Now()
		}
	}
	return nil
}

func (s *Syncer) syncLoop(ctx context.Context) error {
	peersForSync := func() (peers []*Peer) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, p := range s.peers {
			if p.Synced() {
				continue
			}
			peers = append(peers, p)
		}
		sort.Slice(peers, func(i, j int) bool {
			return peers[i].t.SupportsV2() && !peers[j].t.SupportsV2()
		})
		if len(peers) > 3 {
			peers = peers[:3]
		}
		return
	}

	ticker := time.NewTicker(s.config.SyncInterval)
	defer ticker.Stop()
	sleep := func() bool {
		select {
		case <-ticker.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for fst := true; fst || sleep(); fst = false {
		for _, p := range peersForSync() {
			history, err := s.cm.History()
			if err != nil {
				return err // generally fatal
			}
			p.setSynced(true)
			s.log.Debug("syncing with peer", zap.Stringer("peer", p))
			oldTip := s.cm.Tip()
			oldTime := time.Now()
			lastPrint := time.Now()
			startTime, startHeight := oldTime, oldTip.Height
			var sentBlocks uint64
			addBlocks := func(blocks []types.Block) error {
				if err := s.cm.AddBlocks(blocks); err != nil {
					return err
				}
				sentBlocks += uint64(len(blocks))
				endTime, endHeight := time.Now(), s.cm.Tip().Height
				err = s.pm.UpdatePeerInfo(p.t.Addr, func(info *PeerInfo) {
					info.SyncedBlocks += endHeight - startHeight
					info.SyncDuration += endTime.Sub(startTime)
				})
				if err != nil {
					return fmt.Errorf("syncLoop: failed to update peer info: %w", err)
				}
				startTime, startHeight = endTime, endHeight
				if time.Since(lastPrint) > 30*time.Second {
					s.log.Debug("syncing with peer", zap.Stringer("peer", p), zap.Uint64("blocks", sentBlocks), zap.Duration("elapsed", endTime.Sub(oldTime)))
					lastPrint = time.Now()
				}
				return nil
			}
			if p.t.SupportsV2() {
				history := history[:]
				err = func() error {
					for {
						blocks, rem, err := p.SendV2Blocks(history, s.config.MaxSendBlocks, s.config.SendBlocksTimeout)
						if err != nil {
							return err
						} else if err := addBlocks(blocks); err != nil {
							return err
						} else if rem == 0 {
							return nil
						}
						history = []types.BlockID{blocks[len(blocks)-1].ID()}
					}
				}()
			} else {
				err = p.SendBlocks(history, s.config.SendBlocksTimeout, addBlocks)
			}
			totalBlocks := s.cm.Tip().Height - oldTip.Height
			if err != nil {
				s.log.Debug("syncing with peer failed", zap.Stringer("peer", p), zap.Error(err), zap.Uint64("blocks", totalBlocks))
			} else if newTip := s.cm.Tip(); newTip != oldTip {
				s.log.Debug("finished syncing with peer", zap.Stringer("peer", p), zap.Stringer("newTip", newTip), zap.Uint64("blocks", totalBlocks))
			} else {
				s.log.Debug("finished syncing with peer, tip unchanged", zap.Stringer("peer", p), zap.Uint64("blocks", sentBlocks))
			}
		}
	}
	return nil
}

// Run spawns goroutines for accepting inbound connections, forming outbound
// connections, and syncing the blockchain from active peers. It blocks until an
// error occurs, upon which all connections are closed and goroutines are
// terminated.
func (s *Syncer) Run() error {
	ctx, done, err := s.tg.AddContext(context.Background())
	if err != nil {
		return err
	}
	defer done()

	errChan := make(chan error)
	for _, fn := range []func(context.Context) error{s.acceptLoop, s.peerLoop, s.syncLoop} {
		go func() {
			done, err := s.tg.Add()
			if err != nil {
				errChan <- err
				return
			}
			errChan <- fn(ctx)
			done()
		}()
	}
	err = <-errChan

	// when one goroutine exits, shutdown and wait for the others
	s.l.Close()
	s.mu.Lock()
	for _, p := range s.peers {
		p.Close()
	}
	s.mu.Unlock()
	<-errChan
	<-errChan

	// wait for all peer goroutines to exit
	s.mu.Lock()
	for len(s.peers) != 0 {
		s.peerRemoved.Wait()
	}
	s.mu.Unlock()

	if errors.Is(err, net.ErrClosed) {
		return nil // graceful shutdown
	}
	return err
}

// Close closes the Syncer's net.Listener.
func (s *Syncer) Close() error {
	err := s.l.Close()
	s.tg.Stop()
	return err
}

// Connect forms an outbound connection to a peer.
func (s *Syncer) Connect(ctx context.Context, addr string) (*Peer, error) {
	ctx, done, err := s.tg.AddContext(ctx)
	if err != nil {
		return nil, err
	}
	defer done()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(s.config.ConnectTimeout))
	defer conn.SetDeadline(time.Time{})
	t, err := gateway.Dial(conn, s.header)
	if err != nil {
		conn.Close()
		return nil, err
	} else if s.alreadyConnected(t.UniqueID) {
		conn.Close()
		return nil, errors.New("already connected")
	}
	p := &Peer{
		t:        t,
		ConnAddr: conn.RemoteAddr().String(),
		Inbound:  false,
	}
	go func() {
		done, err := s.tg.Add()
		if err != nil {
			return
		}
		s.runPeer(p)
		done()
	}()

	// runPeer does this too, but doing it outside the goroutine prevents a race
	// where the peer is absent from Peers() despite Connect() having returned
	// successfully
	s.mu.Lock()
	s.peers[p.t.Addr] = p
	s.mu.Unlock()
	return p, nil
}

// BroadcastHeader broadcasts a header to all peers.
func (s *Syncer) BroadcastHeader(bh types.BlockHeader) error { return s.relayHeader(bh, nil) }

// BroadcastV2Header broadcasts a v2 header to all peers.
func (s *Syncer) BroadcastV2Header(bh types.BlockHeader) error {
	return s.relayV2Header(bh, nil)
}

// BroadcastV2BlockOutline broadcasts a v2 block outline to all peers.
func (s *Syncer) BroadcastV2BlockOutline(b gateway.V2BlockOutline) error {
	return s.relayV2BlockOutline(b, nil)
}

// BroadcastTransactionSet broadcasts a transaction set to all peers.
func (s *Syncer) BroadcastTransactionSet(txns []types.Transaction) error {
	return s.relayTransactionSet(txns, nil)
}

// BroadcastV2TransactionSet broadcasts a v2 transaction set to all peers.
func (s *Syncer) BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) error {
	return s.relayV2TransactionSet(index, txns, nil)
}

// Peers returns the set of currently-connected peers.
func (s *Syncer) Peers() []*Peer {
	s.mu.Lock()
	defer s.mu.Unlock()
	var peers []*Peer
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	return peers
}

// PeerInfo returns the metadata for the specified peer or ErrPeerNotFound if
// the peer wasn't found in the store.
func (s *Syncer) PeerInfo(addr string) (PeerInfo, error) {
	return s.pm.PeerInfo(addr)
}

// Addr returns the address of the Syncer.
func (s *Syncer) Addr() string {
	return s.l.Addr().String()
}

// New returns a new Syncer.
func New(l net.Listener, cm ChainManager, pm PeerStore, header gateway.Header, opts ...Option) *Syncer {
	config := config{
		MaxInboundPeers:            64,
		MaxOutboundPeers:           16,
		MaxInflightRPCs:            3,
		ConnectTimeout:             10 * time.Second,
		RPCTimeout:                 2 * time.Minute,
		ShareNodesTimeout:          5 * time.Second,
		SendBlockTimeout:           60 * time.Second,
		SendTransactionsTimeout:    60 * time.Second,
		RelayHeaderTimeout:         5 * time.Second,
		RelayBlockOutlineTimeout:   60 * time.Second,
		RelayTransactionSetTimeout: 60 * time.Second,
		SendBlocksTimeout:          120 * time.Second,
		MaxSendBlocks:              10,
		PeerDiscoveryInterval:      5 * time.Second,
		SyncInterval:               5 * time.Second,
		BanDuration:                24 * time.Hour,
		Logger:                     zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&config)
	}
	s := &Syncer{
		l:       l,
		cm:      cm,
		pm:      pm,
		header:  header,
		config:  config,
		log:     config.Logger,
		peers:   make(map[string]*Peer),
		strikes: make(map[string]int),
		tg:      threadgroup.New(),
	}
	s.peerRemoved = sync.Cond{L: &s.mu}
	return s
}
