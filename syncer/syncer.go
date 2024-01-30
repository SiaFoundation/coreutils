package syncer

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

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
	FirstSeen    time.Time     `json:"firstSeen"`
	LastConnect  time.Time     `json:"lastConnect,omitempty"`
	SyncedBlocks uint64        `json:"syncedBlocks,omitempty"`
	SyncDuration time.Duration `json:"syncDuration,omitempty"`
}

// A PeerStore stores peers and bans.
type PeerStore interface {
	AddPeer(peer string)
	Peers() []string
	UpdatePeerInfo(peer string, fn func(*PeerInfo))
	PeerInfo(peer string) (PeerInfo, bool)

	// Ban temporarily bans one or more IPs. The addr should either be a single
	// IP with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
	Ban(addr string, duration time.Duration, reason string)
	Banned(peer string) bool
}

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

	mu      sync.Mutex
	peers   map[string]*Peer
	strikes map[string]int
}

func (s *Syncer) resync(p *Peer, reason string) {
	if p.Synced() {
		p.setSynced(false)
		s.log.Debug("resync triggered", zap.String("peer", p.t.Addr), zap.String("reason", reason))
	}
}

func (s *Syncer) ban(p *Peer, err error) {
	s.log.Debug("banning peer", zap.Stringer("peer", p), zap.Error(err))
	p.setErr(errors.New("banned"))
	s.pm.Ban(p.ConnAddr, 24*time.Hour, err.Error())

	host, _, err := net.SplitHostPort(p.ConnAddr)
	if err != nil {
		return // shouldn't happen
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
		if ban {
			s.pm.Ban(subnet, 24*time.Hour, "too many strikes")
		}
	}
}

func (s *Syncer) runPeer(p *Peer) {
	s.pm.AddPeer(p.t.Addr)
	s.pm.UpdatePeerInfo(p.t.Addr, func(info *PeerInfo) {
		info.LastConnect = time.Now()
	})
	s.mu.Lock()
	s.peers[p.t.Addr] = p
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.peers, p.t.Addr)
		s.mu.Unlock()
	}()

	inflight := make(chan struct{}, s.config.MaxInflightRPCs)
	for {
		if p.Err() != nil {
			return
		}
		id, stream, err := p.acceptRPC()
		if err != nil {
			p.setErr(err)
			return
		}
		inflight <- struct{}{}
		go func() {
			defer stream.Close()
			// NOTE: we do not set any deadlines on the stream. If a peer is
			// slow, fine; we don't need to worry about resource exhaustion
			// unless we have tons of peers.
			if err := s.handleRPC(id, stream, p); err != nil {
				s.log.Debug("rpc failed", zap.Stringer("peer", p), zap.Stringer("rpc", id), zap.Error(err))
			}
			<-inflight
		}()
	}
}

func (s *Syncer) relayHeader(h gateway.BlockHeader, origin *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p == origin {
			continue
		}
		go p.RelayHeader(h, s.config.RelayHeaderTimeout)
	}
}

func (s *Syncer) relayTransactionSet(txns []types.Transaction, origin *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p == origin {
			continue
		}
		go p.RelayTransactionSet(txns, s.config.RelayTransactionSetTimeout)
	}
}

func (s *Syncer) relayV2Header(bh gateway.V2BlockHeader, origin *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p == origin || !p.t.SupportsV2() {
			continue
		}
		go p.RelayV2Header(bh, s.config.RelayHeaderTimeout)
	}
}

func (s *Syncer) relayV2BlockOutline(pb gateway.V2BlockOutline, origin *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p == origin || !p.t.SupportsV2() {
			continue
		}
		go p.RelayV2BlockOutline(pb, s.config.RelayBlockOutlineTimeout)
	}
}

func (s *Syncer) relayV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction, origin *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.peers {
		if p == origin || !p.t.SupportsV2() {
			continue
		}
		go p.RelayV2TransactionSet(index, txns, s.config.RelayTransactionSetTimeout)
	}
}

func (s *Syncer) allowConnect(peer string, inbound bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.l == nil {
		return errors.New("syncer is shutting down")
	}
	if s.pm.Banned(peer) {
		return errors.New("banned")
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

func (s *Syncer) acceptLoop() error {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			if err := s.allowConnect(conn.RemoteAddr().String(), true); err != nil {
				s.log.Debug("rejected inbound connection", zap.Stringer("remoteAddress", conn.RemoteAddr()), zap.Error(err))
			} else if t, err := gateway.Accept(conn, s.header); err != nil {
				s.log.Debug("failed to accept inbound connection", zap.Stringer("remoteAddress", conn.RemoteAddr()), zap.Error(err))
			} else if s.alreadyConnected(t.UniqueID) {
				s.log.Debug("already connected to peer", zap.Stringer("remoteAddress", conn.RemoteAddr()))
			} else {
				s.runPeer(&Peer{
					t:        t,
					ConnAddr: conn.RemoteAddr().String(),
					Inbound:  true,
				})
			}
		}()
	}
}

func (s *Syncer) peerLoop(closeChan <-chan struct{}) error {
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
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, p := range s.pm.Peers() {
			// TODO: don't include port in comparison
			if _, ok := s.peers[p]; !ok && time.Since(lastTried[p]) > 5*time.Minute {
				peers = append(peers, p)
			}
		}
		// TODO: weighted random selection?
		frand.Shuffle(len(peers), reflect.Swapper(peers))
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
				s.pm.AddPeer(n)
			}
		}
	}

	ticker := time.NewTicker(s.config.PeerDiscoveryInterval)
	defer ticker.Stop()
	sleep := func() bool {
		select {
		case <-ticker.C:
			return true
		case <-closeChan:
			return false
		}
	}
	closing := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.l == nil
	}
	for fst := true; fst || sleep(); fst = false {
		if numOutbound() >= s.config.MaxOutboundPeers {
			continue
		}
		candidates := peersForConnect()
		if len(candidates) == 0 {
			discoverPeers()
			continue
		}
		for _, p := range candidates {
			if numOutbound() >= s.config.MaxOutboundPeers || closing() {
				break
			}
			// NOTE: we don't bother logging failure here, since it's common and
			// not particularly interesting or actionable
			if _, err := s.Connect(p); err == nil {
				s.log.Debug("connected to peer", zap.String("peer", p))
			}
			lastTried[p] = time.Now()
		}
	}
	return nil
}

func (s *Syncer) syncLoop(closeChan <-chan struct{}) error {
	peersForSync := func() (peers []*Peer) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, p := range s.peers {
			if p.Synced() {
				continue
			}
			if peers = append(peers, p); len(peers) >= 3 {
				break
			}
		}
		return
	}

	ticker := time.NewTicker(s.config.SyncInterval)
	defer ticker.Stop()
	sleep := func() bool {
		select {
		case <-ticker.C:
			return true
		case <-closeChan:
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
				s.pm.UpdatePeerInfo(p.t.Addr, func(info *PeerInfo) {
					info.SyncedBlocks += endHeight - startHeight
					info.SyncDuration += endTime.Sub(startTime)
				})
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
// terminated. To gracefully shutdown a Syncer, close its net.Listener.
func (s *Syncer) Run() error {
	errChan := make(chan error)
	closeChan := make(chan struct{})
	go func() { errChan <- s.acceptLoop() }()
	go func() { errChan <- s.peerLoop(closeChan) }()
	go func() { errChan <- s.syncLoop(closeChan) }()
	err := <-errChan

	// when one goroutine exits, shutdown and wait for the others
	close(closeChan)
	s.l.Close()
	s.mu.Lock()
	s.l = nil
	for _, p := range s.peers {
		p.Close()
	}
	s.mu.Unlock()
	<-errChan
	<-errChan

	// wait for all peer goroutines to exit
	// TODO: a cond would be nicer than polling here
	s.mu.Lock()
	for len(s.peers) != 0 {
		s.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		s.mu.Lock()
	}
	s.mu.Unlock()

	if errors.Is(err, net.ErrClosed) {
		return nil // graceful shutdown
	}
	return err
}

// Connect forms an outbound connection to a peer.
func (s *Syncer) Connect(addr string) (*Peer, error) {
	if err := s.allowConnect(addr, false); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ConnectTimeout)
	defer cancel()
	// slightly gross polling hack so that we shutdown quickly
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				s.mu.Lock()
				if s.l == nil {
					cancel()
				}
				s.mu.Unlock()
			}
		}
	}()
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
	go s.runPeer(p)

	// runPeer does this too, but doing it outside the goroutine prevents a race
	// where the peer is absent from Peers() despite Connect() having returned
	// successfully
	s.mu.Lock()
	s.peers[p.t.Addr] = p
	s.mu.Unlock()
	return p, nil
}

// BroadcastHeader broadcasts a header to all peers.
func (s *Syncer) BroadcastHeader(h gateway.BlockHeader) { s.relayHeader(h, nil) }

// BroadcastV2Header broadcasts a v2 header to all peers.
func (s *Syncer) BroadcastV2Header(h gateway.V2BlockHeader) { s.relayV2Header(h, nil) }

// BroadcastV2BlockOutline broadcasts a v2 block outline to all peers.
func (s *Syncer) BroadcastV2BlockOutline(b gateway.V2BlockOutline) { s.relayV2BlockOutline(b, nil) }

// BroadcastTransactionSet broadcasts a transaction set to all peers.
func (s *Syncer) BroadcastTransactionSet(txns []types.Transaction) { s.relayTransactionSet(txns, nil) }

// BroadcastV2TransactionSet broadcasts a v2 transaction set to all peers.
func (s *Syncer) BroadcastV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction) {
	s.relayV2TransactionSet(index, txns, nil)
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

// PeerInfo returns metadata about the specified peer.
func (s *Syncer) PeerInfo(peer string) (PeerInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, ok := s.pm.PeerInfo(peer)
	return info, ok
}

// Addr returns the address of the Syncer.
func (s *Syncer) Addr() string {
	return s.l.Addr().String()
}

// New returns a new Syncer.
func New(l net.Listener, cm ChainManager, pm PeerStore, header gateway.Header, opts ...Option) *Syncer {
	config := config{
		MaxInboundPeers:            8,
		MaxOutboundPeers:           8,
		MaxInflightRPCs:            3,
		ConnectTimeout:             5 * time.Second,
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
		Logger:                     zap.NewNop(),
	}
	for _, opt := range opts {
		opt(&config)
	}
	return &Syncer{
		l:       l,
		cm:      cm,
		pm:      pm,
		header:  header,
		config:  config,
		log:     config.Logger,
		peers:   make(map[string]*Peer),
		strikes: make(map[string]int),
	}
}
