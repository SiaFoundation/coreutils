package syncer

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// A Peer is a connected gateway peer.
type Peer struct {
	t *gateway.Transport

	ConnAddr string
	Inbound  bool
	mu       sync.Mutex
	synced   bool
	err      error
}

// String implements fmt.Stringer.
func (p *Peer) String() string {
	if p.Inbound {
		return "<-" + p.ConnAddr
	}
	return "->" + p.ConnAddr
}

// Addr returns the peer's reported dialback address.
func (p *Peer) Addr() string { return p.t.Addr }

// Version returns the peer's reported version.
func (p *Peer) Version() string { return p.t.Version }

// UniqueID returns the peer's reported UniqueID.
func (p *Peer) UniqueID() gateway.UniqueID { return p.t.UniqueID }

// Err returns the error that caused the peer to disconnect, if any.
func (p *Peer) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Peer) setErr(err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = err
		p.t.Close()
	}
	return p.err
}

// Synced returns the peer's sync status.
func (p *Peer) Synced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.synced
}

func (p *Peer) setSynced(synced bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.synced = synced
}

// Close closes the peer's connection.
func (p *Peer) Close() error {
	p.setErr(errors.New("closing"))
	return nil
}

func (p *Peer) callRPC(r gateway.Object, timeout time.Duration) error {
	s, err := p.t.DialStream()
	if err != nil {
		return fmt.Errorf("couldn't open stream: %w", err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(timeout))
	if err := s.WriteID(r); err != nil {
		return fmt.Errorf("couldn't write RPC ID: %w", err)
	} else if err := s.WriteRequest(r); err != nil {
		return fmt.Errorf("couldn't write request: %w", err)
	} else if err := s.ReadResponse(r); err != nil {
		return fmt.Errorf("couldn't read response: %w", err)
	}
	return nil
}

// ShareNodes requests a list of potential peers from the peer.
func (p *Peer) ShareNodes(timeout time.Duration) ([]string, error) {
	r := &gateway.RPCShareNodes{}
	err := p.callRPC(r, timeout)
	return r.Peers, err
}

// DiscoverIP requests our external IP as seen by the peer.
func (p *Peer) DiscoverIP(timeout time.Duration) (string, error) {
	r := &gateway.RPCDiscoverIP{}
	err := p.callRPC(r, timeout)
	return r.IP, err
}

// SendBlock requests a single block from the peer.
func (p *Peer) SendBlock(id types.BlockID, timeout time.Duration) (types.Block, error) {
	r := &gateway.RPCSendBlk{ID: id}
	err := p.callRPC(r, timeout)
	return r.Block, err
}

// RelayHeader relays a header to the peer.
func (p *Peer) RelayHeader(h types.BlockHeader, timeout time.Duration) error {
	return p.callRPC(&gateway.RPCRelayHeader{Header: h}, timeout)
}

// RelayTransactionSet relays a transaction set to the peer.
func (p *Peer) RelayTransactionSet(txns []types.Transaction, timeout time.Duration) error {
	return p.callRPC(&gateway.RPCRelayTransactionSet{Transactions: txns}, timeout)
}

// SendBlocks downloads blocks from p, starting from the most recent element of
// history known to p. The blocks are sent in batches, and fn is called on each
// batch.
func (p *Peer) SendBlocks(history [32]types.BlockID, timeout time.Duration, fn func([]types.Block) error) error {
	s, err := p.t.DialStream()
	if err != nil {
		return fmt.Errorf("couldn't open stream: %w", err)
	}
	defer s.Close()

	s.SetDeadline(time.Now().Add(timeout))
	r := &gateway.RPCSendBlocks{History: history}
	if err := s.WriteID(r); err != nil {
		return fmt.Errorf("couldn't write RPC ID: %w", err)
	} else if err := s.WriteRequest(r); err != nil {
		return fmt.Errorf("couldn't write request: %w", err)
	}
	rma := &gateway.RPCSendBlocksMoreAvailable{MoreAvailable: true}
	for rma.MoreAvailable {
		s.SetDeadline(time.Now().Add(timeout))
		if err := s.ReadResponse(r); err != nil {
			return fmt.Errorf("couldn't read response: %w", err)
		} else if err := s.ReadResponse(rma); err != nil {
			return fmt.Errorf("couldn't read response: %w", err)
		} else if err := fn(r.Blocks); err != nil {
			return err
		}
	}
	return nil
}

// SendTransactions requests a subset of a block's transactions from the peer.
func (p *Peer) SendTransactions(index types.ChainIndex, txnHashes []types.Hash256, timeout time.Duration) ([]types.Transaction, []types.V2Transaction, error) {
	r := &gateway.RPCSendTransactions{Index: index, Hashes: txnHashes}
	err := p.callRPC(r, timeout)
	return r.Transactions, r.V2Transactions, err
}

// SendCheckpoint requests a checkpoint from the peer. The checkpoint is
// validated.
func (p *Peer) SendCheckpoint(index types.ChainIndex, timeout time.Duration) (types.Block, consensus.State, error) {
	r := &gateway.RPCSendCheckpoint{Index: index}
	err := p.callRPC(r, timeout)
	if err == nil {
		if r.Block.V2 == nil || len(r.Block.MinerPayouts) != 1 {
			err = errors.New("checkpoint is not a v2 block")
		} else if r.Block.ID() != index.ID {
			err = errors.New("checkpoint has wrong index")
		} else if r.Block.V2.Commitment != r.State.Commitment(r.Block.MinerPayouts[0].Address, r.Block.Transactions, r.Block.V2Transactions()) {
			err = errors.New("checkpoint has wrong commitment")
		}
	}
	return r.Block, r.State, err
}

// RelayV2Header relays a v2 block header to the peer.
func (p *Peer) RelayV2Header(bh types.BlockHeader, timeout time.Duration) error {
	return p.callRPC(&gateway.RPCRelayV2Header{Header: bh}, timeout)
}

// RelayV2BlockOutline relays a v2 block outline to the peer.
func (p *Peer) RelayV2BlockOutline(b gateway.V2BlockOutline, timeout time.Duration) error {
	return p.callRPC(&gateway.RPCRelayV2BlockOutline{Block: b}, timeout)
}

// RelayV2TransactionSet relays a v2 transaction set to the peer.
func (p *Peer) RelayV2TransactionSet(index types.ChainIndex, txns []types.V2Transaction, timeout time.Duration) error {
	return p.callRPC(&gateway.RPCRelayV2TransactionSet{Index: index, Transactions: txns}, timeout)
}

// SendV2Blocks requests up to n blocks from p, starting from the most recent
// element of history known to p. The peer also returns the number of remaining
// blocks left to sync.
func (p *Peer) SendV2Blocks(history []types.BlockID, maxBlocks uint64, timeout time.Duration) ([]types.Block, uint64, error) {
	r := &gateway.RPCSendV2Blocks{History: history, Max: maxBlocks}
	err := p.callRPC(r, timeout)
	return r.Blocks, r.Remaining, err
}

func (p *Peer) acceptRPC() (types.Specifier, *gateway.Stream, error) {
	s, err := p.t.AcceptStream()
	if err != nil {
		return types.Specifier{}, nil, err
	}
	s.SetDeadline(time.Now().Add(5 * time.Second))
	id, err := s.ReadID()
	if err != nil {
		s.Close()
		return types.Specifier{}, nil, err
	}
	s.SetDeadline(time.Time{})
	return id, s, nil
}

func (s *Syncer) handleRPC(id types.Specifier, stream *gateway.Stream, origin *Peer) error {
	log := s.log.With(zap.Stringer("origin", origin), zap.Stringer("id", id))
	switch r := gateway.ObjectForID(id).(type) {
	case *gateway.RPCShareNodes:
		peers, err := s.pm.Peers()
		if err != nil {
			return fmt.Errorf("failed to fetch peers: %w", err)
		} else if n := len(peers); n > 10 {
			frand.Shuffle(n, func(i, j int) {
				peers[i], peers[j] = peers[j], peers[i]
			})
			peers = peers[:10]
		}
		for _, p := range peers {
			r.Peers = append(r.Peers, p.Address)
		}
		if err := stream.WriteResponse(r); err != nil {
			return err
		}
		return nil

	case *gateway.RPCDiscoverIP:
		r.IP, _, _ = net.SplitHostPort(origin.t.Addr)
		if err := stream.WriteResponse(r); err != nil {
			return err
		}
		return nil

	case *gateway.RPCRelayHeader:
		if err := stream.ReadRequest(r); err != nil {
			return err
		}
		cs, ok := s.cm.State(r.Header.ParentID)
		if !ok {
			s.resync(origin, fmt.Sprintf("peer relayed a header with unknown parent (%v)", r.Header.ParentID))
			return nil
		}
		bid := r.Header.ID()
		if _, ok := s.cm.State(bid); ok {
			return nil // already seen
		} else if bid.CmpWork(cs.ChildTarget) < 0 {
			return s.ban(origin, errors.New("peer sent header with insufficient work"))
		} else if r.Header.ParentID != s.cm.Tip().ID {
			// block extends a sidechain, which peer (if honest) believes to be the
			// heaviest chain
			s.resync(origin, "peer relayed a header that does not attach to our tip")
			return nil
		}
		// request + validate full block
		if b, err := origin.SendBlock(r.Header.ID(), s.config.SendBlockTimeout); err != nil {
			// log-worthy, but not ban-worthy
			log.Warn("couldn't retrieve new block after header relay", zap.Stringer("header", r.Header.ID()), zap.Error(err))
			return nil
		} else if err := s.cm.AddBlocks([]types.Block{b}); err != nil {
			return s.ban(origin, err)
		}
		s.relayHeader(r.Header, origin) // non-blocking
		return nil

	case *gateway.RPCRelayTransactionSet:
		if err := stream.ReadRequest(r); err != nil {
			return err
		}
		if len(r.Transactions) == 0 {
			return s.ban(origin, errors.New("peer sent an empty transaction set"))
		} else if known, err := s.cm.AddPoolTransactions(r.Transactions); !known {
			if err != nil {
				// too risky to ban here (txns are probably just outdated), but at least
				// log it if we think we're synced
				if b, ok := s.cm.Block(s.cm.Tip().ID); ok && time.Since(b.Timestamp) < 2*s.cm.TipState().BlockInterval() {
					log.Debug("invalid transaction set received", zap.Error(err))
				}
			} else {
				s.relayTransactionSet(r.Transactions, origin) // non-blocking
			}
		}
		return nil

	case *gateway.RPCSendBlk:
		err := stream.ReadRequest(r)
		if err != nil {
			return err
		}
		var ok bool
		r.Block, ok = s.cm.Block(r.ID)
		if !ok {
			return fmt.Errorf("block %v not found", r.ID)
		} else if err := stream.WriteResponse(r); err != nil {
			return err
		}
		return nil

	case *gateway.RPCSendBlocks:
		err := stream.ReadRequest(r)
		if err != nil {
			return err
		}
		for {
			var rem uint64
			r.Blocks, rem, err = s.cm.BlocksForHistory(r.History[:], 10)
			if err != nil {
				return err
			} else if err := stream.WriteResponse(r); err != nil {
				return err
			} else if err := stream.WriteResponse(&gateway.RPCSendBlocksMoreAvailable{MoreAvailable: rem > 0}); err != nil {
				return err
			} else if rem == 0 {
				return nil
			}
			r.History[0] = r.Blocks[len(r.Blocks)-1].ID()
		}

	case *gateway.RPCSendTransactions:
		err := stream.ReadRequest(r)
		if err != nil {
			return err
		}

		if b, ok := s.cm.Block(r.Index.ID); ok {
			// get txns from block
			want := make(map[types.Hash256]bool)
			for _, h := range r.Hashes {
				want[h] = true
			}
			for _, txn := range b.Transactions {
				if want[txn.MerkleLeafHash()] {
					r.Transactions = append(r.Transactions, txn)
				}
			}
			for _, txn := range b.V2Transactions() {
				if want[txn.MerkleLeafHash()] {
					r.V2Transactions = append(r.V2Transactions, txn)
				}
			}
		} else {
			// get txns from txpool
			r.Transactions, r.V2Transactions = s.cm.TransactionsForPartialBlock(r.Hashes)
		}
		if err := stream.WriteResponse(r); err != nil {
			return err
		}
		return nil

	case *gateway.RPCSendCheckpoint:
		err := stream.ReadRequest(r)
		if err != nil {
			return err
		}
		var ok1, ok2 bool
		r.Block, ok1 = s.cm.Block(r.Index.ID)
		r.State, ok2 = s.cm.State(r.Block.ParentID)
		if !ok1 || !ok2 {
			return fmt.Errorf("checkpoint %v::%v not found", r.Index.Height, r.Index.ID)
		} else if err := stream.WriteResponse(r); err != nil {
			return err
		}
		return nil

	case *gateway.RPCRelayV2Header:
		if err := stream.ReadRequest(r); err != nil {
			return err
		}
		cs, ok := s.cm.State(r.Header.ParentID)
		if !ok {
			s.resync(origin, fmt.Sprintf("peer relayed a v2 header with unknown parent (%v)", r.Header.ParentID))
			return nil
		}
		bid := r.Header.ID()
		if _, ok := s.cm.State(bid); ok {
			return nil // already seen
		} else if bid.CmpWork(cs.ChildTarget) < 0 {
			return s.ban(origin, errors.New("peer sent v2 header with insufficient work"))
		} else if r.Header.ParentID != s.cm.Tip().ID {
			// block extends a sidechain, which peer (if honest) believes to be the
			// heaviest chain
			s.resync(origin, "peer relayed a v2 header that does not attach to our tip")
			return nil
		}
		// header is sufficiently valid; relay it
		//
		// NOTE: The purpose of header announcements is to inform the network as
		// quickly as possible that a new block has been found. A proper
		// BlockOutline should follow soon after, allowing peers to obtain the
		// actual block. As such, we take no action here other than relaying.
		s.relayV2Header(r.Header, origin) // non-blocking
		return nil

	case *gateway.RPCRelayV2BlockOutline:
		if err := stream.ReadRequest(r); err != nil {
			return err
		}
		cs, ok := s.cm.State(r.Block.ParentID)
		if !ok {
			s.resync(origin, fmt.Sprintf("peer relayed a v2 outline with unknown parent (%v)", r.Block.ParentID))
			return nil
		}
		bid := r.Block.ID(cs)
		if _, ok := s.cm.State(bid); ok {
			return nil // already seen
		} else if bid.CmpWork(cs.ChildTarget) < 0 {
			return s.ban(origin, errors.New("peer sent v2 outline with insufficient work"))
		} else if r.Block.ParentID != s.cm.Tip().ID {
			// block extends a sidechain, which peer (if honest) believes to be the
			// heaviest chain
			s.resync(origin, "peer relayed a v2 outline that does not attach to our tip")
			return nil
		}
		log.Debug("received v2 block outline", zap.Stringer("blockID", bid), zap.Stringer("origin", origin))
		// block has sufficient work and attaches to our tip, but may be missing
		// transactions; first, check for them in our txpool; then, if block is
		// still incomplete, request remaining transactions from the peer
		txns, v2txns := s.cm.TransactionsForPartialBlock(r.Block.Missing())
		b, missing := r.Block.Complete(cs, txns, v2txns)
		if len(missing) > 0 {
			index := types.ChainIndex{Height: r.Block.Height, ID: bid}
			txns, v2txns, err := origin.SendTransactions(index, missing, s.config.SendTransactionsTimeout)
			if err != nil {
				// log-worthy, but not ban-worthy
				log.Debug("couldn't retrieve missing transactions from peer", zap.Stringer("blockID", bid), zap.Stringer("origin", origin), zap.Error(err))
				return nil
			}
			b, missing = r.Block.Complete(cs, txns, v2txns)
			if len(missing) > 0 {
				// inexcusable
				return s.ban(origin, errors.New("peer sent wrong missing transactions for a block it relayed"))
			}
		}
		if err := s.cm.AddBlocks([]types.Block{b}); err != nil {
			return s.ban(origin, err)
		}
		log.Debug("added v2 block", zap.Stringer("blockID", bid), zap.Stringer("origin", origin))
		// when we forward the block, exclude any txns that were in our txpool,
		// since they're probably present in our peers' txpools as well
		//
		// NOTE: crucially, we do NOT exclude any txns we had to request from the
		// sending peer, since other peers probably don't have them either
		r.Block.RemoveTransactions(txns, v2txns)
		go s.relayV2BlockOutline(r.Block, origin) // non-blocking
		return nil

	case *gateway.RPCRelayV2TransactionSet:
		if err := stream.ReadRequest(r); err != nil {
			return err
		}
		if _, ok := s.cm.Block(r.Index.ID); !ok {
			s.resync(origin, fmt.Sprintf("peer %v relayed a v2 transaction set with unknown basis (%v)", origin, r.Index))
		} else if len(r.Transactions) == 0 {
			return s.ban(origin, errors.New("peer sent an empty transaction set"))
		} else if known, err := s.cm.AddV2PoolTransactions(r.Index, r.Transactions); !known {
			if err != nil {
				s.log.Debug("received invalid transaction set", zap.Stringer("origin", origin), zap.Error(err))
			} else {
				s.relayV2TransactionSet(r.Index, r.Transactions, origin) // non-blocking
			}
		}
		return nil

	case *gateway.RPCSendV2Blocks:
		err := stream.ReadRequest(r)
		if err != nil {
			return err
		}
		if r.Max > 100 {
			r.Max = 100
		}
		r.Blocks, r.Remaining, err = s.cm.BlocksForHistory(r.History, r.Max)
		if err != nil {
			return err
		} else if err := stream.WriteResponse(r); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unrecognized RPC: %q", id)
	}
}
