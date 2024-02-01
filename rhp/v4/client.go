package rhp

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

var (
	defaultOptions = Options{
		DialTimeout: time.Minute,
		IdleTimeout: 30 * time.Second,
		RPCTimeout:  5 * time.Minute,
	}
)

type (
	Option func(o *Options)

	// Options are used to configure the client during creation.
	Options struct {
		DialTimeout time.Duration // timeout for dialing a new connection
		IdleTimeout time.Duration // timeout for idle connections before recreating them
		RPCTimeout  time.Duration // timeout for RPCs
	}

	// Client is a client for the v4 renter-host protocol. After successful
	// creation, a client will try to maintain a connection to the host and
	// recreate it if necessary. In addition to various timeouts (see Options),
	// every RPC can be interrupted using a context.
	Client struct {
		addr    string
		hostKey types.PublicKey

		dialTimeout time.Duration
		idleTimeout time.Duration
		rpcTimeout  time.Duration

		mu          sync.Mutex
		openStreams int
		lastSuccess time.Time
		t           *rhpv4.Transport
	}
)

// WithDialTimeout overwrites the default dial timeout of 1 minute.
func WithDialTimeout(d time.Duration) Option {
	return func(opts *Options) {
		opts.DialTimeout = d
	}
}

// WithIdleTimeout overwrites the default idle timeout of 30 seconds.
func WithIdleTimeout(d time.Duration) Option {
	return func(opts *Options) {
		opts.IdleTimeout = d
	}
}

// WithRPCTimeout overwrites the default RPC timeout of 5 minutes.
func WithRPCTimeout(d time.Duration) Option {
	return func(opts *Options) {
		opts.RPCTimeout = d
	}
}

// NewClient creates a new client for the v4 renter-host protocol.
func NewClient(ctx context.Context, addr string, hostKey types.PublicKey, opts ...Option) (*Client, error) {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	c := &Client{
		addr:        addr,
		hostKey:     hostKey,
		openStreams: 0,
		lastSuccess: time.Now(),
		dialTimeout: o.DialTimeout,
		idleTimeout: o.IdleTimeout,
		rpcTimeout:  o.RPCTimeout,
	}
	return c, c.resetTransport(ctx)
}

// do performs an RPC with a host in a way that allows the caller to interrupt
// it
func (c *Client) do(ctx context.Context, rpc rhpv4.RPC) error {
	done := make(chan struct{})
	var doErr error
	var s *rhpv4.Stream
	go func() {
		defer close(done)

		// defer recover
		defer func() {
			if r := recover(); r != nil {
				doErr = fmt.Errorf("a panic occurred while executing the rpc: %v", r)
			}
		}()

		// reset the transport if it hasn't been used in a while
		c.mu.Lock()
		if c.t == nil || (c.openStreams == 0 && time.Since(c.lastSuccess) > c.idleTimeout) {
			if err := c.resetTransport(ctx); err != nil {
				c.mu.Unlock()
				doErr = err
				return
			}
		}
		c.openStreams++
		c.mu.Unlock()

		defer func() {
			c.mu.Lock()
			c.openStreams--
			c.mu.Unlock()
		}()

		// dial a stream with a sane deadline
		var err error
		s, err = c.t.DialStream()
		if err != nil {
			doErr = fmt.Errorf("failed to dial stream: %w", err)
			return
		} else if err = s.SetDeadline(time.Now().Add(c.rpcTimeout)); err != nil {
			doErr = fmt.Errorf("failed to set deadline: %w", err)
			return
		}
		defer s.Close()

		// write rpc id
		if err := s.WriteID(rpc); err != nil {
			doErr = fmt.Errorf("failed to write rpc id: %w", err)
			return
		}

		// the write succeeded, the connection is still intact
		defer func() {
			c.mu.Lock()
			c.lastSuccess = time.Now()
			c.mu.Unlock()
		}()

		// perform remaining rpc
		if err := s.WriteRequest(rpc); err != nil {
			doErr = fmt.Errorf("failed to write rpc request: %w", err)
			return
		} else if err := s.ReadResponse(rpc); err != nil {
			doErr = fmt.Errorf("failed to read rpc response: %w", err)
			return
		}
	}()
	select {
	case <-ctx.Done():
		// Caller interrupted the RPC - optimistically set deadline to abort
		// goroutine as soon as possible
		s.SetDeadline(time.Now())
		return ctx.Err()
	case <-done:
		return doErr
	}
}

func (c *Client) resetTransport(ctx context.Context) error {
	conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout)
	if err != nil {
		return fmt.Errorf("failed to dial tcp connection: %w", err)
	} else if conn.(*net.TCPConn).SetKeepAlive(true); err != nil {
		return fmt.Errorf("failed to set keepalive: %w", err)
	} else if conn.SetDeadline(time.Now().Add(c.dialTimeout)); err != nil {
		return fmt.Errorf("failed to set dial deadline on tcp connection: %w", err)
	} else if t, err := rhpv4.Dial(conn, c.hostKey); err != nil {
		return fmt.Errorf("failed to dial mux: %w", err)
	} else if err := conn.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("failed to revoke deadline on tcp connection")
	} else {
		c.t = t
	}
	return nil
}

// AuditContract probabilistically audits a contract, checking whether the host
// has missing sectors. The input specifies the number of sectors we randomly
// pick so the higher the nummer, the less likely it is that the host is missing
// sectors. Any missing sectors found are returned.
func (c *Client) AuditContract(ctx context.Context, n int) ([]interface{}, error) {
	panic("implement me")
}

// Settings returns the host's current settings, including its prices.
func (c *Client) Settings(ctx context.Context) (rhpv4.HostSettings, error) {
	rpc := rhpv4.RPCSettings{}
	if err := c.do(ctx, &rpc); err != nil {
		return rhpv4.HostSettings{}, fmt.Errorf("RPCSettings failed: %w", err)
	}
	return rpc.Settings, nil
}

// FormContract forms a new contract with the host.
func (c *Client) FormContract(ctx context.Context, hp rhpv4.HostPrices, contract types.V2FileContract, inputs []types.V2SiacoinInput) (types.V2FileContract, error) {
	rpc := rhpv4.RPCFormContract{
		Prices:       hp,
		Contract:     contract,
		RenterInputs: inputs,
	}
	if err := c.do(ctx, &rpc); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCFormContract failed: %w", err)
	}
	panic("incomplete rpc - missing outputs")
	// TODO: verify host signatures
	return rpc.Contract, nil
}

// RenewContract renews a contract with the host, immediately unlocking
func (c *Client) RenewContract(ctx context.Context, hp rhpv4.HostPrices, finalRevision, initialRevision types.V2FileContract) (types.V2FileContractRenewal, error) {
	panic("incomplete rpc -- missing inputs and outputs")
}

// PinSectors pins sectors to a contract. Commonly used to pin sectors uploaded
// with 'UploadSector'. PinSectors will first overwrite the provided gaps and
// then start appending roots to the end of the contract. So if more roots than
// gaps were provided and the method returns an error, it is safe to assume all
// gaps were filled. PinSectors fails if more roots than gaps are provided since
// it sorts the gaps to find duplicates which makes it hard for the caller to
// know which gaps got filled.
func (c *Client) PinSectors(ctx context.Context, contract types.V2FileContract, hp rhpv4.HostPrices, roots []types.Hash256, gaps []uint64) (types.V2FileContract, error) {
	// sanity check input - no duplicate gaps, at most one gap per root
	if len(gaps) > len(roots) {
		return types.V2FileContract{}, fmt.Errorf("more gaps than roots provided")
	}
	slices.Sort(gaps)
	for i := 1; i < len(gaps); i++ {
		if gaps[i] == gaps[i-1] {
			return types.V2FileContract{}, fmt.Errorf("gap %v is duplicated", gaps[i])
		}
	}

	actions := make([]rhpv4.WriteAction, len(roots))
	for i := range roots {
		if len(gaps) > 0 {
			actions[i] = rhpv4.WriteAction{}
			panic("incomplete type")
			gaps = gaps[1:]
		} else {
			actions[i] = rhpv4.WriteAction{
				Type: rhpv4.ActionAppend,
				Root: roots[i],
			}
		}
	}
	rpcModify := rhpv4.RPCModifySectors{
		Actions: actions,
	}
	if err := c.do(ctx, &rpcModify); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCModifySectors failed: %w", err)
	}

	// TODO: verify proof & build new revision
	var rev types.V2FileContract

	rpcRevise := rhpv4.RPCReviseContract{
		Prices:   hp,
		Revision: rev,
	}
	if err := c.do(ctx, &rpcRevise); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCReviseSectors failed: %w", err)
	}

	// TODO: verify host signatures
	return rpcRevise.Revision, nil
}

// PruneContract prunes the sectors with the given indices from a contract.
func (c *Client) PruneContract(ctx context.Context, contract types.V2FileContract, hp rhpv4.HostPrices, nSectors uint64, sectorIndices []uint64) (types.V2FileContract, error) {
	if len(sectorIndices) == 0 {
		return types.V2FileContract{}, nil // nothing to do
	} else if nSectors == 0 {
		return types.V2FileContract{}, fmt.Errorf("trying to prune empty contract")
	}

	// sanity check input - no out-of-bounds indices, no duplicates
	lastIndex := nSectors - 1
	slices.Sort(sectorIndices)
	if sectorIndices[len(sectorIndices)-1] > lastIndex {
		return types.V2FileContract{}, fmt.Errorf("sector index %v is out of bounds for contract with %v sectors", sectorIndices[len(sectorIndices)-1], nSectors)
	}
	for i := 1; i < len(sectorIndices); i++ {
		if sectorIndices[i] == sectorIndices[i-1] {
			return types.V2FileContract{}, fmt.Errorf("sector index %v is duplicated", sectorIndices[i])
		}
	}

	// swap out sectors to delete
	actions := make([]rhpv4.WriteAction, len(sectorIndices))
	for i := range sectorIndices {
		actions[i] = rhpv4.WriteAction{
			Type: rhpv4.ActionSwap,
			A:    uint64(sectorIndices[i]),
			B:    lastIndex,
		}
		lastIndex--
	}

	// trim the swapped sectors
	actions = append(actions, rhpv4.WriteAction{
		N: uint64(len(actions)),
	})

	// modify sector
	rpcModify := rhpv4.RPCModifySectors{
		Actions: actions,
	}
	if err := c.do(ctx, &rpcModify); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCModifySectors failed: %w", err)
	}

	// TODO: check proof & build new revision
	var rev types.V2FileContract

	// revise contract
	rpcRevise := rhpv4.RPCReviseContract{
		Prices:   hp,
		Revision: rev,
	}
	if err := c.do(ctx, &rpcRevise); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCReviseSectors failed: %w", err)
	}

	// TODO: verify host signatures
	return rpcRevise.Revision, nil
}

// LatestRevision returns the latest revision for a given contract.
func (c *Client) LatestRevision(ctx context.Context, contractID types.FileContractID) (types.V2FileContract, error) {
	rpc := rhpv4.RPCLatestRevision{
		ContractID: contractID,
	}
	if err := c.do(ctx, &rpc); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCLatestRevision failed: %w", err)
	}
	return rpc.Contract, nil
}

// ReadSector reads a sector from the host.
func (c *Client) ReadSector(ctx context.Context, hp rhpv4.HostPrices, root types.Hash256, offset, length uint64) ([]byte, error) {
	// sanity check input - offset must be segment-aligned
	if offset%64 != 0 {
		return nil, fmt.Errorf("offset %v is not segment-aligned", offset)
	}
	rpc := rhpv4.RPCReadSector{
		Prices: hp,
		Root:   root,
		Offset: offset,
		Length: length,
	}
	if err := c.do(ctx, &rpc); err != nil {
		return nil, fmt.Errorf("RPCReadSector failed: %w", err)
	}
	// TODO: validate proof
	return rpc.Sector, nil
}

// WriteSector stores a sector in the host's temporary storage. To make it
// permanent, use 'PinSectors'. The provided data will be extended to
// rhpv4.SectorSize by the host so the client still pays for the full sector.
func (c *Client) WriteSector(ctx context.Context, hp rhpv4.HostPrices, data []byte) (types.Hash256, error) {
	if len(data) == 0 {
		return types.Hash256{}, fmt.Errorf("empty sector")
	} else if len(data) > rhpv4.SectorSize {
		return types.Hash256{}, fmt.Errorf("sector too large")
	}
	rpc := rhpv4.RPCWriteSector{
		Prices: hp,
		Sector: data,
	}
	if err := c.do(ctx, &rpc); err != nil {
		return types.Hash256{}, fmt.Errorf("RPCWriteSector failed: %w", err)
	}
	var sectorData [rhpv4.SectorSize]byte
	copy(sectorData[:], data)
	if rhpv2.SectorRoot((&sectorData)) != rpc.Root {
		return types.Hash256{}, fmt.Errorf("root mismatch")
	}
	panic("unfinished rpc - missing payment")
	return rpc.Root, nil
}

// SectorRoots returns 'length' roots of a contract starting at the given
// 'offset'.
func (c *Client) SectorRoots(ctx context.Context, hp rhpv4.HostPrices, offset, length uint64) ([]types.Hash256, error) {
	rpc := rhpv4.RPCSectorRoots{
		Prices: hp,
		Offset: offset,
		Length: length,
	}
	if err := c.do(ctx, &rpc); err != nil {
		return nil, fmt.Errorf("RPCSectorRoots failed: %w", err)
	}
	// TODO: verify proof
	panic("unfinished rpc - missing payment")
	return rpc.Roots, nil
}

// AccountBalance returns the balance of a given account.
func (c *Client) AccountBalance(ctx context.Context, account types.PublicKey) (types.Currency, error) {
	rpc := rhpv4.RPCAccountBalance{
		Account: types.PublicKey{},
	}
	if err := c.do(ctx, &rpc); err != nil {
		return types.Currency{}, fmt.Errorf("RPCAccountBalance failed: %w", err)
	}
	return rpc.Balance, nil
}

// FundAccount adds to the balance to an account and returns the new balance.
func (c *Client) FundAccount(ctx context.Context, contract types.V2FileContract, account types.PrivateKey) (types.V2FileContract, types.Currency, error) {
	// TODO: build new revision and signature
	var rev types.V2FileContract
	var sig types.Signature

	rpc := rhpv4.RPCFundAccount{
		Account:         account.PublicKey(),
		Revision:        rev,
		RenterSignature: sig,
	}
	if err := c.do(ctx, &rpc); err != nil {
		return types.V2FileContract{}, types.Currency{}, fmt.Errorf("RPCFundAccount failed: %w", err)
	}

	// TODO: verify host signature
	return rpc.Revision, rpc.NewBalance, nil
}

// ReviseContract is a more generic version of 'PinSectors' and 'PruneContract'.
// It's allows for arbitrary actions to be performed. Most users should use
// 'PinSectors' and 'PruneContract' instead.
func (c *Client) ReviseContract(ctx context.Context, hp rhpv4.HostPrices, actions []rhpv4.WriteAction) (types.V2FileContract, error) {
	modifyRPC := rhpv4.RPCModifySectors{
		Actions: actions,
	}
	if err := c.do(ctx, &modifyRPC); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCModifySectors failed: %w", err)
	}

	// TODO: verify proof & build new revision
	var rev types.V2FileContract

	reviseRPC := rhpv4.RPCReviseContract{
		Prices:   hp,
		Revision: rev,
	}
	if err := c.do(ctx, &reviseRPC); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCReviseContract failed: %w", err)
	}

	// TODO: verify host signatures
	return reviseRPC.Revision, nil
}
