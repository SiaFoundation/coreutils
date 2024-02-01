package rhp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	Options struct {
		// DialTimeout is the timeout for dialing a new connection.
		// Defaults to 1 minute.
		DialTimeout time.Duration

		// IdleTimeout is the timeout for how long a client can be idle before
		// the underlying connection is reset.
		IdleTimeout time.Duration

		// RPCTimeout is the timeout for how long an RPC can run. Defaults to 5 minutes.
		RPCTimeout time.Duration
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

func NewClient(ctx context.Context, addr string, hostKey types.PublicKey, opts Options) (*Client, error) {
	if opts.DialTimeout == 0 {
		opts.DialTimeout = time.Minute
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = 30 * time.Second
	}
	if opts.RPCTimeout == 0 {
		opts.RPCTimeout = 5 * time.Minute
	}
	c := &Client{
		addr:        addr,
		hostKey:     hostKey,
		openStreams: 0,
		lastSuccess: time.Now(),
		idleTimeout: opts.IdleTimeout,
		rpcTimeout:  opts.RPCTimeout,
	}
	return c, c.resetTransport(ctx)
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

// do performs an RPC with a host in a way that allows the caller to interrupt
// it
func (c *Client) do(ctx context.Context, rpc rhpv4.RPC) error {
	done := make(chan struct{})
	var doErr error
	var s *rhpv4.Stream
	go func() {
		defer close(done)

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
		return rhpv4.HostSettings{}, err
	}
	return rpc.Settings, nil
}

// FormContract forms a new contract with the host.
func (c *Client) FormContract(ctx context.Context) (types.V2FileContract, error) {
	panic("implement me")
}

// RenewContract renews a contract with the host, immediately unlocking
func (c *Client) RenewContract(ctx context.Context) (types.V2FileContractRenewal, error) {
	panic("implement me")
}

// PinSectors pins sectors to a contract. Commonly used to pin sectors uploaded
// with 'UploadSector'.
func (c *Client) PinSectors(ctx context.Context, roots []types.Hash256) error {
	panic("implement me")
}

// PruneContract prunes the sectors with the given indices from a contract.
func (c *Client) PruneContract(ctx context.Context, sectorIndices []int) error {
	panic("implement me")
}

// LatestRevision returns the latest revision for a given contract.
func (c *Client) LatestRevision(ctx context.Context) (types.V2FileContract, error) {
	panic("implement me")
}

// ReadSector reads a sector from the host.
func (c *Client) ReadSector(ctx context.Context) ([]byte, error) {
	panic("implement me")
}

// WriteSector stores a sector in the host's temporary storage. To make it
// permanent, use 'PinSectors'.
func (c *Client) WriteSector(ctx context.Context, data [rhpv4.SectorSize]byte) error {
	panic("implement me")
}

// SectorRoots returns the roots of a contract.
func (c *Client) SectorRoots(ctx context.Context) ([]types.Hash256, error) {
	panic("implement me")
}

// AccountBalance returns the balance of a given account.
func (c *Client) AccountBalance(ctx context.Context) (types.Currency, error) {
	panic("implement me")
}

// FundAccount adds to the balance to an account and returns the new balance.
func (c *Client) FundAccount(ctx context.Context) (types.Currency, error) {
	panic("implement me")
}
