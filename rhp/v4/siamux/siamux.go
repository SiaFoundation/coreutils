package siamux

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/mux"
	"go.uber.org/zap"
)

const (
	// defaultDialTimeout is the default timeout applied when dialing a
	// TCP connection. An earlier timeout may be enforced by passing
	// a context to the Dial func.
	defaultDialTimeout = time.Minute
	// defaultMuxHandshakeTimeout is the default timeout applied when upgrading a
	// connection to a siamux connection
	defaultMuxHandshakeTimeout = 10 * time.Second

	// Protocol is the identifier for the SiaMux transport protocol.
	Protocol chain.Protocol = "siamux"
)

// client is a TransportClient that uses the SiaMux multiplexer.
type client struct {
	m       *mux.Mux
	peerKey types.PublicKey
	close   chan struct{}
}

// Close implements the [TransportClient] interface.
func (c *client) Close() error {
	select {
	case <-c.close:
	default:
		close(c.close)
	}
	return c.m.Close()
}

// FrameSize implements the [TransportClient] interface.
func (c *client) FrameSize() int {
	// 4320 is the max frame size for SiaMux - 16 byte chacha20 poly1305 overhead - 8 byte length prefix
	return 4296 // from SiaMux handshake.go
}

func (c *client) PeerKey() types.PublicKey {
	return c.peerKey
}

// DialStream implements the [TransportClient] interface.
func (c *client) DialStream() (net.Conn, error) {
	s := c.m.DialStream()
	return s, nil
}

// Dial creates a new TransportClient using the SiaMux transport.
func Dial(ctx context.Context, addr string, peerKey types.PublicKey) (rhp4.TransportClient, error) {
	deadline, ok := ctx.Deadline()
	if !ok || deadline.IsZero() {
		deadline = time.Now().Add(defaultDialTimeout)
	}

	conn, err := (&net.Dialer{
		Deadline: deadline,
	}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %q: %w", addr, err)
	}
	return Upgrade(ctx, conn, peerKey)
}

// Upgrade upgrades an existing connection to use the SiaMux transport.
func Upgrade(ctx context.Context, conn net.Conn, peerKey types.PublicKey) (rhp4.TransportClient, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	deadline, ok := ctx.Deadline()
	if !ok || deadline.IsZero() {
		deadline = time.Now().Add(defaultMuxHandshakeTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("failed to set deadline: %w", err)
	}
	m, err := mux.Dial(conn, peerKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to establish siamux connection: %w", err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("failed to clear deadline: %w", err)
	}
	return &client{
		m:       m,
		peerKey: peerKey,
		close:   make(chan struct{}),
	}, nil
}

// A transport is a rhp4.Transport that wraps a mux.Mux.
type transport struct {
	m *mux.Mux
}

// Close implements the rhp4.Transport interface.
func (t *transport) Close() error {
	return t.m.Close()
}

// AcceptStream implements the rhp4.Transport interface.
func (t *transport) AcceptStream() (net.Conn, error) {
	return t.m.AcceptStream()
}

// serveOption contains options for the Serve function.
type serveOption struct {
	TCPKeepalivePeriod time.Duration
}

// ServeOption is a functional parameter for the Serve function.
type ServeOption func(*serveOption)

// WithTCPKeepalivePeriod sets the TCP keepalive period for accepted tcp connections.
func WithTCPKeepalivePeriod(d time.Duration) ServeOption {
	return func(opt *serveOption) {
		opt.TCPKeepalivePeriod = d
	}
}

// Serve serves RHP4 connections on the listener l using the SiaMux transport.
func Serve(l net.Listener, s *rhp4.Server, log *zap.Logger, opts ...ServeOption) {
	so := serveOption{
		TCPKeepalivePeriod: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(&so)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Fatal("failed to accept connection", zap.Error(err))
			}
			return
		}
		log := log.With(zap.Stringer("peerAddress", conn.RemoteAddr()))

		if tcpConn, ok := conn.(*net.TCPConn); ok && so.TCPKeepalivePeriod > 0 {
			err := tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
				Enable:   true,
				Idle:     so.TCPKeepalivePeriod, // send first probe after TCPKeepalivePeriod of idle time
				Interval: 5 * time.Second,       // 5 seconds between probes
				Count:    6,                     // consider the connection dead after 6 failed probes
			})
			if err != nil {
				log.Error("failed to set TCP keepalive config", zap.Error(err))
			}
		}

		go func() {
			defer conn.Close()

			if err := conn.SetDeadline(time.Now().Add(defaultMuxHandshakeTimeout)); err != nil {
				log.Error("failed to set deadline", zap.Error(err))
				return
			}

			m, err := mux.Accept(conn, ed25519.PrivateKey(s.HostKey()))
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					log.Debug("failed to upgrade connection", zap.Error(err))
				}
				return
			}
			if err := conn.SetDeadline(time.Time{}); err != nil {
				log.Error("failed to clear deadline", zap.Error(err))
				return
			}
			s.Serve(&transport{m}, log)
		}()
	}
}
