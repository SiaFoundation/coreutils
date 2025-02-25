package siamux

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/mux"
	"go.uber.org/zap"
)

const (
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

func (c *client) FrameSize() int {
	return 1440 * 3 // from SiaMux handshake.go
}

func (c *client) PeerKey() types.PublicKey {
	return c.peerKey
}

// DialStream implements the [TransportClient] interface.
func (c *client) DialStream() (net.Conn, error) {
	select {
	case <-c.close:
		return nil, fmt.Errorf("transport closed")
	default:
	}
	s := c.m.DialStream()
	return s, nil
}

// Dial creates a new TransportClient using the SiaMux transport.
func Dial(ctx context.Context, addr string, peerKey types.PublicKey) (rhp4.TransportClient, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %q: %w", addr, err)
	}

	return Upgrade(ctx, conn, peerKey)
}

// Upgrade upgrades an existing connection to use the SiaMux transport.
func Upgrade(ctx context.Context, conn net.Conn, peerKey types.PublicKey) (rhp4.TransportClient, error) {
	m, err := mux.Dial(conn, peerKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to establish siamux connection: %w", err)
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

// Serve serves RHP4 connections on the listener l using the SiaMux transport.
func Serve(l net.Listener, s *rhp4.Server, log *zap.Logger) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Fatal("failed to accept connection", zap.Error(err))
			}
			return
		}
		log := log.With(zap.Stringer("peerAddress", conn.RemoteAddr()))

		go func() {
			defer conn.Close()
			m, err := mux.Accept(conn, ed25519.PrivateKey(s.HostKey()))
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					log.Fatal("failed to upgrade connection", zap.Error(err))
				}
				return
			}
			s.Serve(&transport{m}, log)
		}()
	}
}
