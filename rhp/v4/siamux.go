package rhp

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
)

const (
	// ProtocolTCPSiaMux is the identifier for the SiaMux transport.
	ProtocolTCPSiaMux chain.Protocol = "siamux"
)

// siaMuxClientTransport is a TransportClient that uses the SiaMux multiplexer.
type siaMuxClientTransport struct {
	m       *mux.Mux
	peerKey types.PublicKey
	close   chan struct{}
}

// Close implements the [TransportClient] interface.
func (t *siaMuxClientTransport) Close() error {
	select {
	case <-t.close:
	default:
		close(t.close)
	}
	return t.m.Close()
}

func (t *siaMuxClientTransport) FrameSize() int {
	return 1440 * 3 // from SiaMux handshake.go
}

func (t *siaMuxClientTransport) PeerKey() types.PublicKey {
	return t.peerKey
}

// DialStream implements the [TransportClient] interface.
func (t *siaMuxClientTransport) DialStream() (net.Conn, error) {
	select {
	case <-t.close:
		return nil, fmt.Errorf("transport closed")
	default:
	}
	s := t.m.DialStream()
	return s, nil
}

// DialSiaMux creates a new TransportClient using the SiaMux transport.
func DialSiaMux(ctx context.Context, addr string, peerKey types.PublicKey) (TransportClient, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %q: %w", addr, err)
	}

	return UpgradeConnSiamux(ctx, conn, peerKey)
}

// UpgradeConnSiamux upgrades an existing connection to use the SiaMux transport.
func UpgradeConnSiamux(ctx context.Context, conn net.Conn, peerKey types.PublicKey) (TransportClient, error) {
	m, err := mux.Dial(conn, peerKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to establish siamux connection: %w", err)
	}
	return &siaMuxClientTransport{
		m:       m,
		peerKey: peerKey,
		close:   make(chan struct{}),
	}, nil
}

// A muxTransport is a rhp4.Transport that wraps a mux.Mux.
type muxTransport struct {
	m *mux.Mux
}

// Close implements the rhp4.Transport interface.
func (mt *muxTransport) Close() error {
	return mt.m.Close()
}

// AcceptStream implements the rhp4.Transport interface.
func (mt *muxTransport) AcceptStream() (net.Conn, error) {
	return mt.m.AcceptStream()
}

// ServeSiaMux serves RHP4 connections on l using the provided server and logger.
func ServeSiaMux(l net.Listener, s *Server, log *zap.Logger) {
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
			s.Serve(&muxTransport{m}, log)
		}()
	}
}
