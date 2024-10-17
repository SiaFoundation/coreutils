package rhp

import (
	"context"
	"fmt"
	"net"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/mux/v2"
)

// A Protocol is a string identifying a network protocol that a host may be
// reached on.
const (
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

// DialStream implements the [TransportClient] interface. The stream lifetime is
// scoped to the context; if the context is canceled, the stream is closed.
func (t *siaMuxClientTransport) DialStream(ctx context.Context) net.Conn {
	s := t.m.DialStream()
	go func() {
		select {
		case <-ctx.Done():
		case <-t.close:
		}
		s.Close()
	}()
	return s
}

// DialSiaMux creates a new TransportClient using the SiaMux multiplexer.
func DialSiaMux(ctx context.Context, addr string, peerKey types.PublicKey) (TransportClient, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %q: %w", addr, err)
	}

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
