package rhp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// ProtocolQUIC is the identifier for the QUIC/WebTransport transport.
	ProtocolQUIC chain.Protocol = "quic"
)

type (
	quicStream struct {
		quic.Stream

		localAddr  net.Addr
		remoteAddr net.Addr
	}

	quicClientTransport struct {
		conn    quic.Connection
		peerKey types.PublicKey
		close   chan struct{}
	}

	// A QUICTransportOption can be used to configure the QUIC transport client
	QUICTransportOption func(*quic.Config, *tls.Config)
)

// WithTLSConfig is a QUICTransportOption that sets the TLSConfig
// for the QUIC connection.
func WithTLSConfig(fn func(*tls.Config)) QUICTransportOption {
	return func(qc *quic.Config, tc *tls.Config) {
		fn(tc)
	}
}

// LocalAddr Implements net.Conn
func (qs *quicStream) LocalAddr() net.Addr {
	return qs.localAddr
}

// RemoteAddr Implements net.Conn
func (qs *quicStream) RemoteAddr() net.Addr {
	return qs.remoteAddr
}

// Close Implements [TransportClient]
func (qt *quicClientTransport) Close() error {
	return qt.conn.CloseWithError(0, "")
}

// FrameSize Implements [TransportClient]
func (qt *quicClientTransport) FrameSize() int {
	return 1440 * 3
}

// PeerKey Implements [TransportClient]
func (qt *quicClientTransport) PeerKey() types.PublicKey {
	return qt.peerKey
}

// DialStream Implements [TransportClient]
func (qt *quicClientTransport) DialStream() (net.Conn, error) {
	s, err := qt.conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return &quicStream{Stream: s, localAddr: qt.conn.LocalAddr(), remoteAddr: qt.conn.RemoteAddr()}, nil
}

// DialQUIC creates a new TransportClient using the QUIC transport.
func DialQUIC(ctx context.Context, addr string, peerKey types.PublicKey, opts ...QUICTransportOption) (TransportClient, error) {
	tc := &tls.Config{
		Rand:       frand.Reader,
		NextProtos: []string{"sia/rhp4"},
	}
	qc := &quic.Config{
		EnableDatagrams: true,
	}
	for _, opt := range opts {
		opt(qc, tc)
	}

	conn, err := quic.DialAddr(ctx, addr, tc, qc)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %q: %w", addr, err)
	}
	return &quicClientTransport{
		conn:    conn,
		peerKey: peerKey,
		close:   make(chan struct{}),
	}, nil
}

type (
	qtTransport struct {
		qc quic.Connection
	}
)

// AcceptStream Implements [TransportServer]
func (qt *qtTransport) AcceptStream() (net.Conn, error) {
	s, err := qt.qc.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &quicStream{
		Stream:     s,
		localAddr:  qt.qc.LocalAddr(),
		remoteAddr: qt.qc.RemoteAddr(),
	}, nil
}

// Close Implements [TransportServer]
func (qt *qtTransport) Close() error {
	return qt.qc.CloseWithError(0, "")
}

// wtTranposrt is a rhp4.Transport that wraps a WebTransport Session
type wtTransport struct {
	sess *webtransport.Session
}

type wtStream struct {
	webtransport.Stream

	localAddr, remoteAddr net.Addr
}

func (wts *wtStream) LocalAddr() net.Addr {
	return wts.localAddr
}

func (wts *wtStream) RemoteAddr() net.Addr {
	return wts.remoteAddr
}

// Close implements the rhp4.Transport interface.
func (wt *wtTransport) Close() error {
	return wt.sess.CloseWithError(0, "")
}

// AcceptStream implements the rhp4.Transport interface.
func (wt *wtTransport) AcceptStream() (net.Conn, error) {
	conn, err := wt.sess.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &wtStream{
		Stream:     conn,
		localAddr:  wt.sess.LocalAddr(),
		remoteAddr: wt.sess.RemoteAddr(),
	}, nil
}

// ServeQUIC serves RHP4 connections on l using the provided server and logger.
func ServeQUIC(l *quic.Listener, s *Server, log *zap.Logger) {
	wts := &webtransport.Server{
		// no need for TLS config since it will already be negotiated
	}
	defer wts.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/sia/rhp/v4", func(w http.ResponseWriter, r *http.Request) {
		sess, err := wts.Upgrade(w, r)
		if err != nil {
			log.Debug("webtransport upgrade failed", zap.Error(err))
			return
		}
		defer sess.CloseWithError(0, "")

		err = s.Serve(&wtTransport{
			sess: sess,
		}, log)
		if err != nil {
			log.Debug("failed to serve connection", zap.Error(err))
		}
	})
	wts.H3.Handler = mux

	for {
		conn, err := l.Accept(context.Background())
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, quic.ErrServerClosed) {
				log.Fatal("failed to accept connection", zap.Error(err))
			}
			return
		}
		log.Debug("accepted connection")
		proto := conn.ConnectionState().TLS.NegotiatedProtocol
		log := log.With(zap.String("peerAddress", conn.RemoteAddr().String()), zap.String("protocol", proto))
		log.Debug("serving connection")
		switch proto {
		case http3.NextProtoH3: // webtransport
			go func() {
				defer conn.CloseWithError(0, "")
				wts.ServeQUICConn(conn)
			}()
		case "sia/rhp4": // quic
			go func() {
				defer conn.CloseWithError(0, "")
				log.Debug("serving rhp4 connection")
				if err := s.Serve(&qtTransport{qc: conn}, log); err != nil {
					log.Debug("failed to serve connection", zap.Error(err))
				}
			}()
		default:
			conn.CloseWithError(10, "invalid alpn") // TODO: define standard error codes
		}
	}
}
