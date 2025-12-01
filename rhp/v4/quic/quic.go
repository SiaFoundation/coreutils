package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// Protocol is the identifier for the QUIC/WebTransport transport.
	Protocol chain.Protocol = "quic"

	// TLSNextProtoRHP4 is the ALPN identifier for the Quic RHP4 protocol.
	TLSNextProtoRHP4 = "sia/rhp4"

	// maxIncomingStreams is the maximum number of incoming streams allowed per
	// QUIC connection by the server.
	maxIncomingStreams = 1000
)

type (
	stream struct {
		*quic.Stream

		localAddr  net.Addr
		remoteAddr net.Addr
	}

	client struct {
		conn    *quic.Conn
		peerKey types.PublicKey
	}

	// A CertManager provides a valid TLS certificate based on the given ClientHelloInfo.
	CertManager interface {
		// GetCertificate returns a Certificate based on the given ClientHelloInfo. It will only be called if the client supplies SNI information or if Certificates is empty.
		// If GetCertificate is nil or returns nil, then the certificate is retrieved from NameToCertificate. If NameToCertificate is nil, the best element of Certificates will be used.
		// Once a Certificate is returned it should not be modified.
		GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
	}

	// A ClientOption can be used to configure the QUIC transport client
	ClientOption func(*quic.Config, *tls.Config)
)

// WithTLSConfig is a QUICTransportOption that sets the TLSConfig
// for the QUIC connection.
func WithTLSConfig(fn func(*tls.Config)) ClientOption {
	return func(_ *quic.Config, tc *tls.Config) {
		fn(tc)
	}
}

func (s *stream) Close() error {
	err := s.Stream.Close()
	_, _ = s.Read([]byte{1}) // read until EOF for stream to be fully closed
	return err
}

// LocalAddr implements net.Conn
func (s *stream) LocalAddr() net.Addr {
	return s.localAddr
}

// RemoteAddr implements net.Conn
func (s *stream) RemoteAddr() net.Addr {
	return s.remoteAddr
}

// Close implements [TransportClient]
func (c *client) Close() error {
	return c.conn.CloseWithError(0, "")
}

// FrameSize implements [TransportClient]
func (c *client) FrameSize() int {
	return 1440 * 3
}

// PeerKey implements [TransportClient]
func (c *client) PeerKey() types.PublicKey {
	return c.peerKey
}

// DialStream implements [TransportClient]
func (c *client) DialStream(ctx context.Context) (net.Conn, error) {
	s, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return &stream{Stream: s, localAddr: c.conn.LocalAddr(), remoteAddr: c.conn.RemoteAddr()}, nil
}

// Dial creates a new TransportClient using the QUIC transport.
func Dial(ctx context.Context, addr string, peerKey types.PublicKey, opts ...ClientOption) (rhp4.TransportClient, error) {
	tc := &tls.Config{
		Rand:       frand.Reader,
		NextProtos: []string{TLSNextProtoRHP4},
	}
	qc := &quic.Config{
		EnableDatagrams:      true,
		HandshakeIdleTimeout: 15 * time.Second,
		KeepAlivePeriod:      30 * time.Second,
		MaxIdleTimeout:       30 * time.Minute,
	}
	for _, opt := range opts {
		opt(qc, tc)
	}

	conn, err := quic.DialAddr(ctx, addr, tc, qc)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %q: %w", addr, err)
	}
	return &client{
		conn:    conn,
		peerKey: peerKey,
	}, nil
}

type (
	transport struct {
		qc *quic.Conn
	}
)

// AcceptStream implements [TransportServer]
func (t *transport) AcceptStream() (net.Conn, error) {
	s, err := t.qc.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &stream{
		Stream:     s,
		localAddr:  t.qc.LocalAddr(),
		remoteAddr: t.qc.RemoteAddr(),
	}, nil
}

// Close implements [TransportServer]
func (t *transport) Close() error {
	return t.qc.CloseWithError(0, "")
}

// webTransport is a rhp4.Transport that wraps a WebTransport Session
type webTransport struct {
	sess *webtransport.Session
}

type webStream struct {
	*webtransport.Stream

	localAddr, remoteAddr net.Addr
}

func (ws *webStream) LocalAddr() net.Addr {
	return ws.localAddr
}

func (ws *webStream) RemoteAddr() net.Addr {
	return ws.remoteAddr
}

// Close implements the rhp4.Transport interface.
func (wt *webTransport) Close() error {
	return wt.sess.CloseWithError(0, "")
}

// AcceptStream implements the rhp4.Transport interface.
func (wt *webTransport) AcceptStream() (net.Conn, error) {
	conn, err := wt.sess.AcceptStream(context.Background())
	if err != nil {
		return nil, err
	}
	return &webStream{
		Stream:     conn,
		localAddr:  wt.sess.LocalAddr(),
		remoteAddr: wt.sess.RemoteAddr(),
	}, nil
}

// Listen listens for QUIC connections on conn using the provided certificate.
// It is a wrapper around quic.Listen that sets the appropriate ALPN protocol
// for RHP4 and WebTransport.
func Listen(conn net.PacketConn, certs CertManager) (*quic.Listener, error) {
	return quic.Listen(conn, &tls.Config{
		GetCertificate: certs.GetCertificate,
		NextProtos:     []string{TLSNextProtoRHP4, http3.NextProtoH3},
	}, &quic.Config{
		EnableDatagrams:    true,
		KeepAlivePeriod:    30 * time.Second,
		MaxIdleTimeout:     30 * time.Minute,
		MaxIncomingStreams: maxIncomingStreams,
	})
}

// Serve serves RHP4 connections on the listener l using the QUIC transport.
func Serve(l *quic.Listener, s *rhp4.Server, log *zap.Logger) {
	wts := &webtransport.Server{
		CheckOrigin: func(*http.Request) bool {
			return true // allow all origins
		},
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

		err = s.Serve(&webTransport{
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
				log.Debug("failed to accept connection", zap.Error(err))
			}
			return
		}
		proto := conn.ConnectionState().TLS.NegotiatedProtocol
		log := log.With(zap.String("peerAddress", conn.RemoteAddr().String()), zap.String("protocol", proto))
		log.Debug("serving connection")
		switch proto {
		case http3.NextProtoH3: // webtransport
			go func() {
				defer conn.CloseWithError(0, "")
				wts.ServeQUICConn(conn)
			}()
		case TLSNextProtoRHP4: // quic
			go func() {
				defer conn.CloseWithError(0, "")
				if err := s.Serve(&transport{qc: conn}, log); err != nil {
					log.Debug("failed to serve connection", zap.Error(err))
				}
			}()
		default:
			conn.CloseWithError(10, "invalid alpn") // TODO: define standard error codes
		}
	}
}
