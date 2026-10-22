package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/testutil/certs"
)

// setupTestPair sets up a QUIC server and client for testing.
func setupTestPair(tb testing.TB) (*quic.Listener, rhp.TransportClient) {
	tb.Helper()

	udpAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		tb.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() })

	l, err := Listen(conn, &certs.EphemeralCertManager{})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })

	client, err := Dial(context.Background(), conn.LocalAddr().String(), types.PublicKey{}, WithTLSConfig(func(tc *tls.Config) {
		tc.InsecureSkipVerify = true
	}))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { client.Close() })
	return l, client
}

// TestStreamLimit is a regression test that ensures we can perform more than
// maxIncomingStreams RPCs on a single QUIC connection. This should be the case
// if both sides close their streams.
// Before fixing the issue, this test would hang for the full 10 minute test
// timeout.
func TestStreamLimit(t *testing.T) {
	server, client := setupTestPair(t)

	// the server just accepts streams and cancels them
	go func() {
		for {
			conn, err := server.Accept(context.Background())
			if err != nil {
				return
			}

			transport := &transport{
				qc:               conn,
				streamMiddleware: func(c net.Conn) net.Conn { return c },
			}

			for {
				stream, err := transport.AcceptStream()
				if err != nil {
					return
				}
				stream.Close()
			}
		}
	}()

	// open the maximum number of streams + 1 which should neither block forever
	// nor return an error
	var wg sync.WaitGroup
	for range maxIncomingStreams + 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			stream, err := client.DialStream(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			stream.Close()
		}()
	}
	wg.Wait()
}

// TestWebTransportStreamLimit is the WebTransport equivalent of
// TestStreamLimit. It ensures we can perform more than maxIncomingStreams RPCs
// on a single WebTransport session as long as both sides close their streams.
func TestWebTransportStreamLimit(t *testing.T) {
	udpAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	l, err := Listen(conn, &certs.EphemeralCertManager{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	mux := http.NewServeMux()
	wts := &webtransport.Server{
		H3: &http3.Server{
			Handler:         mux,
			EnableDatagrams: true,
		},
		Config: &webtransport.Config{
			MaxIncomingStreams:    1 << 60,
			MaxIncomingUniStreams: 1 << 60,
			MaxIncomingData:       1 << 60,
		},
	}
	t.Cleanup(func() { wts.Close() })
	webtransport.ConfigureHTTP3Server(wts.H3)

	// the server just accepts streams and closes them
	mux.HandleFunc("/sia/rhp/v4", func(w http.ResponseWriter, r *http.Request) {
		sess, err := wts.Upgrade(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		transport := &webTransport{
			sess:             sess,
			streamMiddleware: func(c net.Conn) net.Conn { return c },
		}
		for {
			stream, err := transport.AcceptStream()
			if err != nil {
				return
			}
			stream.Close()
		}
	})
	go func() {
		for {
			qc, err := l.Accept(context.Background())
			if err != nil {
				return
			}
			go wts.ServeQUICConn(qc)
		}
	}()

	dialer := webtransport.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	t.Cleanup(func() { dialer.Close() })

	_, sess, err := dialer.Dial(context.Background(), fmt.Sprintf("https://%s/sia/rhp/v4", conn.LocalAddr()), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.CloseWithError(0, "") })

	// open the maximum number of streams + 1 which should neither block
	// nor return an error
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// limit the number of concurrent streams. Otherwise quic-go closes the
	// connection with INTERNAL_ERROR once more than 16k STOP_SENDING frames
	// are queued at once.
	sema := make(chan struct{}, 1000)

	var wg sync.WaitGroup
	for range maxIncomingStreams + 1 {
		wg.Add(1)
		sema <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sema }()

			stream, err := sess.OpenStreamSync(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			// write some data so the server has unread data in the
			// stream when it closes it
			if _, err := stream.Write([]byte{1}); err != nil {
				t.Error(err)
				return
			}
			stream.CancelRead(0)
			stream.Close()
		}()
	}
	wg.Wait()
}
