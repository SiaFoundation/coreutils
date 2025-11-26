package quic

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"

	"github.com/quic-go/quic-go"
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

func TestStreamLimit(t *testing.T) {
	server, client := setupTestPair(t)

	// the server just accepts streams and cancels them
	go func() {
		for {
			conn, err := server.Accept(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.CloseWithError(1, "cleanup")

			transport := &transport{conn}

			for {
				stream, err := transport.AcceptStream()
				if err != nil {
					t.Error(err)
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
