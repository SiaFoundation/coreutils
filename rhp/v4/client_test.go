//go:build ignore

package rhp_test

import (
	"context"
	"net"
	"testing"
	"time"

	crhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/host"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap/zaptest"
)

type muxTransport struct {
	t *crhp4.Transport
}

func (mt *muxTransport) AcceptStream() (host.Stream, error) {
	return mt.t.AcceptStream()
}

func (mt *muxTransport) Close() error { return mt.t.Close() }

func TestRPCSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	hostKey := types.GeneratePrivateKey()
	//renterKey := types.GeneratePrivateKey()

	network, genesisBlock := testutil.Network()
	chainStore, state, err := chain.NewDBStore(chain.NewMemDB(), network, genesisBlock)
	if err != nil {
		t.Fatal(err)
	}
	cm := chain.NewManager(chainStore, state)

	h := host.NewServer(hostKey, cm)

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			t, err := crhp4.Accept(conn, hostKey)
			if err != nil {
				panic(err)
			}
			mt := &muxTransport{t}
			go func() {
				defer t.Close()

				h.Serve(mt, log.Named("host"))
			}()
		}
	}()

	client, err := rhp4.NewClient(context.Background(), l.Addr().String(), hostKey.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	//defer client.Close()

	settings, err := client.Settings(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if time.Until(settings.Prices.ValidUntil) <= 0 {
		t.Fatal("price table is expired")
	}
}
