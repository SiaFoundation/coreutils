package rhp_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/testutil"
	"lukechampine.com/frand"
)

// scriptedHost implements rhp4.TransportClient over a net.Pipe. Each dialed
// stream is served by handle, which plays the host side of the RPC.
type scriptedHost struct {
	hostKey types.PrivateKey
	handle  func(stream net.Conn)
}

// releaseCountingSigner wraps fundAndSign and counts transactions released.
type releaseCountingSigner struct {
	*fundAndSign
	released int
}

func (h *scriptedHost) DialStream(context.Context) (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		h.handle(server)
	}()
	return client, nil
}

func (h *scriptedHost) FrameSize() int           { return 4096 }
func (h *scriptedHost) PeerKey() types.PublicKey { return h.hostKey.PublicKey() }
func (h *scriptedHost) Close() error             { return nil }
func (s *releaseCountingSigner) ReleaseInputs(txns []types.V2Transaction) {
	s.released += len(txns)
	s.fundAndSign.ReleaseInputs(txns)
}

func signedPrices(hostKey types.PrivateKey, tipHeight uint64) proto4.HostPrices {
	p := proto4.HostPrices{
		ContractPrice: types.Siacoins(1),
		Collateral:    types.NewCurrency64(1),
		StoragePrice:  types.NewCurrency64(1),
		IngressPrice:  types.NewCurrency64(1),
		EgressPrice:   types.NewCurrency64(1),
		TipHeight:     tipHeight,
		ValidUntil:    time.Now().Add(time.Hour),
	}
	p.Signature = hostKey.SignHash(p.SigHash())
	return p
}

func signedToken(hostKey types.PublicKey, accountKey types.PrivateKey) proto4.AccountToken {
	tok := proto4.AccountToken{
		HostKey:    hostKey,
		Account:    proto4.Account(accountKey.PublicKey()),
		ValidUntil: time.Now().Add(time.Hour),
	}
	tok.Signature = accountKey.SignHash(tok.SigHash())
	return tok
}

func signedContract(cs consensus.State, renterKey, hostKey types.PrivateKey, filesize, expirationHeight uint64) types.V2FileContract {
	fc := types.V2FileContract{
		Capacity:         filesize,
		Filesize:         filesize,
		ProofHeight:      expirationHeight - proto4.ProofWindow,
		ExpirationHeight: expirationHeight,
		RenterOutput:     types.SiacoinOutput{Value: types.Siacoins(100), Address: frand.Entropy256()},
		HostOutput:       types.SiacoinOutput{Value: types.Siacoins(100), Address: frand.Entropy256()},
		MissedHostValue:  types.Siacoins(100),
		TotalCollateral:  types.Siacoins(100),
		RenterPublicKey:  renterKey.PublicKey(),
		HostPublicKey:    hostKey.PublicKey(),
		RevisionNumber:   1,
	}
	sigHash := cs.ContractSigHash(fc)
	fc.RenterSignature = renterKey.SignHash(sigHash)
	fc.HostSignature = hostKey.SignHash(sigHash)
	return fc
}

func assertClientError(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", substr)
	}
	var rpcErr *proto4.RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != proto4.ErrorCodeClientError {
		t.Fatalf("expected client error, got %v", err)
	} else if !strings.Contains(err.Error(), substr) {
		t.Fatalf("expected error containing %q, got %v", substr, err)
	}
}

func TestRPCSettingsRejectsInvalidPrices(t *testing.T) {
	hostKey, otherKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
		if _, err := proto4.ReadID(stream); err != nil {
			return
		}
		_ = proto4.WriteResponse(stream, &proto4.RPCSettingsResponse{
			Settings: proto4.HostSettings{Prices: signedPrices(otherKey, 0)},
		})
	}}

	_, err := rhp4.RPCSettings(context.Background(), host)
	if !errors.Is(err, proto4.ErrInvalidSignature) {
		t.Fatalf("expected %v, got %v", proto4.ErrInvalidSignature, err)
	}
}

func TestRPCLatestRevisionRejectsMalformed(t *testing.T) {
	n, _ := testutil.V2Network()
	cs := n.GenesisState()
	hostKey, renterKey, otherKey := types.GeneratePrivateKey(), types.GeneratePrivateKey(), types.GeneratePrivateKey()

	tests := []struct {
		name    string
		mutate  func(*types.V2FileContract)
		wantErr string
	}{
		{"host key mismatch", func(fc *types.V2FileContract) { fc.HostPublicKey = otherKey.PublicKey() }, "host key mismatch"},
		{"invalid renter signature", func(fc *types.V2FileContract) { fc.RenterSignature[0] ^= 1 }, "failed to validate renter signature"},
		{"invalid host signature", func(fc *types.V2FileContract) { fc.HostSignature[0] ^= 1 }, "failed to validate host signature"},
		{"substituted renter key", func(fc *types.V2FileContract) {
			// a correctly re-signed contract whose renter key the host controls
			fc.RenterPublicKey = otherKey.PublicKey()
			sigHash := cs.ContractSigHash(*fc)
			fc.RenterSignature = otherKey.SignHash(sigHash)
			fc.HostSignature = hostKey.SignHash(sigHash)
		}, "renter key mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := signedContract(cs, renterKey, hostKey, 0, 200)
			tt.mutate(&fc)
			host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
				var req proto4.RPCLatestRevisionRequest
				if _, err := proto4.ReadID(stream); err != nil {
					return
				} else if err := proto4.ReadRequest(stream, &req); err != nil {
					return
				}
				_ = proto4.WriteResponse(stream, &proto4.RPCLatestRevisionResponse{Contract: fc})
			}}

			_, err := rhp4.RPCLatestRevision(context.Background(), host, cs, renterKey.PublicKey(), frand.Entropy256())
			assertClientError(t, err, tt.wantErr)
		})
	}
}

func TestRPCReadSectorRejectsBadLength(t *testing.T) {
	hostKey, accountKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	const length = proto4.LeafSize
	host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
		var req proto4.RPCReadSectorRequest
		if _, err := proto4.ReadID(stream); err != nil {
			return
		} else if err := proto4.ReadRequest(stream, &req); err != nil {
			return
		}
		_ = proto4.WriteResponse(stream, &proto4.RPCReadSectorResponse{DataLength: length - 1})
	}}

	var buf strings.Builder
	_, err := rhp4.RPCReadSector(context.Background(), host, signedPrices(hostKey, 0), signedToken(hostKey.PublicKey(), accountKey), &buf, frand.Entropy256(), 0, length)
	assertClientError(t, err, "expected 64 bytes, got 63")
	if buf.Len() != 0 {
		t.Fatalf("expected no data written, got %d bytes", buf.Len())
	}
}

func TestRPCSectorRootsRejectsBadCount(t *testing.T) {
	n, _ := testutil.V2Network()
	cs := n.GenesisState()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
		var req proto4.RPCSectorRootsRequest
		if _, err := proto4.ReadID(stream); err != nil {
			return
		} else if err := proto4.ReadRequest(stream, &req); err != nil {
			return
		}
		_ = proto4.WriteResponse(stream, &proto4.RPCSectorRootsResponse{Roots: make([]types.Hash256, 1)})
	}}

	contract := rhp4.ContractRevision{ID: frand.Entropy256(), Revision: signedContract(cs, renterKey, hostKey, 2*proto4.SectorSize, 200)}
	_, err := rhp4.RPCSectorRoots(context.Background(), host, cs, signedPrices(hostKey, 0), &fundAndSign{pk: renterKey}, contract, 0, 2)
	assertClientError(t, err, "expected 2 roots, got 1")
}

func TestRPCReplenishRejectsMalformedDeposits(t *testing.T) {
	n, _ := testutil.V2Network()
	cs := n.GenesisState()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	accounts := []proto4.Account{frand.Entropy256(), frand.Entropy256()}
	contract := rhp4.ContractRevision{ID: frand.Entropy256(), Revision: signedContract(cs, renterKey, hostKey, 0, 200)}

	tests := []struct {
		name     string
		deposits []proto4.AccountDeposit
		wantErr  string
	}{
		{"wrong count", []proto4.AccountDeposit{{Account: accounts[0], Amount: types.Siacoins(1)}}, "expected 2 deposits, got 1"},
		{"wrong identity", []proto4.AccountDeposit{{Account: accounts[1], Amount: types.Siacoins(1)}, {Account: accounts[0], Amount: types.Siacoins(1)}}, "expected deposit 0 for"},
	}
	for _, tt := range tests {
		host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
			var req proto4.RPCReplenishAccountsRequest
			if _, err := proto4.ReadID(stream); err != nil {
				return
			} else if err := proto4.ReadRequest(stream, &req); err != nil {
				return
			}
			_ = proto4.WriteResponse(stream, &proto4.RPCReplenishAccountsResponse{Deposits: tt.deposits})
		}}
		t.Run("accounts/"+tt.name, func(t *testing.T) {
			_, err := rhp4.RPCReplenishAccounts(context.Background(), host, rhp4.RPCReplenishAccountsParams{
				Accounts: accounts,
				Target:   types.Siacoins(1),
				Contract: contract,
			}, cs, &fundAndSign{pk: renterKey})
			assertClientError(t, err, tt.wantErr)
		})
		t.Run("pools/"+tt.name, func(t *testing.T) {
			_, err := rhp4.RPCReplenishPools(context.Background(), host, rhp4.RPCReplenishPoolsParams{
				Pools:    accounts,
				Target:   types.Siacoins(1),
				Contract: contract,
			}, cs, &fundAndSign{pk: renterKey})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestRPCRenewRefreshRejectsMalformedTransaction(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	cm, w := startTestNode(t, n, genesis)
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	cs := cm.TipState()
	prices := signedPrices(hostKey, cs.Index.Height)
	hostAddress := types.Address(frand.Entropy256())
	existing := signedContract(cs, renterKey, hostKey, 0, cs.Index.Height+50)
	contractID := types.FileContractID(frand.Entropy256())
	hostInput := types.V2SiacoinInput{
		Parent: types.SiacoinElement{
			ID:            frand.Entropy256(),
			SiacoinOutput: types.SiacoinOutput{Value: types.Siacoins(1000), Address: hostAddress},
		},
		SatisfiedPolicy: types.SatisfiedPolicy{Policy: types.AnyoneCanSpend()},
	}

	// signRenewal adds valid host signatures so only the transaction checks
	// can reject the response
	signRenewal := func(renewal *types.V2FileContractRenewal) {
		renewal.HostSignature = hostKey.SignHash(cs.RenewalSigHash(*renewal))
		renewal.NewContract.HostSignature = hostKey.SignHash(cs.ContractSigHash(renewal.NewContract))
	}

	// each mutation produces a final transaction the client must reject
	mutations := []struct {
		name    string
		mutate  func(txn *types.V2Transaction)
		wantErr string
	}{
		{"parent mismatch", func(txn *types.V2Transaction) {
			txn.FileContractResolutions[0].Parent.ID = frand.Entropy256()
		}, "resolution parent mismatch"},
		{"transaction ID mismatch", func(txn *types.V2Transaction) {
			txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{Value: types.Siacoins(1), Address: hostAddress})
		}, "transaction ID mismatch"},
	}

	for _, m := range mutations {
		t.Run("renew/"+m.name, func(t *testing.T) {
			host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
				var req proto4.RPCRenewContractRequest
				var sigs proto4.RPCRenewContractSecondResponse
				if _, err := proto4.ReadID(stream); err != nil {
					return
				} else if err := proto4.ReadRequest(stream, &req); err != nil {
					return
				} else if err := proto4.WriteResponse(stream, &proto4.RPCRenewContractResponse{HostInputs: []types.V2SiacoinInput{hostInput}}); err != nil {
					return
				} else if err := proto4.ReadResponse(stream, &sigs); err != nil {
					return
				}
				renewal, _ := proto4.RenewContract(existing, req.Prices, hostAddress, req.Renewal)
				signRenewal(&renewal)
				txn := types.V2Transaction{FileContractResolutions: []types.V2FileContractResolution{{
					Parent:     types.V2FileContractElement{ID: contractID},
					Resolution: &renewal,
				}}}
				m.mutate(&txn)
				_ = proto4.WriteResponse(stream, &proto4.RPCRenewContractThirdResponse{TransactionSet: []types.V2Transaction{txn}})
			}}

			signer := &releaseCountingSigner{fundAndSign: &fundAndSign{w, renterKey}}
			_, err := rhp4.RPCRenewContract(context.Background(), host, cm, signer, cs, prices, hostAddress, existing, proto4.RPCRenewContractParams{
				ContractID:  contractID,
				Allowance:   types.Siacoins(10),
				Collateral:  types.Siacoins(10),
				ProofHeight: cs.Index.Height + 100,
			})
			assertClientError(t, err, m.wantErr)
			if signer.released != 1 {
				t.Fatalf("expected 1 transaction released, got %d", signer.released)
			}
		})

		t.Run("refresh/"+m.name, func(t *testing.T) {
			host := &scriptedHost{hostKey: hostKey, handle: func(stream net.Conn) {
				var req proto4.RPCRefreshContractRequest
				var sigs proto4.RPCRefreshContractSecondResponse
				if _, err := proto4.ReadID(stream); err != nil {
					return
				} else if err := proto4.ReadRequest(stream, &req); err != nil {
					return
				} else if err := proto4.WriteResponse(stream, &proto4.RPCRefreshContractResponse{HostInputs: []types.V2SiacoinInput{hostInput}}); err != nil {
					return
				} else if err := proto4.ReadResponse(stream, &sigs); err != nil {
					return
				}
				renewal, _ := proto4.RefreshContractFullRollover(existing, req.Prices, hostAddress, req.Refresh)
				signRenewal(&renewal)
				txn := types.V2Transaction{FileContractResolutions: []types.V2FileContractResolution{{
					Parent:     types.V2FileContractElement{ID: contractID},
					Resolution: &renewal,
				}}}
				m.mutate(&txn)
				_ = proto4.WriteResponse(stream, &proto4.RPCRefreshContractThirdResponse{TransactionSet: []types.V2Transaction{txn}})
			}}

			signer := &releaseCountingSigner{fundAndSign: &fundAndSign{w, renterKey}}
			_, err := rhp4.RPCRefreshContractFullRollover(context.Background(), host, cm, signer, cs, prices, hostAddress, existing, proto4.RPCRefreshContractParams{
				ContractID: contractID,
				Allowance:  types.Siacoins(10),
				Collateral: types.Siacoins(10),
			})
			assertClientError(t, err, m.wantErr)
			if signer.released != 1 {
				t.Fatalf("expected 1 transaction released, got %d", signer.released)
			}
		})
	}
}
