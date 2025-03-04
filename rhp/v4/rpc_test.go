package rhp_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type blockingSettingsReporter struct {
	blockChan chan struct{}
}

func (b *blockingSettingsReporter) RHP4Settings() proto4.HostSettings {
	<-b.blockChan
	return proto4.HostSettings{}
}

func (b *blockingSettingsReporter) Unblock() {
	close(b.blockChan)
}

type fundAndSign struct {
	w  *wallet.SingleAddressWallet
	pk types.PrivateKey
}

func (fs *fundAndSign) FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error) {
	return fs.w.FundV2Transaction(txn, amount, true)
}
func (fs *fundAndSign) ReleaseInputs(txns []types.V2Transaction) {
	fs.w.ReleaseInputs(nil, txns)
}

func (fs *fundAndSign) SignV2Inputs(txn *types.V2Transaction, toSign []int) {
	fs.w.SignV2Inputs(txn, toSign)
}
func (fs *fundAndSign) SignHash(h types.Hash256) types.Signature {
	return fs.pk.SignHash(h)
}
func (fs *fundAndSign) PublicKey() types.PublicKey {
	return fs.pk.PublicKey()
}
func (fs *fundAndSign) Address() types.Address {
	return fs.w.Address()
}

func testRenterHostPairSiaMux(tb testing.TB, hostKey types.PrivateKey, cm rhp4.ChainManager, s rhp4.Syncer, w rhp4.Wallet, c rhp4.Contractor, sr rhp4.Settings, ss rhp4.Sectors, log *zap.Logger) rhp4.TransportClient {
	rs := rhp4.NewServer(hostKey, cm, s, c, w, sr, ss, rhp4.WithPriceTableValidity(2*time.Minute))
	hostAddr := testutil.ServeSiaMux(tb, rs, log.Named("siamux"))

	transport, err := siamux.Dial(context.Background(), hostAddr, hostKey.PublicKey())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { transport.Close() })

	return transport
}

func testRenterHostPairQUIC(tb testing.TB, hostKey types.PrivateKey, cm rhp4.ChainManager, s rhp4.Syncer, w rhp4.Wallet, c rhp4.Contractor, sr rhp4.Settings, ss rhp4.Sectors, log *zap.Logger) rhp4.TransportClient {
	rs := rhp4.NewServer(hostKey, cm, s, c, w, sr, ss, rhp4.WithPriceTableValidity(2*time.Minute))
	hostAddr := testutil.ServeQUIC(tb, rs, log.Named("quic"))

	transport, err := quic.Dial(context.Background(), hostAddr, hostKey.PublicKey(), quic.WithTLSConfig(func(tc *tls.Config) {
		tc.InsecureSkipVerify = true
	}))
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { transport.Close() })
	return transport
}

func startTestNode(tb testing.TB, n *consensus.Network, genesis types.Block) (*chain.Manager, *syncer.Syncer, *wallet.SingleAddressWallet) {
	db, tipstate, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		tb.Fatal(err)
	}
	cm := chain.NewManager(db, tipstate)

	syncerListener, err := net.Listen("tcp", ":0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { syncerListener.Close() })

	s := syncer.New(syncerListener, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: "localhost:1234",
	})
	go s.Run()
	tb.Cleanup(func() { s.Close() })

	ws := testutil.NewEphemeralWalletStore()
	w, err := wallet.NewSingleAddressWallet(types.GeneratePrivateKey(), cm, ws)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { w.Close() })

	reorgCh := make(chan struct{}, 1)
	tb.Cleanup(func() { close(reorgCh) })

	go func() {
		for range reorgCh {
			reverted, applied, err := cm.UpdatesSince(w.Tip(), 1000)
			if err != nil {
				tb.Error(err)
			}

			err = ws.UpdateChainState(func(tx wallet.UpdateTx) error {
				return w.UpdateChainState(tx, reverted, applied)
			})
			if err != nil {
				tb.Error(err)
			}
		}
	}()

	stop := cm.OnReorg(func(_ types.ChainIndex) {
		select {
		case reorgCh <- struct{}{}:
		default:
		}
	})
	tb.Cleanup(stop)

	return cm, s, w
}

func mineAndSync(tb testing.TB, cm *chain.Manager, addr types.Address, n int, tippers ...interface{ Tip() types.ChainIndex }) {
	tb.Helper()

	testutil.MineBlocks(tb, cm, addr, n)

	for {
		time.Sleep(time.Millisecond)
		equals := true
		for _, tipper := range tippers {
			if tipper.Tip() != cm.Tip() {
				equals = false
				tb.Log("waiting for tip to sync")
				break
			}
		}
		if equals {
			return
		}
	}
}

func assertValidRevision(t *testing.T, cm rhp4.ChainManager, c rhp4.Contractor, revision rhp4.ContractRevision) {
	t.Helper()

	basis, fce, err := c.V2FileContractElement(revision.ID)
	if err != nil {
		t.Fatal(err)
	}
	revisionTxn := types.V2Transaction{
		FileContractRevisions: []types.V2FileContractRevision{
			{
				Parent:   fce,
				Revision: revision.Revision,
			},
		},
	}
	if _, err := cm.AddV2PoolTransactions(basis, []types.V2Transaction{revisionTxn}); err != nil {
		t.Fatal(err)
	}
}

func TestFormContract(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	testFormContract := func(t *testing.T, transport rhp4.TransportClient) {
		settings, err := rhp4.RPCSettings(context.Background(), transport)
		if err != nil {
			t.Fatal(err)
		}

		fundAndSign := &fundAndSign{w, renterKey}
		renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
		result, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w.Address(),
			Allowance:       renterAllowance,
			Collateral:      hostCollateral,
			ProofHeight:     cm.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(result.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, result.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, result.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}
	}

	t.Run("siamux", func(t *testing.T) {
		transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())
		testFormContract(t, transport)
	})

	t.Run("quic", func(t *testing.T) {
		transport := testRenterHostPairQUIC(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())
		testFormContract(t, transport)
	})
}

func TestFormContractBasis(t *testing.T) {
	t.Run("siamux", func(t *testing.T) {
		n, genesis := testutil.V2Network()
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

		cm1, s1, w1 := startTestNode(t, n, genesis)
		cm2, _, w2 := startTestNode(t, n, genesis)

		// fund both wallets
		mineAndSync(t, cm1, w2.Address(), int(n.MaturityDelay+20))
		mineAndSync(t, cm1, w1.Address(), int(n.MaturityDelay+20))

		// manually sync the second wallet just before tip
		_, applied, err := cm1.UpdatesSince(types.ChainIndex{}, int(w1.Tip().Height-5))
		if err != nil {
			t.Fatal(err)
		}
		var blocks []types.Block
		for _, cau := range applied {
			blocks = append(blocks, cau.Block)
		}
		if err := cm2.AddBlocks(blocks); err != nil {
			t.Fatal(err)
		}

		// wait for the second wallet to sync
		for cm2.Tip() != w2.Tip() {
			time.Sleep(time.Millisecond)
		}

		sr := testutil.NewEphemeralSettingsReporter()
		sr.Update(proto4.HostSettings{
			Release:             "test",
			AcceptingContracts:  true,
			WalletAddress:       w1.Address(),
			MaxCollateral:       types.Siacoins(10000),
			MaxContractDuration: 1000,
			RemainingStorage:    100 * proto4.SectorSize,
			TotalStorage:        100 * proto4.SectorSize,
			Prices: proto4.HostPrices{
				ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
				StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
				IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
				EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
				Collateral:    types.NewCurrency64(200),
			},
		})
		ss := testutil.NewEphemeralSectorStore()
		c := testutil.NewEphemeralContractor(cm1)

		transport := testRenterHostPairSiaMux(t, hostKey, cm1, s1, w1, c, sr, ss, zap.NewNop())

		settings, err := rhp4.RPCSettings(context.Background(), transport)
		if err != nil {
			t.Fatal(err)
		}

		balance, err := w2.Balance()
		if err != nil {
			t.Fatal(err)
		}

		fundAndSign := &fundAndSign{w2, renterKey}
		result, err := rhp4.RPCFormContract(context.Background(), transport, cm2, fundAndSign, cm1.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w2.Address(),
			Allowance:       balance.Confirmed.Mul64(96).Div64(100), // almost the whole balance to force as many inputs as possible
			Collateral:      types.ZeroCurrency,
			ProofHeight:     cm1.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm1.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}
	})

	t.Run("quic", func(t *testing.T) {
		n, genesis := testutil.V2Network()
		hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

		cm1, s1, w1 := startTestNode(t, n, genesis)
		cm2, _, w2 := startTestNode(t, n, genesis)

		// fund both wallets
		mineAndSync(t, cm1, w2.Address(), int(n.MaturityDelay+20))
		mineAndSync(t, cm1, w1.Address(), int(n.MaturityDelay+20))

		// manually sync the second wallet just before tip
		_, applied, err := cm1.UpdatesSince(types.ChainIndex{}, int(w1.Tip().Height-5))
		if err != nil {
			t.Fatal(err)
		}
		var blocks []types.Block
		for _, cau := range applied {
			blocks = append(blocks, cau.Block)
		}
		if err := cm2.AddBlocks(blocks); err != nil {
			t.Fatal(err)
		}

		// wait for the second wallet to sync
		for cm2.Tip() != w2.Tip() {
			time.Sleep(time.Millisecond)
		}

		sr := testutil.NewEphemeralSettingsReporter()
		sr.Update(proto4.HostSettings{
			Release:             "test",
			AcceptingContracts:  true,
			WalletAddress:       w1.Address(),
			MaxCollateral:       types.Siacoins(10000),
			MaxContractDuration: 1000,
			RemainingStorage:    100 * proto4.SectorSize,
			TotalStorage:        100 * proto4.SectorSize,
			Prices: proto4.HostPrices{
				ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
				StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
				IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
				EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
				Collateral:    types.NewCurrency64(200),
			},
		})
		ss := testutil.NewEphemeralSectorStore()
		c := testutil.NewEphemeralContractor(cm1)

		transport := testRenterHostPairQUIC(t, hostKey, cm1, s1, w1, c, sr, ss, zap.NewNop())

		settings, err := rhp4.RPCSettings(context.Background(), transport)
		if err != nil {
			t.Fatal(err)
		}

		balance, err := w2.Balance()
		if err != nil {
			t.Fatal(err)
		}

		fundAndSign := &fundAndSign{w2, renterKey}
		result, err := rhp4.RPCFormContract(context.Background(), transport, cm2, fundAndSign, cm1.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w2.Address(),
			Allowance:       balance.Confirmed.Mul64(96).Div64(100), // almost the whole balance to force as many inputs as possible
			Collateral:      types.ZeroCurrency,
			ProofHeight:     cm1.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm1.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}
	})
}

func TestRPCRefresh(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}
	fundAndSign := &fundAndSign{w, renterKey}

	formContractUploadSector := func(t *testing.T, renterAllowance, hostCollateral, accountBalance types.Currency) rhp4.ContractRevision {
		t.Helper()

		result, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w.Address(),
			Allowance:       renterAllowance,
			Collateral:      hostCollateral,
			ProofHeight:     cm.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		revision := result.Contract

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		// mine a few blocks to confirm the contract
		mineAndSync(t, cm, types.VoidAddress, 10, w, c)

		// fund an account to transfer funds to the host
		cs := cm.TipState()
		account := proto4.Account(renterKey.PublicKey())
		fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
			{Account: account, Amount: accountBalance},
		})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = fundResult.Revision

		// upload data
		at := account.Token(renterKey, hostKey.PublicKey())
		wRes, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, at, bytes.NewReader(bytes.Repeat([]byte{1}, proto4.LeafSize)), proto4.LeafSize)
		if err != nil {
			t.Fatal(err)
		}
		aRes, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, []types.Hash256{wRes.Root})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = aRes.Revision

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if rs.Renewed {
			t.Fatal("expected contract to not be renewed")
		} else if !rs.Revisable {
			t.Fatal("expected contract to be revisable")
		}
		return revision
	}

	t.Run("no allowance or collateral", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// refresh the contract
		_, err = rhp4.RPCRefreshContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRefreshContractParams{
			ContractID: revision.ID,
			Allowance:  types.ZeroCurrency,
			Collateral: types.ZeroCurrency,
		})
		if err == nil {
			t.Fatal(err)
		} else if !strings.Contains(err.Error(), "allowance must be greater than zero") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("valid refresh", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))
		// refresh the contract
		refreshResult, err := rhp4.RPCRefreshContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRefreshContractParams{
			ContractID: revision.ID,
			Allowance:  types.Siacoins(10),
			Collateral: types.Siacoins(20),
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(refreshResult.RenewalSet.Basis, refreshResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(refreshResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, refreshResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, refreshResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})
}

func TestRPCRenew(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}
	fundAndSign := &fundAndSign{w, renterKey}

	formContractUploadSector := func(t *testing.T, renterAllowance, hostCollateral, accountBalance types.Currency) rhp4.ContractRevision {
		t.Helper()

		result, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
			RenterPublicKey: renterKey.PublicKey(),
			RenterAddress:   w.Address(),
			Allowance:       renterAllowance,
			Collateral:      hostCollateral,
			ProofHeight:     cm.Tip().Height + 50,
		})
		if err != nil {
			t.Fatal(err)
		}
		revision := result.Contract
		sigHash := cm.TipState().ContractSigHash(revision.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, revision.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, revision.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(result.FormationSet.Basis, result.FormationSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		// mine a few blocks to confirm the contract
		mineAndSync(t, cm, types.VoidAddress, 10, w, c)

		// fund an account to transfer funds to the host
		cs := cm.TipState()
		account := proto4.Account(renterKey.PublicKey())
		fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
			{Account: account, Amount: accountBalance},
		})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = fundResult.Revision

		// upload data
		at := account.Token(renterKey, hostKey.PublicKey())
		wRes, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, at, bytes.NewReader(bytes.Repeat([]byte{1}, proto4.LeafSize)), proto4.LeafSize)
		if err != nil {
			t.Fatal(err)
		}
		aRes, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, []types.Hash256{wRes.Root})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = aRes.Revision
		return revision
	}

	t.Run("same duration", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		_, err = rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(150),
			Collateral:  types.Siacoins(300),
			ProofHeight: revision.Revision.ProofHeight,
		})
		if err == nil {
			t.Fatal(err)
		} else if !strings.Contains(err.Error(), "renewal proof height must be greater than existing proof height") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("partial rollover", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(150),
			Collateral:  types.Siacoins(300),
			ProofHeight: revision.Revision.ProofHeight + 10,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(renewResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})

	t.Run("full rollover", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(50),
			Collateral:  types.Siacoins(100),
			ProofHeight: revision.Revision.ProofHeight + 10,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(renewResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})

	t.Run("no rollover", func(t *testing.T) {
		revision := formContractUploadSector(t, types.Siacoins(100), types.Siacoins(200), types.Siacoins(25))

		// renew the contract
		renewResult, err := rhp4.RPCRenewContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
			ContractID:  revision.ID,
			Allowance:   types.Siacoins(150),
			Collateral:  types.Siacoins(300),
			ProofHeight: revision.Revision.ProofHeight + 10,
		})
		if err != nil {
			t.Fatal(err)
		}

		// verify the transaction set is valid
		if known, err := cm.AddV2PoolTransactions(renewResult.RenewalSet.Basis, renewResult.RenewalSet.Transactions); err != nil {
			t.Fatal(err)
		} else if !known {
			t.Fatal("expected transaction set to be known")
		}

		sigHash := cm.TipState().ContractSigHash(renewResult.Contract.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.RenterSignature) {
			t.Fatal("renter signature verification failed")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, renewResult.Contract.Revision.HostSignature) {
			t.Fatal("host signature verification failed")
		}

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		} else if !rs.Renewed {
			t.Fatal("expected contract to be renewed")
		} else if rs.Revisable {
			t.Fatal("expected contract to not be revisable")
		}
	})
}

func TestRPCTimeout(t *testing.T) {
	n, genesis := testutil.V2Network()
	cm, s, w := startTestNode(t, n, genesis)

	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	isTimeoutErr := func(err error) bool {
		t.Helper()
		return err != nil && (strings.Contains(err.Error(), "deadline exceeded") || strings.Contains(err.Error(), "i/o timeout"))
	}

	assertRPCTimeout := func(transport rhp4.TransportClient, timeout bool) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := rhp4.RPCSettings(ctx, transport)
		if timeout && !isTimeoutErr(err) {
			t.Fatal("expected timeout", err)
		} else if !timeout && err != nil {
			t.Fatal(err)
		}
	}

	t.Run("siamux", func(t *testing.T) {
		sr := &blockingSettingsReporter{blockChan: make(chan struct{})}
		transport := testRenterHostPairSiaMux(t, types.GeneratePrivateKey(), cm, s, w, c, sr, ss, zap.NewNop())
		assertRPCTimeout(transport, true)
		sr.Unblock()
		assertRPCTimeout(transport, false)
	})

	t.Run("quic", func(t *testing.T) {
		sr := &blockingSettingsReporter{blockChan: make(chan struct{})}
		transport := testRenterHostPairQUIC(t, types.GeneratePrivateKey(), cm, s, w, c, sr, ss, zap.NewNop())
		assertRPCTimeout(transport, true)
		sr.Unblock()
		assertRPCTimeout(transport, false)
	})
}

func TestSiamuxDialUpgradeTimeout(t *testing.T) {
	n, genesis := testutil.V2Network()
	cm, s, w := startTestNode(t, n, genesis)

	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	isTimeoutErr := func(err error) bool {
		t.Helper()
		return err != nil && (strings.Contains(err.Error(), "use of closed network connection") || strings.Contains(err.Error(), "operation was canceled"))
	}

	sr := testutil.NewEphemeralSettingsReporter()
	hk := types.GeneratePrivateKey()
	rs := rhp4.NewServer(hk, cm, s, c, w, sr, ss, rhp4.WithPriceTableValidity(2*time.Minute))
	hostAddr := testutil.ServeSiaMux(t, rs, zap.NewNop())

	t.Run("dial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := siamux.Dial(ctx, hostAddr, hk.PublicKey())
		if !isTimeoutErr(err) {
			t.Fatal(err)
		}
	})

	t.Run("upgrade", func(t *testing.T) {
		conn, err := net.Dial("tcp", hostAddr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = siamux.Upgrade(ctx, conn, hk.PublicKey())
		if !isTimeoutErr(err) {
			t.Fatal(err)
		}
	})
}

func TestReplenishAccounts(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zaptest.NewLogger(t))

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(1000), types.Siacoins(2000)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	// mine to confirm the contract
	mineAndSync(t, cm, types.VoidAddress, 1, w, c)

	cs := cm.TipState()

	var balances []rhp4.AccountBalance
	// add some random unknown accounts
	for i := 0; i < 5; i++ {
		balances = append(balances, rhp4.AccountBalance{
			Account: proto4.Account(frand.Entropy256()),
			Balance: types.ZeroCurrency,
		})
	}

	var deposits []proto4.AccountDeposit
	for i := 0; i < 10; i++ {
		// fund an account with random values below 1SC
		account1 := frand.Entropy256()
		deposits = append(deposits, proto4.AccountDeposit{
			Account: account1,
			Amount:  types.NewCurrency64(frand.Uint64n(math.MaxUint64)),
		})

		// fund an account with 1SC
		account2 := frand.Entropy256()
		deposits = append(deposits, proto4.AccountDeposit{
			Account: account2,
			Amount:  types.Siacoins(1),
		})

		// fund an account with > 1SC
		account3 := frand.Entropy256()
		deposits = append(deposits, proto4.AccountDeposit{
			Account: account3,
			Amount:  types.Siacoins(1 + uint32(frand.Uint64n(5))),
		})
	}

	// fund the initial set of accounts
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, deposits)
	if err != nil {
		t.Fatal(err)
	}
	if len(fundResult.Balances) != len(deposits) {
		t.Fatalf("expected %v, got %v", len(deposits), len(balances))
	}
	for i, deposit := range deposits {
		if fundResult.Balances[i].Account != deposit.Account {
			t.Fatalf("expected %v, got %v", deposit.Account, fundResult.Balances[i].Account)
		} else if !fundResult.Balances[i].Balance.Equals(deposit.Amount) {
			t.Fatalf("expected %v, got %v", deposit.Amount, fundResult.Balances[i].Balance)
		}
	}
	revision.Revision = fundResult.Revision
	balances = append(balances, fundResult.Balances...)

	replenishParams := rhp4.RPCReplenishAccountsParams{
		Contract: revision,
		Target:   types.Siacoins(1),
	}
	var expectedCost types.Currency
	for _, balance := range balances {
		replenishParams.Accounts = append(replenishParams.Accounts, balance.Account)
		if balance.Balance.Cmp(replenishParams.Target) < 0 {
			expectedCost = expectedCost.Add(replenishParams.Target.Sub(balance.Balance))
		}
	}

	// replenish the accounts
	replenishResult, err := rhp4.RPCReplenishAccounts(context.Background(), transport, replenishParams, cs, fundAndSign)
	if err != nil {
		t.Fatal(err)
	} else if !replenishResult.Usage.AccountFunding.Equals(expectedCost) {
		t.Fatalf("expected %v, got %v", expectedCost, replenishResult.Usage.AccountFunding)
	} else if len(replenishResult.Deposits) != len(replenishParams.Accounts) {
		t.Fatalf("expected %v, got %v", len(replenishParams.Accounts), len(replenishResult.Deposits))
	}
	revisionTransfer := revision.Revision.RenterOutput.Value.Sub(replenishResult.Revision.RenterOutput.Value)
	if !revisionTransfer.Equals(replenishResult.Usage.AccountFunding) {
		t.Fatalf("expected %v, got %v", replenishResult.Usage.AccountFunding, revisionTransfer)
	}
	revision.Revision = replenishResult.Revision
	assertValidRevision(t, cm, c, revision)

	balanceMap := make(map[proto4.Account]types.Currency)
	for _, account := range balances {
		balance, err := rhp4.RPCAccountBalance(context.Background(), transport, account.Account)
		if err != nil {
			t.Fatal(err)
		}
		balanceMap[account.Account] = balance
		if account.Balance.Cmp(replenishParams.Target) < 0 {
			if !balance.Equals(replenishParams.Target) {
				t.Fatalf("expected %v, got %v", replenishParams.Target, balance)
			}
		} else if !balance.Equals(account.Balance) {
			// balances that were already >= 1SC should not have changed
			t.Fatalf("expected %v, got %v", account.Balance, balance)
		}
	}

	// call replenish again with the same accounts, the balances should not change
	replenishParams.Contract = revision
	replenishResult, err = rhp4.RPCReplenishAccounts(context.Background(), transport, replenishParams, cs, fundAndSign)
	if err != nil {
		t.Fatal(err)
	} else if !replenishResult.Usage.AccountFunding.Equals(types.ZeroCurrency) {
		t.Fatalf("expected %v, got %v", expectedCost, replenishResult.Usage.AccountFunding)
	} else if len(replenishResult.Deposits) != len(replenishParams.Accounts) {
		t.Fatalf("expected %v, got %v", len(replenishParams.Accounts), len(replenishResult.Deposits))
	}
	for _, deposit := range replenishResult.Deposits {
		if !deposit.Amount.IsZero() {
			t.Fatalf("expected zero, got %v", deposit.Amount)
		}
	}

	for _, account := range balances {
		balance, err := rhp4.RPCAccountBalance(context.Background(), transport, account.Account)
		if err != nil {
			t.Fatal(err)
		}

		expected, ok := balanceMap[account.Account]
		if !ok {
			t.Fatalf("expected account not found")
		} else if !balance.Equals(expected) {
			t.Fatalf("expected %v, got %v", expected, balance)
		}
	}

	assertValidRevision(t, cm, c, revision)
}

func TestAccounts(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()
	account := proto4.Account(renterKey.PublicKey())

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()

	// test operations against unknown account
	token := account.Token(renterKey, hostKey.PublicKey())
	_, err = rhp4.RPCVerifySector(context.Background(), transport, settings.Prices, token, types.Hash256{1})
	if err == nil || !strings.Contains(err.Error(), proto4.ErrNotEnoughFunds.Error()) {
		t.Fatal(err)
	}

	balance, err := rhp4.RPCAccountBalance(context.Background(), transport, account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.IsZero() {
		t.Fatal("expected zero balance")
	}

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revised := fundResult.Revision

	renterOutputValue := revision.Revision.RenterOutput.Value.Sub(accountFundAmount)
	hostOutputValue := revision.Revision.HostOutput.Value.Add(accountFundAmount)

	// verify the contract was modified correctly
	switch {
	case fundResult.Balances[0].Account != account:
		t.Fatalf("expected %v, got %v", account, fundResult.Balances[0].Account)
	case fundResult.Balances[0].Balance != accountFundAmount:
		t.Fatalf("expected %v, got %v", accountFundAmount, fundResult.Balances[0].Balance)
	case !fundResult.Usage.RenterCost().Equals(accountFundAmount):
		t.Fatalf("expected %v, got %v", accountFundAmount, fundResult.Usage.RenterCost())
	case !revised.HostOutput.Value.Equals(hostOutputValue):
		t.Fatalf("expected %v, got %v", hostOutputValue, revised.HostOutput.Value)
	case !revised.RenterOutput.Value.Equals(renterOutputValue):
		t.Fatalf("expected %v, got %v", renterOutputValue, revised.RenterOutput.Value)
	case !revised.MissedHostValue.Equals(revision.Revision.MissedHostValue):
		t.Fatalf("expected %v, got %v", revision.Revision.MissedHostValue, revised.MissedHostValue)
	case revised.RevisionNumber != revision.Revision.RevisionNumber+1:
		t.Fatalf("expected %v, got %v", revision.Revision.RevisionNumber+1, revised.RevisionNumber)
	}

	revisionSigHash := cs.ContractSigHash(revised)
	if !renterKey.PublicKey().VerifyHash(revisionSigHash, revised.RenterSignature) {
		t.Fatal("revision signature verification failed")
	} else if !hostKey.PublicKey().VerifyHash(revisionSigHash, revised.HostSignature) {
		t.Fatal("revision signature verification failed")
	}

	// verify the account balance
	balance, err = rhp4.RPCAccountBalance(context.Background(), transport, account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(accountFundAmount) {
		t.Fatalf("expected %v, got %v", accountFundAmount, balance)
	}

	// drain account and try using it
	_ = c.DebitAccount(account, proto4.Usage{RPC: accountFundAmount})
	_, err = rhp4.RPCVerifySector(context.Background(), transport, settings.Prices, token, types.Hash256{1})
	if err == nil || !strings.Contains(err.Error(), proto4.ErrNotEnoughFunds.Error()) {
		t.Fatal(err)
	}
}

func TestReadWriteSector(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())
	data := frand.Bytes(1024)

	// store the sector
	writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// verify the sector root
	var sector [proto4.SectorSize]byte
	copy(sector[:], data)
	if writeResult.Root != proto4.SectorRoot(&sector) {
		t.Fatal("root mismatch")
	}

	// read the sector back
	buf := bytes.NewBuffer(nil)
	_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, writeResult.Root, 0, 64)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf.Bytes(), data[:64]) {
		t.Fatal("data mismatch")
	}
}

func TestAppendSectors(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	// mine to confirm the contract
	mineAndSync(t, cm, types.VoidAddress, 1, w, c)

	assertLastRevision := func(t *testing.T) {
		t.Helper()

		rs, err := rhp4.RPCLatestRevision(context.Background(), transport, revision.ID)
		if err != nil {
			t.Fatal(err)
		}
		lastRev := rs.Contract
		if !reflect.DeepEqual(lastRev, revision.Revision) {
			t.Log(lastRev)
			t.Log(revision.Revision)
			t.Fatalf("expected last revision to match")
		}

		sigHash := cm.TipState().ContractSigHash(revision.Revision)
		if !renterKey.PublicKey().VerifyHash(sigHash, lastRev.RenterSignature) {
			t.Fatal("renter signature invalid")
		} else if !hostKey.PublicKey().VerifyHash(sigHash, lastRev.HostSignature) {
			t.Fatal("host signature invalid")
		}

		if revision.Revision.RevisionNumber > 0 {
			assertValidRevision(t, cm, c, revision)
		}
	}
	assertLastRevision(t)

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	assertLastRevision(t)

	token := account.Token(renterKey, hostKey.PublicKey())

	// store random sectors
	roots := make([]types.Hash256, 0, 10)
	for i := 0; i < 10; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:])
		root := proto4.SectorRoot(&sector)

		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sector[:]), proto4.SectorSize)
		if err != nil {
			t.Fatal(err)
		} else if writeResult.Root != root {
			t.Fatal("root mismatch")
		}
		roots = append(roots, root)
	}

	// corrupt a random root
	excludedIndex := frand.Intn(len(roots))
	roots[excludedIndex] = frand.Entropy256()

	// append the sectors to the contract
	appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, roots)
	if err != nil {
		t.Fatal(err)
	} else if len(appendResult.Sectors) != len(roots)-1 {
		t.Fatalf("expected %v, got %v", len(roots)-1, len(appendResult.Sectors))
	}
	roots = append(roots[:excludedIndex], roots[excludedIndex+1:]...)
	if appendResult.Revision.FileMerkleRoot != proto4.MetaRoot(roots) {
		t.Fatal("root mismatch")
	}
	revision.Revision = appendResult.Revision
	assertLastRevision(t)

	// read the sectors back
	buf := bytes.NewBuffer(make([]byte, 0, proto4.SectorSize))
	for _, root := range roots {
		buf.Reset()

		_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, root, 0, proto4.SectorSize)
		if err != nil {
			t.Fatal(err)
		} else if proto4.SectorRoot((*[proto4.SectorSize]byte)(buf.Bytes())) != root {
			t.Fatal("data mismatch")
		}
	}
}

func TestVerifySector(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())
	data := frand.Bytes(1024)

	// store the sector
	writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}

	// verify the sector root
	var sector [proto4.SectorSize]byte
	copy(sector[:], data)
	if writeResult.Root != proto4.SectorRoot(&sector) {
		t.Fatal("root mismatch")
	}

	// verify the host is storing the sector
	_, err = rhp4.RPCVerifySector(context.Background(), transport, settings.Prices, token, writeResult.Root)
	if err != nil {
		t.Fatal(err)
	}
}

func TestRPCFreeSectors(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	// mine to confirm
	mineAndSync(t, cm, types.VoidAddress, 1, w, c)

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())

	roots := make([]types.Hash256, 10)
	for i := range roots {
		// store random sectors on the host
		data := frand.Bytes(1024)

		// store the sector
		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		roots[i] = writeResult.Root
	}

	assertRevision := func(t *testing.T, revision rhp4.ContractRevision, roots []types.Hash256) {
		t.Helper()

		expectedRoot := proto4.MetaRoot(roots)
		n := len(roots)
		contract := revision.Revision

		if contract.Filesize/proto4.SectorSize != uint64(n) {
			t.Fatalf("expected %v sectors, got %v", n, contract.Filesize/proto4.SectorSize)
		} else if contract.FileMerkleRoot != expectedRoot {
			t.Fatalf("expected %v, got %v", expectedRoot, contract.FileMerkleRoot)
		}

		if contract.RevisionNumber > 0 {
			assertValidRevision(t, cm, c, revision)
		}
	}

	// append all the sector roots to the contract
	appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, roots)
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = appendResult.Revision
	assertRevision(t, revision, roots)

	// randomly remove half the sectors
	indices := make([]uint64, len(roots)/2)
	for i, n := range frand.Perm(len(roots))[:len(roots)/2] {
		indices[i] = uint64(n)
	}
	newRoots := append([]types.Hash256(nil), roots...)
	for i, n := range indices {
		newRoots[n] = newRoots[len(newRoots)-i-1]
	}
	newRoots = newRoots[:len(newRoots)-len(indices)]

	removeResult, err := rhp4.RPCFreeSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, indices)
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = removeResult.Revision
	assertRevision(t, revision, newRoots)
}

func TestRPCSectorRoots(t *testing.T) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(t, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	revision := formResult.Contract

	// mine to confirm
	mineAndSync(t, cm, types.VoidAddress, 1, w, c)

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())

	roots := make([]types.Hash256, 0, 50)

	checkRoots := func(t *testing.T, expected []types.Hash256) {
		t.Helper()

		rootsResult, err := rhp4.RPCSectorRoots(context.Background(), transport, cs, settings.Prices, renterKey, revision, 0, uint64(len(expected)))
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expected) {
			t.Fatalf("expected %v roots, got %v", len(expected), len(roots))
		}
		for i := range rootsResult.Roots {
			if roots[i] != expected[i] {
				t.Fatalf("expected %v, got %v", expected[i], roots[i])
			}
		}
		revision.Revision = rootsResult.Revision
	}

	for i := 0; i < cap(roots); i++ {
		// store random sectors on the host
		data := frand.Bytes(1024)

		// store the sector
		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(data), uint64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, writeResult.Root)

		appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, []types.Hash256{writeResult.Root})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = appendResult.Revision
		assertValidRevision(t, cm, c, revision)
		checkRoots(t, roots)
	}
}

func BenchmarkWrite(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(b, n, genesis)

	// fund the wallet
	mineAndSync(b, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(b, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}
	revision := formResult.Contract

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())

	var sectors [][proto4.SectorSize]byte
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	for i := 0; i < b.N; i++ {
		// store the sector
		_, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sectors[i][:]), proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(b, n, genesis)

	// fund the wallet
	mineAndSync(b, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(b, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}
	revision := formResult.Contract

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())

	var sectors [][proto4.SectorSize]byte
	roots := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)

		// store the sector
		writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sectors[i][:]), proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		}
		roots = append(roots, writeResult.Root)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	buf := bytes.NewBuffer(make([]byte, 0, proto4.SectorSize))
	for i := 0; i < b.N; i++ {
		buf.Reset()
		// store the sector
		_, err = rhp4.RPCReadSector(context.Background(), transport, settings.Prices, token, buf, roots[i], 0, proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		} else if !bytes.Equal(buf.Bytes(), sectors[i][:]) {
			b.Fatal("data mismatch")
		}
	}
}

func BenchmarkContractUpload(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(b, n, genesis)

	// fund the wallet
	mineAndSync(b, cm, w.Address(), int(n.MaturityDelay+20), w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(1).Div64(5), // 0.2 SC
			StoragePrice:  types.NewCurrency64(100),   // 100 H / byte / block
			IngressPrice:  types.NewCurrency64(100),   // 100 H / byte
			EgressPrice:   types.NewCurrency64(100),   // 100 H / byte
			Collateral:    types.NewCurrency64(200),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPairSiaMux(b, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(context.Background(), transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	formResult, err := rhp4.RPCFormContract(context.Background(), transport, cm, fundAndSign, cm.TipState(), settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}
	revision := formResult.Contract

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	fundResult, err := rhp4.RPCFundAccounts(context.Background(), transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = fundResult.Revision
	token := account.Token(renterKey, hostKey.PublicKey())

	var sectors [][proto4.SectorSize]byte
	roots := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)
		roots = append(roots, proto4.SectorRoot(&sector))
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			writeResult, err := rhp4.RPCWriteSector(context.Background(), transport, settings.Prices, token, bytes.NewReader(sectors[i][:]), proto4.SectorSize)
			if err != nil {
				b.Error(err)
			} else if writeResult.Root != roots[i] {
				b.Errorf("expected %v, got %v", roots[i], writeResult.Root)
			}
		}(i)
	}

	wg.Wait()

	appendResult, err := rhp4.RPCAppendSectors(context.Background(), transport, cs, settings.Prices, renterKey, revision, roots)
	if err != nil {
		b.Fatal(err)
	} else if appendResult.Revision.Filesize != uint64(b.N)*proto4.SectorSize {
		b.Fatalf("expected %v sectors, got %v", b.N, appendResult.Revision.Filesize/proto4.SectorSize)
	}
}
