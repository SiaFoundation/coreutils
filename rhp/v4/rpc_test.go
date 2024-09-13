package rhp_test

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/mux"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"lukechampine.com/frand"
)

type muxTransport struct {
	m *mux.Mux
}

func (mt *muxTransport) DialStream() net.Conn {
	return mt.m.DialStream()
}
func (mt *muxTransport) Close() error {
	return mt.m.Close()
}

type fundAndSign struct {
	w  *wallet.SingleAddressWallet
	pk types.PrivateKey
}

func (fs *fundAndSign) FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error) {
	return fs.w.FundV2Transaction(txn, amount, true)
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

func testRenterHostPair(tb testing.TB, hostKey types.PrivateKey, cm rhp4.ServerChainManager, s rhp4.Syncer, w rhp4.Wallet, c rhp4.Contractor, sr rhp4.SettingsReporter, ss rhp4.SectorStore, log *zap.Logger) rhp4.TransportClient {
	rs := rhp4.NewServer(hostKey, cm, s, c, w, sr, ss, rhp4.WithContractProofWindowBuffer(10), rhp4.WithPriceTableValidity(2*time.Minute), rhp4.WithLog(log.Named("rhp4")))
	hostAddr := testutil.ServeSiaMux(tb, rs, log.Named("siamux"))

	conn, err := net.Dial("tcp", hostAddr)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() })

	hostPK := hostKey.PublicKey()
	m, err := mux.Dial(conn, hostPK[:])
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { m.Close() })

	return &muxTransport{m}
}

func startTestNode(tb testing.TB, n *consensus.Network, genesis types.Block, log *zap.Logger) (*chain.Manager, *syncer.Syncer, *wallet.SingleAddressWallet) {
	db, tipstate, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		tb.Fatal(err)
	}
	cm := chain.NewManager(db, tipstate, chain.WithLog(log.Named("chain")))

	syncerListener, err := net.Listen("tcp", ":0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { syncerListener.Close() })

	s := syncer.New(syncerListener, cm, testutil.NewMemPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: "localhost:1234",
	}, syncer.WithLogger(log.Named("syncer")))
	go s.Run(context.Background())
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

	stop := cm.OnReorg(func(index types.ChainIndex) {
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
		time.Sleep(time.Millisecond)
	}
}

func TestSettings(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey := types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet
	mineAndSync(t, cm, w.Address(), 150)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
		RemainingStorage:    100 * proto4.SectorSize,
		TotalStorage:        100 * proto4.SectorSize,
		Prices: proto4.HostPrices{
			ContractPrice: types.Siacoins(uint32(frand.Uint64n(10000))),
			StoragePrice:  types.Siacoins(uint32(frand.Uint64n(10000))),
			IngressPrice:  types.Siacoins(uint32(frand.Uint64n(10000))),
			EgressPrice:   types.Siacoins(uint32(frand.Uint64n(10000))),
			Collateral:    types.Siacoins(uint32(frand.Uint64n(10000))),
		},
	})
	ss := testutil.NewEphemeralSectorStore()
	c := testutil.NewEphemeralContractor(cm)

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	} else if settings.Prices.ValidUntil.Before(time.Now()) {
		t.Fatal("settings expired")
	}

	// verify the signature
	sigHash := settings.Prices.SigHash()
	if !hostKey.PublicKey().VerifyHash(sigHash, settings.Prices.Signature) {
		t.Fatal("signature verification failed")
	}

	// adjust the calculated fields to match the expected values
	expected := sr.RHP4Settings()
	expected.ProtocolVersion = settings.ProtocolVersion
	expected.Prices.Signature = settings.Prices.Signature
	expected.Prices.ValidUntil = settings.Prices.ValidUntil
	expected.Prices.TipHeight = settings.Prices.TipHeight

	if !reflect.DeepEqual(settings, expected) {
		t.Error("retrieved", settings)
		t.Error("expected", expected)
		t.Fatal("settings mismatch")
	}
}

func TestFormContract(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	_, finalizedSet, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
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
	if known, err := cm.AddV2PoolTransactions(finalizedSet.Basis, finalizedSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}
}

func TestFormContractBasis(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	_, finalizedSet, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
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
	if known, err := cm.AddV2PoolTransactions(finalizedSet.Basis, finalizedSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}
}

func TestRenewContractPartialRollover(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, formationSet, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
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
	if known, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}

	// mine a few blocks to confirm the contract
	mineAndSync(t, cm, types.VoidAddress, 10, w, c)

	// fund an account to transfer funds to the host
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	revised, _, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = revised

	// renew the contract
	revision, renewalSet, err := rhp4.RPCRenewContract(transport, cm, fundAndSign, settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
		ContractID:  revision.ID,
		Allowance:   types.Siacoins(150),
		Collateral:  types.Siacoins(300),
		ProofHeight: revision.Revision.ProofHeight + 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the transaction set is valid
	if known, err := cm.AddV2PoolTransactions(renewalSet.Basis, renewalSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}
}

func TestRenewContractFullRollover(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, formationSet, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
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
	if known, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}

	// mine a few blocks to confirm the contract
	mineAndSync(t, cm, types.VoidAddress, 10, w, c)

	// fund an account to transfer funds to the host
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	revised, _, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = revised

	// renew the contract
	revision, renewalSet, err := rhp4.RPCRenewContract(transport, cm, fundAndSign, settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
		ContractID:  revision.ID,
		Allowance:   types.Siacoins(50),
		Collateral:  types.Siacoins(100),
		ProofHeight: revision.Revision.ProofHeight + 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the transaction set is valid
	if known, err := cm.AddV2PoolTransactions(renewalSet.Basis, renewalSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}
}

func TestRenewContractNoRollover(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, formationSet, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
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
	if known, err := cm.AddV2PoolTransactions(formationSet.Basis, formationSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}

	// mine a few blocks to confirm the contract
	mineAndSync(t, cm, types.VoidAddress, 10, w, c)

	// fund an account to transfer funds to the host
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(100)
	revised, _, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = revised

	// renew the contract
	revision, renewalSet, err := rhp4.RPCRenewContract(transport, cm, fundAndSign, settings.Prices, revision.Revision, proto4.RPCRenewContractParams{
		ContractID:  revision.ID,
		Allowance:   types.Siacoins(150),
		Collateral:  types.Siacoins(300),
		ProofHeight: revision.Revision.ProofHeight + 10,
	})
	if err != nil {
		t.Fatal(err)
	}

	// verify the transaction set is valid
	if known, err := cm.AddV2PoolTransactions(renewalSet.Basis, renewalSet.Transactions); err != nil {
		t.Fatal(err)
	} else if !known {
		t.Fatal("expected transaction set to be known")
	}
}

func TestAccounts(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	revised, balances, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}

	renterOutputValue := revision.Revision.RenterOutput.Value.Sub(accountFundAmount)
	hostOutputValue := revision.Revision.HostOutput.Value.Add(accountFundAmount)

	// verify the account was funded
	if !balances[0].Equals(accountFundAmount) {
		t.Fatalf("expected %v, got %v", accountFundAmount, balances[0])
	}

	// verify the contract was modified correctly
	switch {
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
	balance, err := rhp4.RPCAccountBalance(transport, account)
	if err != nil {
		t.Fatal(err)
	} else if !balance.Equals(accountFundAmount) {
		t.Fatalf("expected %v, got %v", accountFundAmount, balance)
	}
}

func TestReadWriteSector(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	_, _, err = rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	data := frand.Bytes(1024)

	// store the sector
	root, err := rhp4.RPCWriteSector(transport, settings.Prices, token, data, 5)
	if err != nil {
		t.Fatal(err)
	}

	// verify the sector root
	var sector [proto4.SectorSize]byte
	copy(sector[:], data)
	if root != proto4.SectorRoot(&sector) {
		t.Fatal("root mismatch")
	}

	// read the sector back
	buf, err := rhp4.RPCReadSector(transport, settings.Prices, token, root, 0, 64)
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(buf, data[:64]) {
		t.Fatal("data mismatch")
	}
}

func TestRPCModifySectors(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	revised, _, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = revised

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	roots := make([]types.Hash256, 50)
	for i := range roots {
		// store random sectors on the host
		data := frand.Bytes(1024)

		// store the sector
		root, err := rhp4.RPCWriteSector(transport, settings.Prices, token, data, 5)
		if err != nil {
			t.Fatal(err)
		}
		roots[i] = root
	}

	assertRevision := func(t *testing.T, revision types.V2FileContract, roots []types.Hash256) {
		t.Helper()

		expectedRoot := proto4.MetaRoot(roots)
		n := len(roots)

		if revision.Filesize/proto4.SectorSize != uint64(n) {
			t.Fatalf("expected %v sectors, got %v", n, revision.Filesize/proto4.SectorSize)
		} else if revision.FileMerkleRoot != expectedRoot {
			t.Fatalf("expected %v, got %v", expectedRoot, revision.FileMerkleRoot)
		}
	}

	// append all the sector roots to the contract
	var actions []proto4.WriteAction
	for _, root := range roots {
		actions = append(actions, proto4.WriteAction{
			Type: proto4.ActionAppend,
			Root: root,
		})
	}
	revised, err = rhp4.RPCModifySectors(transport, cs, settings.Prices, renterKey, revision, actions)
	if err != nil {
		t.Fatal(err)
	}
	assertRevision(t, revised, roots)
	revision.Revision = revised

	// swap two random sectors
	swapA, swapB := frand.Uint64n(25), frand.Uint64n(25)+25
	actions = []proto4.WriteAction{
		{Type: proto4.ActionSwap, A: swapA, B: swapB},
	}
	revised, err = rhp4.RPCModifySectors(transport, cs, settings.Prices, renterKey, revision, actions)
	if err != nil {
		t.Fatal(err)
	}
	roots[swapA], roots[swapB] = roots[swapB], roots[swapA]
	assertRevision(t, revised, roots)
	revision.Revision = revised

	// delete the last 10 sectors
	actions = []proto4.WriteAction{
		{Type: proto4.ActionTrim, N: 10},
	}
	revised, err = rhp4.RPCModifySectors(transport, cs, settings.Prices, renterKey, revision, actions)
	if err != nil {
		t.Fatal(err)
	}
	trimmed, roots := roots[:len(roots)-10], roots[:40]
	assertRevision(t, revised, roots)
	revision.Revision = revised

	// update a random sector with one of the trimmed sectors
	updateIdx := frand.Uint64n(40)
	trimmedIdx := frand.Uint64n(10)
	actions = []proto4.WriteAction{
		{Type: proto4.ActionUpdate, Root: trimmed[trimmedIdx], A: updateIdx},
	}

	revised, err = rhp4.RPCModifySectors(transport, cs, settings.Prices, renterKey, revision, actions)
	if err != nil {
		t.Fatal(err)
	}
	roots[updateIdx] = trimmed[trimmedIdx]
	assertRevision(t, revised, roots)
}

func TestRPCSectorRoots(t *testing.T) {
	log := zaptest.NewLogger(t)
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(t, n, genesis, log)

	// fund the wallet with two UTXOs
	mineAndSync(t, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(t, hostKey, cm, s, w, c, sr, ss, log)

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		t.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		t.Fatal(err)
	}

	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())

	accountFundAmount := types.Siacoins(25)
	revised, _, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		t.Fatal(err)
	}
	revision.Revision = revised

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	tokenSigHash := token.SigHash()
	token.Signature = renterKey.SignHash(tokenSigHash)

	roots := make([]types.Hash256, 0, 50)

	checkRoots := func(t *testing.T, expected []types.Hash256) {
		t.Helper()

		revised, roots, err := rhp4.RPCSectorRoots(transport, cs, settings.Prices, renterKey, revision, 0, uint64(len(expected)))
		if err != nil {
			t.Fatal(err)
		} else if len(roots) != len(expected) {
			t.Fatalf("expected %v roots, got %v", len(expected), len(roots))
		}
		for i := range roots {
			if roots[i] != expected[i] {
				t.Fatalf("expected %v, got %v", expected[i], roots[i])
			}
		}
		revision.Revision = revised
	}

	for i := 0; i < cap(roots); i++ {
		// store random sectors on the host
		data := frand.Bytes(1024)

		// store the sector
		root, err := rhp4.RPCWriteSector(transport, settings.Prices, token, data, 5)
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)

		revised, err := rhp4.RPCModifySectors(transport, cs, settings.Prices, renterKey, revision, []proto4.WriteAction{
			{Type: proto4.ActionAppend, Root: root},
		})
		if err != nil {
			t.Fatal(err)
		}
		revision.Revision = revised

		checkRoots(t, roots)
	}
}

func BenchmarkWrite(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(b, n, genesis, zap.NewNop())

	// fund the wallet with two UTXOs
	mineAndSync(b, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(b, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	_, _, err = rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	token.Signature = renterKey.SignHash(token.SigHash())

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
		_, err := rhp4.RPCWriteSector(transport, settings.Prices, token, sectors[i][:], 5)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(b, n, genesis, zap.NewNop())

	// fund the wallet with two UTXOs
	mineAndSync(b, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(b, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	_, _, err = rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	token.Signature = renterKey.SignHash(token.SigHash())

	var sectors [][proto4.SectorSize]byte
	roots := make([]types.Hash256, 0, b.N)
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)

		// store the sector
		root, err := rhp4.RPCWriteSector(transport, settings.Prices, token, sectors[i][:], 5)
		if err != nil {
			b.Fatal(err)
		}
		roots = append(roots, root)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	for i := 0; i < b.N; i++ {
		// store the sector
		buf, err := rhp4.RPCReadSector(transport, settings.Prices, token, roots[i], 0, proto4.SectorSize)
		if err != nil {
			b.Fatal(err)
		} else if !bytes.Equal(buf, sectors[i][:]) {
			b.Fatal("data mismatch")
		}
	}
}

func BenchmarkContractUpload(b *testing.B) {
	n, genesis := testutil.V2Network()
	hostKey, renterKey := types.GeneratePrivateKey(), types.GeneratePrivateKey()

	cm, s, w := startTestNode(b, n, genesis, zap.NewNop())

	// fund the wallet with two UTXOs
	mineAndSync(b, cm, w.Address(), 146, w)

	sr := testutil.NewEphemeralSettingsReporter()
	sr.Update(proto4.HostSettings{
		Release:             "test",
		AcceptingContracts:  true,
		WalletAddress:       w.Address(),
		MaxCollateral:       types.Siacoins(10000),
		MaxContractDuration: 1000,
		MaxSectorDuration:   3 * 144,
		MaxModifyActions:    100,
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

	transport := testRenterHostPair(b, hostKey, cm, s, w, c, sr, ss, zap.NewNop())

	settings, err := rhp4.RPCSettings(transport)
	if err != nil {
		b.Fatal(err)
	}

	fundAndSign := &fundAndSign{w, renterKey}
	renterAllowance, hostCollateral := types.Siacoins(100), types.Siacoins(200)
	revision, _, err := rhp4.RPCFormContract(transport, cm, fundAndSign, settings.Prices, hostKey.PublicKey(), settings.WalletAddress, proto4.RPCFormContractParams{
		RenterPublicKey: renterKey.PublicKey(),
		RenterAddress:   w.Address(),
		Allowance:       renterAllowance,
		Collateral:      hostCollateral,
		ProofHeight:     cm.Tip().Height + 50,
	})
	if err != nil {
		b.Fatal(err)
	}

	// fund an account
	cs := cm.TipState()
	account := proto4.Account(renterKey.PublicKey())
	accountFundAmount := types.Siacoins(25)
	revised, _, err := rhp4.RPCFundAccounts(transport, cs, renterKey, revision, []proto4.AccountDeposit{
		{Account: account, Amount: accountFundAmount},
	})
	if err != nil {
		b.Fatal(err)
	}
	revision.Revision = revised

	token := proto4.AccountToken{
		Account:    account,
		ValidUntil: time.Now().Add(time.Hour),
	}
	token.Signature = renterKey.SignHash(token.SigHash())

	var sectors [][proto4.SectorSize]byte
	for i := 0; i < b.N; i++ {
		var sector [proto4.SectorSize]byte
		frand.Read(sector[:256])
		sectors = append(sectors, sector)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(proto4.SectorSize)

	var wg sync.WaitGroup
	actions := make([]proto4.WriteAction, b.N)
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			root, err := rhp4.RPCWriteSector(transport, settings.Prices, token, sectors[i][:], 5)
			if err != nil {
				b.Error(err)
			}
			actions[i] = proto4.WriteAction{
				Type: proto4.ActionAppend,
				Root: root,
			}
		}(i)
	}

	wg.Wait()

	revised, err = rhp4.RPCModifySectors(transport, cs, settings.Prices, renterKey, revision, actions)
	if err != nil {
		b.Fatal(err)
	} else if revised.Filesize != uint64(b.N)*proto4.SectorSize {
		b.Fatalf("expected %v sectors, got %v", b.N, revised.Filesize/proto4.SectorSize)
	}
}