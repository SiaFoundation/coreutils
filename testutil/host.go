package testutil

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"

	"go.sia.tech/core/consensus"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/quic"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// An EphemeralCertManager is an in-memory minimal rhp4.CertManager for testing.
// Calls to GetCertificate will return a new self-signed certificate each time.
type EphemeralCertManager struct{}

// GetCertificate returns a new self-signed certificate each time it is called.
func (ec *EphemeralCertManager) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	key, err := rsa.GenerateKey(frand.Reader, 2048)
	if err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(frand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cert: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to create tls cert: %w", err)
	}
	return &cert, nil
}

// An EphemeralSectorStore is an in-memory minimal rhp4.SectorStore for testing.
type EphemeralSectorStore struct {
	mu      sync.Mutex
	sectors map[types.Hash256]*[proto4.SectorSize]byte
}

var _ rhp4.Sectors = (*EphemeralSectorStore)(nil)

// DeleteSector deletes a sector from the store.
func (es *EphemeralSectorStore) DeleteSector(root types.Hash256) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	if _, ok := es.sectors[root]; !ok {
		return proto4.ErrSectorNotFound
	}
	delete(es.sectors, root)
	return nil
}

// HasSector checks if a sector is stored in the store.
func (es *EphemeralSectorStore) HasSector(root types.Hash256) (bool, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	_, ok := es.sectors[root]
	return ok, nil
}

// ReadSector reads a sector from the EphemeralSectorStore.
func (es *EphemeralSectorStore) ReadSector(root types.Hash256) (*[proto4.SectorSize]byte, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	sector, ok := es.sectors[root]
	if !ok {
		return nil, proto4.ErrSectorNotFound
	}
	return sector, nil
}

// StoreSector stores a sector in the EphemeralSectorStore.
func (es *EphemeralSectorStore) StoreSector(root types.Hash256, sector *[proto4.SectorSize]byte, _ uint64) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.sectors[root] = sector
	return nil
}

// An EphemeralContractor is an in-memory minimal rhp4.Contractor for testing.
type EphemeralContractor struct {
	tip              types.ChainIndex
	contractElements map[types.FileContractID]types.V2FileContractElement
	contracts        map[types.FileContractID]types.V2FileContract
	roots            map[types.FileContractID][]types.Hash256
	locks            map[types.FileContractID]bool

	accounts map[proto4.Account]types.Currency

	mu       sync.Mutex
	shutdown chan struct{}
}

var _ rhp4.Contractor = (*EphemeralContractor)(nil)

// Close closes the contractor by interrupting its background loop
func (ec *EphemeralContractor) Close() error {
	close(ec.shutdown)
	return nil
}

// V2FileContractElement returns the contract state element for the given contract ID.
func (ec *EphemeralContractor) V2FileContractElement(contractID types.FileContractID) (types.ChainIndex, types.V2FileContractElement, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	element, ok := ec.contractElements[contractID]
	if !ok {
		return types.ChainIndex{}, types.V2FileContractElement{}, errors.New("contract not found")
	}

	// deep-copy element's proof to avoid passing a reference to the
	// EphemeralContractor's internal state
	element.StateElement.MerkleProof = append([]types.Hash256(nil), element.StateElement.MerkleProof...)
	return ec.tip, element, nil
}

// LockV2Contract locks a contract and returns its current state.
func (ec *EphemeralContractor) LockV2Contract(contractID types.FileContractID) (rhp4.RevisionState, func(), error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if ec.locks[contractID] {
		return rhp4.RevisionState{}, nil, errors.New("contract already locked")
	}
	ec.locks[contractID] = true

	rev, ok := ec.contracts[contractID]
	if !ok {
		return rhp4.RevisionState{}, nil, errors.New("contract not found")
	}

	_, renewed := ec.contracts[contractID.V2RenewalID()]

	var once sync.Once
	return rhp4.RevisionState{
			Revision:  rev,
			Revisable: !renewed && ec.tip.Height < rev.ProofHeight,
			Renewed:   renewed,
			Roots:     ec.roots[contractID],
		}, func() {
			once.Do(func() {
				ec.mu.Lock()
				defer ec.mu.Unlock()
				ec.locks[contractID] = false
			})
		}, nil
}

// AddV2Contract adds a new contract to the host.
func (ec *EphemeralContractor) AddV2Contract(formationSet rhp4.TransactionSet, _ proto4.Usage) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if len(formationSet.Transactions) == 0 {
		return errors.New("expected at least one transaction")
	}
	formationTxn := formationSet.Transactions[len(formationSet.Transactions)-1]
	if len(formationTxn.FileContracts) != 1 {
		return errors.New("expected exactly one contract")
	}
	fc := formationTxn.FileContracts[0]

	sigHash := consensus.State{}.ContractSigHash(fc)
	if !fc.RenterPublicKey.VerifyHash(sigHash, fc.RenterSignature) {
		return errors.New("invalid renter signature")
	} else if !fc.HostPublicKey.VerifyHash(sigHash, fc.HostSignature) {
		return errors.New("invalid host signature")
	}

	contractID := formationTxn.V2FileContractID(formationTxn.ID(), 0)
	if _, ok := ec.contracts[contractID]; ok {
		return errors.New("contract already exists")
	}
	ec.contracts[contractID] = fc
	ec.roots[contractID] = []types.Hash256{}
	return nil
}

// RenewV2Contract finalizes an existing contract and adds the renewed contract
// to the host. The existing contract must be locked before calling this method.
func (ec *EphemeralContractor) RenewV2Contract(renewalSet rhp4.TransactionSet, _ proto4.Usage) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	if len(renewalSet.Transactions) == 0 {
		return errors.New("expected at least one transaction")
	}
	renewalTxn := renewalSet.Transactions[len(renewalSet.Transactions)-1]
	if len(renewalTxn.FileContractResolutions) != 1 {
		return errors.New("expected exactly one resolution")
	}
	resolution := renewalTxn.FileContractResolutions[0]
	renewal, ok := resolution.Resolution.(*types.V2FileContractRenewal)
	if !ok {
		return errors.New("expected renewal resolution")
	}
	existingID := types.FileContractID(resolution.Parent.ID)

	existing, ok := ec.contracts[existingID]
	if !ok {
		return errors.New("contract not found")
	}

	contractID := existingID.V2RenewalID()
	if _, ok := ec.contracts[contractID]; ok {
		return errors.New("contract already exists")
	}

	sigHash := consensus.State{}.ContractSigHash(renewal.NewContract)
	if !existing.RenterPublicKey.VerifyHash(sigHash, renewal.NewContract.RenterSignature) {
		return errors.New("invalid renter signature")
	} else if !existing.HostPublicKey.VerifyHash(sigHash, renewal.NewContract.HostSignature) {
		return errors.New("invalid host signature")
	}

	ec.contracts[contractID] = renewal.NewContract
	ec.roots[contractID] = append([]types.Hash256(nil), ec.roots[existingID]...)
	return nil
}

// ReviseV2Contract atomically revises a contract and updates its sector roots
// and usage.
func (ec *EphemeralContractor) ReviseV2Contract(contractID types.FileContractID, revision types.V2FileContract, roots []types.Hash256, _ proto4.Usage) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	existing, ok := ec.contracts[contractID]
	if !ok {
		return errors.New("contract not found")
	} else if revision.RevisionNumber <= existing.RevisionNumber {
		return errors.New("revision number must be greater than existing")
	}

	sigHash := consensus.State{}.ContractSigHash(revision)
	if !existing.RenterPublicKey.VerifyHash(sigHash, revision.RenterSignature) {
		return errors.New("invalid renter signature")
	} else if !existing.HostPublicKey.VerifyHash(sigHash, revision.HostSignature) {
		return errors.New("invalid host signature")
	}

	ec.contracts[contractID] = revision
	ec.roots[contractID] = append([]types.Hash256(nil), roots...)
	return nil
}

// AccountBalance returns the balance of an account.
func (ec *EphemeralContractor) AccountBalance(account proto4.Account) (types.Currency, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	balance := ec.accounts[account]
	return balance, nil
}

// AccountBalances returns the balances of multiple accounts.
// The order of the returned balances corresponds to the order of the input
// accounts. If an account is not found, its balance will be types.ZeroCurrency.
func (ec *EphemeralContractor) AccountBalances(accounts []proto4.Account) ([]types.Currency, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	balances := make([]types.Currency, 0, len(accounts))
	for _, account := range accounts {
		balances = append(balances, ec.accounts[account])
	}
	return balances, nil
}

// CreditAccountsWithContract credits accounts with the given deposits and
// revises the contract revision. The contract must be locked before calling
// this method.
func (ec *EphemeralContractor) CreditAccountsWithContract(deposits []proto4.AccountDeposit, contractID types.FileContractID, revision types.V2FileContract, _ proto4.Usage) ([]types.Currency, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	existing, ok := ec.contracts[contractID]
	if !ok {
		return nil, errors.New("contract not found")
	} else if revision.RevisionNumber <= existing.RevisionNumber {
		return nil, errors.New("revision number must be greater than existing")
	}

	sigHash := consensus.State{}.ContractSigHash(revision)
	if !existing.RenterPublicKey.VerifyHash(sigHash, revision.RenterSignature) {
		return nil, errors.New("invalid renter signature")
	} else if !existing.HostPublicKey.VerifyHash(sigHash, revision.HostSignature) {
		return nil, errors.New("invalid host signature")
	}

	var balance = make([]types.Currency, 0, len(deposits))
	for _, deposit := range deposits {
		ec.accounts[deposit.Account] = ec.accounts[deposit.Account].Add(deposit.Amount)
		balance = append(balance, ec.accounts[deposit.Account])
	}

	ec.contracts[contractID] = revision
	return balance, nil
}

// DebitAccount debits an account by the given amount.
func (ec *EphemeralContractor) DebitAccount(account proto4.Account, usage proto4.Usage) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	balance, ok := ec.accounts[account]
	if !ok || balance.Cmp(usage.RenterCost()) < 0 {
		return proto4.ErrNotEnoughFunds
	}
	ec.accounts[account] = balance.Sub(usage.RenterCost())
	return nil
}

// UpdateChainState updates the EphemeralContractor's state based on the
// reverted and applied chain updates.
func (ec *EphemeralContractor) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	for _, cru := range reverted {
		for _, fced := range cru.V2FileContractElementDiffs() {
			if _, ok := ec.contracts[fced.V2FileContractElement.ID]; !ok {
				continue
			}
			switch {
			case fced.Created:
				delete(ec.contractElements, fced.V2FileContractElement.ID)
			case fced.Resolution != nil:
				ec.contractElements[fced.V2FileContractElement.ID] = fced.V2FileContractElement.Copy()
			case fced.Revision != nil:
				fced.V2FileContractElement.V2FileContract = *fced.Revision
				ec.contractElements[fced.V2FileContractElement.ID] = fced.V2FileContractElement.Copy()
			}
		}

		for id, fce := range ec.contractElements {
			cru.UpdateElementProof(&fce.StateElement)
			ec.contractElements[id] = fce.Move()
		}
		ec.tip = cru.State.Index
	}
	for _, cau := range applied {
		for _, fced := range cau.V2FileContractElementDiffs() {
			if _, ok := ec.contracts[fced.V2FileContractElement.ID]; !ok {
				continue
			}
			switch {
			case fced.Created:
				ec.contractElements[fced.V2FileContractElement.ID] = fced.V2FileContractElement.Copy()
			case fced.Resolution != nil:
				delete(ec.contractElements, fced.V2FileContractElement.ID)
			case fced.Revision != nil:
				fced.V2FileContractElement.V2FileContract = *fced.Revision
				ec.contractElements[fced.V2FileContractElement.ID] = fced.V2FileContractElement.Copy()
			}
		}

		for id, fce := range ec.contractElements {
			cau.UpdateElementProof(&fce.StateElement)
			ec.contractElements[id] = fce.Move()
		}
		ec.tip = cau.State.Index
	}
	return nil
}

// Tip returns the current chain tip.
func (ec *EphemeralContractor) Tip() (types.ChainIndex, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.tip, nil
}

// An EphemeralSettingsReporter is an in-memory minimal rhp4.SettingsReporter
// for testing.
type EphemeralSettingsReporter struct {
	mu       sync.Mutex
	settings proto4.HostSettings
}

var _ rhp4.Settings = (*EphemeralSettingsReporter)(nil)

// RHP4Settings implements the rhp4.SettingsReporter interface.
func (esr *EphemeralSettingsReporter) RHP4Settings() proto4.HostSettings {
	esr.mu.Lock()
	defer esr.mu.Unlock()
	return esr.settings
}

// Update updates the settings reported by the EphemeralSettingsReporter.
func (esr *EphemeralSettingsReporter) Update(settings proto4.HostSettings) {
	esr.mu.Lock()
	defer esr.mu.Unlock()
	esr.settings = settings
}

// ServeSiaMux starts a RHP4 host listening on a random port and returns the address.
func ServeSiaMux(tb testing.TB, s *rhp4.Server, log *zap.Logger, opts ...func(*siamux.ServeOption)) string {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })

	go siamux.Serve(l, s, log, opts...)
	return l.Addr().String()
}

// ServeQUIC starts a RHP4 host listening on a random port and returns the address.
func ServeQUIC(tb testing.TB, s *rhp4.Server, log *zap.Logger) string {
	udpAddr, err := net.ResolveUDPAddr("udp", "localhost:0")
	if err != nil {
		tb.Fatal(err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { conn.Close() })

	l, err := quic.Listen(conn, &EphemeralCertManager{})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })
	go quic.Serve(l, s, log)
	return conn.LocalAddr().String()
}

// NewEphemeralSettingsReporter creates an EphemeralSettingsReporter for testing.
func NewEphemeralSettingsReporter() *EphemeralSettingsReporter {
	return &EphemeralSettingsReporter{}
}

// NewEphemeralSectorStore creates an EphemeralSectorStore for testing.
func NewEphemeralSectorStore() *EphemeralSectorStore {
	return &EphemeralSectorStore{
		sectors: make(map[types.Hash256]*[proto4.SectorSize]byte),
	}
}

// NewEphemeralContractor creates an EphemeralContractor for testing.
func NewEphemeralContractor(cm *chain.Manager) *EphemeralContractor {
	ec := &EphemeralContractor{
		contractElements: make(map[types.FileContractID]types.V2FileContractElement),
		contracts:        make(map[types.FileContractID]types.V2FileContract),
		roots:            make(map[types.FileContractID][]types.Hash256),
		locks:            make(map[types.FileContractID]bool),
		accounts:         make(map[proto4.Account]types.Currency),
		shutdown:         make(chan struct{}),
	}

	reorgCh := make(chan types.ChainIndex, 1)
	cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- index:
		default:
		}
	})

	go func() {
		for {
			select {
			case <-reorgCh:
			case <-ec.shutdown:
				return
			}
			for {
				reverted, applied, err := cm.UpdatesSince(ec.tip, 1000)
				if err != nil {
					panic(err)
				} else if len(reverted) == 0 && len(applied) == 0 {
					break
				}

				if err := ec.UpdateChainState(reverted, applied); err != nil {
					panic(err)
				}
			}
		}
	}()
	return ec
}
