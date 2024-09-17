package testutil

import (
	"crypto/ed25519"
	"errors"
	"net"
	"sync"
	"testing"

	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/mux/v2"
	"go.uber.org/zap"
)

// An EphemeralSectorStore is an in-memory minimal rhp4.SectorStore for testing.
type EphemeralSectorStore struct {
	mu      sync.Mutex
	sectors map[types.Hash256][proto4.SectorSize]byte
}

var _ rhp4.SectorStore = (*EphemeralSectorStore)(nil)

// ReadSector reads a sector from the EphemeralSectorStore.
func (es *EphemeralSectorStore) ReadSector(root types.Hash256) ([proto4.SectorSize]byte, error) {
	es.mu.Lock()
	defer es.mu.Unlock()
	sector, ok := es.sectors[root]
	if !ok {
		return [proto4.SectorSize]byte{}, errors.New("sector not found")
	}
	return sector, nil
}

// WriteSector stores a sector in the EphemeralSectorStore.
func (es *EphemeralSectorStore) WriteSector(root types.Hash256, sector *[proto4.SectorSize]byte, expiration uint64) error {
	es.mu.Lock()
	defer es.mu.Unlock()
	es.sectors[root] = *sector
	return nil
}

// An EphemeralContractor is an in-memory minimal rhp4.Contractor for testing.
type EphemeralContractor struct {
	mu sync.Mutex // protects the fields below

	tip              types.ChainIndex
	contractElements map[types.FileContractID]types.V2FileContractElement
	contracts        map[types.FileContractID]types.V2FileContract
	roots            map[types.FileContractID][]types.Hash256
	locks            map[types.FileContractID]bool

	accounts map[proto4.Account]types.Currency
}

var _ rhp4.Contractor = (*EphemeralContractor)(nil)

// ContractElement returns the contract state element for the given contract ID.
func (ec *EphemeralContractor) ContractElement(contractID types.FileContractID) (types.ChainIndex, types.V2FileContractElement, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	element, ok := ec.contractElements[contractID]
	if !ok {
		return types.ChainIndex{}, types.V2FileContractElement{}, errors.New("contract not found")
	}
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

	var once sync.Once
	return rhp4.RevisionState{
			Revision: rev,
			Roots:    ec.roots[contractID],
		}, func() {
			once.Do(func() {
				ec.mu.Lock()
				defer ec.mu.Unlock()
				ec.locks[contractID] = false
			})
		}, nil
}

// AddV2Contract adds a new contract to the host.
func (ec *EphemeralContractor) AddV2Contract(formationSet rhp4.TransactionSet, _ rhp4.Usage) error {
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
func (ec *EphemeralContractor) RenewV2Contract(renewalSet rhp4.TransactionSet, _ rhp4.Usage) error {
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
	} else if existing.RevisionNumber == types.MaxRevisionNumber {
		return errors.New("contract already at max revision")
	}

	contractID := existingID.V2RenewalID()
	if _, ok := ec.contracts[contractID]; ok {
		return errors.New("contract already exists")
	}

	ec.contracts[existingID] = renewal.FinalRevision
	ec.contracts[contractID] = renewal.NewContract
	ec.roots[contractID] = append([]types.Hash256(nil), ec.roots[existingID]...)
	return nil
}

// ReviseV2Contract atomically revises a contract and updates its sector roots
// and usage.
func (ec *EphemeralContractor) ReviseV2Contract(contractID types.FileContractID, revision types.V2FileContract, roots []types.Hash256, _ rhp4.Usage) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	existing, ok := ec.contracts[contractID]
	if !ok {
		return errors.New("contract not found")
	} else if revision.RevisionNumber <= existing.RevisionNumber {
		return errors.New("revision number must be greater than existing")
	}

	ec.contracts[contractID] = revision
	ec.roots[contractID] = append([]types.Hash256(nil), roots...)
	return nil
}

// AccountBalance returns the balance of an account.
func (ec *EphemeralContractor) AccountBalance(account proto4.Account) (types.Currency, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	balance, ok := ec.accounts[account]
	if !ok {
		return types.Currency{}, errors.New("account not found")
	}
	return balance, nil
}

// CreditAccountsWithContract credits accounts with the given deposits and
// revises the contract revision. The contract must be locked before calling
// this method.
func (ec *EphemeralContractor) CreditAccountsWithContract(deposits []proto4.AccountDeposit, contractID types.FileContractID, revision types.V2FileContract) ([]types.Currency, error) {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	var balance = make([]types.Currency, 0, len(deposits))
	for _, deposit := range deposits {
		ec.accounts[deposit.Account] = ec.accounts[deposit.Account].Add(deposit.Amount)
		balance = append(balance, ec.accounts[deposit.Account])
	}
	ec.contracts[contractID] = revision
	return balance, nil
}

// DebitAccount debits an account by the given amount.
func (ec *EphemeralContractor) DebitAccount(account proto4.Account, amount types.Currency) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	balance, ok := ec.accounts[account]
	if !ok {
		return errors.New("account not found")
	} else if balance.Cmp(amount) < 0 {
		return errors.New("insufficient funds")
	}
	ec.accounts[account] = balance.Sub(amount)
	return nil
}

// UpdateChainState updates the EphemeralContractor's state based on the
// reverted and applied chain updates.
func (ec *EphemeralContractor) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	ec.mu.Lock()
	defer ec.mu.Unlock()

	for _, cru := range reverted {
		cru.ForEachV2FileContractElement(func(fce types.V2FileContractElement, created bool, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
			if _, ok := ec.contracts[types.FileContractID(fce.ID)]; !ok {
				return
			}

			switch {
			case created:
				delete(ec.contractElements, types.FileContractID(fce.ID))
			case res != nil:
				ec.contractElements[types.FileContractID(fce.ID)] = fce
			case rev != nil:
				ec.contractElements[types.FileContractID(fce.ID)] = *rev
			}
		})

		for id, fce := range ec.contractElements {
			cru.UpdateElementProof(&fce.StateElement)
			ec.contractElements[id] = fce
		}
		ec.tip = cru.State.Index
	}
	for _, cau := range applied {
		cau.ForEachV2FileContractElement(func(fce types.V2FileContractElement, created bool, rev *types.V2FileContractElement, res types.V2FileContractResolutionType) {
			if _, ok := ec.contracts[types.FileContractID(fce.ID)]; !ok {
				return
			}

			switch {
			case created:
				ec.contractElements[types.FileContractID(fce.ID)] = fce
			case res != nil:
				delete(ec.contractElements, types.FileContractID(fce.ID))
			case rev != nil:
				ec.contractElements[types.FileContractID(fce.ID)] = *rev
			}
		})

		for id, fce := range ec.contractElements {
			cau.UpdateElementProof(&fce.StateElement)
			ec.contractElements[id] = fce
		}
		ec.tip = cau.State.Index
	}
	return nil
}

// Tip returns the current chain tip.
func (ec *EphemeralContractor) Tip() types.ChainIndex {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return ec.tip
}

// An EphemeralSettingsReporter is an in-memory minimal rhp4.SettingsReporter
// for testing.
type EphemeralSettingsReporter struct {
	mu       sync.Mutex
	settings proto4.HostSettings
}

var _ rhp4.SettingsReporter = (*EphemeralSettingsReporter)(nil)

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

// A muxTransport is a rhp4.Transport that wraps a mux.Mux.
type muxTransport struct {
	m *mux.Mux
}

// Close implements the rhp4.Transport interface.
func (mt *muxTransport) Close() error {
	return mt.m.Close()
}

// AcceptStream implements the rhp4.Transport interface.
func (mt *muxTransport) AcceptStream() (net.Conn, error) {
	return mt.m.AcceptStream()
}

// ServeSiaMux starts a RHP4 host listening on a random port and returns the address.
func ServeSiaMux(tb testing.TB, s *rhp4.Server, log *zap.Logger) string {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			log := log.With(zap.Stringer("peerAddress", conn.RemoteAddr()))

			go func() {
				defer conn.Close()
				m, err := mux.Accept(conn, ed25519.PrivateKey(s.HostKey()))
				if err != nil {
					panic(err)
				}
				s.Serve(&muxTransport{m}, log)
			}()
		}
	}()
	return l.Addr().String()
}

// NewEphemeralSettingsReporter creates an EphemeralSettingsReporter for testing.
func NewEphemeralSettingsReporter() *EphemeralSettingsReporter {
	return &EphemeralSettingsReporter{}
}

// NewEphemeralSectorStore creates an EphemeralSectorStore for testing.
func NewEphemeralSectorStore() *EphemeralSectorStore {
	return &EphemeralSectorStore{
		sectors: make(map[types.Hash256][proto4.SectorSize]byte),
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
	}

	reorgCh := make(chan types.ChainIndex, 1)
	cm.OnReorg(func(index types.ChainIndex) {
		select {
		case reorgCh <- index:
		default:
		}
	})

	go func() {
		for range reorgCh {
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
