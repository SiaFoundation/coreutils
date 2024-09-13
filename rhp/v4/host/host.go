package host

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var protocolVersion = [3]byte{0, 0, 1}

type (
	// Usage contains the revenue and risked collateral for a contract.
	Usage struct {
		RPCRevenue       types.Currency `json:"rpc"`
		StorageRevenue   types.Currency `json:"storage"`
		EgressRevenue    types.Currency `json:"egress"`
		IngressRevenue   types.Currency `json:"ingress"`
		AccountFunding   types.Currency `json:"accountFunding"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
	}

	// A TransactionSet contains the transaction set and basis for a v2 contract.
	TransactionSet struct {
		TransactionSet []types.V2Transaction
		Basis          types.ChainIndex
	}
)

type (
	// A Transport is a generic multiplexer for incoming streams.
	Transport interface {
		AcceptStream() (net.Conn, error)
		Close() error
	}

	// ChainManager defines the interface required by the contract manager to
	// interact with the consensus set.
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State

		// V2TransactionSet returns the full transaction set and basis necessary for
		// broadcasting a transaction. If the provided basis does not match the current
		// tip, the transaction will be updated. The transaction set includes the parents
		// and the transaction itself in an order valid for broadcasting.
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
		// AddV2PoolTransactions validates a transaction set and adds it to the
		// transaction pool.
		AddV2PoolTransactions(types.ChainIndex, []types.V2Transaction) (known bool, err error)
		// RecommendedFee returns the recommended fee per weight
		RecommendedFee() types.Currency
		// UpdatesSince returns at most max updates on the path between index and the
		// Manager's current tip.
		UpdatesSince(index types.ChainIndex, maxBlocks int) (rus []chain.RevertUpdate, aus []chain.ApplyUpdate, err error)
	}

	// A Syncer broadcasts transactions to its peers
	Syncer interface {
		// BroadcastV2TransactionSet broadcasts a transaction set to the network.
		BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction)
	}

	// A Wallet manages Siacoins and funds transactions
	Wallet interface {
		// Address returns the host's address
		Address() types.Address

		// FundTransaction funds a transaction with the specified amount of
		// Siacoins. If useUnconfirmed is true, the transaction may spend
		// unconfirmed outputs. The outputs spent by the transaction are locked
		// until they are released by ReleaseInputs.
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency, useUnconfirmed bool) (types.ChainIndex, []int, error)
		// SignV2Inputs signs the inputs of a transaction.
		SignV2Inputs(txn *types.V2Transaction, toSign []int)
		// ReleaseInputs releases the inputs of a transaction. It should only
		// be used if the transaction is not going to be broadcast
		ReleaseInputs(txns []types.Transaction, v2txns []types.V2Transaction)
	}

	// A SectorStore is an interface for reading and writing sectors
	SectorStore interface {
		ReadSector(types.Hash256) ([rhp4.SectorSize]byte, error)
		// StoreSector stores a sector and returns its root hash.
		StoreSector(root types.Hash256, data *[rhp4.SectorSize]byte, expiration uint64) error
	}

	// A RevisionState pairs a contract revision with its sector roots
	RevisionState struct {
		Revision types.V2FileContract
		Roots    []types.Hash256
	}

	// Contractor is an interface for managing a host's contracts
	Contractor interface {
		// LockV2Contract locks a contract and returns its current state.
		// The returned function must be called to release the lock.
		LockV2Contract(types.FileContractID) (RevisionState, func(), error)
		// AddV2Contract adds a new contract to the host
		AddV2Contract(TransactionSet, Usage) error
		// RenewV2Contract finalizes an existing contract and adds its renewal
		RenewV2Contract(TransactionSet, Usage) error
		// ReviseV2Contract atomically revises a contract and updates its sector
		// roots and usage
		ReviseV2Contract(contractID types.FileContractID, revision types.V2FileContract, roots []types.Hash256, usage Usage) error
		// ContractElement returns the contract state element for the given
		// contract ID
		ContractElement(types.FileContractID) (types.ChainIndex, types.V2FileContractElement, error)

		AccountBalance(rhp4.Account) (types.Currency, error)
		CreditAccountsWithContract([]rhp4.AccountDeposit, types.FileContractID, types.V2FileContract) ([]types.Currency, error)
		DebitAccount(rhp4.Account, types.Currency) error
	}

	// SettingsReporter reports the host's current settings
	SettingsReporter interface {
		RHP4Settings() rhp4.HostSettings
	}

	// A Server handles incoming RHP4 RPC
	Server struct {
		hostKey                   types.PrivateKey
		priceTableValidity        time.Duration
		contractProofWindowBuffer uint64

		log *zap.Logger

		chain      ChainManager
		syncer     Syncer
		wallet     Wallet
		sectors    SectorStore
		contractor Contractor
		settings   SettingsReporter
	}
)

func (s *Server) lockContractForRevision(contractID types.FileContractID) (RevisionState, func(), error) {
	rev, unlock, err := s.contractor.LockV2Contract(contractID)
	switch {
	case err != nil:
		return RevisionState{}, nil, err
	case rev.Revision.ProofHeight-s.contractProofWindowBuffer <= s.chain.Tip().Height:
		unlock()
		return RevisionState{}, nil, errorBadRequest("contract too close to proof window")
	case rev.Revision.RevisionNumber >= types.MaxRevisionNumber:
		unlock()
		return RevisionState{}, nil, errorBadRequest("contract is locked for revision")
	}
	return rev, unlock, nil
}

func (s *Server) handleHostStream(stream net.Conn, log *zap.Logger) {
	defer stream.Close()

	stream.SetDeadline(time.Now().Add(30 * time.Second)) // set an initial timeout
	rpcStart := time.Now()
	id, err := rhp4.ReadID(stream)
	if err != nil {
		log.Debug("failed to read RPC ID", zap.Error(err))
		return
	}

	switch id {
	case rhp4.RPCSettingsID:
		err = s.handleRPCSettings(stream)
	case rhp4.RPCAccountBalanceID:
		err = s.handleRPCAccountBalance(stream)
	case rhp4.RPCFormContractID:
		err = s.handleRPCFormContract(stream)
	case rhp4.RPCFundAccountsID:
		err = s.handleRPCFundAccounts(stream)
	case rhp4.RPCLatestRevisionID:
		err = s.handleRPCLatestRevision(stream)
	case rhp4.RPCModifySectorsID:
		err = s.handleRPCModifySectors(stream)
	case rhp4.RPCReadSectorID:
		err = s.handleRPCReadSector(stream)
	case rhp4.RPCRenewContractID:
		err = s.handleRPCRenewContract(stream)
	case rhp4.RPCSectorRootsID:
		err = s.handleRPCSectorRoots(stream)
	case rhp4.RPCWriteSectorID:
		err = s.handleRPCWriteSector(stream)
	default:
		log.Debug("unrecognized RPC", zap.Stringer("rpc", id))
		rhp4.WriteResponse(stream, &rhp4.RPCError{Code: rhp4.ErrorCodeBadRequest, Description: "unrecognized RPC"})
		return
	}
	if err != nil {
		var re *rhp4.RPCError
		if ok := errors.As(err, &re); ok {
			rhp4.WriteResponse(stream, re)
			log.Debug("RPC failed", zap.Error(err), zap.Duration("elapsed", time.Since(rpcStart)))
		} else {
			rhp4.WriteResponse(stream, rhp4.ErrHostInternalError.(*rhp4.RPCError))
			log.Error("RPC failed", zap.Error(err), zap.Duration("elapsed", time.Since(rpcStart)))
		}
		return
	}
	log.Info("RPC success", zap.Duration("elapsed", time.Since(rpcStart)))
}

// HostKey returns the host's private key
func (s *Server) HostKey() types.PrivateKey {
	return s.hostKey
}

// Serve accepts incoming streams on the provided multiplexer and handles them
func (s *Server) Serve(t Transport, log *zap.Logger) error {
	defer t.Close()

	for {
		stream, err := t.AcceptStream()
		if errors.Is(err, net.ErrClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}
		log := log.With(zap.String("streamID", hex.EncodeToString(frand.Bytes(4))))
		log.Debug("accepted stream")
		go func() {
			defer func() {
				if err := stream.Close(); err != nil {
					log.Debug("failed to close stream", zap.Error(err))
				} else {
					log.Debug("closed stream")
				}
			}()
			s.handleHostStream(stream, log)
		}()
	}
}

// NewServer creates a new RHP4 server
func NewServer(pk types.PrivateKey, cm ChainManager, syncer Syncer, contracts Contractor, wallet Wallet, settings SettingsReporter, sectors SectorStore, opts ...ServerOption) *Server {
	s := &Server{
		hostKey:                   pk,
		priceTableValidity:        30 * time.Minute,
		contractProofWindowBuffer: 10,

		log: zap.NewNop(),

		chain:      cm,
		syncer:     syncer,
		wallet:     wallet,
		sectors:    sectors,
		contractor: contracts,
		settings:   settings,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}
