package host

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var (
	protocolVersion = [3]byte{0, 0, 1}
)

type (
	// A Transport is a generic multiplexer interface used to handle incoming
	// streams.
	Transport interface {
		AcceptStream() (net.Conn, error)
		Close() error
	}

	// A Server provides a minimal reference implementation an RHP4 host. It
	// should be used only for testing or as a reference for implementing a
	// production host.
	Server struct {
		privKey types.PrivateKey
		config  config
		chain   ChainManager
		sectors *MemSectorStore
	}

	// A ChainManager provides access to the current state of the blockchain.
	ChainManager interface {
		TipState() consensus.State
	}

	// A Wallet funds and signs transactions.
	Wallet interface {
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		ReleaseInputs(txns ...types.Transaction)
	}

	// Settings contains the host's internal settings.
	Settings struct {
		NetAddresses         []rhp.NetAddress `json:"protocols"`
		AcceptingContracts   bool             `json:"acceptingContracts"`
		MaxDuration          uint64           `json:"maxDuration"`
		CollateralMultiplier float64          `json:"collateralMultiplier"`
		ContractPrice        types.Currency   `json:"contractPrice"`
		StoragePrice         types.Currency   `json:"storagePrice"`
		IngressPrice         types.Currency   `json:"ingressPrice"`
		EgressPrice          types.Currency   `json:"egressPrice"`
		MaxCollateral        types.Currency   `json:"maxCollateral"`
		PriceTableValidity   time.Duration    `json:"priceTableValidity"`
	}
)

func (s *Server) handleHostStream(stream net.Conn, log *zap.Logger) {
	defer stream.Close()

	stream.SetDeadline(time.Now().Add(30 * time.Second)) // set an initial timeout
	rpcStart := time.Now()
	id, err := rhp.ReadID(stream)
	if err != nil {
		log.Debug("failed to read RPC ID", zap.Error(err))
		return
	}

	req := rhp.RequestforID(id)
	if err := rhp.ReadRequest(stream, req); err != nil {
		log.Debug("failed to read RPC request", zap.Error(err))
		return
	}

	switch req := req.(type) {
	case *rhp.RPCSettingsRequest:
		err = s.handleRPCSettings(stream, req, log.Named(id.String()))
	case *rhp.RPCReadSectorRequest:
		panic("not implemented")
	case *rhp.RPCWriteSectorRequest:
		panic("not implemented")
	case *rhp.RPCModifySectorsRequest:
		panic("not implemented")
	case *rhp.RPCFundAccountRequest:
		panic("not implemented")
	case *rhp.RPCFormContractRequest:
		panic("not implemented")
	case *rhp.RPCRenewContractRequest:
		panic("not implemented")
	case *rhp.RPCLatestRevisionRequest:
		panic("not implemented")
	case *rhp.RPCSectorRootsRequest:
		panic("not implemented")
	case *rhp.RPCAccountBalanceRequest:
		panic("not implemented")
	default:
		log.Debug("unrecognized RPC", zap.Stringer("rpc", id))
		rhp.WriteResponse(stream, &rhp.RPCError{Code: 0, Description: "unrecognized RPC"})
		return
	}
	if err != nil {
		log.Warn("RPC failed", zap.Error(err), zap.Duration("elapsed", time.Since(rpcStart)))
		return
	}
	log.Info("RPC success", zap.Duration("elapsed", time.Since(rpcStart)))
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

// UpdateSettings updates the host's internal settings.
func (s *Server) UpdateSettings(settings Settings) error {
	s.config.Settings = settings
	return nil
}

// SetMaxSectors sets the maximum number of sectors the host can store.
func (s *Server) SetMaxSectors(n uint64) {
	s.sectors.maxSectors = n
}

// NewServer creates a new reference host server with the given private key and chain
// manager. The server will use the provided options to configure its internal
// settings.
//
// A transport must be set up and then called with the Serve method to start
// an RHP session.
func NewServer(privKey types.PrivateKey, cm ChainManager, opts ...Option) *Server {
	cfg := config{
		Settings: Settings{
			AcceptingContracts:   true,
			MaxDuration:          1000,
			CollateralMultiplier: 2.0,
			ContractPrice:        types.Siacoins(1).Div64(4),
			StoragePrice:         types.Siacoins(1).Div64(4320).Div64(1e12),
			IngressPrice:         types.Siacoins(1).Div64(1e12),
			EgressPrice:          types.Siacoins(1).Div64(1e12),
			MaxCollateral:        types.Siacoins(1000),
			PriceTableValidity:   10 * time.Minute,
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Server{
		privKey: privKey,
		config:  cfg,
		chain:   cm,
		sectors: NewMemSectorStore(100),
	}
}
