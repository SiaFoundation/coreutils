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

const (
	protocolVersion = "reference 0.0.1"
)

type (
	// A Stream is a multiplexed stream
	Stream interface {
		ReadID() (types.Specifier, error)
		ReadRequest(r rhp.RPC) error
		WriteResponse(r rhp.RPC) error
		WriteResponseErr(err error) error

		Close() error
		SetDeadline(time.Time)
	}

	// A Transport is a generic multiplexer interface used to handle incoming
	// streams.
	Transport interface {
		Address() string
		AcceptStream() (Stream, error)
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
		TipState() (consensus.State, error)
	}

	// A Wallet funds and signs transactions.
	Wallet interface {
		FundTransaction(txn *types.Transaction, amount types.Currency, useUnconfirmed bool) ([]types.Hash256, error)
		SignTransaction(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
		ReleaseInputs(txns ...types.Transaction)
	}

	// Settings contains the host's internal settings.
	Settings struct {
		Protocols            []rhp.Protocol `json:"protocols"`
		AcceptingContracts   bool           `json:"acceptingContracts"`
		MaxDuration          uint64         `json:"maxDuration"`
		CollateralMultiplier float64        `json:"collateralMultiplier"`
		ContractPrice        types.Currency `json:"contractPrice"`
		StoragePrice         types.Currency `json:"storagePrice"`
		IngressPrice         types.Currency `json:"ingressPrice"`
		EgressPrice          types.Currency `json:"egressPrice"`
		MaxCollateral        types.Currency `json:"maxCollateral"`
		PriceTableValidity   time.Duration  `json:"priceTableValidity"`
	}
)

func (s *Server) handleHostStream(stream Stream, log *zap.Logger) {
	defer stream.Close()

	stream.SetDeadline(time.Now().Add(30 * time.Second)) // set an initial timeout
	id, err := stream.ReadID()
	if err != nil {
		log.Debug("failed to read RPC ID", zap.Error(err))
		return
	}
	rpcStart := time.Now()
	rpc := rhp.RPCforID(id)
	switch rpc := rpc.(type) {
	case *rhp.RPCSettings:
		err = s.handleRPCSettings(stream, rpc, log.Named(id.String()))
	case *rhp.RPCFormContract:
		panic("not implemented")
	case *rhp.RPCSignatures:
		panic("not implemented")
	case *rhp.RPCReviseContract:
		panic("not implemented")
	case *rhp.RPCRenewContract:
		panic("not implemented")
	case *rhp.RPCLatestRevision:
		panic("not implemented")
	case *rhp.RPCReadSector:
		panic("not implemented")
	case *rhp.RPCWriteSector:
		panic("not implemented")
	case *rhp.RPCModifySectors:
		panic("not implemented")
	case *rhp.RPCSectorRoots:
		panic("not implemented")
	case *rhp.RPCAccountBalance:
		panic("not implemented")
	case *rhp.RPCFundAccount:
		panic("not implemented")
	default:
		log.Debug("unrecognized RPC", zap.Stringer("rpc", id))
		stream.WriteResponseErr(fmt.Errorf("unrecognized RPC ID %q", id))
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
	for {
		stream, err := t.AcceptStream()
		if errors.Is(err, net.ErrClosed) {
			return nil
		} else if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		log.Debug("stream accepted")

		go func() {
			defer func() {
				log.Debug("closing stream")
				stream.Close()
			}()

			log := log.With(zap.String("streamID", hex.EncodeToString(frand.Bytes(4))))
			go s.handleHostStream(stream, log)
		}()
	}
}

func (s *Server) UpdateSettings(settings Settings) error {
	s.config.Settings = settings
	return nil
}

func (s *Server) SetMaxSectors(n uint64) {
	s.sectors.maxSectors = n
}

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
		},
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Server{
		privKey: privKey,
		config:  cfg,

		sectors: NewMemSectorStore(100),
	}
}
