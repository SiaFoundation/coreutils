package host

import (
	"net"
	"time"

	"go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

func validatePriceTable(pk types.PrivateKey, pt rhp.HostPrices) error {
	if pt.ValidUntil.Before(time.Now()) {
		return ErrPriceTableExpired
	}
	// sigHash := pt.SigHash()
	// if !pk.VerifyHash(sigHash, pt.Signature) {
	// 	return ErrInvalidSignature
	// }
	return nil
}

func (s *Server) handleRPCSettings(stream net.Conn, req *rhp.RPCSettingsRequest, _ *zap.Logger) error {
	pt := rhp.HostPrices{
		ContractPrice: s.config.Settings.ContractPrice,
		Collateral:    s.config.Settings.StoragePrice.Mul64(uint64(s.config.Settings.CollateralMultiplier * 1000)).Div64(1000),
		StoragePrice:  s.config.Settings.StoragePrice,
		IngressPrice:  s.config.Settings.IngressPrice,
		EgressPrice:   s.config.Settings.EgressPrice,
		TipHeight:     s.chain.TipState().Index.Height,
		ValidUntil:    time.Now().Add(s.config.Settings.PriceTableValidity),
	}

	// sigHash := pt.SigHash()
	// pt.Signature = s.privKey.SignHash(sigHash)

	err := rhp.WriteResponse(stream, &rhp.RPCSettingsResponse{
		Settings: rhp.HostSettings{
			Version:            protocolVersion,
			NetAddresses:       s.config.Settings.NetAddresses,
			AcceptingContracts: s.config.Settings.AcceptingContracts,
			MaxDuration:        s.config.Settings.MaxDuration,
			Prices:             pt,
		},
	})
	return err
}

/*
func (s *Server) handleRPCReadSector(stream net.Conn, rpc *rhp.RPCReadSector, _ *zap.Logger) error {
	if err := validatePriceTable(s.privKey, rpc.Prices); err != nil {
		stream.WriteResponseErr(err)
		return err
	}
	if rpc.Length+rpc.Offset > rhp.SectorSize {
		stream.WriteResponseErr(ErrOffsetOutOfBounds)
		return ErrOffsetOutOfBounds
	}

	sector, ok := s.sectors.Read(rpc.Root)
	if !ok {
		stream.WriteResponseErr(ErrSectorNotFound)
		return fmt.Errorf("failed to read sector: %w", ErrSectorNotFound)
	}

	// TODO: response is missing proof
	rpc.Sector = sector[rpc.Offset : rpc.Offset+rpc.Length]
	if err := stream.WriteResponse(rpc); err != nil {
		return fmt.Errorf("failed to write RPCReadSector: %w", err)
	}
	return nil
}

func (s *Server) handleRPCWriteSector(stream Stream, rpc *rhp.RPCWriteSector, _ *zap.Logger) error {
	if err := validatePriceTable(s.privKey, rpc.Prices); err != nil {
		stream.WriteResponseErr(err)
		return err
	} else if len(rpc.Sector) > rhp.SectorSize {
		stream.WriteResponseErr(ErrSectorTooLarge)
		return ErrSectorTooLarge
	}

	sector := ([rhp.SectorSize]byte)(rpc.Sector)
	// TODO: stream sector root calculation
	rpc.Root = rhp2.SectorRoot(&sector)

	if err := s.sectors.Write(rpc.Root, sector); err != nil {
		stream.WriteResponseErr(err)
		return fmt.Errorf("failed to write sector: %w", err)
	} else if err := stream.WriteResponse(rpc); err != nil {
		return fmt.Errorf("failed to write RPCWriteSector: %w", err)
	}
	return nil
}

func (s *Server) handleRPCModifySectors(stream Stream, rpc *rhp.RPCModifySectors, _ *zap.Logger) error {
	panic("implement me")
}

func (s *Server) handleRPCAccountBalance(stream Stream, rpc *rhp.RPCAccountBalance, _ *zap.Logger) error {
	panic("implement me")
}

func (s *Server) handleRPCFundAccount(stream Stream, rpc *rhp.RPCFundAccount, _ *zap.Logger) error {
	panic("implement me")
}

func (s *Server) handleRPCSectorRoots(stream Stream, rpc *rhp.RPCSectorRoots, _ *zap.Logger) error {
	panic("missing contract payment")
}
*/
