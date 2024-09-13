package host

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
)

const maxBasisDiff = 18

func errorBadRequest(f string, p ...any) error {
	return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, fmt.Sprintf(f, p...))
}

func errorDecodingError(f string, p ...any) error {
	return rhp4.NewRPCError(rhp4.ErrorCodeDecoding, fmt.Sprintf(f, p...))
}

func (s *Server) handleRPCSettings(stream net.Conn) error {
	settings := s.settings.RHP4Settings()
	settings.ProtocolVersion = protocolVersion
	settings.Prices.ValidUntil = time.Now().Add(s.priceTableValidity)
	sigHash := settings.Prices.SigHash()
	settings.Prices.Signature = s.hostKey.SignHash(sigHash)

	return rhp4.WriteResponse(stream, &rhp4.RPCSettingsResponse{
		Settings: settings,
	})
}

func (s *Server) handleRPCReadSector(stream net.Conn) error {
	var req rhp4.RPCReadSectorRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	prices, token := req.Prices, req.Token
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return fmt.Errorf("price table invalid: %w", err)
	} else if err := token.Validate(); err != nil {
		return fmt.Errorf("token invalid: %w", err)
	}

	switch {
	case req.Length%rhp4.LeafSize != 0:
		return errorBadRequest("requested length must be a multiple of segment size %v", rhp4.LeafSize)
	case req.Offset+req.Length > rhp4.SectorSize:
		return errorBadRequest("requested offset %v and length %v exceed sector size %v", req.Offset, req.Length, rhp4.SectorSize)
	}

	if err := s.contractor.DebitAccount(req.Token.Account, prices.RPCReadSectorCost(req.Length)); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	sector, err := s.sectors.ReadSector(req.Root)
	if err != nil {
		return fmt.Errorf("failed to read sector: %w", err)
	}

	segment := sector[req.Offset : req.Offset+req.Length]

	return rhp4.WriteResponse(stream, &rhp4.RPCReadSectorResponse{
		Sector: segment,
		Proof:  nil, // TODO implement proof
	})
}

func (s *Server) handleRPCWriteSector(stream net.Conn) error {
	elapsed := func(context string) func() {
		start := time.Now()
		return func() {
			s.log.Debug("RPCWriteSector", zap.String("context", context), zap.Duration("duration", time.Since(start)))
		}
	}
	log := elapsed("write sector streaming")
	var req rhp4.RPCWriteSectorStreamingRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}
	log()

	log = elapsed("validate prices")
	prices, token := req.Prices, req.Token
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return fmt.Errorf("price table invalid: %w", err)
	}
	log()

	log = elapsed("validate token")
	if err := token.Validate(); err != nil {
		return fmt.Errorf("token invalid: %w", err)
	}
	log()

	settings := s.settings.RHP4Settings()

	log = elapsed("validate request")
	switch {
	case req.DataLength > rhp4.SectorSize:
		return errorBadRequest("sector size %v exceeds maximum %v", req.DataLength, rhp4.SectorSize)
	case req.DataLength%rhp4.LeafSize != 0:
		return errorBadRequest("sector length %v must be a multiple of segment size %v", req.DataLength, rhp4.LeafSize)
	case req.Duration > settings.MaxSectorDuration:
		return errorBadRequest("sector duration %v exceeds maximum %v", req.Duration, settings.MaxSectorDuration)
	}
	log()

	log = elapsed("debit account")
	if err := s.contractor.DebitAccount(req.Token.Account, prices.RPCWriteSectorCost(uint64(req.DataLength), req.Duration)); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}
	log()

	log = elapsed("read sector")
	var sector [rhp4.SectorSize]byte
	if _, err := io.ReadFull(stream, sector[:req.DataLength]); err != nil {
		return errorDecodingError("failed to read sector data: %v", err)
	}
	log()

	log = elapsed("calculate root")
	root := rhp4.SectorRoot(&sector)
	log()

	log = elapsed("store sector")
	if err := s.sectors.StoreSector(root, &sector, req.Duration); err != nil {
		return fmt.Errorf("failed to store sector: %w", err)
	}
	log()
	return rhp4.WriteResponse(stream, &rhp4.RPCWriteSectorResponse{
		Root: root,
	})
}

func (s *Server) handleRPCModifySectors(stream net.Conn) error {
	var req rhp4.RPCModifySectorsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	prices := req.Prices
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return fmt.Errorf("price table invalid: %w", err)
	}
	settings := s.settings.RHP4Settings()

	if err := rhp4.ValidateModifyActions(req.Actions, settings.MaxModifyActions); err != nil {
		return fmt.Errorf("modify actions invalid: %w", err)
	}

	cs := s.chain.TipState()

	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer unlock()

	if !req.ValidChallengeSignature(state.Revision) {
		return errorBadRequest("invalid challenge signature")
	}

	fc := state.Revision
	duration := fc.ExpirationHeight - prices.TipHeight
	cost, collateral := prices.RPCModifySectorsCost(req.Actions, duration)

	// validate the payment without modifying the contract
	if fc.RenterOutput.Value.Cmp(cost) < 0 {
		return rhp4.NewRPCError(rhp4.ErrorCodePayment, fmt.Sprintf("renter output value %v is less than cost %v", fc.RenterOutput.Value, cost))
	} else if fc.MissedHostValue.Cmp(collateral) < 0 {
		return rhp4.NewRPCError(rhp4.ErrorCodePayment, fmt.Sprintf("missed host value %v is less than collateral %v", fc.MissedHostValue, collateral))
	}

	roots := state.Roots
	for _, action := range req.Actions {
		switch action.Type {
		case rhp4.ActionAppend:
			roots = append(roots, action.Root)
		case rhp4.ActionTrim:
			if action.N > uint64(len(roots)) {
				return errorBadRequest("trim count %v exceeds sector count %v", action.N, len(roots))
			}
			roots = roots[:len(roots)-int(action.N)]
		case rhp4.ActionUpdate:
			if action.A >= uint64(len(roots)) {
				return errorBadRequest("update index %v exceeds sector count %v", action.A, len(roots))
			}
			roots[action.A] = action.Root
		case rhp4.ActionSwap:
			if action.A >= uint64(len(roots)) || action.B >= uint64(len(roots)) {
				return errorBadRequest("swap indices %v and %v exceed sector count %v", action.A, action.B, len(roots))
			}
			roots[action.A], roots[action.B] = roots[action.B], roots[action.A]
		default:
			return errorBadRequest("unknown action type %v", action.Type)
		}
	}

	resp := rhp4.RPCModifySectorsResponse{
		Proof: []types.Hash256{rhp4.MetaRoot(roots)}, // TODO implement proof
	}
	if err := rhp4.WriteResponse(stream, &resp); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	var renterSigResponse rhp4.RPCModifySectorsSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResponse); err != nil {
		return errorDecodingError("failed to read renter signature response: %v", err)
	}

	// revise contract
	revision, err := rhp4.ReviseForModifySectors(fc, req, resp)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	sigHash := cs.ContractSigHash(revision)

	// validate the renter signature
	if !fc.RenterPublicKey.VerifyHash(sigHash, renterSigResponse.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}
	revision.RenterSignature = renterSigResponse.RenterSignature
	// sign the revision
	revision.HostSignature = s.hostKey.SignHash(sigHash)

	err = s.contractor.ReviseV2Contract(req.ContractID, revision, roots, Usage{
		StorageRevenue: cost,
	})
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	return rhp4.WriteResponse(stream, &rhp4.RPCModifySectorsThirdResponse{
		HostSignature: revision.HostSignature,
	})
}

func (s *Server) handleRPCFundAccounts(stream net.Conn) error {
	var req rhp4.RPCFundAccountsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer unlock()

	var totalDeposits types.Currency
	for _, deposit := range req.Deposits {
		totalDeposits = totalDeposits.Add(deposit.Amount)
	}

	fc := state.Revision
	fc.RevisionNumber++
	if err := rhp4.PayWithContract(&fc, totalDeposits, types.ZeroCurrency); err != nil {
		return fmt.Errorf("failed to pay with contract: %w", err)
	}

	sigHash := s.chain.TipState().ContractSigHash(fc)
	if !fc.RenterPublicKey.VerifyHash(sigHash, req.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}

	fc.HostSignature = s.hostKey.SignHash(sigHash)

	balances, err := s.contractor.CreditAccountsWithContract(req.Deposits, req.ContractID, fc)
	if err != nil {
		return fmt.Errorf("failed to credit account: %w", err)
	}

	return rhp4.WriteResponse(stream, &rhp4.RPCFundAccountsResponse{
		Balances:      balances,
		HostSignature: fc.HostSignature,
	})
}

func (s *Server) handleRPCLatestRevision(stream net.Conn) error {
	var req rhp4.RPCLatestRevisionRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	state, unlock, err := s.contractor.LockV2Contract(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer unlock()

	return rhp4.WriteResponse(stream, &rhp4.RPCLatestRevisionResponse{
		Contract: state.Revision,
	})
}

func (s *Server) handleRPCSectorRoots(stream net.Conn) error {
	var req rhp4.RPCSectorRootsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	prices := req.Prices
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return fmt.Errorf("price table invalid: %w", err)
	}

	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer unlock()

	// update the revision
	revision, err := rhp4.ReviseForSectorRoots(state.Revision, prices, req.Length)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	cs := s.chain.TipState()
	sigHash := cs.ContractSigHash(revision)

	// validate the request
	switch {
	case req.Offset+req.Length > uint64(len(state.Roots)):
		return errorBadRequest("requested offset %v and length %v exceed sector count %v", req.Offset, req.Length, len(state.Roots))
	case !revision.RenterPublicKey.VerifyHash(sigHash, req.RenterSignature):
		return rhp4.ErrInvalidSignature
	}

	// sign the revision
	revision.HostSignature = s.hostKey.SignHash(sigHash)

	// update the contract
	err = s.contractor.ReviseV2Contract(req.ContractID, revision, state.Roots, Usage{
		EgressRevenue: prices.RPCSectorRootsCost(req.Length),
	})
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}

	// send the response
	return rhp4.WriteResponse(stream, &rhp4.RPCSectorRootsResponse{
		Proof:         nil, // TODO: proof
		Roots:         state.Roots,
		HostSignature: revision.HostSignature,
	})
}

func (s *Server) handleRPCAccountBalance(stream net.Conn) error {
	var req rhp4.RPCAccountBalanceRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	balance, err := s.contractor.AccountBalance(req.Account)
	if err != nil {
		return fmt.Errorf("failed to get account balance: %w", err)
	}

	return rhp4.WriteResponse(stream, &rhp4.RPCAccountBalanceResponse{
		Balance: balance,
	})
}

func (s *Server) handleRPCFormContract(stream net.Conn) error {
	var req rhp4.RPCFormContractRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	prices := req.Prices
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return err
	}

	fc := req.Contract

	// validate contract
	settings := s.settings.RHP4Settings()
	minHostRevenue := prices.ContractPrice.Add(fc.TotalCollateral)
	switch {
	case fc.Filesize != 0:
		return errorBadRequest("filesize must be 0")
	case fc.FileMerkleRoot != (types.Hash256{}):
		return errorBadRequest("file merkle root must be empty")
	case fc.ProofHeight < req.Prices.TipHeight+s.contractProofWindowBuffer:
		return errorBadRequest("proof height %v is too low", fc.ProofHeight)
	case fc.ExpirationHeight < fc.ProofHeight:
		return errorBadRequest("expiration height %v is before proof height %v", fc.ExpirationHeight, fc.ProofHeight)
	case fc.ExpirationHeight-req.Prices.TipHeight > settings.MaxContractDuration:
		return errorBadRequest("duration %v exceeds maximum %v", fc.ExpirationHeight-req.Prices.TipHeight, settings.MaxContractDuration)
	case fc.TotalCollateral.Cmp(settings.MaxCollateral) > 0:
		return errorBadRequest("total collateral %v exceeds maximum %v", fc.TotalCollateral, settings.MaxCollateral)
	case !fc.MissedHostValue.Equals(fc.TotalCollateral):
		return errorBadRequest("missed host value %v must equal total collateral %v", fc.MissedHostValue, fc.HostOutput.Value)
	case fc.HostOutput.Value.Cmp(minHostRevenue) < 0:
		return errorBadRequest("host output value %v must be greater than or equal to expected %v", fc.HostOutput.Value, minHostRevenue)
	}

	formationRevenue := fc.HostOutput.Value.Sub(fc.TotalCollateral)
	formationTxn := types.V2Transaction{
		MinerFee:      req.MinerFee,
		FileContracts: []types.V2FileContract{fc},
	}

	// calculate the renter inputs
	var renterInputs types.Currency
	for _, sce := range req.RenterInputs {
		formationTxn.SiacoinInputs = append(formationTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: sce,
		})
		renterInputs = renterInputs.Add(sce.SiacoinOutput.Value)
	}

	// calculate the required funding
	cs := s.chain.TipState()
	requiredRenterFunding := rhp4.ContractRenterCost(cs, prices, fc, formationTxn.MinerFee)
	// validate the renter added enough inputs
	if n := renterInputs.Cmp(requiredRenterFunding); n < 0 {
		return errorBadRequest("renter funding %v is less than required funding %v", renterInputs, requiredRenterFunding)
	} else if n > 0 {
		// if the renter added too much, add a change output
		formationTxn.SiacoinOutputs = append(formationTxn.SiacoinOutputs, types.SiacoinOutput{
			Address: fc.RenterOutput.Address,
			Value:   renterInputs.Sub(requiredRenterFunding),
		})
	}

	// validate the renter's contract signature
	sigHash := cs.ContractSigHash(fc)
	if !fc.RenterPublicKey.VerifyHash(sigHash, fc.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}

	// fund the host collateral
	basis, toSign, err := s.wallet.FundV2Transaction(&formationTxn, fc.TotalCollateral, true)
	if errors.Is(err, wallet.ErrNotEnoughFunds) {
		return rhp4.ErrHostFundError
	} else if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	// sign the transaction inputs
	s.wallet.SignV2Inputs(&formationTxn, toSign)

	// send the host inputs to the renter
	hostInputsResp := rhp4.RPCFormContractResponse{
		HostInputs: formationTxn.SiacoinInputs[len(req.RenterInputs):],
	}
	if err := rhp4.WriteResponse(stream, &hostInputsResp); err != nil {
		return fmt.Errorf("failed to send host inputs: %w", err)
	}

	// read the renter's signatures
	var renterSigResp rhp4.RPCFormContractSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResp); err != nil {
		return errorDecodingError("failed to read renter signatures: %v", err)
	} else if len(renterSigResp.RenterSatisfiedPolicies) != len(req.RenterInputs) {
		return errorBadRequest("expected %v satisfied policies, got %v", len(req.RenterInputs), len(renterSigResp.RenterSatisfiedPolicies))
	}

	for i := range formationTxn.SiacoinInputs[:len(req.RenterInputs)] {
		formationTxn.SiacoinInputs[i].SatisfiedPolicy = renterSigResp.RenterSatisfiedPolicies[i]
	}

	// add our signature to the contract
	formationTxn.FileContracts[0].HostSignature = s.hostKey.SignHash(sigHash)

	if basis != req.Basis {
		// update renter input basis to reflect our funding basis
		if err := updateSiacoinElementBasis(s.chain, req.Basis, basis, formationTxn.SiacoinInputs[:len(req.RenterInputs)]); err != nil {
			return errorBadRequest("failed to update renter inputs from %q to %q: %v", req.Basis, basis, err)
		}
	}

	// add the renter's parents to our transaction pool to ensure they are valid
	// and update the proofs.
	if len(req.RenterParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(req.Basis, req.RenterParents); err != nil {
			return errorBadRequest("failed to add formation parents to transaction pool: %v", err)
		}
	}

	// get the full updated transaction set
	basis, formationSet, err := s.chain.V2TransactionSet(basis, formationTxn)
	if err != nil {
		return fmt.Errorf("failed to get transaction set: %w", err)
	} else if _, err = s.chain.AddV2PoolTransactions(basis, formationSet); err != nil {
		return errorBadRequest("failed to broadcast formation transaction: %v", err)
	}
	s.syncer.BroadcastV2TransactionSet(basis, formationSet)

	// add the contract to the contractor
	err = s.contractor.AddV2Contract(TransactionSet{
		TransactionSet: formationSet,
		Basis:          basis,
	}, Usage{
		RPCRevenue: formationRevenue,
	})
	if err != nil {
		return fmt.Errorf("failed to add contract: %w", err)
	}

	// send the finalized transaction set to the renter
	return rhp4.WriteResponse(stream, &rhp4.RPCFormContractThirdResponse{
		Basis:          basis,
		TransactionSet: formationSet,
	})
}

func (s *Server) handleRPCRenewContract(stream net.Conn) error {
	var req rhp4.RPCRenewContractRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	// validate prices
	prices := req.Prices
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return fmt.Errorf("price table invalid: %w", err)
	}

	// lock the contract
	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract %q: %w", req.ContractID, err)
	}
	defer unlock()

	// validate challenge signature
	if !req.ValidChallengeSignature(state.Revision) {
		return errorBadRequest("invalid challenge signature")
	}

	renewal := req.Renewal
	existing := state.Revision

	// validate the final revision
	settings := s.settings.RHP4Settings()
	switch {
	case renewal.FinalRevision.RevisionNumber != types.MaxRevisionNumber:
		return errorBadRequest("expected max revision number, got %v", renewal.FinalRevision.RevisionNumber)
	case renewal.FinalRevision.Filesize != 0:
		return errorBadRequest("expected filesize 0, got %v", renewal.FinalRevision.Filesize)
	case renewal.FinalRevision.FileMerkleRoot != (types.Hash256{}):
		return errorBadRequest("expected empty file merkle root, got %v", renewal.FinalRevision.FileMerkleRoot)
	case renewal.FinalRevision.HostOutput.Address != existing.HostOutput.Address:
		return errorBadRequest("host output address must not change")
	case !renewal.FinalRevision.HostOutput.Value.Equals(existing.HostOutput.Value):
		return errorBadRequest("host output value must not change")
	case !renewal.FinalRevision.MissedHostValue.Equals(existing.MissedHostValue):
		return errorBadRequest("missed host value must not")
	case renewal.FinalRevision.RenterPublicKey != existing.RenterPublicKey:
		return errorBadRequest("renter public key must not change")
	case renewal.FinalRevision.HostPublicKey != existing.HostPublicKey:
		return errorBadRequest("host public key must not change")
	}

	// validate the new contract
	switch {
	case renewal.NewContract.ProofHeight < req.Prices.TipHeight+s.contractProofWindowBuffer:
		return errorBadRequest("proof height %v is too low", renewal.NewContract.ProofHeight)
	case renewal.NewContract.ExpirationHeight < renewal.NewContract.ProofHeight:
		return errorBadRequest("expiration height %v is before proof height %v", renewal.NewContract.ExpirationHeight, renewal.NewContract.ProofHeight)
	case renewal.NewContract.ExpirationHeight-req.Prices.TipHeight > settings.MaxContractDuration:
		return errorBadRequest("duration %v exceeds maximum %v", renewal.NewContract.ExpirationHeight-req.Prices.TipHeight, settings.MaxContractDuration)
	case renewal.NewContract.TotalCollateral.Cmp(settings.MaxCollateral) > 0:
		return errorBadRequest("total collateral %v exceeds maximum %v", renewal.NewContract.TotalCollateral, settings.MaxCollateral)
	case renewal.NewContract.ExpirationHeight < state.Revision.ExpirationHeight:
		return errorBadRequest("expiration height %v is before current %v", renewal.NewContract.ExpirationHeight, state.Revision.ExpirationHeight)
	case renewal.NewContract.MissedHostValue.Cmp(renewal.NewContract.HostOutput.Value) > 0:
		return errorBadRequest("missed host value %v must be less than or equal to host output value %v", renewal.NewContract.MissedHostValue, renewal.NewContract.HostOutput.Value)
	case renewal.NewContract.TotalCollateral.Cmp(renewal.NewContract.HostOutput.Value) > 0:
		return errorBadRequest("total collateral %v must be less than or equal to host output value %v", renewal.NewContract.TotalCollateral, renewal.NewContract.HostOutput.Value)
	case renewal.NewContract.RenterPublicKey != existing.RenterPublicKey:
		return errorBadRequest("renter public key must not change")
	case renewal.NewContract.HostPublicKey != existing.HostPublicKey:
		return errorBadRequest("host public key must not change")
	case renewal.NewContract.HostOutput.Address != existing.HostOutput.Address:
		return errorBadRequest("host output address must not change")
	case renewal.NewContract.Filesize != existing.Filesize:
		return errorBadRequest("filesize must match existing contract")
	case renewal.NewContract.FileMerkleRoot != existing.FileMerkleRoot:
		return errorBadRequest("file merkle root must match existing contract")
	case renewal.HostRollover.Cmp(existing.TotalCollateral) > 0:
		return errorBadRequest("host rollover %v must be less than or equal to existing locked collateral %v", renewal.HostRollover, existing.TotalCollateral)
	case renewal.HostRollover.Cmp(renewal.NewContract.TotalCollateral) > 0:
		return errorBadRequest("host rollover %v must be less than or equal to total collateral %v", renewal.HostRollover, renewal.NewContract.TotalCollateral)
	}

	// calculate the contract revenue
	expectedRevenue := prices.ContractPrice
	if renewal.NewContract.ExpirationHeight > existing.ExpirationHeight {
		expectedRevenue = prices.StoragePrice.Mul64(existing.Filesize).Mul64(renewal.NewContract.ExpirationHeight - existing.ExpirationHeight)
	}

	// validate the valid host output
	minHostOutputValue := renewal.NewContract.TotalCollateral.Add(expectedRevenue) // more is fine
	if renewal.NewContract.HostOutput.Value.Cmp(minHostOutputValue) < 0 {
		return errorBadRequest("expected host output value at least %v, got %v", minHostOutputValue, renewal.NewContract.HostOutput.Value)
	}
	usage := Usage{
		RPCRevenue:     prices.ContractPrice,
		StorageRevenue: renewal.NewContract.HostOutput.Value.Sub(renewal.NewContract.TotalCollateral).Sub(prices.ContractPrice),
	}

	// if the missed payout is less than the locked collateral, validate the
	// risked collateral for this renewal.
	if renewal.NewContract.MissedHostValue.Cmp(renewal.NewContract.TotalCollateral) < 0 {
		duration := renewal.NewContract.ExpirationHeight - req.Prices.TipHeight
		maxRiskedCollateral := prices.Collateral.Mul64(existing.Filesize).Mul64(duration)
		riskedCollateral := renewal.NewContract.TotalCollateral.Sub(renewal.NewContract.MissedHostValue)

		if riskedCollateral.Cmp(maxRiskedCollateral) > 0 {
			return errorBadRequest("risked collateral %v exceeds maximum %v", riskedCollateral, maxRiskedCollateral)
		}
		usage.RiskedCollateral = riskedCollateral
	}

	// validate the renter's signature
	cs := s.chain.TipState()
	renewalSigHash := cs.RenewalSigHash(renewal)
	if !existing.RenterPublicKey.VerifyHash(renewalSigHash, renewal.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}

	// create the renewal transaction
	renewalTxn := types.V2Transaction{
		MinerFee: req.MinerFee,
		FileContractResolutions: []types.V2FileContractResolution{
			{
				Resolution: &renewal,
			},
		},
	}
	// calculate the renter funding
	requiredRenterFunding := rhp4.ContractRenterCost(cs, prices, renewal.NewContract, renewalTxn.MinerFee)

	// add the renter inputs
	renterInputSum := renewal.RenterRollover
	for _, si := range req.RenterInputs {
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: si,
		})
		renterInputSum = renterInputSum.Add(si.SiacoinOutput.Value)
	}

	// validate the renter added enough inputs
	if !renterInputSum.Equals(requiredRenterFunding) {
		return errorBadRequest("expected renter to fund %v, got %v", requiredRenterFunding, renterInputSum)
	}

	fceBasis, fce, err := s.contractor.ContractElement(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to get contract element: %w", err)
	}
	renewalTxn.FileContractResolutions[0].Parent = fce

	basis := cs.Index // start with a decent basis and overwrite it if a setup transaction is needed
	var renewalParents []types.V2Transaction
	setupFundAmount := renewal.NewContract.TotalCollateral.Sub(renewal.HostRollover)
	if !setupFundAmount.IsZero() {
		// fund the locked collateral
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: renewal.NewContract.HostOutput.Address, Value: setupFundAmount},
			},
		}

		var toSign []int
		basis, toSign, err = s.wallet.FundV2Transaction(&setupTxn, setupFundAmount, true)
		if errors.Is(err, wallet.ErrNotEnoughFunds) {
			return rhp4.ErrHostFundError
		} else if err != nil {
			return fmt.Errorf("failed to fund transaction: %w", err)
		}
		// sign the transaction inputs
		s.wallet.SignV2Inputs(&setupTxn, toSign)

		basis, renewalParents, err = s.chain.V2TransactionSet(basis, setupTxn)
		if err != nil {
			return fmt.Errorf("failed to get setup transaction set: %w", err)
		}

		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: renewalParents[len(renewalParents)-1].EphemeralSiacoinOutput(0),
		})
		s.wallet.SignV2Inputs(&renewalTxn, []int{len(renewalTxn.SiacoinInputs) - 1})
	}
	// sign the renewal
	renewal.HostSignature = s.hostKey.SignHash(renewalSigHash)

	// update renter input basis to reflect our funding basis
	if basis != req.Basis {
		if err := updateSiacoinElementBasis(s.chain, req.Basis, basis, renewalTxn.SiacoinInputs[:len(req.RenterInputs)]); err != nil {
			return errorBadRequest("failed to update renter inputs from %q to %q: %v", req.Basis, basis, err)
		}
	}
	// update the file contract element to reflect the funding basis
	if fceBasis != basis {
		if err := updateStateElementBasis(s.chain, fceBasis, basis, &fce.StateElement); err != nil {
			return errorBadRequest("failed to update file contract basis: %v", err)
		}
	}

	// send the host inputs to the renter
	hostInputsResp := rhp4.RPCRenewContractResponse{
		HostInputs: renewalTxn.SiacoinInputs[len(req.RenterInputs):],
	}
	if err := rhp4.WriteResponse(stream, &hostInputsResp); err != nil {
		return fmt.Errorf("failed to send host inputs: %w", err)
	}

	// read the renter's signatures
	var renterSigResp rhp4.RPCRenewContractSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResp); err != nil {
		return errorDecodingError("failed to read renter signatures: %v", err)
	} else if len(renterSigResp.RenterSatisfiedPolicies) != len(req.RenterInputs) {
		return errorBadRequest("expected %v satisfied policies, got %v", len(req.RenterInputs), len(renterSigResp.RenterSatisfiedPolicies))
	}
	// apply the renter's signatures
	for i := range renewalTxn.SiacoinInputs[:len(req.RenterInputs)] {
		renewalTxn.SiacoinInputs[i].SatisfiedPolicy = renterSigResp.RenterSatisfiedPolicies[i]
	}

	// add the renter's parents to our transaction pool to ensure they are valid
	// and update the proofs.
	if len(req.RenterParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(req.Basis, req.RenterParents); err != nil {
			return errorBadRequest("failed to add formation parents to transaction pool: %v", err)
		}
	}
	// add our setup parents to the transaction pool
	if len(renewalParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(basis, renewalParents); err != nil {
			return errorBadRequest("failed to add setup parents to transaction pool: %v", err)
		}
	}

	// get the full updated transaction set for the setup transaction
	basis, setupSet, err := s.chain.V2TransactionSet(basis, renewalTxn)
	if err != nil {
		return fmt.Errorf("failed to get transaction set: %w", err)
	} else if _, err = s.chain.AddV2PoolTransactions(basis, setupSet); err != nil {
		return errorBadRequest("failed to broadcast setup transaction: %v", err)
	}

	// get the full updated transaction set for the renewal transaction
	basis, renewalSet, err := s.chain.V2TransactionSet(basis, renewalTxn)
	if err != nil {
		return fmt.Errorf("failed to get transaction set: %w", err)
	} else if _, err = s.chain.AddV2PoolTransactions(basis, renewalSet); err != nil {
		return errorBadRequest("failed to broadcast formation transaction: %v", err)
	}
	s.syncer.BroadcastV2TransactionSet(basis, renewalSet)

	// add the contract to the contractor
	err = s.contractor.RenewV2Contract(TransactionSet{
		TransactionSet: renewalSet,
		Basis:          basis,
	}, usage)
	if err != nil {
		return fmt.Errorf("failed to add contract: %w", err)
	}

	// send the finalized transaction set to the renter
	return rhp4.WriteResponse(stream, &rhp4.RPCRenewContractThirdResponse{
		Basis:          basis,
		TransactionSet: renewalSet,
	})
}

func updateStateElementBasis(cm ChainManager, base, target types.ChainIndex, element *types.StateElement) error {
	reverted, applied, err := cm.UpdatesSince(base, 144)
	if err != nil {
		return err
	}

	if len(reverted)+len(applied) > maxBasisDiff {
		return fmt.Errorf("too many updates between %v and %v", base, target)
	}

	for _, cru := range reverted {
		revertedIndex := types.ChainIndex{
			Height: cru.State.Index.Height + 1,
			ID:     cru.Block.ParentID,
		}
		if revertedIndex == target {
			return nil
		}

		cru.UpdateElementProof(element)
		base = revertedIndex
	}

	for _, cau := range applied {
		if cau.State.Index == target {
			return nil
		}

		modified := make(map[types.Hash256]types.StateElement)
		cau.ForEachSiacoinElement(func(sce types.SiacoinElement, created bool, spent bool) {
			if created {
				modified[sce.ID] = sce.StateElement
			}
		})

		cau.UpdateElementProof(element)
		base = cau.State.Index
	}

	if base != target {
		return fmt.Errorf("failed to update basis to target %v, current %v", target, base)
	}
	return nil
}

// updateSiacoinElementBasis is a helper to update a transaction's siacoin elements
// to the target basis. If an error is returned, inputs must be considered invalid.
func updateSiacoinElementBasis(cm ChainManager, base, target types.ChainIndex, inputs []types.V2SiacoinInput) error {
	reverted, applied, err := cm.UpdatesSince(base, 144)
	if err != nil {
		return err
	}

	if len(reverted)+len(applied) > maxBasisDiff {
		return fmt.Errorf("too many updates between %v and %v", base, target)
	}

	for _, cru := range reverted {
		revertedIndex := types.ChainIndex{
			Height: cru.State.Index.Height + 1,
			ID:     cru.Block.ParentID,
		}
		if revertedIndex == target {
			return nil
		}

		modified := make(map[types.Hash256]types.StateElement)
		cru.ForEachSiacoinElement(func(sce types.SiacoinElement, created bool, spent bool) {
			if created {
				modified[sce.ID] = types.StateElement{
					ID:        sce.ID,
					LeafIndex: types.UnassignedLeafIndex,
				}
			}
		})

		for i := range inputs {
			if se, ok := modified[inputs[i].Parent.ID]; ok {
				inputs[i].Parent.StateElement = se
			}

			if inputs[i].Parent.LeafIndex == types.UnassignedLeafIndex {
				continue
			} else if inputs[i].Parent.LeafIndex >= cru.State.Elements.NumLeaves {
				return fmt.Errorf("siacoin input %v is not in the correct state", inputs[i].Parent.ID)
			}
			cru.UpdateElementProof(&inputs[i].Parent.StateElement)
		}
		base = revertedIndex
	}

	for _, cau := range applied {
		if cau.State.Index == target {
			return nil
		}

		modified := make(map[types.Hash256]types.StateElement)
		cau.ForEachSiacoinElement(func(sce types.SiacoinElement, created bool, spent bool) {
			if created {
				modified[sce.ID] = sce.StateElement
			}
		})

		for i := range inputs {
			if se, ok := modified[inputs[i].Parent.ID]; ok {
				inputs[i].Parent.StateElement = se
			}

			if inputs[i].Parent.LeafIndex == types.UnassignedLeafIndex {
				continue
			} else if inputs[i].Parent.LeafIndex >= cau.State.Elements.NumLeaves {
				return fmt.Errorf("siacoin input %v is not in the correct state", inputs[i].Parent.ID)
			}
			cau.UpdateElementProof(&inputs[i].Parent.StateElement)
		}
		base = cau.State.Index
	}

	if base != target {
		return fmt.Errorf("failed to update basis to target %v, current %v", target, base)
	}
	return nil
}
