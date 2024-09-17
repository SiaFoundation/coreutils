package rhp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// maxBasisDiff is the maximum number of blocks by which a transaction's basis
// can differ from the current tip.
const maxBasisDiff = 20

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
)

type (
	// A TransportListener is a generic multiplexer for incoming streams.
	TransportListener interface {
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
		// WriteSector stores a sector and returns its root hash.
		WriteSector(root types.Hash256, data *[rhp4.SectorSize]byte, expiration uint64) error
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
	case rev.Revision.ProofHeight <= s.chain.Tip().Height+s.contractProofWindowBuffer:
		unlock()
		return RevisionState{}, nil, errorBadRequest("contract too close to proof window")
	case rev.Revision.RevisionNumber >= types.MaxRevisionNumber:
		unlock()
		return RevisionState{}, nil, errorBadRequest("contract is locked for revision")
	}
	return rev, unlock, nil
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
		return errorBadRequest("price table invalid: %v", err)
	} else if err := token.Validate(); err != nil {
		return errorBadRequest("account token invalid: %v", err)
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
	var req rhp4.RPCWriteSectorStreamingRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}
	prices, token := req.Prices, req.Token
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return errorBadRequest("price table invalid: %v", err)
	} else if err := token.Validate(); err != nil {
		return errorBadRequest("account token invalid: %v", err)
	}

	settings := s.settings.RHP4Settings()

	switch {
	case req.DataLength > rhp4.SectorSize:
		return errorBadRequest("sector size %v exceeds maximum %v", req.DataLength, rhp4.SectorSize)
	case req.DataLength%rhp4.LeafSize != 0:
		return errorBadRequest("sector length %v must be a multiple of segment size %v", req.DataLength, rhp4.LeafSize)
	case req.Duration > settings.MaxSectorDuration:
		return errorBadRequest("sector duration %v exceeds maximum %v", req.Duration, settings.MaxSectorDuration)
	case settings.RemainingStorage < rhp4.SectorSize:
		return rhp4.ErrNotEnoughStorage
	}

	// read the sector data
	var sector [rhp4.SectorSize]byte
	if _, err := io.ReadFull(stream, sector[:req.DataLength]); err != nil {
		return errorDecodingError("failed to read sector data: %v", err)
	}

	// calculate the cost
	cost := prices.RPCWriteSectorCost(req.DataLength, req.Duration)
	// debit the account
	if err := s.contractor.DebitAccount(req.Token.Account, cost); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	// store the sector
	root := rhp4.SectorRoot(&sector)
	if err := s.sectors.WriteSector(root, &sector, req.Duration); err != nil {
		return fmt.Errorf("failed to store sector: %w", err)
	}
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
		return errorBadRequest("price table invalid: %v", err)
	}
	settings := s.settings.RHP4Settings()

	if err := rhp4.ValidateModifyActions(req.Actions, settings.MaxModifyActions); err != nil {
		return errorBadRequest("modify actions invalid: %v", err)
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

	// validate the request fields
	if err := req.Validate(state.Revision); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	// update the revision
	revision, err := rhp4.ReviseForSectorRoots(state.Revision, prices, req.Length)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}

	// validate the renter's signature
	cs := s.chain.TipState()
	sigHash := cs.ContractSigHash(revision)
	if !state.Revision.RenterPublicKey.VerifyHash(sigHash, req.RenterSignature) {
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

	ourKey := s.hostKey.PublicKey()
	prices := req.Prices
	if err := prices.Validate(ourKey); err != nil {
		return err
	}

	// get current settings and tip
	settings := s.settings.RHP4Settings()
	// set the prices to match the signed prices
	settings.Prices = req.Prices
	tip := s.chain.Tip()

	// validate the request
	if err := req.Validate(settings, tip); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	formationTxn := types.V2Transaction{
		MinerFee:      req.MinerFee,
		FileContracts: []types.V2FileContract{rhp4.NewContract(prices, req.Contract, ourKey, settings.WalletAddress)},
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
	renterCost, hostCost := rhp4.ContractCost(cs, prices, formationTxn.FileContracts[0], formationTxn.MinerFee)
	// validate the renter added enough inputs
	if n := renterInputs.Cmp(renterCost); n < 0 {
		return errorBadRequest("renter funding %v is less than required funding %v", renterInputs, renterCost)
	} else if n > 0 {
		// if the renter added too much, add a change output
		formationTxn.SiacoinOutputs = append(formationTxn.SiacoinOutputs, types.SiacoinOutput{
			Address: req.Contract.RenterAddress,
			Value:   renterInputs.Sub(renterCost),
		})
	}

	// fund the host collateral
	basis, toSign, err := s.wallet.FundV2Transaction(&formationTxn, hostCost, true)
	if errors.Is(err, wallet.ErrNotEnoughFunds) {
		return rhp4.ErrHostFundError
	} else if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	// sign the transaction inputs
	s.wallet.SignV2Inputs(&formationTxn, toSign)

	// update renter input basis to reflect our funding basis
	if basis != req.Basis {
		if err := updateSiacoinElementBasis(s.chain, req.Basis, basis, formationTxn.SiacoinInputs[:len(req.RenterInputs)]); err != nil {
			return errorBadRequest("failed to update renter inputs from %q to %q: %v", req.Basis, basis, err)
		}
	}

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

	// validate the renter's contract signature
	formationSigHash := cs.ContractSigHash(formationTxn.FileContracts[0])
	if !req.Contract.RenterPublicKey.VerifyHash(formationSigHash, renterSigResp.RenterContractSignature) {
		return rhp4.ErrInvalidSignature
	}
	formationTxn.FileContracts[0].RenterSignature = renterSigResp.RenterContractSignature

	// add the renter signatures to the transaction
	for i := range formationTxn.SiacoinInputs[:len(req.RenterInputs)] {
		formationTxn.SiacoinInputs[i].SatisfiedPolicy = renterSigResp.RenterSatisfiedPolicies[i]
	}

	// add our signature to the contract
	formationTxn.FileContracts[0].HostSignature = s.hostKey.SignHash(formationSigHash)

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
		Transactions: formationSet,
		Basis:        basis,
	}, Usage{
		RPCRevenue: settings.Prices.ContractPrice,
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

	// get the settings and update the prices to match the signed prices
	settings := s.settings.RHP4Settings()
	settings.Prices = prices
	// get the current tip
	tip := s.chain.Tip()

	// validate the request
	if err := req.Validate(settings, tip); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	// lock the existing contract
	state, unlock, err := s.lockContractForRevision(req.Renewal.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract %q: %w", req.Renewal.ContractID, err)
	}
	defer unlock()

	// validate challenge signature
	existing := state.Revision
	if !req.ValidChallengeSignature(existing) {
		return errorBadRequest("invalid challenge signature")
	}

	renewal := rhp4.NewRenewal(existing, prices, req.Renewal)

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
	cs := s.chain.TipState()
	renterCost, hostCost := rhp4.RenewalCost(cs, prices, renewal, renewalTxn.MinerFee)

	// add the renter inputs
	var renterInputSum types.Currency
	for _, si := range req.RenterInputs {
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: si,
		})
		renterInputSum = renterInputSum.Add(si.SiacoinOutput.Value)
	}

	// validate the renter added enough inputs
	if !renterInputSum.Equals(renterCost) {
		return errorBadRequest("expected renter to fund %v, got %v", renterInputSum, renterCost)
	}

	fceBasis, fce, err := s.contractor.ContractElement(req.Renewal.ContractID)
	if err != nil {
		return fmt.Errorf("failed to get contract element: %w", err)
	}
	renewalTxn.FileContractResolutions[0].Parent = fce

	basis := cs.Index // start with a decent basis and overwrite it if a setup transaction is needed
	var renewalParents []types.V2Transaction
	if !hostCost.IsZero() {
		// fund the locked collateral
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: renewal.NewContract.HostOutput.Address, Value: hostCost},
			},
		}

		var toSign []int
		basis, toSign, err = s.wallet.FundV2Transaction(&setupTxn, hostCost, true)
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

	// validate the renter's signature
	renewalSigHash := cs.RenewalSigHash(renewal)
	if !existing.RenterPublicKey.VerifyHash(renewalSigHash, renterSigResp.RenterRenewalSignature) {
		return rhp4.ErrInvalidSignature
	}
	renewal.RenterSignature = renterSigResp.RenterRenewalSignature

	// apply the renter's signatures
	for i := range renewalTxn.SiacoinInputs[:len(req.RenterInputs)] {
		renewalTxn.SiacoinInputs[i].SatisfiedPolicy = renterSigResp.RenterSatisfiedPolicies[i]
	}

	// sign the renewal
	renewal.HostSignature = s.hostKey.SignHash(renewalSigHash)

	// add the renter's parents to our transaction pool to ensure they are valid
	// and update the proofs.
	if len(req.RenterParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(req.Basis, req.RenterParents); err != nil {
			return errorBadRequest("failed to add formation parents to transaction pool: %v", err)
		}
	}

	// add the setup parents to the transaction pool
	if len(renewalParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(basis, renewalParents); err != nil {
			return errorBadRequest("failed to add setup parents to transaction pool: %v", err)
		}
	}

	// get the full updated transaction set for the renewal transaction
	basis, renewalSet, err := s.chain.V2TransactionSet(basis, renewalTxn)
	if err != nil {
		return fmt.Errorf("failed to get transaction set: %w", err)
	} else if _, err = s.chain.AddV2PoolTransactions(basis, renewalSet); err != nil {
		return errorBadRequest("failed to broadcast setup transaction: %v", err)
	}
	// broadcast the transaction set
	s.syncer.BroadcastV2TransactionSet(basis, renewalSet)

	// add the contract to the contractor
	err = s.contractor.RenewV2Contract(TransactionSet{
		Transactions: renewalSet,
		Basis:        basis,
	}, Usage{
		RPCRevenue:       prices.ContractPrice,
		StorageRevenue:   renewal.NewContract.HostOutput.Value.Sub(renewal.NewContract.TotalCollateral).Sub(prices.ContractPrice),
		RiskedCollateral: renewal.NewContract.TotalCollateral.Sub(renewal.NewContract.MissedHostValue),
	})
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

func (s *Server) handleHostStream(stream net.Conn, log *zap.Logger) {
	defer stream.Close()

	stream.SetDeadline(time.Now().Add(30 * time.Second)) // set an initial timeout
	rpcStart := time.Now()
	id, err := rhp4.ReadID(stream)
	if err != nil {
		log.Debug("failed to read RPC ID", zap.Error(err))
		return
	}
	log = log.With(zap.Stringer("rpc", id))

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
func (s *Server) Serve(t TransportListener, log *zap.Logger) error {
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

// errorBadRequest is a helper to create an rpc BadRequest error
func errorBadRequest(f string, p ...any) error {
	return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, fmt.Sprintf(f, p...))
}

// errorDecodingError is a helper to create an rpc Decoding error
func errorDecodingError(f string, p ...any) error {
	return rhp4.NewRPCError(rhp4.ErrorCodeDecoding, fmt.Sprintf(f, p...))
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
