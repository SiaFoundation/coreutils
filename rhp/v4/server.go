package rhp

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var protocolVersion = [3]byte{4, 0, 0}

type (
	// A TransportMux is a generic multiplexer for incoming streams.
	TransportMux interface {
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

		// UpdateV2TransactionSet updates the basis of a transaction set from "from" to "to".
		// If from and to are equal, the transaction set is returned as-is.
		// Any transactions that were confirmed are removed from the set.
		// Any ephemeral state elements that were created by an update are updated.
		//
		// If it is undesirable to modify the transaction set, deep-copy it
		// before calling this method.
		UpdateV2TransactionSet(txns []types.V2Transaction, from, to types.ChainIndex) ([]types.V2Transaction, error)
	}

	// A Syncer broadcasts transactions to its peers.
	Syncer interface {
		// BroadcastV2TransactionSet broadcasts a transaction set to the network.
		BroadcastV2TransactionSet(types.ChainIndex, []types.V2Transaction)
	}

	// A Wallet manages Siacoins and funds transactions.
	Wallet interface {
		// Address returns the host's address
		Address() types.Address

		// FundV2Transaction funds a transaction with the specified amount of
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

	// A Sectors is an interface for reading and writing sectors.
	Sectors interface {
		// HasSector returns true if the sector is stored.
		HasSector(root types.Hash256) (bool, error)
		// ReadSector retrieves a sector by its root
		ReadSector(root types.Hash256) (*[rhp4.SectorSize]byte, error)
		// StoreSector writes a sector to disk
		StoreSector(root types.Hash256, data *[rhp4.SectorSize]byte, expiration uint64) error
	}

	// A RevisionState pairs a contract revision with its sector roots.
	RevisionState struct {
		Revision  types.V2FileContract
		Renewed   bool
		Revisable bool
		Roots     []types.Hash256
	}

	// Contractor is an interface for managing a host's contracts.
	Contractor interface {
		// LockV2Contract locks a contract and returns its current state.
		// The returned function must be called to release the lock.
		LockV2Contract(types.FileContractID) (RevisionState, func(), error)
		// AddV2Contract adds a new contract to the host.
		AddV2Contract(TransactionSet, rhp4.Usage) error
		// RenewV2Contract finalizes an existing contract and adds its renewal.
		RenewV2Contract(TransactionSet, rhp4.Usage) error
		// ReviseV2Contract atomically revises a contract and updates its sector
		// roots and usage.
		ReviseV2Contract(contractID types.FileContractID, revision types.V2FileContract, roots []types.Hash256, usage rhp4.Usage) error
		// V2FileContractElement returns the contract state element for the given
		// contract ID.
		V2FileContractElement(types.FileContractID) (types.ChainIndex, types.V2FileContractElement, error)

		// AccountBalance returns the balance of an account.
		AccountBalance(rhp4.Account) (types.Currency, error)
		// AccountBalances returns the balances of multiple accounts.
		// The returned slice must be in the same order and have the
		// same length as the input slice. If an account is not found,
		// its balance will be types.ZeroCurrency.
		AccountBalances([]rhp4.Account) ([]types.Currency, error)
		// CreditAccountsWithContract atomically revises a contract and credits the account.
		CreditAccountsWithContract([]rhp4.AccountDeposit, types.FileContractID, types.V2FileContract, rhp4.Usage) ([]types.Currency, error)
		// DebitAccount debits an account.
		DebitAccount(rhp4.Account, rhp4.Usage) error
	}

	// Settings reports the host's current settings.
	Settings interface {
		RHP4Settings() rhp4.HostSettings
	}

	// A Server handles incoming RHP4 RPC.
	Server struct {
		hostKey            types.PrivateKey
		priceTableValidity time.Duration

		chain      ChainManager
		syncer     Syncer
		wallet     Wallet
		sectors    Sectors
		contractor Contractor
		settings   Settings
	}
)

func (s *Server) lockContractForRevision(contractID types.FileContractID) (RevisionState, func(), error) {
	rs, unlock, err := s.contractor.LockV2Contract(contractID)
	if err != nil {
		return RevisionState{}, nil, fmt.Errorf("failed to lock contract: %w", err)
	} else if !rs.Revisable {
		unlock()
		return RevisionState{}, nil, errorBadRequest("contract is not revisable")
	}
	return rs, unlock, nil
}

func (s *Server) handleRPCSettings(stream net.Conn) error {
	settings := s.settings.RHP4Settings()
	settings.ProtocolVersion = protocolVersion
	settings.Prices.TipHeight = s.chain.Tip().Height
	settings.Prices.ValidUntil = time.Now().Add(s.priceTableValidity)
	sigHash := settings.Prices.SigHash()
	settings.Prices.Signature = s.hostKey.SignHash(sigHash)

	return rhp4.WriteResponse(stream, &rhp4.RPCSettingsResponse{
		Settings: settings,
	})
}

func (s *Server) handleRPCReadSector(stream net.Conn, log *zap.Logger) error {
	st := time.Now()
	lap := func(context string) {
		log.Debug(context, zap.Duration("elapsed", time.Since(st)))
		st = time.Now()
	}
	var req rhp4.RPCReadSectorRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}
	lap("read request")

	if err := req.Validate(s.hostKey.PublicKey()); err != nil {
		return errorBadRequest("request invalid: %v", err)
	}
	prices, token := req.Prices, req.Token
	lap("validate request")

	if exists, err := s.sectors.HasSector(req.Root); err != nil {
		return fmt.Errorf("failed to check sector: %w", err)
	} else if !exists {
		return rhp4.ErrSectorNotFound
	}
	lap("check sector")

	err := s.contractor.DebitAccount(token.Account, prices.RPCReadSectorCost(req.Length))
	if err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}
	lap("debit account")

	sector, err := s.sectors.ReadSector(req.Root)
	if err != nil {
		return fmt.Errorf("failed to read sector: %w", err)
	}
	lap("read sector")

	segment := sector[req.Offset : req.Offset+req.Length]
	start := req.Offset / rhp4.LeafSize
	end := (req.Offset + req.Length + rhp4.LeafSize - 1) / rhp4.LeafSize
	proof := rhp4.BuildSectorProof(sector, start, end)
	lap("build proof")

	if err := rhp4.WriteResponse(stream, &rhp4.RPCReadSectorResponse{
		Proof:      proof,
		DataLength: uint64(len(segment)),
	}); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	} else if _, err := stream.Write(segment); err != nil {
		return fmt.Errorf("failed to write sector data: %w", err)
	}
	return nil
}

func (s *Server) handleRPCWriteSector(stream net.Conn) error {
	var req rhp4.RPCWriteSectorRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	} else if err := req.Validate(s.hostKey.PublicKey()); err != nil {
		return errorBadRequest("request invalid: %v", err)
	}
	prices := req.Prices

	var sector [rhp4.SectorSize]byte
	sr := io.LimitReader(stream, int64(req.DataLength))
	if req.DataLength < rhp4.SectorSize {
		// if the data is less than a full sector, the reader needs to be padded
		// with zeros to calculate the sector root
		sr = io.MultiReader(sr, bytes.NewReader(sector[req.DataLength:]))
	}

	buf := bytes.NewBuffer(sector[:0])
	root, err := rhp4.ReaderRoot(io.TeeReader(sr, buf))
	if err != nil {
		return errorDecodingError("failed to read sector data: %v", err)
	}

	usage := prices.RPCWriteSectorCost(req.DataLength)
	if err = s.contractor.DebitAccount(req.Token.Account, usage); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	if err := s.sectors.StoreSector(root, &sector, prices.TipHeight+rhp4.TempSectorDuration); err != nil {
		return fmt.Errorf("failed to store sector: %w", err)
	}
	return rhp4.WriteResponse(stream, &rhp4.RPCWriteSectorResponse{
		Root: root,
	})
}

func (s *Server) handleRPCFreeSectors(stream net.Conn) error {
	var req rhp4.RPCFreeSectorsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer unlock()

	if !req.ValidChallengeSignature(state.Revision) {
		return errorBadRequest("invalid challenge signature")
	}

	fc := state.Revision
	if err := req.Validate(s.hostKey.PublicKey(), fc); err != nil {
		return errorBadRequest("request invalid: %v", err)
	}
	prices := req.Prices

	// validate that all indices are within the expected range
	for _, i := range req.Indices {
		if i >= uint64(len(state.Roots)) {
			return errorBadRequest("index %v exceeds sector count %v", i, len(state.Roots))
		}
	}

	oldSubtreeHashes, oldLeafHashes := rhp4.BuildFreeSectorsProof(state.Roots, req.Indices)

	// modify the sector roots
	//
	// NOTE: must match the behavior of BuildFreeSectorsProof
	for i, n := range req.Indices {
		state.Roots[n] = state.Roots[len(state.Roots)-i-1]
	}
	state.Roots = state.Roots[:len(state.Roots)-len(req.Indices)]
	resp := rhp4.RPCFreeSectorsResponse{
		OldSubtreeHashes: oldSubtreeHashes,
		OldLeafHashes:    oldLeafHashes,
		NewMerkleRoot:    rhp4.MetaRoot(state.Roots),
	}
	if err := rhp4.WriteResponse(stream, &resp); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}
	var renterSigResponse rhp4.RPCFreeSectorsSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResponse); err != nil {
		return errorDecodingError("failed to read renter signature response: %v", err)
	}

	revision, usage, err := rhp4.ReviseForFreeSectors(fc, prices, resp.NewMerkleRoot, len(req.Indices))
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	cs := s.chain.TipState()
	sigHash := cs.ContractSigHash(revision)

	if !fc.RenterPublicKey.VerifyHash(sigHash, renterSigResponse.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}
	revision.RenterSignature = renterSigResponse.RenterSignature
	revision.HostSignature = s.hostKey.SignHash(sigHash)

	err = s.contractor.ReviseV2Contract(req.ContractID, revision, state.Roots, usage)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	return rhp4.WriteResponse(stream, &rhp4.RPCFreeSectorsThirdResponse{
		HostSignature: revision.HostSignature,
	})
}

func (s *Server) handleRPCAppendSectors(stream net.Conn) error {
	var req rhp4.RPCAppendSectorsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	if err := req.Validate(s.hostKey.PublicKey()); err != nil {
		return errorBadRequest("request invalid: %v", err)
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
	roots := state.Roots
	accepted := make([]bool, len(req.Sectors))
	var appended uint64
	for i, root := range req.Sectors {
		if ok, err := s.sectors.HasSector(root); err != nil {
			return fmt.Errorf("failed to check sector: %w", err)
		} else if !ok {
			continue
		}
		accepted[i] = true
		roots = append(roots, root)
		appended++
	}

	subtreeRoots, newRoot := rhp4.BuildAppendProof(state.Roots, roots[len(state.Roots):])
	resp := rhp4.RPCAppendSectorsResponse{
		Accepted:      accepted,
		SubtreeRoots:  subtreeRoots,
		NewMerkleRoot: newRoot,
	}
	if err := rhp4.WriteResponse(stream, &resp); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	revision, usage, err := rhp4.ReviseForAppendSectors(fc, req.Prices, newRoot, appended)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	sigHash := cs.ContractSigHash(revision)

	var renterSigResponse rhp4.RPCAppendSectorsSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResponse); err != nil {
		return errorDecodingError("failed to read renter signature response: %v", err)
	} else if !fc.RenterPublicKey.VerifyHash(sigHash, renterSigResponse.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}

	revision.RenterSignature = renterSigResponse.RenterSignature
	revision.HostSignature = s.hostKey.SignHash(sigHash)

	err = s.contractor.ReviseV2Contract(req.ContractID, revision, roots, usage)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}
	return rhp4.WriteResponse(stream, &rhp4.RPCAppendSectorsThirdResponse{
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

	revision, usage, err := rhp4.ReviseForFundAccounts(state.Revision, totalDeposits)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}

	sigHash := s.chain.TipState().ContractSigHash(revision)
	if !revision.RenterPublicKey.VerifyHash(sigHash, req.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}
	revision.RenterSignature = req.RenterSignature
	revision.HostSignature = s.hostKey.SignHash(sigHash)

	balances, err := s.contractor.CreditAccountsWithContract(req.Deposits, req.ContractID, revision, usage)
	if err != nil {
		return fmt.Errorf("failed to credit account: %w", err)
	}

	return rhp4.WriteResponse(stream, &rhp4.RPCFundAccountsResponse{
		Balances:      balances,
		HostSignature: revision.HostSignature,
	})
}

func (s *Server) handleRPCReplenishAccounts(stream net.Conn) error {
	var req rhp4.RPCReplenishAccountsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	} else if err := req.Validate(); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	// lock the existing contract
	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract %q: %w", req.ContractID, err)
	}
	defer unlock()

	// validate challenge signature
	existing := state.Revision
	if !req.ValidChallengeSignature(existing) {
		return fmt.Errorf("failed to validate challenge signature: %w", rhp4.ErrInvalidSignature)
	}

	balances, err := s.contractor.AccountBalances(req.Accounts)
	if err != nil {
		return fmt.Errorf("failed to get account balances: %w", err)
	}

	var depositSum types.Currency
	var costResp rhp4.RPCReplenishAccountsResponse
	for i, balance := range balances {
		deposit := rhp4.AccountDeposit{
			Account: req.Accounts[i],
		}

		value, underflows := req.Target.SubWithUnderflow(balance)
		if !underflows {
			deposit.Amount = value
		}
		depositSum = depositSum.Add(deposit.Amount)
		costResp.Deposits = append(costResp.Deposits, deposit)
	}

	if err := rhp4.WriteResponse(stream, &costResp); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	} else if depositSum.IsZero() {
		return nil
	}

	revision, usage, err := rhp4.ReviseForReplenish(existing, depositSum)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}

	var renterSigResp rhp4.RPCReplenishAccountsSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResp); err != nil {
		return errorDecodingError("failed to read renter signature response: %v", err)
	}

	sigHash := s.chain.TipState().ContractSigHash(revision)
	if !revision.RenterPublicKey.VerifyHash(sigHash, renterSigResp.RenterSignature) {
		return rhp4.ErrInvalidSignature
	}
	revision.RenterSignature = renterSigResp.RenterSignature
	revision.HostSignature = s.hostKey.SignHash(sigHash)

	_, err = s.contractor.CreditAccountsWithContract(costResp.Deposits, req.ContractID, revision, usage)
	if err != nil {
		return fmt.Errorf("failed to credit accounts: %w", err)
	}

	return rhp4.WriteResponse(stream, &rhp4.RPCReplenishAccountsThirdResponse{
		HostSignature: revision.HostSignature,
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
	unlock()

	return rhp4.WriteResponse(stream, &rhp4.RPCLatestRevisionResponse{
		Contract:  state.Revision,
		Revisable: state.Revisable,
		Renewed:   state.Renewed,
	})
}

func (s *Server) handleRPCSectorRoots(stream net.Conn) error {
	var req rhp4.RPCSectorRootsRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	state, unlock, err := s.lockContractForRevision(req.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract: %w", err)
	}
	defer unlock()

	// validate the request fields
	if err := req.Validate(s.hostKey.PublicKey(), state.Revision); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}
	prices := req.Prices

	// update the revision
	revision, usage, err := rhp4.ReviseForSectorRoots(state.Revision, prices, req.Length)
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
	revision.RenterSignature = req.RenterSignature

	// update the contract
	err = s.contractor.ReviseV2Contract(req.ContractID, revision, state.Roots, usage)
	if err != nil {
		return fmt.Errorf("failed to revise contract: %w", err)
	}

	roots := state.Roots[req.Offset : req.Offset+req.Length]
	proof := rhp4.BuildSectorRootsProof(state.Roots, req.Offset, req.Offset+req.Length)

	// send the response
	return rhp4.WriteResponse(stream, &rhp4.RPCSectorRootsResponse{
		Proof:         proof,
		Roots:         roots,
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
	settings := s.settings.RHP4Settings()
	tip := s.chain.Tip()
	if err := req.Validate(ourKey, tip, settings.MaxCollateral, settings.MaxContractDuration); err != nil {
		return err
	}
	prices := req.Prices

	fc, usage := rhp4.NewContract(prices, req.Contract, ourKey, settings.WalletAddress)
	formationTxn := types.V2Transaction{
		MinerFee:      req.MinerFee,
		FileContracts: []types.V2FileContract{fc},
	}

	// calculate the renter inputs
	var renterInputs types.Currency
	for _, sce := range req.RenterInputs {
		formationTxn.SiacoinInputs = append(formationTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: sce.Move(),
		})
		renterInputs = renterInputs.Add(sce.SiacoinOutput.Value)
	}

	// calculate the required funding
	cs := s.chain.TipState()
	renterCost, hostCost := rhp4.ContractCost(cs, prices, formationTxn.FileContracts[0], formationTxn.MinerFee)
	// validate the renter added enough inputs
	if renterInputs.Cmp(renterCost) < 0 {
		return errorBadRequest("renter funding %v is less than required funding %v", renterInputs, renterCost)
	} else if !renterInputs.Equals(renterCost) {
		// if the renter added too much, add a change output
		formationTxn.SiacoinOutputs = append(formationTxn.SiacoinOutputs, types.SiacoinOutput{
			Address: req.Contract.RenterAddress,
			Value:   renterInputs.Sub(renterCost),
		})
	}

	var broadcast bool
	// fund the host collateral
	basis, toSign, err := s.wallet.FundV2Transaction(&formationTxn, hostCost, true)
	if errors.Is(err, wallet.ErrNotEnoughFunds) {
		return rhp4.ErrHostFundError
	} else if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer func() {
		if broadcast {
			return
		}
		// release the inputs if the transaction is not going to be broadcast
		s.wallet.ReleaseInputs(nil, []types.V2Transaction{formationTxn})
	}()
	// sign the transaction inputs
	s.wallet.SignV2Inputs(&formationTxn, toSign)
	// send the host inputs to the renter
	hostInputsResp := rhp4.RPCFormContractResponse{
		HostInputs: formationTxn.SiacoinInputs[len(req.RenterInputs):],
	}
	if err := rhp4.WriteResponse(stream, &hostInputsResp); err != nil {
		return fmt.Errorf("failed to send host inputs: %w", err)
	}

	// update renter input basis to reflect our funding basis
	if basis != req.Basis {
		hostInputs := formationTxn.SiacoinInputs[len(req.RenterInputs):]
		formationTxn.SiacoinInputs = formationTxn.SiacoinInputs[:len(req.RenterInputs)]
		txnset, err := s.chain.UpdateV2TransactionSet([]types.V2Transaction{formationTxn}, req.Basis, basis)
		if err != nil {
			return errorBadRequest("failed to update renter inputs from %q to %q: %v", req.Basis, basis, err)
		}
		formationTxn = txnset[0]
		formationTxn.SiacoinInputs = append(formationTxn.SiacoinInputs, hostInputs...)
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
	for i, policy := range renterSigResp.RenterSatisfiedPolicies {
		formationTxn.SiacoinInputs[i].SatisfiedPolicy = policy
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

	// add the contract to the contractor
	err = s.contractor.AddV2Contract(TransactionSet{
		Transactions: formationSet,
		Basis:        basis,
	}, usage)
	if err != nil {
		return fmt.Errorf("failed to add contract: %w", err)
	}
	// broadcast the finalized contract formation set
	broadcast = true // set broadcast so the UTXOs will not be released if the renter happens to disconnect before receiving the last response
	s.syncer.BroadcastV2TransactionSet(basis, formationSet)

	// send the finalized transaction set to the renter
	return rhp4.WriteResponse(stream, &rhp4.RPCFormContractThirdResponse{
		Basis:          basis,
		TransactionSet: formationSet,
	})
}

func (s *Server) handleRPCRefreshContract(stream net.Conn) error {
	var req rhp4.RPCRefreshContractRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	}

	// validate prices
	prices := req.Prices
	if err := prices.Validate(s.hostKey.PublicKey()); err != nil {
		return fmt.Errorf("price table invalid: %w", err)
	}

	// lock the existing contract
	state, unlock, err := s.lockContractForRevision(req.Refresh.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract %q: %w", req.Refresh.ContractID, err)
	}
	defer unlock()

	// validate challenge signature
	existing := state.Revision
	if !req.ValidChallengeSignature(existing) {
		return errorBadRequest("invalid challenge signature")
	}

	// validate the request
	settings := s.settings.RHP4Settings()
	remainingCollateral := state.Revision.TotalCollateral.Sub(state.Revision.MissedHostValue)
	if err := req.Validate(s.hostKey.PublicKey(), remainingCollateral, state.Revision.TotalCollateral, state.Revision.RenterOutput.Value, state.Revision.ExpirationHeight, settings.MaxCollateral); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	cs := s.chain.TipState()
	renewal, usage := rhp4.RefreshContract(existing, prices, req.Refresh)
	renterCost, hostCost := rhp4.RefreshCost(cs, prices, renewal, req.MinerFee)
	renewalTxn := types.V2Transaction{
		MinerFee: req.MinerFee,
	}

	// add the renter inputs
	var renterInputSum types.Currency
	for _, si := range req.RenterInputs {
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: si.Move(),
		})
		renterInputSum = renterInputSum.Add(si.SiacoinOutput.Value)
	}

	if n := renterInputSum.Cmp(renterCost); n < 0 {
		return errorBadRequest("expected renter to fund %v, got %v", renterInputSum, renterCost)
	} else if n > 0 {
		// if the renter added too much, add a change output
		renewalTxn.SiacoinOutputs = append(renewalTxn.SiacoinOutputs, types.SiacoinOutput{
			Address: renewal.NewContract.RenterOutput.Address,
			Value:   renterInputSum.Sub(renterCost),
		})
	}

	elementBasis, fce, err := s.contractor.V2FileContractElement(req.Refresh.ContractID)
	if err != nil {
		return fmt.Errorf("failed to get contract element: %w", err)
	}

	var broadcast bool
	basis, toSign, err := s.wallet.FundV2Transaction(&renewalTxn, hostCost, true)
	if errors.Is(err, wallet.ErrNotEnoughFunds) {
		return rhp4.ErrHostFundError
	} else if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer func() {
		if broadcast {
			return
		}
		// release the locked UTXOs if the transaction is not going to be broadcast
		s.wallet.ReleaseInputs(nil, []types.V2Transaction{renewalTxn})
	}()

	// update renter inputs to reflect our chain state
	if basis != req.Basis {
		hostInputs := renewalTxn.SiacoinInputs[len(req.RenterInputs):]
		renewalTxn.SiacoinInputs = renewalTxn.SiacoinInputs[:len(req.RenterInputs)]
		updated, err := s.chain.UpdateV2TransactionSet([]types.V2Transaction{renewalTxn}, req.Basis, basis)
		if err != nil {
			return errorBadRequest("failed to update renter inputs from %q to %q: %v", req.Basis, basis, err)
		}
		renewalTxn = updated[0]
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, hostInputs...)
	}

	if elementBasis != basis {
		tempTxn := types.V2Transaction{
			FileContractResolutions: []types.V2FileContractResolution{
				{Parent: fce.Move(), Resolution: &renewal},
			},
		}
		updated, err := s.chain.UpdateV2TransactionSet([]types.V2Transaction{tempTxn}, elementBasis, basis)
		if err != nil {
			return fmt.Errorf("failed to update contract element: %w", err)
		}
		fce = updated[0].FileContractResolutions[0].Parent.Move()
	}
	renewalTxn.FileContractResolutions = []types.V2FileContractResolution{
		{Parent: fce.Move(), Resolution: &renewal},
	}
	s.wallet.SignV2Inputs(&renewalTxn, toSign)
	// send the host inputs to the renter
	hostInputsResp := rhp4.RPCRefreshContractResponse{
		HostInputs: renewalTxn.SiacoinInputs[len(req.RenterInputs):],
	}
	if err := rhp4.WriteResponse(stream, &hostInputsResp); err != nil {
		return fmt.Errorf("failed to send host inputs: %w", err)
	}

	// read the renter's signatures
	var renterSigResp rhp4.RPCRefreshContractSecondResponse
	if err := rhp4.ReadResponse(stream, &renterSigResp); err != nil {
		return errorDecodingError("failed to read renter signatures: %v", err)
	} else if len(renterSigResp.RenterSatisfiedPolicies) != len(req.RenterInputs) {
		return errorBadRequest("expected %v satisfied policies, got %v", len(req.RenterInputs), len(renterSigResp.RenterSatisfiedPolicies))
	}

	// validate the renter's signature
	renewalSigHash := cs.RenewalSigHash(renewal)
	if !existing.RenterPublicKey.VerifyHash(renewalSigHash, renterSigResp.RenterRenewalSignature) {
		return fmt.Errorf("failed to validate renter renewal signature: %w", rhp4.ErrInvalidSignature)
	}
	renewal.RenterSignature = renterSigResp.RenterRenewalSignature
	renewal.HostSignature = s.hostKey.SignHash(renewalSigHash)

	contractSigHash := cs.ContractSigHash(renewal.NewContract)
	if !existing.RenterPublicKey.VerifyHash(contractSigHash, renterSigResp.RenterContractSignature) {
		return fmt.Errorf("failed to validate renter contract signature: %w", rhp4.ErrInvalidSignature)
	}
	renewal.NewContract.RenterSignature = renterSigResp.RenterContractSignature
	renewal.NewContract.HostSignature = s.hostKey.SignHash(contractSigHash)

	// apply the renter's signatures
	for i, policy := range renterSigResp.RenterSatisfiedPolicies {
		renewalTxn.SiacoinInputs[i].SatisfiedPolicy = policy
	}

	// add the renter's parents to our transaction pool to ensure they are valid
	// and update the proofs.
	if len(req.RenterParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(req.Basis, req.RenterParents); err != nil {
			return errorBadRequest("failed to add renewal parents to transaction pool: %v", err)
		}
	}

	// get the full updated transaction set for the renewal transaction
	basis, renewalSet, err := s.chain.V2TransactionSet(basis, renewalTxn)
	if err != nil {
		return fmt.Errorf("failed to get transaction set: %w", err)
	} else if _, err = s.chain.AddV2PoolTransactions(basis, renewalSet); err != nil {
		return errorBadRequest("failed to broadcast renewal set: %v", err)
	}

	// add the contract to the contractor
	err = s.contractor.RenewV2Contract(TransactionSet{
		Transactions: renewalSet,
		Basis:        basis,
	}, usage)
	if err != nil {
		return fmt.Errorf("failed to add contract: %w", err)
	}

	broadcast = true // set broadcast so the UTXOs will not be released if the renter happens to disconnect before receiving the last response
	s.syncer.BroadcastV2TransactionSet(basis, renewalSet)

	// send the finalized transaction set to the renter
	return rhp4.WriteResponse(stream, &rhp4.RPCRefreshContractThirdResponse{
		Basis:          basis,
		TransactionSet: renewalSet,
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

	// lock the existing contract
	state, unlock, err := s.lockContractForRevision(req.Renewal.ContractID)
	if err != nil {
		return fmt.Errorf("failed to lock contract %q: %w", req.Renewal.ContractID, err)
	}
	defer unlock()

	settings := s.settings.RHP4Settings()
	tip := s.chain.Tip()

	// validate the request
	if err := req.Validate(s.hostKey.PublicKey(), tip, state.Revision.Filesize, state.Revision.ProofHeight, settings.MaxCollateral, settings.MaxContractDuration); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	// validate challenge signature
	existing := state.Revision
	if !req.ValidChallengeSignature(existing) {
		return errorBadRequest("invalid challenge signature")
	}

	cs := s.chain.TipState()
	renewal, usage := rhp4.RenewContract(existing, prices, req.Renewal)
	renterCost, hostCost := rhp4.RenewalCost(cs, prices, renewal, req.MinerFee)
	renewalTxn := types.V2Transaction{
		MinerFee: req.MinerFee,
	}

	// add the renter inputs
	var renterInputSum types.Currency
	for _, si := range req.RenterInputs {
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: si.Move(),
		})
		renterInputSum = renterInputSum.Add(si.SiacoinOutput.Value)
	}

	if n := renterInputSum.Cmp(renterCost); n < 0 {
		return errorBadRequest("expected renter to fund %v, got %v", renterInputSum, renterCost)
	} else if n > 0 {
		// if the renter added too much, add a change output
		renewalTxn.SiacoinOutputs = append(renewalTxn.SiacoinOutputs, types.SiacoinOutput{
			Address: renewal.NewContract.RenterOutput.Address,
			Value:   renterInputSum.Sub(renterCost),
		})
	}

	elementBasis, fce, err := s.contractor.V2FileContractElement(req.Renewal.ContractID)
	if err != nil {
		return fmt.Errorf("failed to get contract element: %w", err)
	}

	var broadcast bool
	basis, toSign, err := s.wallet.FundV2Transaction(&renewalTxn, hostCost, true)
	if errors.Is(err, wallet.ErrNotEnoughFunds) {
		return rhp4.ErrHostFundError
	} else if err != nil {
		return fmt.Errorf("failed to fund transaction: %w", err)
	}
	defer func() {
		if broadcast {
			return
		}
		// release the locked UTXOs if the transaction is not going to be broadcast
		s.wallet.ReleaseInputs(nil, []types.V2Transaction{renewalTxn})
	}()

	// update renter inputs to reflect our chain state
	if basis != req.Basis {
		hostInputs := renewalTxn.SiacoinInputs[len(req.RenterInputs):]
		renewalTxn.SiacoinInputs = renewalTxn.SiacoinInputs[:len(req.RenterInputs)]
		updated, err := s.chain.UpdateV2TransactionSet([]types.V2Transaction{renewalTxn}, req.Basis, basis)
		if err != nil {
			return errorBadRequest("failed to update renter inputs from %q to %q: %v", req.Basis, basis, err)
		}
		renewalTxn = updated[0]
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, hostInputs...)
	}

	if elementBasis != basis {
		tempTxn := types.V2Transaction{
			FileContractResolutions: []types.V2FileContractResolution{
				{Parent: fce.Move(), Resolution: &renewal},
			},
		}
		updated, err := s.chain.UpdateV2TransactionSet([]types.V2Transaction{tempTxn}, elementBasis, basis)
		if err != nil {
			return fmt.Errorf("failed to update contract element: %w", err)
		}
		fce = updated[0].FileContractResolutions[0].Parent.Move()
	}
	renewalTxn.FileContractResolutions = []types.V2FileContractResolution{
		{Parent: fce.Move(), Resolution: &renewal},
	}
	s.wallet.SignV2Inputs(&renewalTxn, toSign)
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
		return fmt.Errorf("failed to validate renewal signature: %w", rhp4.ErrInvalidSignature)
	}
	renewal.RenterSignature = renterSigResp.RenterRenewalSignature
	renewal.HostSignature = s.hostKey.SignHash(renewalSigHash)

	contractSighash := cs.ContractSigHash(renewal.NewContract)
	if !existing.RenterPublicKey.VerifyHash(contractSighash, renterSigResp.RenterContractSignature) {
		return fmt.Errorf("failed to validate contract signature: %w", rhp4.ErrInvalidSignature)
	}
	renewal.NewContract.RenterSignature = renterSigResp.RenterContractSignature
	renewal.NewContract.HostSignature = s.hostKey.SignHash(contractSighash)

	// apply the renter's signatures
	for i, policy := range renterSigResp.RenterSatisfiedPolicies {
		renewalTxn.SiacoinInputs[i].SatisfiedPolicy = policy
	}

	// add the renter's parents to our transaction pool to ensure they are valid
	// and update the proofs.
	if len(req.RenterParents) > 0 {
		if _, err := s.chain.AddV2PoolTransactions(req.Basis, req.RenterParents); err != nil {
			return errorBadRequest("failed to add formation parents to transaction pool: %v", err)
		}
	}

	// get the full updated transaction set for the renewal transaction
	basis, renewalSet, err := s.chain.V2TransactionSet(basis, renewalTxn)
	if err != nil {
		return fmt.Errorf("failed to get transaction set: %w", err)
	} else if _, err = s.chain.AddV2PoolTransactions(basis, renewalSet); err != nil {
		return errorBadRequest("failed to broadcast renewal set: %v", err)
	}

	// add the contract to the contractor
	err = s.contractor.RenewV2Contract(TransactionSet{
		Transactions: renewalSet,
		Basis:        basis,
	}, usage)
	if err != nil {
		return fmt.Errorf("failed to add contract: %w", err)
	}

	broadcast = true // set broadcast so the UTXOs will not be released if the renter happens to disconnect before receiving the last response
	s.syncer.BroadcastV2TransactionSet(basis, renewalSet)

	// send the finalized transaction set to the renter
	return rhp4.WriteResponse(stream, &rhp4.RPCRenewContractThirdResponse{
		Basis:          basis,
		TransactionSet: renewalSet,
	})
}

func (s *Server) handleRPCVerifySector(stream net.Conn) error {
	var req rhp4.RPCVerifySectorRequest
	if err := rhp4.ReadRequest(stream, &req); err != nil {
		return errorDecodingError("failed to read request: %v", err)
	} else if err := req.Validate(s.hostKey.PublicKey()); err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}
	prices, token := req.Prices, req.Token

	if err := s.contractor.DebitAccount(token.Account, prices.RPCVerifySectorCost()); err != nil {
		return fmt.Errorf("failed to debit account: %w", err)
	}

	sector, err := s.sectors.ReadSector(req.Root)
	if err != nil {
		return rhp4.NewRPCError(rhp4.ErrorCodeBadRequest, err.Error())
	}

	proof := rhp4.BuildSectorProof(sector, req.LeafIndex, req.LeafIndex+1)
	resp := rhp4.RPCVerifySectorResponse{
		Proof: proof,
		Leaf:  ([64]byte)(sector[rhp4.LeafSize*req.LeafIndex:]),
	}
	return rhp4.WriteResponse(stream, &resp)
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
	// contract
	case rhp4.RPCFormContractID:
		err = s.handleRPCFormContract(stream)
	case rhp4.RPCRefreshContractID:
		err = s.handleRPCRefreshContract(stream)
	case rhp4.RPCRenewContractID:
		err = s.handleRPCRenewContract(stream)
	case rhp4.RPCLatestRevisionID:
		err = s.handleRPCLatestRevision(stream)
	case rhp4.RPCFreeSectorsID:
		err = s.handleRPCFreeSectors(stream)
	case rhp4.RPCSectorRootsID:
		err = s.handleRPCSectorRoots(stream)
	// account
	case rhp4.RPCAccountBalanceID:
		err = s.handleRPCAccountBalance(stream)
	case rhp4.RPCFundAccountsID:
		err = s.handleRPCFundAccounts(stream)
	case rhp4.RPCReplenishAccountsID:
		err = s.handleRPCReplenishAccounts(stream)
	// sector
	case rhp4.RPCAppendSectorsID:
		err = s.handleRPCAppendSectors(stream)
	case rhp4.RPCReadSectorID:
		err = s.handleRPCReadSector(stream, log.Named("RPCReadSector"))
	case rhp4.RPCWriteSectorID:
		err = s.handleRPCWriteSector(stream)
	case rhp4.RPCVerifySectorID:
		err = s.handleRPCVerifySector(stream)
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
func (s *Server) Serve(t TransportMux, log *zap.Logger) error {
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
func NewServer(pk types.PrivateKey, cm ChainManager, syncer Syncer, contracts Contractor, wallet Wallet, settings Settings, sectors Sectors, opts ...ServerOption) *Server {
	s := &Server{
		hostKey:            pk,
		priceTableValidity: 30 * time.Minute,

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
