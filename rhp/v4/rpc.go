package rhp

import (
	"context"
	"errors"
	"fmt"
	"net"

	"go.sia.tech/core/consensus"
	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	// A TransportClient is a generic multiplexer for outgoing streams.
	TransportClient interface {
		DialStreamContext(context.Context) net.Conn
		Close() error
	}

	// A ChainReader reports the chain state and manages the transaction pool.
	ChainReader interface {
		Tip() types.ChainIndex
		TipState() consensus.State

		// V2TransactionSet returns the full transaction set and basis necessary
		// for broadcasting a transaction. The transaction will be updated if
		// the provided basis does not match the current tip. The transaction set
		// includes the parents and the transaction itself in an order valid
		// for broadcasting.
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// A ContractSigner is a minimal interface for contract signing
	ContractSigner interface {
		SignHash(types.Hash256) types.Signature
	}

	// A TransactionInputSigner is an interface for signing v2 transactions using
	// a single private key.
	TransactionInputSigner interface {
		SignV2Inputs(*types.V2Transaction, []int)
	}

	// A TransactionFunder is an interface for funding v2 transactions.
	TransactionFunder interface {
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error)
		ReleaseInputs([]types.V2Transaction)
	}

	// FormContractSigner is the minimal interface required to fund and sign a
	// contract formation transaction.
	FormContractSigner interface {
		ContractSigner
		TransactionInputSigner
		TransactionFunder
	}
)

type (
	// A TransactionSet is a set of transactions that are valid as of the
	// provided chain index.
	TransactionSet struct {
		Basis        types.ChainIndex      `json:"basis"`
		Transactions []types.V2Transaction `json:"transactions"`
	}

	// ContractRevision pairs a contract ID with a revision.
	ContractRevision struct {
		ID       types.FileContractID `json:"id"`
		Revision types.V2FileContract `json:"revision"`
	}
)

// RPCSettings returns the current settings of the host
func RPCSettings(ctx context.Context, t TransportClient) (rhp4.HostSettings, error) {
	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCSettingsID, nil); err != nil {
		return rhp4.HostSettings{}, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCSettingsResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return rhp4.HostSettings{}, fmt.Errorf("failed to read response: %w", err)
	}
	return resp.Settings, nil
}

// RPCReadSector reads a sector from the host
func RPCReadSector(ctx context.Context, t TransportClient, prices rhp4.HostPrices, token rhp4.AccountToken, root types.Hash256, offset, length uint64) ([]byte, error) {
	if offset%rhp4.LeafSize != 0 {
		return nil, fmt.Errorf("offset must be a multiple of %d bytes", rhp4.LeafSize)
	} else if offset+length > rhp4.SectorSize {
		return nil, fmt.Errorf("read exceeds sector bounds")
	}

	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCReadSectorID, &rhp4.RPCReadSectorRequest{
		Prices: prices,
		Token:  token,
		Root:   root,
		Offset: offset,
		Length: length,
	}); err != nil {
		return nil, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCReadSectorResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	// TODO: verify proof + stream to writer
	return resp.Sector, nil
}

// RPCWriteSector writes a sector to the host
func RPCWriteSector(ctx context.Context, t TransportClient, prices rhp4.HostPrices, token rhp4.AccountToken, data []byte, duration uint64) (types.Hash256, error) {
	if len(data) > rhp4.SectorSize {
		return types.Hash256{}, fmt.Errorf("sector must be less than %d bytes", rhp4.SectorSize)
	} else if len(data)%rhp4.LeafSize != 0 {
		return types.Hash256{}, fmt.Errorf("sector must be a multiple of %d bytes", rhp4.LeafSize)
	}

	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCWriteSectorID, &rhp4.RPCWriteSectorRequest{
		Prices:   prices,
		Token:    token,
		Sector:   data,
		Duration: duration,
	}); err != nil {
		return types.Hash256{}, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCWriteSectorResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return types.Hash256{}, fmt.Errorf("failed to read response: %w", err)
	}

	var sector [rhp4.SectorSize]byte
	copy(sector[:], data)
	root := rhp4.SectorRoot(&sector)
	if root != resp.Root {
		return types.Hash256{}, fmt.Errorf("invalid root returned: expected %v, got %v", root, resp.Root)
	}
	return root, nil
}

// RPCModifySectors modifies sectors on the host
func RPCModifySectors(ctx context.Context, t TransportClient, cs consensus.State, prices rhp4.HostPrices, sk types.PrivateKey, contract ContractRevision, actions []rhp4.WriteAction) (types.V2FileContract, error) {
	s := t.DialStreamContext(ctx)
	defer s.Close()

	req := rhp4.RPCModifySectorsRequest{
		ContractID: contract.ID,
		Prices:     prices,
		Actions:    actions,
	}
	req.ChallengeSignature = sk.SignHash(req.ChallengeSigHash(contract.Revision.RevisionNumber + 1))

	if err := rhp4.WriteRequest(s, rhp4.RPCModifySectorsID, &req); err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCModifySectorsResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to read response: %w", err)
	}

	revision, err := rhp4.ReviseForModifySectors(contract.Revision, req, resp)
	if err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to revise contract: %w", err)
	}

	sigHash := cs.ContractSigHash(revision)
	revision.RenterSignature = sk.SignHash(sigHash)

	signatureResp := rhp4.RPCModifySectorsSecondResponse{
		RenterSignature: revision.RenterSignature,
	}
	if err := rhp4.WriteResponse(s, &signatureResp); err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to write signature response: %w", err)
	}

	var hostSignature rhp4.RPCModifySectorsThirdResponse
	if err := rhp4.ReadResponse(s, &hostSignature); err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to read host signatures: %w", err)
	}
	// validate the host signature
	if !contract.Revision.HostPublicKey.VerifyHash(sigHash, hostSignature.HostSignature) {
		return contract.Revision, rhp4.ErrInvalidSignature
	}
	// return the signed revision
	return revision, nil
}

// RPCFundAccounts funds accounts on the host
func RPCFundAccounts(ctx context.Context, t TransportClient, cs consensus.State, signer ContractSigner, contract ContractRevision, deposits []rhp4.AccountDeposit) (types.V2FileContract, []types.Currency, error) {
	var total types.Currency
	for _, deposit := range deposits {
		total = total.Add(deposit.Amount)
	}
	revision, err := rhp4.ReviseForFundAccount(contract.Revision, total)
	if err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to revise contract: %w", err)
	}
	sigHash := cs.ContractSigHash(revision)
	revision.RenterSignature = signer.SignHash(sigHash)

	req := rhp4.RPCFundAccountsRequest{
		ContractID:      contract.ID,
		Deposits:        deposits,
		RenterSignature: revision.RenterSignature,
	}

	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCFundAccountsID, &req); err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCFundAccountsResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// validate the host's signature
	if !contract.Revision.HostPublicKey.VerifyHash(sigHash, resp.HostSignature) {
		return contract.Revision, nil, rhp4.ErrInvalidSignature
	}
	revision.HostSignature = resp.HostSignature
	return revision, resp.Balances, nil
}

// RPCLatestRevision returns the latest revision of a contract
func RPCLatestRevision(ctx context.Context, t TransportClient, contractID types.FileContractID) (types.V2FileContract, error) {
	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCLatestRevisionID, &rhp4.RPCLatestRevisionRequest{ContractID: contractID}); err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCLatestRevisionResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return types.V2FileContract{}, fmt.Errorf("failed to read response: %w", err)
	}
	return resp.Contract, nil
}

// RPCSectorRoots returns the sector roots for a contract
func RPCSectorRoots(ctx context.Context, t TransportClient, cs consensus.State, prices rhp4.HostPrices, signer ContractSigner, contract ContractRevision, offset, length uint64) (types.V2FileContract, []types.Hash256, error) {
	revision, err := rhp4.ReviseForSectorRoots(contract.Revision, prices, length)
	if err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to revise contract: %w", err)
	}
	sigHash := cs.ContractSigHash(revision)
	revision.RenterSignature = signer.SignHash(sigHash)

	req := rhp4.RPCSectorRootsRequest{
		Prices:          prices,
		ContractID:      contract.ID,
		Offset:          offset,
		Length:          length,
		RenterSignature: revision.RenterSignature,
	}

	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCSectorRootsID, &req); err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCSectorRootsResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// validate host signature
	if !contract.Revision.HostPublicKey.VerifyHash(sigHash, resp.HostSignature) {
		return contract.Revision, nil, rhp4.ErrInvalidSignature
	}

	// TODO: validate proof
	return revision, resp.Roots, nil
}

// RPCAccountBalance returns the balance of an account
func RPCAccountBalance(ctx context.Context, t TransportClient, account rhp4.Account) (types.Currency, error) {
	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCAccountBalanceID, &rhp4.RPCAccountBalanceRequest{Account: account}); err != nil {
		return types.Currency{}, fmt.Errorf("failed to write request: %w", err)
	}

	var resp rhp4.RPCAccountBalanceResponse
	if err := rhp4.ReadResponse(s, &resp); err != nil {
		return types.Currency{}, fmt.Errorf("failed to read response: %w", err)
	}
	return resp.Balance, nil
}

// RPCFormContract forms a contract with a host
func RPCFormContract(ctx context.Context, t TransportClient, cr ChainReader, signer FormContractSigner, p rhp4.HostPrices, hostKey types.PublicKey, hostAddress types.Address, params rhp4.RPCFormContractParams) (ContractRevision, TransactionSet, error) {
	cs := cr.TipState()
	fc := rhp4.NewContract(p, params, hostKey, hostAddress)
	formationTxn := types.V2Transaction{
		MinerFee:      types.Siacoins(1),
		FileContracts: []types.V2FileContract{fc},
	}

	renterCost, _ := rhp4.ContractCost(cs, p, fc, formationTxn.MinerFee)
	basis, toSign, err := signer.FundV2Transaction(&formationTxn, renterCost)
	if err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to fund transaction: %w", err)
	}

	basis, formationSet, err := cr.V2TransactionSet(basis, formationTxn)
	if err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to get transaction set: %w", err)
	}
	formationTxn, formationSet = formationSet[len(formationSet)-1], formationSet[:len(formationSet)-1]

	renterSiacoinElements := make([]types.SiacoinElement, 0, len(formationTxn.SiacoinInputs))
	for _, i := range formationTxn.SiacoinInputs {
		renterSiacoinElements = append(renterSiacoinElements, i.Parent)
	}

	s := t.DialStreamContext(ctx)
	defer s.Close()

	req := rhp4.RPCFormContractRequest{
		Prices:        p,
		Contract:      params,
		Basis:         basis,
		MinerFee:      formationTxn.MinerFee,
		RenterInputs:  renterSiacoinElements,
		RenterParents: formationSet,
	}
	if err := rhp4.WriteRequest(s, rhp4.RPCFormContractID, &req); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to write request: %w", err)
	}

	var hostInputsResp rhp4.RPCFormContractResponse
	if err := rhp4.ReadResponse(s, &hostInputsResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to read host inputs response: %w", err)
	}

	// add the host inputs to the transaction
	var hostInputSum types.Currency
	for _, si := range hostInputsResp.HostInputs {
		hostInputSum = hostInputSum.Add(si.Parent.SiacoinOutput.Value)
		formationTxn.SiacoinInputs = append(formationTxn.SiacoinInputs, si)
	}

	if n := hostInputSum.Cmp(fc.TotalCollateral); n < 0 {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected host to fund at least %v, got %v", fc.TotalCollateral, hostInputSum)
	} else if n > 0 {
		// add change output
		formationTxn.SiacoinOutputs = append(formationTxn.SiacoinOutputs, types.SiacoinOutput{
			Address: fc.HostOutput.Address,
			Value:   hostInputSum.Sub(fc.TotalCollateral),
		})
	}

	// sign the renter inputs
	signer.SignV2Inputs(&formationTxn, toSign)
	formationSigHash := cs.ContractSigHash(fc)
	formationTxn.FileContracts[0].RenterSignature = signer.SignHash(formationSigHash)

	renterPolicyResp := rhp4.RPCFormContractSecondResponse{
		RenterContractSignature: formationTxn.FileContracts[0].RenterSignature,
	}
	for _, si := range formationTxn.SiacoinInputs[:len(renterSiacoinElements)] {
		renterPolicyResp.RenterSatisfiedPolicies = append(renterPolicyResp.RenterSatisfiedPolicies, si.SatisfiedPolicy)
	}
	// send the renter signatures
	if err := rhp4.WriteResponse(s, &renterPolicyResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to write signature response: %w", err)
	}

	// read the finalized transaction set
	var hostTransactionSetResp rhp4.RPCFormContractThirdResponse
	if err := rhp4.ReadResponse(s, &hostTransactionSetResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to read final response: %w", err)
	}

	if len(hostTransactionSetResp.TransactionSet) == 0 {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected at least one host transaction")
	}
	hostFormationTxn := hostTransactionSetResp.TransactionSet[len(hostTransactionSetResp.TransactionSet)-1]
	if len(hostFormationTxn.FileContracts) != 1 {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected exactly one contract")
	}

	// check for no funny business
	formationTxnID := formationTxn.ID()
	hostFormationTxnID := hostFormationTxn.ID()
	if formationTxnID != hostFormationTxnID {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected transaction IDs to match %v != %v", formationTxnID, hostFormationTxnID)
	}

	// validate the host signature
	fc.HostSignature = hostFormationTxn.FileContracts[0].HostSignature
	if !fc.HostPublicKey.VerifyHash(formationSigHash, fc.HostSignature) {
		return ContractRevision{}, TransactionSet{}, errors.New("invalid host signature")
	}
	return ContractRevision{
			ID:       formationTxn.V2FileContractID(formationTxnID, 0),
			Revision: fc,
		}, TransactionSet{
			Basis:        hostTransactionSetResp.Basis,
			Transactions: hostTransactionSetResp.TransactionSet,
		}, nil
}

// RPCRenewContract renews a contract with a host
func RPCRenewContract(ctx context.Context, t TransportClient, cr ChainReader, signer FormContractSigner, p rhp4.HostPrices, existing types.V2FileContract, params rhp4.RPCRenewContractParams) (ContractRevision, TransactionSet, error) {
	renewal := rhp4.NewRenewal(existing, p, params)
	renewalTxn := types.V2Transaction{
		MinerFee: types.Siacoins(1),
		FileContractResolutions: []types.V2FileContractResolution{
			{
				Parent: types.V2FileContractElement{
					StateElement: types.StateElement{
						// the other parts of the state element are not required
						// for signing the transaction. Let the host fill them
						// in.
						ID: types.Hash256(params.ContractID),
					},
				},
				Resolution: &renewal,
			},
		},
	}

	cs := cr.TipState()
	renterCost, hostCost := rhp4.RenewalCost(cs, p, renewal, renewalTxn.MinerFee)

	basis := cs.Index // start with a decent basis and overwrite it if a setup transaction is needed
	var renewalParents []types.V2Transaction
	if !renterCost.IsZero() {
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: renewal.NewContract.RenterOutput.Address, Value: renterCost},
			},
		}
		var err error
		var toSign []int
		basis, toSign, err = signer.FundV2Transaction(&setupTxn, renterCost)
		if err != nil {
			return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to fund transaction: %w", err)
		}
		signer.SignV2Inputs(&setupTxn, toSign)

		basis, renewalParents, err = cr.V2TransactionSet(basis, setupTxn)
		if err != nil {
			return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to get transaction set: %w", err)
		}
		setupTxn = renewalParents[len(renewalParents)-1]

		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, types.V2SiacoinInput{
			Parent: setupTxn.EphemeralSiacoinOutput(0),
		})
		signer.SignV2Inputs(&renewalTxn, []int{0})
	}

	renterSiacoinElements := make([]types.SiacoinElement, 0, len(renewalTxn.SiacoinInputs))
	for _, i := range renewalTxn.SiacoinInputs {
		renterSiacoinElements = append(renterSiacoinElements, i.Parent)
	}

	req := rhp4.RPCRenewContractRequest{
		Prices:        p,
		Renewal:       params,
		MinerFee:      renewalTxn.MinerFee,
		Basis:         basis,
		RenterInputs:  renterSiacoinElements,
		RenterParents: renewalParents,
	}
	sigHash := req.ChallengeSigHash(existing.RevisionNumber)
	req.ChallengeSignature = signer.SignHash(sigHash)

	s := t.DialStreamContext(ctx)
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCRenewContractID, &req); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to write request: %w", err)
	}

	var hostInputsResp rhp4.RPCRenewContractResponse
	if err := rhp4.ReadResponse(s, &hostInputsResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to read host inputs response: %w", err)
	}

	// add the host inputs to the transaction
	var hostInputSum types.Currency
	for _, si := range hostInputsResp.HostInputs {
		hostInputSum = hostInputSum.Add(si.Parent.SiacoinOutput.Value)
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, si)
	}

	// verify the host added enough inputs
	if !hostInputSum.Equals(hostCost) {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected host to fund %v, got %v", hostCost, hostInputSum)
	}

	// sign the renter inputs
	signer.SignV2Inputs(&renewalTxn, []int{0})
	// sign the renewal
	renewalSigHash := cs.RenewalSigHash(renewal)
	renewal.RenterSignature = signer.SignHash(renewalSigHash)

	// send the renter signatures
	renterPolicyResp := rhp4.RPCRenewContractSecondResponse{
		RenterRenewalSignature: renewal.RenterSignature,
	}
	for _, si := range renewalTxn.SiacoinInputs[:len(renterSiacoinElements)] {
		renterPolicyResp.RenterSatisfiedPolicies = append(renterPolicyResp.RenterSatisfiedPolicies, si.SatisfiedPolicy)
	}
	if err := rhp4.WriteResponse(s, &renterPolicyResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to write signature response: %w", err)
	}

	// read the finalized transaction set
	var hostTransactionSetResp rhp4.RPCRenewContractThirdResponse
	if err := rhp4.ReadResponse(s, &hostTransactionSetResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to read final response: %w", err)
	}

	if len(hostTransactionSetResp.TransactionSet) == 0 {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected at least one host transaction")
	}
	hostRenewalTxn := hostTransactionSetResp.TransactionSet[len(hostTransactionSetResp.TransactionSet)-1]
	if len(hostRenewalTxn.FileContractResolutions) != 1 {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected exactly one resolution")
	}

	hostRenewal, ok := hostRenewalTxn.FileContractResolutions[0].Resolution.(*types.V2FileContractRenewal)
	if !ok {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected renewal resolution")
	}

	// validate the host signature
	if !existing.HostPublicKey.VerifyHash(renewalSigHash, hostRenewal.HostSignature) {
		return ContractRevision{}, TransactionSet{}, errors.New("invalid host signature")
	}
	return ContractRevision{
			ID:       params.ContractID.V2RenewalID(),
			Revision: renewal.NewContract,
		}, TransactionSet{
			Basis:        hostTransactionSetResp.Basis,
			Transactions: hostTransactionSetResp.TransactionSet,
		}, nil
}
