package rhp

import (
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
		DialStream() net.Conn
		Close() error
	}

	// A ChainManager reports the chain state and manages the mempool.
	ChainManager interface {
		Tip() types.ChainIndex
		TipState() consensus.State

		// V2TransactionSet returns the full transaction set and basis necessary for
		// broadcasting a transaction. If the provided basis does not match the current
		// tip, the transaction will be updated. The transaction set includes the parents
		// and the transaction itself in an order valid for broadcasting.
		V2TransactionSet(basis types.ChainIndex, txn types.V2Transaction) (types.ChainIndex, []types.V2Transaction, error)
	}

	// FundAndSign is an interface for funding and signing v2 transactions
	FundAndSign interface {
		FundV2Transaction(txn *types.V2Transaction, amount types.Currency) (types.ChainIndex, []int, error)
		SignV2Inputs(*types.V2Transaction, []int)
	}
)

// A TransactionSet is a set of transactions is a set of transactions that
// are valid as of the provided chain index.
type TransactionSet struct {
	Basis        types.ChainIndex      `json:"basis"`
	Transactions []types.V2Transaction `json:"transactions"`
}

// ContractRevision pairs a contract ID with a revision.
type ContractRevision struct {
	ID       types.FileContractID `json:"id"`
	Revision types.V2FileContract `json:"revision"`
}

// RPCSettings returns the current settings of the host
func RPCSettings(t TransportClient) (rhp4.HostSettings, error) {
	s := t.DialStream()
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
func RPCReadSector(t TransportClient, prices rhp4.HostPrices, token rhp4.AccountToken, root types.Hash256, offset, length uint64) ([]byte, error) {
	if offset+length > rhp4.SectorSize {
		return nil, fmt.Errorf("read exceeds sector bounds")
	}

	s := t.DialStream()
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
func RPCWriteSector(t TransportClient, prices rhp4.HostPrices, token rhp4.AccountToken, data []byte, duration uint64) (types.Hash256, error) {
	if len(data) > rhp4.SectorSize {
		return types.Hash256{}, fmt.Errorf("sector must be less than %d bytes", rhp4.SectorSize)
	} else if len(data)%rhp4.LeafSize != 0 {
		return types.Hash256{}, fmt.Errorf("sector must be a multiple of %d bytes", rhp4.LeafSize)
	}

	s := t.DialStream()
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

	// TODO: stream?
	var sector [rhp4.SectorSize]byte
	copy(sector[:], data)
	root := rhp4.SectorRoot(&sector)
	if root != resp.Root {
		return types.Hash256{}, fmt.Errorf("invalid root returned: expected %v, got %v", root, resp.Root)
	}
	return root, nil
}

// RPCModifySectors modifies sectors on the host
func RPCModifySectors(t TransportClient, cs consensus.State, prices rhp4.HostPrices, sk types.PrivateKey, contract ContractRevision, actions []rhp4.WriteAction) (types.V2FileContract, error) {
	s := t.DialStream()
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
func RPCFundAccounts(t TransportClient, cs consensus.State, sk types.PrivateKey, contract ContractRevision, deposits []rhp4.AccountDeposit) (types.V2FileContract, []types.Currency, error) {
	var total types.Currency
	for _, deposit := range deposits {
		total = total.Add(deposit.Amount)
	}
	revision, err := rhp4.ReviseForFundAccount(contract.Revision, total)
	if err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to revise contract: %w", err)
	}
	sigHash := cs.ContractSigHash(revision)
	revision.RenterSignature = sk.SignHash(sigHash)

	req := rhp4.RPCFundAccountsRequest{
		ContractID:      contract.ID,
		Deposits:        deposits,
		RenterSignature: revision.RenterSignature,
	}

	s := t.DialStream()
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
func RPCLatestRevision(t TransportClient, contractID types.FileContractID) (types.V2FileContract, error) {
	s := t.DialStream()
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
func RPCSectorRoots(t TransportClient, cs consensus.State, prices rhp4.HostPrices, sk types.PrivateKey, contract ContractRevision, offset, length uint64) (types.V2FileContract, []types.Hash256, error) {
	revision, err := rhp4.ReviseForSectorRoots(contract.Revision, prices, length)
	if err != nil {
		return types.V2FileContract{}, nil, fmt.Errorf("failed to revise contract: %w", err)
	}
	sigHash := cs.ContractSigHash(revision)
	revision.RenterSignature = sk.SignHash(sigHash)

	req := rhp4.RPCSectorRootsRequest{
		Prices:          prices,
		ContractID:      contract.ID,
		Offset:          offset,
		Length:          length,
		RenterSignature: revision.RenterSignature,
	}

	s := t.DialStream()
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
func RPCAccountBalance(t TransportClient, account rhp4.Account) (types.Currency, error) {
	s := t.DialStream()
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
func RPCFormContract(t TransportClient, cm ChainManager, signer FundAndSign, prices rhp4.HostPrices, fc types.V2FileContract) (ContractRevision, TransactionSet, error) {
	formationTxn := types.V2Transaction{
		MinerFee:      types.Siacoins(1),
		FileContracts: []types.V2FileContract{fc},
	}

	cs := cm.TipState()
	renterFundAmount := rhp4.ContractRenterCost(cs, prices, fc, formationTxn.MinerFee)
	basis, toSign, err := signer.FundV2Transaction(&formationTxn, renterFundAmount)
	if err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to fund transaction: %w", err)
	}

	basis, formationSet, err := cm.V2TransactionSet(basis, formationTxn)
	if err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to get transaction set: %w", err)
	}
	formationTxn = formationSet[len(formationSet)-1]

	renterSiacoinElements := make([]types.SiacoinElement, 0, len(formationTxn.SiacoinInputs))
	for _, i := range formationTxn.SiacoinInputs {
		renterSiacoinElements = append(renterSiacoinElements, i.Parent)
	}

	req := rhp4.RPCFormContractRequest{
		Prices:        prices,
		Basis:         basis,
		MinerFee:      formationTxn.MinerFee,
		Contract:      fc,
		RenterInputs:  renterSiacoinElements,
		RenterParents: formationSet[:len(formationSet)-1],
	}

	s := t.DialStream()
	defer s.Close()

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

	var renterPolicyResp rhp4.RPCFormContractSecondResponse
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
	formationTxnID := formationTxn.ID()
	hostFormationTxnID := hostFormationTxn.ID()
	if formationTxnID != hostFormationTxnID {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected transaction IDs to match %v != %v", formationTxnID, hostFormationTxnID)
	}

	// validate the host signature
	fc.HostSignature = hostFormationTxn.FileContracts[0].HostSignature
	if !fc.HostPublicKey.VerifyHash(cs.ContractSigHash(fc), fc.HostSignature) {
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
func RPCRenewContract(t TransportClient, cm ChainManager, signer FundAndSign, prices rhp4.HostPrices, sk types.PrivateKey, contractID types.FileContractID, existing types.V2FileContract, renewal types.V2FileContractRenewal) (ContractRevision, TransactionSet, error) {
	renewalTxn := types.V2Transaction{
		MinerFee: types.Siacoins(1),
		FileContractResolutions: []types.V2FileContractResolution{
			{
				Parent: types.V2FileContractElement{
					StateElement: types.StateElement{
						// the other parts of the state element are not required
						// for signing the transaction. Let the host fill them
						// in.
						ID: types.Hash256(contractID),
					},
				},
				Resolution: &renewal,
			},
		},
	}

	cs := cm.TipState()
	setupFundAmount := rhp4.ContractRenterCost(cs, prices, renewal.NewContract, renewalTxn.MinerFee).Sub(renewal.RenterRollover)

	basis := cs.Index // start with a decent basis and overwrite it if a setup transaction is needed
	var renewalParents []types.V2Transaction
	if !setupFundAmount.IsZero() {
		setupTxn := types.V2Transaction{
			SiacoinOutputs: []types.SiacoinOutput{
				{Address: renewal.NewContract.RenterOutput.Address, Value: setupFundAmount},
			},
		}
		var err error
		var toSign []int
		basis, toSign, err = signer.FundV2Transaction(&setupTxn, setupFundAmount)
		if err != nil {
			return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to fund transaction: %w", err)
		}
		signer.SignV2Inputs(&setupTxn, toSign)

		basis, renewalParents, err = cm.V2TransactionSet(basis, setupTxn)
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
		Prices:        prices,
		ContractID:    contractID,
		Renewal:       renewal,
		MinerFee:      renewalTxn.MinerFee,
		Basis:         basis,
		RenterInputs:  renterSiacoinElements,
		RenterParents: renewalParents,
	}
	sigHash := req.ChallengeSigHash(existing.RevisionNumber)
	req.ChallengeSignature = sk.SignHash(sigHash)

	s := t.DialStream()
	defer s.Close()

	if err := rhp4.WriteRequest(s, rhp4.RPCRenewContractID, &req); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to write request: %w", err)
	}

	var hostInputsResp rhp4.RPCRenewContractResponse
	if err := rhp4.ReadResponse(s, &hostInputsResp); err != nil {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("failed to read host inputs response: %w", err)
	}

	// add the host inputs to the transaction
	hostInputSum := renewal.HostRollover
	for _, si := range hostInputsResp.HostInputs {
		hostInputSum = hostInputSum.Add(si.Parent.SiacoinOutput.Value)
		renewalTxn.SiacoinInputs = append(renewalTxn.SiacoinInputs, si)
	}

	// verify the host added enough inputs
	if !hostInputSum.Equals(renewal.NewContract.TotalCollateral) {
		return ContractRevision{}, TransactionSet{}, fmt.Errorf("expected host to fund %v, got %v", renewal.NewContract.TotalCollateral, hostInputSum)
	}

	// sign the renter inputs
	signer.SignV2Inputs(&renewalTxn, []int{0})

	// send the renter signatures
	var renterPolicyResp rhp4.RPCRenewContractSecondResponse
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
	renewalSigHash := cs.RenewalSigHash(renewal)
	if !existing.HostPublicKey.VerifyHash(renewalSigHash, hostRenewal.HostSignature) {
		return ContractRevision{}, TransactionSet{}, errors.New("invalid host signature")
	}
	return ContractRevision{
			ID:       contractID.V2RenewalID(),
			Revision: renewal.NewContract,
		}, TransactionSet{
			Basis:        hostTransactionSetResp.Basis,
			Transactions: hostTransactionSetResp.TransactionSet,
		}, nil
}
