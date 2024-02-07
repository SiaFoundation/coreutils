package rhp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/mux"
)

var (
	// ErrInvalidHostSig is returned when a host provides a contract with
	// invalid host signature
	ErrInvalidHostSig = errors.New("host signature is invalid")

	// ErrInvalidRenterSig is returned when a host provides a contract with
	// invalid renter signature
	ErrInvalidRenterSig = errors.New("renter signature is invalid")
)

var (
	defaultOptions = Options{
		DialTimeout: time.Minute,
		RPCTimeout:  5 * time.Minute,
	}
)

type (
	Option func(o *Options)

	// Options are used to configure the client during creation.
	Options struct {
		DialTimeout time.Duration // timeout for dialing a new connection
		RPCTimeout  time.Duration // timeout for RPCs
	}

	// Client is a client for the v4 renter-host protocol. After successful
	// creation, a client will try to maintain a connection to the host and
	// recreate it if necessary. In addition to various timeouts (see Options),
	// every RPC can be interrupted using a context.
	Client struct {
		addr    string
		hostKey types.PublicKey
		wg      sync.WaitGroup
		closed  chan struct{}

		dialTimeout time.Duration
		rpcTimeout  time.Duration

		mu           sync.Mutex
		lastMuxReset time.Time
		mux          *mux.Mux
	}
)

// WithDialTimeout overwrites the default dial timeout of 1 minute.
func WithDialTimeout(d time.Duration) Option {
	return func(opts *Options) {
		opts.DialTimeout = d
	}
}

// WithRPCTimeout overwrites the default RPC timeout of 5 minutes.
func WithRPCTimeout(d time.Duration) Option {
	return func(opts *Options) {
		opts.RPCTimeout = d
	}
}

// NewClient creates a new client for the v4 renter-host protocol.
func NewClient(ctx context.Context, addr string, hostKey types.PublicKey, opts ...Option) (*Client, error) {
	o := defaultOptions
	for _, opt := range opts {
		opt(&o)
	}
	c := &Client{
		addr:        addr,
		hostKey:     hostKey,
		dialTimeout: o.DialTimeout,
		rpcTimeout:  o.RPCTimeout,
	}
	return c, c.resetTransport(ctx)
}

// Close closes the stream which prevents new RPCs from being performed and
// blocks until all ongoing RPCs are finished.
func (c *Client) Close() error {
	close(c.closed)
	c.wg.Wait()

	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	if c.mux != nil {
		err = c.mux.Close()
		c.mux = nil
	}
	return err
}

// do blockingly performs an RPC. It will return one of 3 things.
//  1. false, nil: the RPC was successful
//  2. true, nil: the RPC failed due to a transport error and the transport was reset
//  3. false, error: the RPC either failed due to a protocol error or the
//     transport wasn't recoverable
func (c *Client) do(ctx context.Context, s *mux.Stream, rpcFn func(s *mux.Stream) error) (bool, error) {
	// set a sane deadline
	if err := s.SetDeadline(time.Now().Add(c.rpcTimeout)); err != nil {
		return false, fmt.Errorf("failed to set deadline: %w", err)
	}

	// perform rpc
	errRPC := rpcFn(s)

	// check the error code to see if the error is part of the protocol or
	// transport related - if it is transport related, reset the transport
	// and retry the RPC
	if rhpv4.ErrorCode(errRPC) == rhpv4.ErrorCodeTransport {
		c.mu.Lock()
		if time.Since(c.lastMuxReset) < 10*time.Second {
			// don't reset too often
			c.mu.Unlock()
			return false, fmt.Errorf("not resetting mux since it was recently reset: %w", errRPC)
		} else if err := c.resetTransport(ctx); err != nil {
			// failed to reset transport
			c.mu.Unlock()
			return false, fmt.Errorf("%v: %w", err, errRPC)
		}
		c.mu.Unlock()
		return true, nil
	}
	return false, errRPC
}

// withStream creates a stream for the caller to perform an RPC on. It will
// write the RPC id and then leave it to the caller to perform the remaining RPC
// in the callback. The stream will be closed automatically and any panics which
// might occurr will be returned as an error.
// NOTE: rpcFn might be called up to 2 times so it should be written in a way
// that avoids carrying state over from the first call to the second.
func (c *Client) withStream(ctx context.Context, rpcFn func(*mux.Stream) error) (err error) {
	done := make(chan struct{})
	defer close(done)

	// don't perform any RPCs if the client is closed
	select {
	case <-c.closed:
		return errors.New("client is closed")
	default:
	}

	// helper to dial and interrupt stream
	dial := func() *mux.Stream {
		c.mu.Lock()
		s := c.mux.DialStream()
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			select {
			case <-ctx.Done():
			case <-done:
			}
			_ = s.Close()
		}()
		c.mu.Unlock()
		return s
	}

	// defer recover
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("a panic occurred while executing the rpc: %v", r)
		}
	}()

	// dial a stream
	s := dial()

	// perform RPC
	retry, err := c.do(ctx, s, rpcFn)
	if err != nil {
		return err
	} else if retry {
		s = dial() // new stream
		_, err = c.do(ctx, s, rpcFn)
		return err
	}
	return nil
}

// performSingleTripRPC is a helper that performs an RPC that requires a single
// request and response.
func (c *Client) performSingleTripRPC(ctx context.Context, req rhpv4.Request, resp rhpv4.Object) error {
	return c.withStream(ctx, func(s *mux.Stream) error {
		if err := rhpv4.WriteID(s, req); err != nil {
			return fmt.Errorf("failed to write rpc id: %w", err)
		} else if err := rhpv4.WriteRequest(s, req); err != nil {
			return fmt.Errorf("failed to write request: %w", err)
		} else if err := rhpv4.ReadResponse(s, resp); err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}
		return nil
	})
}

// resetTransport resets the client's underlying connection and creates a new
// one.
func (c *Client) resetTransport(ctx context.Context) error {
	// close the old transport
	if c.mux != nil {
		c.mux.Close()
		c.mux = nil
	}
	// create new transport
	if conn, err := net.DialTimeout("tcp", c.addr, c.dialTimeout); err != nil {
		return fmt.Errorf("failed to dial tcp connection: %w", err)
	} else if conn.(*net.TCPConn).SetKeepAlive(true); err != nil {
		return fmt.Errorf("failed to set keepalive: %w", err)
	} else if conn.SetDeadline(time.Now().Add(c.dialTimeout)); err != nil {
		return fmt.Errorf("failed to set dial deadline on tcp connection: %w", err)
	} else if mux, err := mux.Dial(conn, c.hostKey[:]); err != nil {
		return fmt.Errorf("failed to dial mux: %w", err)
	} else if err := conn.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("failed to revoke deadline on tcp connection")
	} else {
		c.mux = mux
		c.lastMuxReset = time.Now()
	}
	return nil
}

// AuditContract probabilistically audits a contract, checking whether the host
// has missing sectors. The input specifies the number of sectors we randomly
// pick so the higher the nummer, the less likely it is that the host is missing
// sectors. Any missing sectors found are returned.
func (c *Client) AuditContract(ctx context.Context, n int) ([]interface{}, error) {
	panic("implement me")
}

// Settings returns the host's current settings, including its prices.
func (c *Client) Settings(ctx context.Context) (rhpv4.HostSettings, error) {
	req := rhpv4.RPCSettingsRequest{}
	var resp rhpv4.RPCSettingsResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return rhpv4.HostSettings{}, fmt.Errorf("RPCSettings failed: %w", err)
	}
	return resp.Settings, nil
}

// FormContract forms a new contract with the host.
func (c *Client) FormContract(ctx context.Context, contract types.V2FileContract, sv SignerVerifier, hp rhpv4.HostPrices, inputs []types.V2SiacoinInput, parents []types.V2Transaction) (types.V2FileContract, []types.V2Transaction, error) {
	var txnSet []types.V2Transaction
	err := c.withStream(ctx, func(s *mux.Stream) error {
		// first roundtrip
		req := rhpv4.RPCFormContractRequest{
			Prices:        hp,
			Contract:      contract,
			RenterInputs:  inputs,
			RenterParents: parents,
		}
		var resp rhpv4.RPCFormContractResponse
		if err := rhpv4.WriteRequest(s, &req); err != nil {
			return err
		} else if err := rhpv4.ReadResponse(s, &resp); err != nil {
			return err
		}

		// prepare txn
		txn := types.V2Transaction{
			FileContracts: []types.V2FileContract{contract},
		}
		sv.SignContract(&txn.FileContracts[0])
		sv.SignInputs(&txn)

		// second roundtrip
		req2 := rhpv4.RPCFormContractSecondResponse{
			RenterContractSignature: contract.RenterSignature,
		}
		for _, input := range txn.SiacoinInputs {
			req2.RenterSatisfiedPolicies = append(req2.RenterSatisfiedPolicies, input.SatisfiedPolicy)
		}
		var resp2 rhpv4.RPCFormContractThirdResponse
		if err := rhpv4.WriteResponse(s, &req2); err != nil {
			return err
		} else if err := rhpv4.ReadResponse(s, &resp2); err != nil {
			return err
		}

		// add host signatures
		txn.FileContracts[0].HostSignature = resp2.HostContractSignature

		// add host inputs to txn
		if len(resp.HostInputs) != len(resp2.HostSatisfiedPolicies) {
			return fmt.Errorf("number of host policies doesn't match number of host inputs: %v != %v", len(resp.HostInputs), len(resp2.HostSatisfiedPolicies))
		}
		for i := range resp.HostInputs {
			sci := resp.HostInputs[i]
			sci.SatisfiedPolicy = resp2.HostSatisfiedPolicies[i]
			txn.SiacoinInputs = append(txn.SiacoinInputs, sci)
		}

		// verify whole transaction
		if err := sv.VerifyTransaction(txn); err != nil {
			return fmt.Errorf("verifying signatures on txn failed")
		}

		// finalise txn
		txnSet = append([]types.V2Transaction{}, parents...) // client parents
		txnSet = append(txnSet, resp.HostParents...)         // host parents
		txnSet = append(txnSet, txn)                         // contract txn

		// update the contract to return
		contract = txn.FileContracts[0]
		return nil
	})
	return contract, txnSet, err
}

// RenewContract renews a contract with the host, immediately unlocking
func (c *Client) RenewContract(ctx context.Context, hp rhpv4.HostPrices, finalRevision, initialRevision types.V2FileContract) (types.V2FileContractRenewal, error) {
	panic("incomplete rpc -- missing inputs and outputs")
}

// PinSectors pins sectors to a contract. Commonly used to pin sectors uploaded
// with 'UploadSector'. PinSectors will first overwrite the provided gaps and
// then start appending roots to the end of the contract. So if more roots than
// gaps were provided and the method returns an error, it is safe to assume all
// gaps were filled. PinSectors fails if more roots than gaps are provided since
// it sorts the gaps to find duplicates which makes it hard for the caller to
// know which gaps got filled.
func (c *Client) PinSectors(ctx context.Context, contract types.V2FileContract, sv SignerVerifier, hp rhpv4.HostPrices, roots []types.Hash256, gaps []uint64) (types.V2FileContract, error) {
	// sanity check input - no duplicate gaps, at most one gap per root
	if len(gaps) > len(roots) {
		return types.V2FileContract{}, fmt.Errorf("more gaps than roots provided")
	}
	slices.Sort(gaps)
	for i := 1; i < len(gaps); i++ {
		if gaps[i] == gaps[i-1] {
			return types.V2FileContract{}, fmt.Errorf("gap %v is duplicated", gaps[i])
		}
	}

	// prepare actions
	actions := make([]rhpv4.WriteAction, len(roots))
	for i := range roots {
		if len(gaps) > 0 {
			actions[i] = rhpv4.WriteAction{
				Type: rhpv4.ActionUpdate,
				A:    gaps[0],
				Root: roots[i],
			}
			gaps = gaps[1:]
		} else {
			actions[i] = rhpv4.WriteAction{
				Type: rhpv4.ActionAppend,
				Root: roots[i],
			}
		}
	}
	newRevision, err := c.reviseContract(ctx, contract, sv, hp, actions)
	if err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCReviseContract failed: %w", err)
	}
	return newRevision, nil
}

// PruneContract prunes the sectors with the given indices from a contract.
func (c *Client) PruneContract(ctx context.Context, contract types.V2FileContract, sv SignerVerifier, hp rhpv4.HostPrices, nSectors uint64, sectorIndices []uint64) (types.V2FileContract, error) {
	if len(sectorIndices) == 0 {
		return types.V2FileContract{}, nil // nothing to do
	} else if nSectors == 0 {
		return types.V2FileContract{}, fmt.Errorf("trying to prune empty contract")
	}

	// sanity check input - no out-of-bounds indices, no duplicates
	lastIndex := nSectors - 1
	slices.Sort(sectorIndices)
	if sectorIndices[len(sectorIndices)-1] > lastIndex {
		return types.V2FileContract{}, fmt.Errorf("sector index %v is out of bounds for contract with %v sectors", sectorIndices[len(sectorIndices)-1], nSectors)
	}
	for i := 1; i < len(sectorIndices); i++ {
		if sectorIndices[i] == sectorIndices[i-1] {
			return types.V2FileContract{}, fmt.Errorf("sector index %v is duplicated", sectorIndices[i])
		}
	}

	// swap out sectors to delete
	actions := make([]rhpv4.WriteAction, len(sectorIndices))
	for i := range sectorIndices {
		actions[i] = rhpv4.WriteAction{
			Type: rhpv4.ActionSwap,
			A:    uint64(sectorIndices[i]),
			B:    lastIndex,
		}
		lastIndex--
	}

	// trim the swapped sectors
	actions = append(actions, rhpv4.WriteAction{
		N: uint64(len(actions)),
	})

	newRevision, err := c.reviseContract(ctx, contract, sv, hp, actions)
	if err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCReviseSectors failed: %w", err)
	}
	return newRevision, nil
}

// LatestRevision returns the latest revision for a given contract.
func (c *Client) LatestRevision(ctx context.Context, sv SignerVerifier, contractID types.FileContractID) (types.V2FileContract, error) {
	req := rhpv4.RPCLatestRevisionRequest{
		ContractID: contractID,
	}
	var resp rhpv4.RPCLatestRevisionResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCLatestRevision failed: %w", err)
	} else if !sv.VerifyHostSignature(resp.Contract) {
		return types.V2FileContract{}, ErrInvalidHostSig
	} else if !sv.VerifyRenterSignature(resp.Contract) {
		return types.V2FileContract{}, ErrInvalidRenterSig
	}
	return resp.Contract, nil
}

// ReadSector reads a sector from the host.
func (c *Client) ReadSector(ctx context.Context, account rhpv4.AccountID, hp rhpv4.HostPrices, root types.Hash256, offset, length uint64) ([]byte, error) {
	// sanity check input - offset must be segment-aligned
	if offset%64 != 0 {
		return nil, fmt.Errorf("offset %v is not segment-aligned", offset)
	}
	req := rhpv4.RPCReadSectorRequest{
		Prices:    hp,
		AccountID: account,
		Root:      root,
		Offset:    offset,
	}
	var resp rhpv4.RPCReadSectorResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return nil, fmt.Errorf("RPCReadSector failed: %w", err)
	}

	// verify proof
	proofStart := offset / rhpv2.LeafSize
	proofEnd := (offset + length) / rhpv2.LeafSize
	v := rhpv2.NewRangeProofVerifier(proofStart, proofEnd)
	if _, err := v.ReadFrom(bytes.NewReader(resp.Sector)); err != nil {
		return nil, errors.New("failed to read response data")
	} else if !v.Verify(resp.Proof, root) {
		return nil, errors.New("failed to verify sector range proof")
	}
	return resp.Sector, nil
}

// WriteSector stores a sector in the host's temporary storage. To make it
// permanent, use 'PinSectors'. The provided data will be extended to
// rhpv4.SectorSize by the host so the client still pays for the full sector.
func (c *Client) WriteSector(ctx context.Context, account rhpv4.AccountID, hp rhpv4.HostPrices, data []byte) (types.Hash256, error) {
	if len(data) == 0 {
		return types.Hash256{}, fmt.Errorf("empty sector")
	} else if len(data) > rhpv4.SectorSize {
		return types.Hash256{}, fmt.Errorf("sector too large")
	}
	req := rhpv4.RPCWriteSectorRequest{
		Prices:    hp,
		AccountID: account,
		Sector:    data,
	}
	var resp rhpv4.RPCWriteSectorResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return types.Hash256{}, fmt.Errorf("RPCWriteSector failed: %w", err)
	}

	var sectorData [rhpv4.SectorSize]byte
	copy(sectorData[:], data)
	if rhpv2.SectorRoot((&sectorData)) != resp.Root {
		return types.Hash256{}, fmt.Errorf("root mismatch")
	}
	return resp.Root, nil
}

// SectorRoots returns 'length' roots of a contract starting at the given
// 'offset'.
func (c *Client) SectorRoots(ctx context.Context, contract types.V2FileContract, sv SignerVerifier, hp rhpv4.HostPrices, offset, length uint64) ([]types.Hash256, error) {
	newRevision := contract
	newRevision.RevisionNumber++
	// TODO: payment
	sv.SignContract(&newRevision)

	req := rhpv4.RPCSectorRootsRequest{
		Prices:          hp,
		RenterSignature: newRevision.RenterSignature,
		Offset:          offset,
		Length:          length,
	}
	var resp rhpv4.RPCSectorRootsResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return nil, fmt.Errorf("RPCSectorRoots failed: %w", err)
	}

	// verify host signature
	newRevision.HostSignature = resp.HostSignature
	if !sv.VerifyHostSignature(newRevision) {
		return nil, ErrInvalidHostSig
	}

	// TODO: verify proof
	return resp.Roots, nil
}

// AccountBalance returns the balance of a given account.
func (c *Client) AccountBalance(ctx context.Context, account rhpv4.AccountID) (types.Currency, error) {
	req := rhpv4.RPCAccountBalanceRequest{
		AccountID: account,
	}
	var resp rhpv4.RPCAccountBalanceResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return types.Currency{}, fmt.Errorf("RPCAccountBalance failed: %w", err)
	}
	return resp.Balance, nil
}

// FundAccount adds to the balance to an account and returns the new balance.
func (c *Client) FundAccount(ctx context.Context, contractID types.FileContractID, contract types.V2FileContract, sv SignerVerifier, deposits []rhpv4.AccountDeposit) ([]types.Currency, types.V2FileContract, error) {
	// build new revision
	newRevision := contract
	newRevision.RevisionNumber++
	// TODO: payouts
	sv.SignContract(&newRevision)

	req := rhpv4.RPCFundAccountRequest{
		ContractID:      contractID,
		Deposits:        deposits,
		RenterSignature: newRevision.RenterSignature,
	}
	var resp rhpv4.RPCFundAccountResponse
	if err := c.performSingleTripRPC(ctx, &req, &resp); err != nil {
		return nil, types.V2FileContract{}, fmt.Errorf("RPCFundAccount failed: %w", err)
	}

	// verify host signature
	newRevision.HostSignature = resp.HostSignature
	if !sv.VerifyHostSignature(newRevision) {
		return nil, types.V2FileContract{}, ErrInvalidHostSig
	}
	return resp.Balances, newRevision, nil
}

// ReviseContract is a more generic version of 'PinSectors' and 'PruneContract'.
// It's allows for arbitrary actions to be performed. Most users should use
// 'PinSectors' and 'PruneContract' instead.
func (c *Client) ReviseContract(ctx context.Context, contract types.V2FileContract, sv SignerVerifier, hp rhpv4.HostPrices, actions []rhpv4.WriteAction) (types.V2FileContract, error) {
	newRevision, err := c.reviseContract(ctx, contract, sv, hp, actions)
	if err != nil {
		return types.V2FileContract{}, fmt.Errorf("RPCReviseContract failed: %w", err)
	}
	return newRevision, nil
}

func (c *Client) reviseContract(ctx context.Context, contract types.V2FileContract, sv SignerVerifier, hp rhpv4.HostPrices, actions []rhpv4.WriteAction) (types.V2FileContract, error) {
	// run rpc
	newRevision := contract
	newRevision.RevisionNumber++
	err := c.withStream(ctx, func(s *mux.Stream) error {
		// TODO: create payment revision

		// sign revision
		sv.SignContract(&newRevision)

		// first roundtrip
		req := rhpv4.RPCModifySectorsRequest{
			Prices:  hp,
			Actions: actions,
		}
		var resp rhpv4.RPCModifySectorsResponse
		if err := rhpv4.WriteRequest(s, &req); err != nil {
			return err
		} else if err := rhpv4.ReadResponse(s, &resp); err != nil {
			return err
		}

		// second roundtrip
		req2 := rhpv4.RPCModifySectorsSecondResponse{
			RenterSignature: newRevision.RenterSignature,
		}
		var resp2 rhpv4.RPCModifySectorsThirdResponse
		if err := rhpv4.WriteResponse(s, &req2); err != nil {
			return err
		} else if err := rhpv4.ReadResponse(s, &resp2); err != nil {
			return err
		}

		// finalise new revision and verify signature
		newRevision.HostSignature = resp2.HostSignature
		if !sv.VerifyHostSignature(newRevision) {
			return errors.New("invalid host signature")
		}
		return nil
	})
	return newRevision, err
}

// SignerVerifier is a minimal interface for signing/verifying contracts and
// transactions as part of performing RPCs.
type SignerVerifier interface {
	// SignContract signs the given contract, setting the
	// 'RenterSignature' in the process
	SignContract(contract *types.V2FileContract)

	// SignInputs signs all inputs within the transaction, setting their
	// signatures in the process
	SignInputs(txn *types.V2Transaction)

	// VerifyHostSignature verifies the host signature on the given contract
	VerifyHostSignature(c types.V2FileContract) bool

	// VerifyRenterSignature verifies the renter signature on the given contract
	VerifyRenterSignature(c types.V2FileContract) bool

	// VerifyTransaction verifies the whole transaction including the renter,
	// host and input signatures
	VerifyTransaction(txn types.V2Transaction) error
}

// singleAddressSignerVerifier is an implementation of the SignerVerifier which
// assumes that all inputs can be signed with a single key
type singleAddressSignerVerifier struct {
	state       consensus.State
	contractKey types.PrivateKey
	walletKey   types.PrivateKey
}

func (s *singleAddressSignerVerifier) Close() error {
	clear(s.contractKey)
	clear(s.walletKey)
	return nil
}

func (s *singleAddressSignerVerifier) SignContract(c *types.V2FileContract) {
	c.RenterSignature = s.contractKey.SignHash(s.state.ContractSigHash(*c))
}

func (s *singleAddressSignerVerifier) SignInputs(txn *types.V2Transaction) {
	sig := s.walletKey.SignHash(s.state.InputSigHash(*txn))
	wpk := s.walletKey.PublicKey()
	for i := range txn.SiacoinInputs {
		txn.SiacoinInputs[i].SatisfiedPolicy = types.SatisfiedPolicy{
			Policy: types.SpendPolicy{
				Type: types.PolicyTypeUnlockConditions(
					types.StandardUnlockConditions(wpk),
				),
			},
			Signatures: []types.Signature{sig}, // same for all inputs in single address wallet
		}
	}
}

func (s *singleAddressSignerVerifier) VerifyHostSignature(c types.V2FileContract) bool {
	return c.HostPublicKey.VerifyHash(s.state.ContractSigHash(c), c.HostSignature)
}

func (s *singleAddressSignerVerifier) VerifyRenterSignature(c types.V2FileContract) bool {
	return c.RenterPublicKey.VerifyHash(s.state.ContractSigHash(c), c.RenterSignature)
}

func (s *singleAddressSignerVerifier) VerifyTransaction(txn types.V2Transaction) error {
	ms := consensus.NewMidState(s.state)
	return consensus.ValidateV2Transaction(ms, txn)
}

func NewSigner(state consensus.State, contractKey, walletKey types.PrivateKey) SignerVerifier {
	return &singleAddressSignerVerifier{
		state:       state,
		contractKey: contractKey,
		walletKey:   walletKey,
	}
}
