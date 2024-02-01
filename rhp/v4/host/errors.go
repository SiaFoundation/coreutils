package host

import (
	"errors"

	"go.sia.tech/coreutils/wallet"
)

var (
	// ErrNotEnoughFunds is returned when a transaction cannot be funded.
	ErrNotEnoughFunds = wallet.ErrNotEnoughFunds
	// ErrNotEnoughStorage is returned when a sector cannot be stored because
	// the host has run out of space
	ErrNotEnoughStorage = errors.New("not enough storage")
	// ErrSectorNotFound is returned when a sector cannot be deleted because it
	// does not exist.
	ErrSectorNotFound = errors.New("sector not found")
	// ErrPriceTableExpired is returned when the renter sent an expired price
	// table.
	ErrPriceTableExpired = errors.New("price table expired")
	// ErrOffsetOutOfBounds is returned when a renter requests a sector with an
	// offset that is out of bounds.
	ErrOffsetOutOfBounds = errors.New("offset out of bounds")
	//ErrContractExists is returned when a renter tries to form a contract with
	// a host that already has a contract with the renter.
	ErrContractExists = errors.New("contract already exists")
)
