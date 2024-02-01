package host

import (
	"errors"

	"go.sia.tech/coreutils/wallet"
)

var (
	ErrNotEnoughFunds    = wallet.ErrNotEnoughFunds
	ErrNotEnoughStorage  = errors.New("not enough storage")
	ErrSectorNotFound    = errors.New("sector not found")
	ErrPriceTableExpired = errors.New("price table expired")
	ErrOffsetOutOfBounds = errors.New("offset out of bounds")
)
