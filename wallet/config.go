package wallet

import (
	"time"

	"go.uber.org/zap"
)

type (
	config struct {
		DefragThreshold     int
		MaxInputsForDefrag  int
		MaxDefragUTXOs      int
		ReservationDuration time.Duration

		MaxRebroadcastPeriod        time.Duration
		RebroadcastDebounceInterval time.Duration

		Log *zap.Logger
	}

	// An Option is a configuration option for a wallet.
	Option func(*config)
)

// WithDefragThreshold sets the transaction defrag threshold.
func WithDefragThreshold(n int) Option {
	return func(c *config) {
		c.DefragThreshold = n
	}
}

// WithMaxInputsForDefrag sets the maximum number of inputs a transaction can
// have to be considered for defragging
func WithMaxInputsForDefrag(n int) Option {
	return func(c *config) {
		c.MaxInputsForDefrag = n
	}
}

// WithMaxDefragUTXOs sets the maximum number of additional utxos that will be
// added to a transaction when defragging
func WithMaxDefragUTXOs(n int) Option {
	return func(c *config) {
		c.MaxDefragUTXOs = n
	}
}

// WithReservationDuration sets the duration that a reservation will be held
// on spent utxos
func WithReservationDuration(d time.Duration) Option {
	if d <= 0 {
		panic("reservation duration must be positive") // developer error
	}

	return func(c *config) {
		c.ReservationDuration = d
	}
}

// WithDebounceInterval sets the debounce interval for rebroadcasting
// transactions after a reorg.
func WithDebounceInterval(d time.Duration) Option {
	if d <= 0 {
		panic("debounce interval must be positive") // developer error
	}
	return func(c *config) {
		c.RebroadcastDebounceInterval = d
	}
}

// WithMaxRebroadcastPeriod sets the maximum period of time a transaction set is
// being rebroadcasted, after this period the broadcasted set is removed from
// the store.
func WithMaxRebroadcastPeriod(d time.Duration) Option {
	if d <= 0 {
		panic("max rebroadcast period must be positive") // developer error
	}
	return func(c *config) {
		c.MaxRebroadcastPeriod = d
	}
}

// WithLogger sets the logger for the wallet
func WithLogger(l *zap.Logger) Option {
	return func(c *config) {
		c.Log = l
	}
}
