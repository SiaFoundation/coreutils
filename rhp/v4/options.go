package rhp

import (
	"time"

	"go.uber.org/zap"
)

// An Option sets an option on a Server.
type Option func(*Server)

// WithLog sets the logger for the server.
func WithLog(log *zap.Logger) Option {
	return func(s *Server) {
		s.log = log
	}
}

// WithPriceTableValidity sets the duration for which a price table is valid.
func WithPriceTableValidity(validity time.Duration) Option {
	return func(s *Server) {
		s.priceTableValidity = validity
	}
}

// WithContractProofWindowBuffer sets the buffer for revising a contract before
// its proof window starts.
func WithContractProofWindowBuffer(buffer uint64) Option {
	return func(s *Server) {
		s.contractProofWindowBuffer = buffer
	}
}
