package host

import (
	"time"

	"go.uber.org/zap"
)

// A ServerOption sets an option on a Server.
type ServerOption func(*Server)

// WithLog sets the logger for the server.
func WithLog(log *zap.Logger) ServerOption {
	return func(s *Server) {
		s.log = log
	}
}

// WithPriceTableValidity sets the duration for which a price table is valid.
func WithPriceTableValidity(validity time.Duration) ServerOption {
	return func(s *Server) {
		s.priceTableValidity = validity
	}
}

// WithContractProofWindowBuffer sets the buffer for revising a contract before
// its proof window starts.
func WithContractProofWindowBuffer(buffer uint64) ServerOption {
	return func(s *Server) {
		s.contractProofWindowBuffer = buffer
	}
}
