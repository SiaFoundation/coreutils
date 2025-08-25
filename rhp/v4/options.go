package rhp

import (
	"time"
)

// An ServerOption sets an option on a Server.
type ServerOption func(*Server)

// WithPriceTableValidity sets the duration for which a price table is valid.
func WithPriceTableValidity(validity time.Duration) ServerOption {
	return func(s *Server) {
		s.priceTableValidity = validity
	}
}

// WithRPCTimeout sets the duration for which an RPC request is allowed to run.
func WithRPCTimeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.rpcTimeout = timeout
	}
}
