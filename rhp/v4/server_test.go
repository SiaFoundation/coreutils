package rhp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/quic-go/quic-go"
	"go.sia.tech/mux/v2"
)

func TestIsPeerClosed(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		expect bool
	}{
		{
			name:   "nil error",
			err:    nil,
			expect: false,
		},
		{
			name:   "unrelated error",
			err:    errors.New("some other error"),
			expect: false,
		},
		{
			name:   "mux peer closed conn",
			err:    mux.ErrPeerClosedConn,
			expect: true,
		},
		{
			name:   "mux peer closed stream",
			err:    mux.ErrPeerClosedStream,
			expect: true,
		},
		{
			name:   "wrapped mux peer closed conn",
			err:    fmt.Errorf("wrapped: %w", mux.ErrPeerClosedConn),
			expect: true,
		},
		{
			name:   "wrapped mux peer closed stream",
			err:    fmt.Errorf("wrapped: %w", mux.ErrPeerClosedStream),
			expect: true,
		},
		{
			name:   "remote QUIC application error",
			err:    &quic.ApplicationError{Remote: true},
			expect: true,
		},
		{
			name:   "local QUIC application error",
			err:    &quic.ApplicationError{Remote: false},
			expect: false,
		},
		{
			name:   "wrapped remote QUIC application error",
			err:    fmt.Errorf("wrapped: %w", &quic.ApplicationError{Remote: true}),
			expect: true,
		},
		{
			name:   "remote QUIC stream error",
			err:    &quic.StreamError{Remote: true},
			expect: true,
		},
		{
			name:   "local QUIC stream error",
			err:    &quic.StreamError{Remote: false},
			expect: false,
		},
		{
			name:   "wrapped remote QUIC stream error",
			err:    fmt.Errorf("wrapped: %w", &quic.StreamError{Remote: true}),
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPeerClosed(tt.err); got != tt.expect {
				t.Errorf("isPeerClosed() = %v, want %v", got, tt.expect)
			}
		})
	}
}
