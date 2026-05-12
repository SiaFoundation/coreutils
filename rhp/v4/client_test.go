package rhp

import (
	"errors"
	"testing"

	rhp4 "go.sia.tech/core/rhp/v4"
)

func TestClientErrWrapping(t *testing.T) {
	sentinel := errors.New("sentinel")

	// clientErr joins an outer ClientError with the inner error.
	err := clientErr("desc", sentinel)
	if code := rhp4.ErrorCode(err); code != rhp4.ErrorCodeClientError {
		t.Fatalf("expected ClientError, got %d", code)
	} else if !errors.Is(err, sentinel) {
		t.Fatal("expected inner sentinel to be in the chain")
	}

	// clientErrf produces a standalone ClientError without an inner.
	err = clientErrf("desc %d", 1)
	if code := rhp4.ErrorCode(err); code != rhp4.ErrorCodeClientError {
		t.Fatalf("expected ClientError, got %d", code)
	}
}
