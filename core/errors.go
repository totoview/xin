package core

import "github.com/pkg/errors"

var (
	// ErrTimeout is a generic timeout error
	ErrTimeout = errors.Errorf("Timeout")

	// ErrNotFound is a lookup failure
	ErrNotFound = errors.Errorf("NotFound")

	// ErrInvalidData is an invalid data error
	ErrInvalidData = errors.Errorf("InvalidData")
)

// WrapErr wraps multiple errors into one
func WrapErr(finalErr error, err error) error {
	if err == nil {
		return finalErr
	}
	if finalErr == nil {
		return errors.WithStack(err)
	}
	return errors.Wrap(finalErr, err.Error())
}
