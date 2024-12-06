package encoder

import (
	"errors"
	"fmt"
)

var (
	ErrElementExist = errors.New("open element already exists")
	ErrClosed       = errors.New("element is closed")

	errElementNoExist = fmt.Errorf("open element does not exist")
)

// EncodingError is an error that occurs during encoding.
type EncodingError struct {
	inner error
}

// Error returns a string representation of an EncodingError.
func (e EncodingError) Error() string {
	return fmt.Sprintf("encoding error: %v", e.inner)
}

// Unwrap returns the inner error of an EncodingError.
func (e EncodingError) Unwrap() error {
	return e.inner
}

// MetadataSizeError is returned when adding elements would cause the metadata
// to exceed the maximum size.
type MetadataSizeError struct {
	Max    int
	Actual int
}

// Error returns a string representation of a MetadataSizeError.
func (e MetadataSizeError) Error() string {
	return fmt.Sprintf("metadata size %d exceeds maximum %d", e.Actual, e.Max)
}
