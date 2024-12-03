// Package encoder provides an API for encoding data objects.
package encoder

import "io"

// Object is a data object. Data objects are hierarchical, split into distinct
// sections that contain their own hierarchy. Only one element of the hierarchy
// can be open at a given time.
type Object struct{}

// New creates a new Object where metadata is limited to the specified size.
func New(maxMetadataSize int) *Object

// OpenStreams opens a streams section in the object. OpenStreams fails if
// there is another open streams section.
//
// If opening a new streams section would exceed the maximum metadata size for
// Object, OpenStreams returns an error.
func (o *Object) OpenStreams() (*Streams, error)

// WriteTo serializes the object to the provided writer. WriteTo returns the
// number of bytes written and any error that occurred during writing. WriteTo
// fails if there is currently an open section.
func (o *Object) WriteTo(w io.Writer) (int64, error)
