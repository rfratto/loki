// Package encoder provides an API for encoding data objects.
package encoder

import "io"

// TODO(rfratto): The implementation of this package is going to require some
// thought.
//
// It seems that the best way to do it is for each element to buffer as much of
// its data in memory as it can (to allow for discarding), and only when it is
// closed will it internally append its data to its parent.
//
// There will need to be some global offset tracking; Column.AppendPage will
// need to know the Object-scoped offset where it would write its data so it
// can put the appropriate offsets in the metadata.
//
// Since only one element can be open at a time, it should be fairly easy to
// pass that state around; the offset can propagate down from Object to any
// element. When an element closes itself and passes its data to its parent,
// the parent can update its offset based on the size of the data it received.
//
// Since elements have an effective "lock" on the section of the object being
// written to (and they accumulate data), there's no need for elements to
// manually propagate the offset upwards; this happens as a side effect of them
// passing their data to their parent.

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
