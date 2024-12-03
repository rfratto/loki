// Package decoder provides an API for decoding data objects.
package decoder

import "io"

// TODO(rfratto): At the moment this is being designed with the intent of
// reading entire data objects, specifically to test the encoder. We'll likely
// end up with a more general-purpose decoder that can decode parts of a data
// object and make more specific queries.

type Object struct{}

// Open a data object from the provided reader. Open returns an error if the
// data object is malformed.
func Open(r io.Reader) (*Object, error)

// TODO(rfratto): rest of basic API
