// Package encoding defines interfaces shared by other packages that encode
// binary data to and from data objects.
//
// Various encoding formats use these interfaces to provide streaming reads and
// writes. Streaming provides benefits such as packing as much data as possible
// into a single page (post-encoding and post-compression) and minimizing the
// cost of scanning through a page, at the cost of slightly slower performance.
//
// Storing as much data as possible in a page is critical for minimizing the
// number of pages that must be read to execute a query, and thus minimizing
// the number of requests made to the storage backend.
//
// This contrasts with the design of parquet-go, which targets a fixed read
// buffer size for loading all rows of a page into memory at once, and does not
// account for encoding and compression. Such a design is fine for disk-based
// storage systems, but is insufficient for object storage.
package encoding

import "io"

// Reader is an interface that combines an [io.Reader] and [io.ByteReader].
type Reader interface {
	io.Reader
	io.ByteReader
}

// Writer is an interface that combines an [io.Writer] and [io.ByteWriter].
type Writer interface {
	io.Writer
	io.ByteWriter
}
