// Store is the file we store the records in.

package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	// enc defines the encoding that we persist record sizes and index entries in.
	enc = binary.BigEndian
)

const (
	// lenWidth defines the number of bytes used to store the record's length.
	lenWidth = 8
)

// store struct is a wrapper around a file with two APIs to append
// and read bytes to and from the file.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a store from the given file.
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}
