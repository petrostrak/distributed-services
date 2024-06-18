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

	mu  sync.RWMutex
	buf *bufio.Writer

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

// Append persists the given bytes to the store.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// The segment will use this position when it creates an associated index
	// entry for this record.
	pos = s.size

	// We write the length of the record so that, when we read the record,
	// we know how many bytes to read.
	err = binary.Write(s.buf, enc, uint64(len(p)))
	if err != nil {
		return 0, 0, err
	}

	// We write to the buffered writer instead of directly to the file to
	// reduce the number of system calls and improve performance.
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read returns the record stored at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// We flush the buffer, in case we're about to try to read
	// a record that the buffer hasn't flushed to disk yet.
	err := s.buf.Flush()
	if err != nil {
		return nil, err
	}

	// We find out how many bytes we have to read to get the whole
	// record.
	size := make([]byte, lenWidth)
	_, err = s.File.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}

	// We fetch the record.
	b := make([]byte, enc.Uint64(size))
	_, err = s.File.ReadAt(b, int64(pos+lenWidth))
	if err != nil {
		return nil, err
	}

	return b, err
}

// ReadAt reads len(p) bytes into p beginning at the off offset in the store's file.
// It implements io.ReaderAt on the store type.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	err := s.buf.Flush()
	if err != nil {
		return 0, nil
	}

	return s.File.ReadAt(p, off)
}

// Close persists any buffered data before closing the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}
