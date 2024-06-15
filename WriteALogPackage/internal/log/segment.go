package log

import (
	"fmt"
	"os"
	"path"
)

type segment struct {
	store      *store
	index      *index
	baseOffset uint64
	nextOffset uint64
	config     Config
}

// newSegment creates a new segment, such as when the current active segment
// hits its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	// If the index is empty, the next record appended to the segment would be
	// the first record and its offset would be the segment's base offset.
	off, _, err := s.index.Read(-1)
	if err != nil {
		return nil, err
	} else {
		// If the index has at least one entry, then that means the offset
		// of the next record written should take the offset at the end of
		// the segment, which we get by adding 1 to the base offset and
		// relative offset.
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}
