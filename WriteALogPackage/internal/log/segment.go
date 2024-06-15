package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/petrostrak/proglog/StructureDataWithProtobuf/api/v1"
	"google.golang.org/protobuf/proto"
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

// Append writes the record to the segment and returns the newly appended record's offset.
// The log returns the offset to the API response.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// The segment appends the data to the store.
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// It adds an index entry. Sinse index offsets are relative to the base
	// offset, we subtract the segment's next offset from its base offse to
	// get the entry's relative offset in the segment.
	err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	)
	if err != nil {
		return 0, err
	}

	// We increment the next offset to prep for a future append call.
	s.nextOffset++

	return cur, nil
}