// Index is the file we store index entries in.

package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	// record's offset (4bytes)
	offWidth uint64 = 4

	// record's position (8bytes)
	posWidth uint64 = 8

	// position of an entry
	entWidth = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates an index for the given file.
func newIndex(f *os.File, c Config) (*index, error) {
	// We create the index with the file.
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// We save the current size of the file so we can track
	// the amount of data in the index file as we add index
	// entries.
	idx.size = uint64(fi.Size())

	// We grow the file to the max index size before memory-mapping
	// the file.
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	// We return the index to the caller.
	return idx, nil
}

// Close makes sure the memory-mapped file has synced its data to the
// persisted file and that the persisted file has flushed its contents
// to stable storage.
func (i *index) Close() error {
	err := i.mmap.Sync(gommap.MS_SYNC)
	if err != nil {
		return err
	}

	err = i.file.Sync()
	if err != nil {
		return err
	}

	err = i.file.Truncate(int64(i.size))
	if err != nil {
		return err
	}

	return i.file.Close()
}

// Read takes in an offset and returns the associated record's position in the store.
// The given offset is relative to the segment's base offset. 0 is always the offset
// of the index's first entry, 1 is the second entry, and so on.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Write appends the given offset and position to the index.
func (i *index) Write(off uint32, pos uint64) error {
	// We validate that we have space to write the entry.
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// If there's space, we then encode the offset and position
	// and write them to the memory-mapped file.
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// We increment the position where the next write will go.
	i.size += uint64(entWidth)

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
