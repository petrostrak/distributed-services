// Index is the file we store index entries in.

package log

import (
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
