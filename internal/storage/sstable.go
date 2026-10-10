package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
	"sync"
	"sync/atomic"
)

// sstableMagic identifies the AegisKV SSTable format in the footer.
const sstableMagic = 0x41454749534B5631 // "AEGISKV1"

// footerSize is the fixed trailing metadata size:
// magic(8) indexOff(8) indexLen(8) bloomOff(8) bloomLen(8) count(8) maxSeq(8).
const footerSize = 56

// tmpSuffix marks a table still being written; such files are removed (or
// ignored) on Open so a crash mid-flush never yields a half-written table.
const tmpSuffix = ".tmp"

// indexEntry locates one data block and bounds its key range.
type indexEntry struct {
	firstKey []byte
	lastKey  []byte
	offset   int64
	length   int64
}

// blockEntry is a decoded key/value pair from a data block.
type blockEntry struct {
	key   []byte
	value []byte
}

// table is an immutable, open SSTable. Data blocks are read on demand; the
// index and bloom filter are held in memory. Forward-only file handles are
// reference counted so compaction can delete files still in use by readers.
type table struct {
	number uint64
	level  int
	path   string
	file   *os.File
	size   int64
	index  []indexEntry
	bloom  *bloomFilter
	minKey []byte // smallest internal key
	maxKey []byte // largest internal key
	maxSeq uint64 // highest sequence number stored

	refs     atomic.Int64
	mu       sync.Mutex
	obsolete bool
	closed   bool
}

// sstableBuilder writes an SSTable to a temporary file and atomically renames
// it into place on finish.
type sstableBuilder struct {
	f         *os.File
	finalPath string
	offset    int64
	blockSize int

	buf        []byte
	blockFirst []byte
	blockLast  []byte

	index     []indexEntry
	bloomKeys [][]byte
	minKey    []byte
	maxKey    []byte
	maxSeq    uint64
	count     int
}

func newSSTableBuilder(path string, blockSize int) (*sstableBuilder, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if blockSize <= 0 {
		blockSize = 4096
	}
	return &sstableBuilder{f: f, finalPath: path, blockSize: blockSize}, nil
}

// add appends an internal key and its value. Keys must arrive in
// compareInternal order (ascending).
func (b *sstableBuilder) add(key, value []byte) error {
	if len(b.buf) >= b.blockSize {
		if err := b.flushBlock(); err != nil {
			return err
		}
	}
	if b.blockFirst == nil {
		b.blockFirst = cloneBytes(key)
	}
	b.blockLast = cloneBytes(key)

	b.buf = binary.BigEndian.AppendUint32(b.buf, uint32(len(key)))
	b.buf = append(b.buf, key...)
	b.buf = binary.BigEndian.AppendUint32(b.buf, uint32(len(value)))
	b.buf = append(b.buf, value...)

	if b.minKey == nil {
		b.minKey = cloneBytes(key)
	}
	b.maxKey = cloneBytes(key)
	if s := seqOf(key); s > b.maxSeq {
		b.maxSeq = s
	}
	b.bloomKeys = append(b.bloomKeys, cloneBytes(userKeyOf(key)))
	b.count++
	return nil
}

func (b *sstableBuilder) flushBlock() error {
	if len(b.buf) == 0 {
		return nil
	}
	if _, err := b.f.WriteAt(b.buf, b.offset); err != nil {
		return err
	}
	b.index = append(b.index, indexEntry{
		firstKey: b.blockFirst,
		lastKey:  b.blockLast,
		offset:   b.offset,
		length:   int64(len(b.buf)),
	})
	b.offset += int64(len(b.buf))
	b.buf = b.buf[:0]
	b.blockFirst = nil
	b.blockLast = nil
	return nil
}

// finish flushes all blocks, writes the filter and index, appends the footer,
// fsyncs, renames the temp file into place, and returns the opened table.
func (b *sstableBuilder) finish(level int, number uint64, bitsPerKey int) (*table, error) {
	if err := b.flushBlock(); err != nil {
		return nil, err
	}

	bloomBytes := buildBloom(b.bloomKeys, bitsPerKey).encode()
	bloomOff := b.offset
	if _, err := b.f.WriteAt(bloomBytes, bloomOff); err != nil {
		return nil, err
	}
	b.offset += int64(len(bloomBytes))

	indexBytes := b.encodeIndex()
	indexOff := b.offset
	if _, err := b.f.WriteAt(indexBytes, indexOff); err != nil {
		return nil, err
	}
	b.offset += int64(len(indexBytes))

	footer := make([]byte, footerSize)
	binary.BigEndian.PutUint64(footer[0:8], sstableMagic)
	binary.BigEndian.PutUint64(footer[8:16], uint64(indexOff))
	binary.BigEndian.PutUint64(footer[16:24], uint64(len(indexBytes)))
	binary.BigEndian.PutUint64(footer[24:32], uint64(bloomOff))
	binary.BigEndian.PutUint64(footer[32:40], uint64(len(bloomBytes)))
	binary.BigEndian.PutUint64(footer[40:48], uint64(b.count))
	binary.BigEndian.PutUint64(footer[48:56], b.maxSeq)
	if _, err := b.f.WriteAt(footer, b.offset); err != nil {
		return nil, err
	}
	if err := b.f.Sync(); err != nil {
		b.f.Close()
		return nil, err
	}
	if err := b.f.Close(); err != nil {
		return nil, err
	}

	final := b.finalPath
	if len(final) > len(tmpSuffix) && final[len(final)-len(tmpSuffix):] == tmpSuffix {
		final = final[:len(final)-len(tmpSuffix)]
		if err := os.Rename(b.finalPath, final); err != nil {
			return nil, err
		}
	}
	return openTable(final, number, level)
}

func (b *sstableBuilder) encodeIndex() []byte {
	var out []byte
	for _, e := range b.index {
		out = binary.BigEndian.AppendUint32(out, uint32(len(e.firstKey)))
		out = append(out, e.firstKey...)
		out = binary.BigEndian.AppendUint32(out, uint32(len(e.lastKey)))
		out = append(out, e.lastKey...)
		out = binary.BigEndian.AppendUint64(out, uint64(e.offset))
		out = binary.BigEndian.AppendUint64(out, uint64(e.length))
	}
	return out
}

// openTable opens and validates an SSTable, loading its index and bloom filter.
func openTable(path string, number uint64, level int) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := fi.Size()
	if size < footerSize {
		f.Close()
		return nil, ErrCorrupt
	}

	footer := make([]byte, footerSize)
	if _, err := f.ReadAt(footer, size-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	if binary.BigEndian.Uint64(footer[0:8]) != sstableMagic {
		f.Close()
		return nil, ErrCorrupt
	}
	indexOff := int64(binary.BigEndian.Uint64(footer[8:16]))
	indexLen := int64(binary.BigEndian.Uint64(footer[16:24]))
	bloomOff := int64(binary.BigEndian.Uint64(footer[24:32]))
	bloomLen := int64(binary.BigEndian.Uint64(footer[32:40]))
	maxSeq := binary.BigEndian.Uint64(footer[48:56])

	indexBytes := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBytes, indexOff); err != nil {
		f.Close()
		return nil, err
	}
	index, err := parseIndex(indexBytes)
	if err != nil {
		f.Close()
		return nil, err
	}
	bloomBytes := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBytes, bloomOff); err != nil {
		f.Close()
		return nil, err
	}

	t := &table{
		number: number,
		level:  level,
		path:   path,
		file:   f,
		size:   size,
		index:  index,
		bloom:  decodeBloom(bloomBytes),
		maxSeq: maxSeq,
	}
	if len(index) > 0 {
		t.minKey = index[0].firstKey
		t.maxKey = index[len(index)-1].lastKey
	}
	return t, nil
}

func parseIndex(data []byte) ([]indexEntry, error) {
	var entries []indexEntry
	for len(data) > 0 {
		first, rest, err := readLenBytes(data)
		if err != nil {
			return nil, err
		}
		last, rest2, err := readLenBytes(rest)
		if err != nil {
			return nil, err
		}
		if len(rest2) < 16 {
			return nil, ErrCorrupt
		}
		offset := int64(binary.BigEndian.Uint64(rest2[0:8]))
		length := int64(binary.BigEndian.Uint64(rest2[8:16]))
		data = rest2[16:]
		entries = append(entries, indexEntry{
			firstKey: cloneBytes(first),
			lastKey:  cloneBytes(last),
			offset:   offset,
			length:   length,
		})
	}
	return entries, nil
}

// ref increments the reference count so the file handle survives compaction.
func (t *table) ref() { t.refs.Add(1) }

// unref releases a reference, closing the file once it is both unreferenced
// and obsolete.
func (t *table) unref() {
	if t.refs.Add(-1) == 0 {
		t.mu.Lock()
		if t.obsolete && !t.closed {
			t.file.Close()
			t.closed = true
		}
		t.mu.Unlock()
	}
}

// markRemoved drops the table from the version set: its file is unlinked (safe
// while a reader still holds the descriptor on Unix) and its descriptor is
// closed once the last reader releases it.
func (t *table) markRemoved() {
	t.mu.Lock()
	t.obsolete = true
	if t.refs.Load() == 0 && !t.closed {
		t.file.Close()
		t.closed = true
	}
	t.mu.Unlock()
	os.Remove(t.path)
}

// closeFile releases the descriptor without deleting the file, used on shutdown.
func (t *table) closeFile() {
	t.mu.Lock()
	if !t.closed {
		t.file.Close()
		t.closed = true
	}
	t.mu.Unlock()
}

func (t *table) readBlock(e indexEntry) ([]blockEntry, error) {
	buf := make([]byte, e.length)
	if _, err := t.file.ReadAt(buf, e.offset); err != nil {
		return nil, err
	}
	return parseBlock(buf)
}

func parseBlock(data []byte) ([]blockEntry, error) {
	var entries []blockEntry
	for len(data) > 0 {
		key, rest, err := readLenBytes(data)
		if err != nil {
			return nil, err
		}
		val, rest2, err := readLenBytes(rest)
		if err != nil {
			return nil, err
		}
		data = rest2
		entries = append(entries, blockEntry{key: key, value: val})
	}
	return entries, nil
}

// blockFor returns the index of the first block whose last key is >= target.
func (t *table) blockFor(target []byte) int {
	return sort.Search(len(t.index), func(i int) bool {
		return compareInternal(t.index[i].lastKey, target) >= 0
	})
}

// findGE returns the first entry with an internal key >= target, or ok=false.
// It first rejects keys outside the table's user-key range or absent from the
// bloom filter, avoiding disk reads for most misses.
func (t *table) findGE(target []byte) ([]byte, []byte, bool) {
	if len(t.index) == 0 {
		return nil, nil, false
	}
	uk := userKeyOf(target)
	if bytes.Compare(uk, userKeyOf(t.minKey)) < 0 || bytes.Compare(uk, userKeyOf(t.maxKey)) > 0 {
		return nil, nil, false
	}
	if !t.bloom.mayContain(uk) {
		return nil, nil, false
	}
	i := t.blockFor(target)
	if i >= len(t.index) {
		return nil, nil, false
	}
	entries, err := t.readBlock(t.index[i])
	if err != nil {
		return nil, nil, false
	}
	for _, e := range entries {
		if compareInternal(e.key, target) >= 0 {
			return e.key, e.value, true
		}
	}
	return nil, nil, false
}

// lookup returns the newest version of the target's user key at or below the
// target, or found=false if the table has none.
func (t *table) lookup(target []byte) ([]byte, byte, bool) {
	k, v, ok := t.findGE(target)
	if !ok || !bytes.Equal(userKeyOf(k), userKeyOf(target)) {
		return nil, 0, false
	}
	return v, typeOf(k), true
}
