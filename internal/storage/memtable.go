package storage

import "bytes"

// MemTable is the in-memory write buffer of the LSM engine. It holds every
// write (values and tombstones) keyed by an internal key so that multiple
// versions of the same user key coexist and can be read at a point in time.
//
// All methods are safe for concurrent use.
type MemTable struct {
	skl *SkipList
}

// NewMemTable returns an empty MemTable.
func NewMemTable() *MemTable {
	return &MemTable{skl: NewSkipListWithComparator(compareInternal)}
}

// Add records a single write at the given sequence number. A TypeDelete record
// stores no value and acts as a tombstone.
func (m *MemTable) Add(seq uint64, typ byte, key, value []byte) {
	m.skl.Insert(makeInternalKey(key, seq, typ), value)
}

// Get returns the newest version of key visible at snapshot (sequence numbers
// <= snapshot). It reports false when the key is absent or the newest visible
// version is a tombstone.
func (m *MemTable) Get(key []byte, snapshot uint64) ([]byte, bool) {
	v, typ, found := m.lookup(key, snapshot)
	if !found || typ == TypeDelete {
		return nil, false
	}
	return v, true
}

// lookup returns the newest version of key at or below snapshot together with
// its record type. Unlike Get it also reports tombstones, which callers need to
// stop a multi-level search at the newest version of a key.
func (m *MemTable) lookup(key []byte, snapshot uint64) ([]byte, byte, bool) {
	it := m.skl.NewIterator()
	it.Seek(makeInternalKey(key, snapshot, TypeDelete))
	if !it.Valid() || !bytes.Equal(userKeyOf(it.Key()), key) {
		return nil, 0, false
	}
	return it.Value(), typeOf(it.Key()), true
}

// NewIterator returns an iterator over internal keys, ordered by user key
// ascending, then newest version first.
func (m *MemTable) NewIterator() *Iterator {
	return m.skl.NewIterator()
}

// Len reports the number of stored versions (including tombstones).
func (m *MemTable) Len() int64 { return m.skl.Len() }

// ApproxBytes reports the approximate heap footprint of the memtable.
func (m *MemTable) ApproxBytes() int64 { return m.skl.ApproxBytes() }
