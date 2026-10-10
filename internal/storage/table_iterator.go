package storage

// tableIterator iterates an SSTable's entries in internal-key order by walking
// its data blocks. A block is loaded on demand and held until exhausted.
type tableIterator struct {
	t       *table
	pos     int
	entries []blockEntry
	ei      int
}

// newTableIterator returns an iterator positioned before the first entry.
func newTableIterator(t *table) *tableIterator {
	return &tableIterator{t: t}
}

func (it *tableIterator) Valid() bool {
	return it.entries != nil && it.ei < len(it.entries)
}

func (it *tableIterator) Key() []byte   { return it.entries[it.ei].key }
func (it *tableIterator) Value() []byte { return it.entries[it.ei].value }
func (it *tableIterator) Close() error  { return nil }

func (it *tableIterator) SeekToFirst() {
	if len(it.t.index) == 0 {
		it.entries = nil
		return
	}
	it.loadBlock(0)
}

func (it *tableIterator) Seek(target []byte) {
	i := it.t.blockFor(target)
	if i >= len(it.t.index) {
		it.entries = nil
		return
	}
	it.loadBlock(i)
	for it.Valid() && compareInternal(it.Key(), target) < 0 {
		it.ei++
	}
}

func (it *tableIterator) loadBlock(i int) {
	entries, err := it.t.readBlock(it.t.index[i])
	if err != nil {
		it.entries = nil
		return
	}
	it.pos = i
	it.entries = entries
	it.ei = 0
}

func (it *tableIterator) Next() {
	if !it.Valid() {
		return
	}
	it.ei++
	if it.ei >= len(it.entries) {
		if it.pos+1 >= len(it.t.index) {
			it.entries = nil
			return
		}
		it.loadBlock(it.pos + 1)
	}
}
