package storage

// version is an immutable snapshot of the level structure. It is swapped
// wholesale under the engine lock; readers capture a pointer and reference the
// tables they will touch so compaction cannot close a file mid-read.
type version struct {
	levels [][]*table
}

func newVersion(maxLevels int) *version {
	return &version{levels: make([][]*table, maxLevels)}
}

// clone produces a shallow copy whose level slices can be edited independently.
func (v *version) clone() *version {
	nv := &version{levels: make([][]*table, len(v.levels))}
	for i, lvl := range v.levels {
		nv.levels[i] = append([]*table(nil), lvl...)
	}
	return nv
}

// refAll increments the reference count of every table in the version.
func (v *version) refAll() {
	for _, lvl := range v.levels {
		for _, t := range lvl {
			t.ref()
		}
	}
}

// unrefAll releases every table reference taken by refAll.
func (v *version) unrefAll() {
	for _, lvl := range v.levels {
		for _, t := range lvl {
			t.unref()
		}
	}
}

// deepestLevel returns the highest level index that holds any table, or -1.
func (v *version) deepestLevel() int {
	for i := len(v.levels) - 1; i >= 0; i-- {
		if len(v.levels[i]) > 0 {
			return i
		}
	}
	return -1
}

// levelSize returns the total on-disk bytes of a level.
func (v *version) levelSize(level int) int64 {
	var total int64
	for _, t := range v.levels[level] {
		total += t.size
	}
	return total
}

// tableSet returns a set of the table pointers present in the version.
func (v *version) tableSet() map[*table]struct{} {
	set := make(map[*table]struct{})
	for _, lvl := range v.levels {
		for _, t := range lvl {
			set[t] = struct{}{}
		}
	}
	return set
}
