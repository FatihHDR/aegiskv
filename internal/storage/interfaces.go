package storage

// iterator is the internal cursor interface shared by memtables and SSTables.
// Keys are internal keys ordered by compareInternal (user key ascending, then
// newest version first).
type iterator interface {
	Seek(target []byte)
	SeekToFirst()
	Valid() bool
	Key() []byte
	Value() []byte
	Next()
	Close() error
}
