package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
)

// DefaultWALFileName is used when Options.WALFileName is empty.
const DefaultWALFileName = "wal.log"

// Options configures a StorageEngine.
type Options struct {
	// Dir is the directory holding the engine's on-disk files.
	Dir string
	// SyncWrites fsyncs the WAL on every write before acknowledging it. It
	// trades throughput for durability (RPO = 0). Defaults to true when unset
	// via Open's semantics below.
	SyncWrites bool
	// WALFileName overrides the write-ahead log file name.
	WALFileName string
}

// KV is a key/value pair returned by Scan.
type KV struct {
	Key   []byte
	Value []byte
}

// Stats summarizes engine state.
type Stats struct {
	Keys int64  // number of entries in the memtable (versions + tombstones)
	Seq  uint64 // last allocated sequence number
}

// Engine is a single-node LSM storage engine: a WAL-backed MemTable. Writes go
// to the WAL first and are only acknowledged once the log write has been
// accepted, guaranteeing crash recovery.
type Engine struct {
	mu     sync.RWMutex
	opts   Options
	mem    *MemTable
	wal    *WAL
	seq    uint64
	closed bool
}

// Open opens (or creates) an engine rooted at opts.Dir, replaying any existing
// WAL into a fresh memtable so previously acknowledged writes survive a crash.
func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, os.ErrInvalid
	}
	if opts.WALFileName == "" {
		opts.WALFileName = DefaultWALFileName
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	e := &Engine{opts: opts, mem: NewMemTable()}
	walPath := filepath.Join(opts.Dir, opts.WALFileName)

	maxSeq, err := ReplayWAL(walPath, func(seq uint64, recs []BatchRecord) error {
		for _, r := range recs {
			e.mem.Add(seq, r.Type, r.Key, r.Value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.seq = maxSeq

	w, err := openWAL(walPath)
	if err != nil {
		return nil, err
	}
	e.wal = w
	return e, nil
}

// Put stores value under key, replacing any existing value.
func (e *Engine) Put(key, value []byte) error {
	return e.append([]BatchRecord{{Type: TypeValue, Key: key, Value: value}})
}

// Delete removes key by writing a tombstone. Deleting a missing key is a no-op
// that still advances the sequence number.
func (e *Engine) Delete(key []byte) error {
	return e.append([]BatchRecord{{Type: TypeDelete, Key: key}})
}

func (e *Engine) append(recs []BatchRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return ErrClosed
	}

	seq := e.seq + 1
	batch := &WriteBatch{Seq: seq, Records: recs}
	if err := e.wal.Append(batch, e.opts.SyncWrites); err != nil {
		// Do not apply to the memtable and do not reuse this sequence number:
		// a traced write may have reached disk.
		return err
	}
	e.seq = seq
	for _, r := range recs {
		e.mem.Add(seq, r.Type, r.Key, r.Value)
	}
	return nil
}

// Get returns the value for key. The second result is false when the key is
// absent or has been deleted.
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, false, ErrClosed
	}
	snap := e.seq
	mem := e.mem
	e.mu.RUnlock()

	v, ok := mem.Get(key, snap)
	if !ok {
		return nil, false, nil
	}
	return cloneBytes(v), true, nil
}

// Scan returns all live key/value pairs with start <= key < end in key order.
// An empty start begins at the smallest key; an empty end is unbounded.
func (e *Engine) Scan(start, end []byte) ([]KV, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	snap := e.seq
	mem := e.mem
	e.mu.RUnlock()

	it := mem.NewIterator()
	if len(start) == 0 {
		it.SeekToFirst()
	} else {
		it.Seek(makeInternalKey(start, snap, TypeDelete))
	}

	var out []KV
	var lastKey []byte
	haveLast := false
	for it.Valid() {
		uk := userKeyOf(it.Key())
		if len(end) > 0 && bytes.Compare(uk, end) >= 0 {
			break
		}
		if haveLast && bytes.Equal(uk, lastKey) {
			it.Next()
			continue
		}
		lastKey = cloneBytes(uk)
		haveLast = true

		// Pick the newest version visible at snap; older versions and any
		// versions above snap are irrelevant.
		var value []byte
		live := false
		for it.Valid() {
			ik := it.Key()
			if !bytes.Equal(userKeyOf(ik), lastKey) {
				break
			}
			if seqOf(ik) <= snap {
				if typeOf(ik) == TypeValue {
					value = cloneBytes(it.Value())
					live = true
				}
				break
			}
			it.Next()
		}
		if live {
			out = append(out, KV{Key: cloneBytes(lastKey), Value: value})
		}
		// Skip the remaining versions of this user key.
		for it.Valid() && bytes.Equal(userKeyOf(it.Key()), lastKey) {
			it.Next()
		}
	}
	return out, nil
}

// Stats returns a point-in-time summary of the engine.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return Stats{Keys: e.mem.Len(), Seq: e.seq}
}

// Close flushes and closes the underlying WAL. It is safe to call more than
// once; subsequent calls are no-ops.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.wal.Close()
}
