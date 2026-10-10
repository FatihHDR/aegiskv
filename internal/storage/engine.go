package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// Options configures a StorageEngine. Zero values fall back to the defaults
// listed on each field.
type Options struct {
	// Dir is the directory holding the engine's on-disk files.
	Dir string
	// SyncWrites fsyncs the WAL before acknowledging every write (RPO = 0).
	SyncWrites bool

	// MemTableSizeBytes is the approximate memtable size that triggers a flush
	// to an SSTable. Default 4 MiB.
	MemTableSizeBytes int64
	// BlockSizeBytes is the target size of an SSTable data block. Default 4 KiB.
	BlockSizeBytes int
	// BloomBitsPerKey sizes the per-table bloom filter. Default 10 (~1% FP).
	BloomBitsPerKey int
	// MaxFileSizeBytes bounds a single compaction output file. Default 2 MiB.
	MaxFileSizeBytes int64

	// L0CompactionTrigger is the number of level-0 tables that triggers
	// compaction. Default 4.
	L0CompactionTrigger int
	// BaseLevelSizeBytes is the size budget of level 1; deeper levels grow by
	// LevelSizeMultiplier. Default 8 MiB.
	BaseLevelSizeBytes int64
	// LevelSizeMultiplier scales the per-level size budget. Default 10.
	LevelSizeMultiplier int
	// MaxLevels is the number of levels. Default 7.
	MaxLevels int

	// DisableBackground runs flushing and compaction only when Flush/Compact
	// are called, instead of on a background goroutine. Useful for tests.
	DisableBackground bool
}

func (o Options) withDefaults() Options {
	if o.MemTableSizeBytes <= 0 {
		o.MemTableSizeBytes = 4 << 20
	}
	if o.BlockSizeBytes <= 0 {
		o.BlockSizeBytes = 4 << 10
	}
	if o.BloomBitsPerKey <= 0 {
		o.BloomBitsPerKey = 10
	}
	if o.MaxFileSizeBytes <= 0 {
		o.MaxFileSizeBytes = 2 << 20
	}
	if o.L0CompactionTrigger <= 0 {
		o.L0CompactionTrigger = 4
	}
	if o.BaseLevelSizeBytes <= 0 {
		o.BaseLevelSizeBytes = 8 << 20
	}
	if o.LevelSizeMultiplier <= 0 {
		o.LevelSizeMultiplier = 10
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = 7
	}
	return o
}

// KV is a key/value pair returned by Scan.
type KV struct {
	Key   []byte
	Value []byte
}

// LevelStats describes one level of the LSM tree.
type LevelStats struct {
	Tables int
	Bytes  int64
}

// Stats summarizes engine state.
type Stats struct {
	Seq          uint64
	Keys         int64 // entries currently in the mutable memtable
	MemTableSize int64
	Levels       []LevelStats
	Flushes      int64
	Compactions  int64
}

// imm is a frozen memtable awaiting flush, together with the WAL number that
// backs it (the WAL is deleted only after the flush is durable).
type imm struct {
	mem *MemTable
	wal uint64
}

// Engine is a single-node LSM storage engine: a WAL-backed memtable that
// flushes to immutable SSTables, which are merged by background leveled
// compaction.
type Engine struct {
	opts Options

	mu       sync.RWMutex
	mem      *MemTable
	memBytes int64
	imms     []*imm
	ver      *version
	seq      uint64
	wal      *WAL
	walNum   uint64
	nextFile uint64
	closed   bool

	snapMu    sync.Mutex
	snapMin   uint64
	snapCount int

	bgMu    sync.Mutex
	workCh  chan struct{}
	closeCh chan struct{}
	wg      sync.WaitGroup

	errMu sync.Mutex
	bgErr error

	flushCount   atomic.Int64
	compactCount atomic.Int64
}

// Open opens (or creates) an engine rooted at opts.Dir. Existing SSTables are
// loaded and any WAL segments are replayed and flushed to a new SSTable so that
// previously acknowledged writes survive a crash.
func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, os.ErrInvalid
	}
	opts = opts.withDefaults()
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}

	e := &Engine{
		opts:    opts,
		mem:     NewMemTable(),
		ver:     newVersion(opts.MaxLevels),
		workCh:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	if err := e.recover(); err != nil {
		return nil, err
	}
	if !opts.DisableBackground {
		e.wg.Add(1)
		go e.backgroundLoop()
	}
	return e, nil
}

func (e *Engine) recover() error {
	sstFiles, walNums, err := scanDir(e.opts.Dir)
	if err != nil {
		return err
	}

	var maxTable, maxTableSeq uint64
	for _, name := range sstFiles {
		level, num, ok := parseSSTName(name)
		if !ok {
			continue
		}
		if level >= e.opts.MaxLevels {
			level = e.opts.MaxLevels - 1
		}
		t, err := openTable(filepath.Join(e.opts.Dir, name), num, level)
		if err != nil {
			return err
		}
		e.ver.levels[level] = append(e.ver.levels[level], t)
		if num > maxTable {
			maxTable = num
		}
		if t.maxSeq > maxTableSeq {
			maxTableSeq = t.maxSeq
		}
	}
	for _, lvl := range e.ver.levels {
		sort.Slice(lvl, func(i, j int) bool { return lvl[i].number < lvl[j].number })
	}
	e.nextFile = maxTable + 1

	var maxSeq, maxWal uint64
	for _, num := range walNums {
		if num > maxWal {
			maxWal = num
		}
		seq, err := ReplayWAL(e.walPath(num), func(seq uint64, recs []BatchRecord) error {
			for _, r := range recs {
				e.mem.Add(seq, r.Type, r.Key, r.Value)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	e.seq = maxSeq
	if maxTableSeq > e.seq {
		e.seq = maxTableSeq
	}
	e.memBytes = e.mem.ApproxBytes()

	// Persist recovered writes durably before discarding the WAL segments.
	if e.mem.Len() > 0 {
		num := e.nextFile
		e.nextFile++
		t, err := e.writeTable(e.mem, 0, num)
		if err != nil {
			return err
		}
		e.ver.levels[0] = append(e.ver.levels[0], t)
		e.mem = NewMemTable()
		e.memBytes = 0
	}
	for _, num := range walNums {
		os.Remove(e.walPath(num))
	}

	e.walNum = maxWal + 1
	w, err := openWAL(e.walPath(e.walNum))
	if err != nil {
		return err
	}
	e.wal = w
	return nil
}

// Put stores value under key, replacing any existing value.
func (e *Engine) Put(key, value []byte) error {
	return e.append([]BatchRecord{{Type: TypeValue, Key: key, Value: value}})
}

// Delete removes key by writing a tombstone.
func (e *Engine) Delete(key []byte) error {
	return e.append([]BatchRecord{{Type: TypeDelete, Key: key}})
}

func (e *Engine) append(recs []BatchRecord) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	seq := e.seq + 1
	batch := &WriteBatch{Seq: seq, Records: recs}
	if err := e.wal.Append(batch, e.opts.SyncWrites); err != nil {
		e.mu.Unlock()
		return err
	}
	e.seq = seq
	var added int64
	for _, r := range recs {
		e.mem.Add(seq, r.Type, r.Key, r.Value)
		added += int64(len(r.Key)+len(r.Value)) + 64
	}
	e.memBytes += added

	rotate := e.memBytes >= e.opts.MemTableSizeBytes
	if rotate {
		e.rotateLocked()
	}
	e.mu.Unlock()
	if rotate {
		e.signalWork()
	}
	return nil
}

// rotateLocked freezes the current memtable and starts a fresh WAL. The caller
// must hold e.mu.
func (e *Engine) rotateLocked() {
	newNum := e.walNum + 1
	w, err := openWAL(e.walPath(newNum))
	if err != nil {
		e.setErr(err)
		return
	}
	e.imms = append(e.imms, &imm{mem: e.mem, wal: e.walNum})
	e.mem = NewMemTable()
	e.memBytes = 0
	e.wal = w
	e.walNum = newNum
}

func (e *Engine) signalWork() {
	select {
	case e.workCh <- struct{}{}:
	default:
	}
}

func (e *Engine) backgroundLoop() {
	defer e.wg.Done()
	for {
		select {
		case <-e.closeCh:
			return
		case <-e.workCh:
			e.bgMu.Lock()
			e.flushAll()
			e.compactAll()
			e.bgMu.Unlock()
		}
	}
}

// Flush forces the current memtable to disk and drains all pending flushes.
func (e *Engine) Flush() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	if e.mem.Len() > 0 {
		e.rotateLocked()
	}
	e.mu.Unlock()

	e.signalWork()
	e.bgMu.Lock()
	e.flushAll()
	e.bgMu.Unlock()
	return e.err()
}

// Compact flushes and then runs leveled compaction until every level is within
// its size budget.
func (e *Engine) Compact() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrClosed
	}
	if e.mem.Len() > 0 {
		e.rotateLocked()
	}
	e.mu.Unlock()

	e.signalWork()
	e.bgMu.Lock()
	e.flushAll()
	e.compactAll()
	e.bgMu.Unlock()
	return e.err()
}

// flushAll writes every frozen memtable to a level-0 SSTable and removes its
// WAL once the table is durable.
func (e *Engine) flushAll() {
	for {
		e.mu.Lock()
		if e.closed || len(e.imms) == 0 {
			e.mu.Unlock()
			return
		}
		im := e.imms[0]
		e.imms = e.imms[1:]
		e.mu.Unlock()

		number := e.allocFile()
		t, err := e.writeTable(im.mem, 0, number)
		if err != nil {
			e.setErr(err)
			return
		}
		e.mu.Lock()
		e.installLocked(addTable(e.ver, 0, t))
		e.mu.Unlock()
		os.Remove(e.walPath(im.wal))
		e.flushCount.Add(1)
	}
}

// writeTable flushes a memtable into a new SSTable at the given level.
func (e *Engine) writeTable(mem *MemTable, level int, number uint64) (*table, error) {
	tmp := e.sstPath(level, number) + tmpSuffix
	b, err := newSSTableBuilder(tmp, e.opts.BlockSizeBytes)
	if err != nil {
		return nil, err
	}
	it := mem.NewIterator()
	it.SeekToFirst()
	for it.Valid() {
		if err := b.add(it.Key(), it.Value()); err != nil {
			b.f.Close()
			os.Remove(tmp)
			return nil, err
		}
		it.Next()
	}
	return b.finish(level, number, e.opts.BloomBitsPerKey)
}

// Get returns the value for key. The second result is false when the key is
// absent or deleted.
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, false, ErrClosed
	}
	snap, mem := e.seq, e.mem
	imms := append([]*imm(nil), e.imms...)
	ver := e.ver
	ver.refAll()
	e.mu.RUnlock()
	defer ver.unrefAll()
	e.registerSnapshot(snap)
	defer e.unregisterSnapshot()

	if v, typ, found := mem.lookup(key, snap); found {
		return valueOrDeleted(v, typ)
	}
	for i := len(imms) - 1; i >= 0; i-- {
		if v, typ, found := imms[i].mem.lookup(key, snap); found {
			return valueOrDeleted(v, typ)
		}
	}

	target := makeInternalKey(key, snap, TypeDelete)
	for i := len(ver.levels[0]) - 1; i >= 0; i-- {
		if v, typ, found := ver.levels[0][i].lookup(target); found {
			return valueOrDeleted(v, typ)
		}
	}
	for lvl := 1; lvl < len(ver.levels); lvl++ {
		for _, t := range ver.levels[lvl] {
			if v, typ, found := t.lookup(target); found {
				return valueOrDeleted(v, typ)
			}
		}
	}
	return nil, false, nil
}

func valueOrDeleted(v []byte, typ byte) ([]byte, bool, error) {
	if typ == TypeValue {
		return cloneBytes(v), true, nil
	}
	return nil, false, nil
}

// Scan returns all live key/value pairs with start <= key < end in key order.
// An empty start begins at the smallest key; an empty end is unbounded.
func (e *Engine) Scan(start, end []byte) ([]KV, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	snap, mem := e.seq, e.mem
	imms := append([]*imm(nil), e.imms...)
	ver := e.ver
	ver.refAll()
	e.mu.RUnlock()
	defer ver.unrefAll()
	e.registerSnapshot(snap)
	defer e.unregisterSnapshot()

	iters := make([]iterator, 0, 1+len(imms)+8)
	iters = append(iters, mem.NewIterator())
	for _, im := range imms {
		iters = append(iters, im.mem.NewIterator())
	}
	for _, lvl := range ver.levels {
		for _, t := range lvl {
			iters = append(iters, newTableIterator(t))
		}
	}

	var seek []byte
	if len(start) > 0 {
		seek = makeInternalKey(start, snap, TypeDelete)
	}
	mi := newMergeIterator(iters, seek)
	defer mi.Close()

	var out []KV
	var lastKey []byte
	haveLast := false
	for mi.Valid() {
		uk := userKeyOf(mi.Key())
		if len(end) > 0 && bytes.Compare(uk, end) >= 0 {
			break
		}
		if haveLast && bytes.Equal(uk, lastKey) {
			mi.Next()
			continue
		}
		lastKey = cloneBytes(uk)
		haveLast = true

		var value []byte
		live := false
		for mi.Valid() {
			if !bytes.Equal(userKeyOf(mi.Key()), lastKey) {
				break
			}
			if seqOf(mi.Key()) <= snap {
				if typeOf(mi.Key()) == TypeValue {
					value = cloneBytes(mi.Value())
					live = true
				}
				break
			}
			mi.Next()
		}
		if live {
			out = append(out, KV{Key: cloneBytes(lastKey), Value: value})
		}
		for mi.Valid() && bytes.Equal(userKeyOf(mi.Key()), lastKey) {
			mi.Next()
		}
	}
	return out, nil
}

// Stats returns a point-in-time summary of the engine.
func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s := Stats{
		Seq:          e.seq,
		Keys:         e.mem.Len(),
		MemTableSize: e.memBytes,
		Flushes:      e.flushCount.Load(),
		Compactions:  e.compactCount.Load(),
	}
	s.Levels = make([]LevelStats, len(e.ver.levels))
	for i, lvl := range e.ver.levels {
		var size int64
		for _, t := range lvl {
			size += t.size
		}
		s.Levels[i] = LevelStats{Tables: len(lvl), Bytes: size}
	}
	return s
}

// Close stops background work and closes open files. It is safe to call more
// than once. Pending writes remain durable in the WAL.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.mu.Unlock()

	close(e.closeCh)
	e.wg.Wait()

	e.mu.Lock()
	err := e.wal.Close()
	ver := e.ver
	e.mu.Unlock()

	for _, lvl := range ver.levels {
		for _, t := range lvl {
			t.closeFile()
		}
	}
	return err
}

// --- internal helpers ---

func (e *Engine) allocFile() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.nextFile
	e.nextFile++
	return n
}

func (e *Engine) currentSeq() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.seq
}

func (e *Engine) registerSnapshot(seq uint64) {
	e.snapMu.Lock()
	if e.snapCount == 0 || seq < e.snapMin {
		e.snapMin = seq
	}
	e.snapCount++
	e.snapMu.Unlock()
}

func (e *Engine) unregisterSnapshot() {
	e.snapMu.Lock()
	e.snapCount--
	if e.snapCount == 0 {
		e.snapMin = 0
	}
	e.snapMu.Unlock()
}

// smallestSnapshot returns the oldest active reader snapshot, or def when there
// are no readers. Compaction must not drop versions a live reader could need.
func (e *Engine) smallestSnapshot(def uint64) uint64 {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	if e.snapCount > 0 {
		return e.snapMin
	}
	return def
}

// installLocked publishes a new version and removes tables it dropped.
func (e *Engine) installLocked(nv *version) {
	old := e.ver
	e.ver = nv
	for t := range diffTables(old, nv) {
		t.markRemoved()
	}
}

func (e *Engine) setErr(err error) {
	e.errMu.Lock()
	if e.bgErr == nil {
		e.bgErr = err
	}
	e.errMu.Unlock()
}

func (e *Engine) err() error {
	e.errMu.Lock()
	defer e.errMu.Unlock()
	return e.bgErr
}

// addTable returns a new version with t appended to a level.
func addTable(v *version, level int, t *table) *version {
	nv := v.clone()
	nv.levels[level] = append(nv.levels[level], t)
	sort.Slice(nv.levels[level], func(i, j int) bool {
		return nv.levels[level][i].number < nv.levels[level][j].number
	})
	return nv
}

// diffTables returns the tables present in old but absent from nv.
func diffTables(old, nv *version) map[*table]struct{} {
	in := nv.tableSet()
	removed := make(map[*table]struct{})
	for _, lvl := range old.levels {
		for _, t := range lvl {
			if _, ok := in[t]; !ok {
				removed[t] = struct{}{}
			}
		}
	}
	return removed
}
