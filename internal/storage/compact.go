package storage

import (
	"bytes"
	"sort"
)

// compactionPlan describes one compaction: the input tables to merge and the
// level their output belongs to.
type compactionPlan struct {
	level          int
	outputLevel    int
	inputs         []*table
	dropTombstones bool
}

// levelBudget returns the size budget of a level beyond level 0.
func (e *Engine) levelBudget(level int) int64 {
	budget := e.opts.BaseLevelSizeBytes
	for i := 1; i < level; i++ {
		budget *= int64(e.opts.LevelSizeMultiplier)
	}
	return budget
}

// pickCompaction selects the most over-full level, or nil when the tree is
// within budget. It must be called without holding e.mu.
func (e *Engine) pickCompaction() *compactionPlan {
	e.mu.RLock()
	ver := e.ver
	trigger := e.opts.L0CompactionTrigger
	e.mu.RUnlock()

	if len(ver.levels[0]) >= trigger {
		return e.planForLevel(ver, 0)
	}
	for lvl := 1; lvl < len(ver.levels)-1; lvl++ {
		if ver.levelSize(lvl) > e.levelBudget(lvl) {
			return e.planForLevel(ver, lvl)
		}
	}
	return nil
}

func (e *Engine) planForLevel(ver *version, level int) *compactionPlan {
	inputs := append([]*table(nil), ver.levels[level]...)
	minUser, maxUser := userRange(inputs)
	next := level + 1

	for _, t := range ver.levels[next] {
		if tableOverlaps(t, minUser, maxUser) {
			inputs = append(inputs, t)
		}
	}

	// Tombstones may be dropped only when nothing older can live below the
	// output level.
	drop := next >= ver.deepestLevel()
	return &compactionPlan{
		level:          level,
		outputLevel:    next,
		inputs:         inputs,
		dropTombstones: drop,
	}
}

// compactAll runs compaction until no level exceeds its budget.
func (e *Engine) compactAll() {
	for {
		plan := e.pickCompaction()
		if plan == nil {
			return
		}
		if err := e.doCompaction(plan); err != nil {
			e.setErr(err)
			return
		}
		e.compactCount.Add(1)
	}
}

// doCompaction merges the plan's inputs in key order, discarding versions that
// are shadowed for every active snapshot and (at the bottom level) tombstones,
// then installs the result as the new output level.
func (e *Engine) doCompaction(plan *compactionPlan) error {
	iters := make([]iterator, 0, len(plan.inputs))
	for _, t := range plan.inputs {
		iters = append(iters, newTableIterator(t))
	}
	mi := newMergeIterator(iters, nil)
	defer mi.Close()

	smallest := e.smallestSnapshot(e.currentSeq())
	w := &tableWriter{engine: e, level: plan.outputLevel}

	var lastUser []byte
	haveLast := false
	var lastSeqForKey uint64

	for mi.Valid() {
		key := mi.Key()
		uk := userKeyOf(key)
		seq := seqOf(key)
		typ := typeOf(key)

		if !haveLast || !bytes.Equal(uk, lastUser) {
			haveLast = true
			lastUser = cloneBytes(uk)
			lastSeqForKey = 0
		}

		drop := false
		// A newer version already encountered is visible to every snapshot, so
		// any older version can never be read and is dropped.
		if lastSeqForKey != 0 && lastSeqForKey <= smallest {
			drop = true
		}
		if !drop && typ == TypeDelete && plan.dropTombstones && seq <= smallest {
			drop = true
		}

		if !drop {
			if err := w.add(key, mi.Value()); err != nil {
				return err
			}
		}
		// Record the newest sequence for this user key even when the record was
		// dropped, so subsequent (older) versions are retired too.
		lastSeqForKey = seq
		mi.Next()
	}

	outputs, err := w.finish()
	if err != nil {
		return err
	}

	inputSet := make(map[*table]struct{}, len(plan.inputs))
	for _, t := range plan.inputs {
		inputSet[t] = struct{}{}
	}

	e.mu.Lock()
	nv := e.ver.clone()
	nv.levels[plan.level] = filterOut(e.ver.levels[plan.level], inputSet)
	kept := filterOut(e.ver.levels[plan.outputLevel], inputSet)
	nv.levels[plan.outputLevel] = append(kept, outputs...)
	sort.Slice(nv.levels[plan.outputLevel], func(i, j int) bool {
		return nv.levels[plan.outputLevel][i].number < nv.levels[plan.outputLevel][j].number
	})
	e.installLocked(nv)
	e.mu.Unlock()
	return nil
}

// tableWriter streams merged entries into one or more output SSTables, rolling
// to a new file at a user-key boundary once MaxFileSizeBytes is reached.
type tableWriter struct {
	engine   *Engine
	level    int
	cur      *sstableBuilder
	curNum   uint64
	curSize  int64
	lastUser []byte
	out      []*table
}

func (w *tableWriter) add(key, value []byte) error {
	uk := userKeyOf(key)
	roll := w.cur != nil && w.curSize >= w.engine.opts.MaxFileSizeBytes &&
		len(w.lastUser) > 0 && !bytes.Equal(uk, w.lastUser)
	if roll {
		if err := w.finishCurrent(); err != nil {
			return err
		}
	}
	if w.cur == nil {
		num := w.engine.allocFile()
		path := w.engine.sstPath(w.level, num) + tmpSuffix
		b, err := newSSTableBuilder(path, w.engine.opts.BlockSizeBytes)
		if err != nil {
			return err
		}
		w.cur = b
		w.curNum = num
		w.curSize = 0
	}
	if err := w.cur.add(key, value); err != nil {
		return err
	}
	w.curSize = w.cur.offset + int64(len(w.cur.buf))
	w.lastUser = cloneBytes(uk)
	return nil
}

func (w *tableWriter) finishCurrent() error {
	if w.cur == nil {
		return nil
	}
	t, err := w.cur.finish(w.level, w.curNum, w.engine.opts.BloomBitsPerKey)
	w.cur = nil
	if err != nil {
		return err
	}
	w.out = append(w.out, t)
	return nil
}

func (w *tableWriter) finish() ([]*table, error) {
	if err := w.finishCurrent(); err != nil {
		return nil, err
	}
	return w.out, nil
}

// userRange returns the smallest and largest user keys spanned by a table set.
func userRange(tables []*table) (minUser, maxUser []byte) {
	for _, t := range tables {
		if len(t.index) == 0 {
			continue
		}
		lo := userKeyOf(t.minKey)
		hi := userKeyOf(t.maxKey)
		if minUser == nil || bytes.Compare(lo, minUser) < 0 {
			minUser = lo
		}
		if maxUser == nil || bytes.Compare(hi, maxUser) > 0 {
			maxUser = hi
		}
	}
	return minUser, maxUser
}

func tableOverlaps(t *table, minUser, maxUser []byte) bool {
	if len(t.index) == 0 || minUser == nil {
		return false
	}
	lo := userKeyOf(t.minKey)
	hi := userKeyOf(t.maxKey)
	return bytes.Compare(hi, minUser) >= 0 && bytes.Compare(lo, maxUser) <= 0
}

func filterOut(tables []*table, remove map[*table]struct{}) []*table {
	out := make([]*table, 0, len(tables))
	for _, t := range tables {
		if _, drop := remove[t]; !drop {
			out = append(out, t)
		}
	}
	return out
}
