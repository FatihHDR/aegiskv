package storage

import "container/heap"

// mergeHeap orders child iterators by their current internal key.
type mergeHeap []iterator

func (h mergeHeap) Len() int           { return len(h) }
func (h mergeHeap) Less(i, j int) bool { return compareInternal(h[i].Key(), h[j].Key()) < 0 }
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(iterator)) }
func (h *mergeHeap) Pop() any          { old := *h; n := len(old); it := old[n-1]; *h = old[:n-1]; return it }

// mergeIterator is a k-way merge over child iterators, yielding entries in
// internal-key order. Ties (identical internal keys from overlapping tables)
// are broken arbitrarily, which is safe because equal internal keys carry
// equal values.
type mergeIterator struct {
	h   mergeHeap
	all []iterator
}

// newMergeIterator positions every child at start (nil means "first") and
// returns a merged view over the non-empty ones.
func newMergeIterator(iters []iterator, start []byte) *mergeIterator {
	h := make(mergeHeap, 0, len(iters))
	for _, it := range iters {
		if start == nil {
			it.SeekToFirst()
		} else {
			it.Seek(start)
		}
		if it.Valid() {
			h = append(h, it)
		}
	}
	heap.Init(&h)
	return &mergeIterator{h: h, all: iters}
}

func (m *mergeIterator) Valid() bool   { return len(m.h) > 0 }
func (m *mergeIterator) Key() []byte   { return m.h[0].Key() }
func (m *mergeIterator) Value() []byte { return m.h[0].Value() }

func (m *mergeIterator) Next() {
	top := m.h[0]
	top.Next()
	if top.Valid() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}

// Close closes all child iterators.
func (m *mergeIterator) Close() error {
	var firstErr error
	for _, it := range m.all {
		if err := it.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.all = nil
	m.h = nil
	return firstErr
}
