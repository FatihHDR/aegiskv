package storage

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxHeight = 16
	// branching is the inverse probability of promoting a node to the next
	// level (1/4), giving expected O(log N) search with low memory overhead.
	branching = 4
)

// skipListNode is a single entry. Forward pointers are stored atomically so
// that readers can traverse the list without taking a lock. Nodes are never
// removed, which keeps lock-free reads safe for the lifetime of the list.
type skipListNode struct {
	key   []byte
	value []byte
	next  []atomic.Pointer[skipListNode]
}

// SkipList is a thread-safe ordered map. Writers are serialized by a single
// mutex, while readers traverse immutable forward pointers lock-free. The key
// ordering is supplied by cmp (defaults to lexical byte order).
type SkipList struct {
	head  *skipListNode
	level atomic.Int32
	count atomic.Int64
	bytes atomic.Int64
	cmp   func(a, b []byte) int
	rng   *rand.Rand
	mu    sync.Mutex
}

// NewSkipList returns a SkipList ordered by unsigned lexicographic byte order.
func NewSkipList() *SkipList {
	return NewSkipListWithComparator(bytes.Compare)
}

// NewSkipListWithComparator returns a SkipList ordered by cmp. cmp must be a
// deterministic total order over the keys that will be inserted.
func NewSkipListWithComparator(cmp func(a, b []byte) int) *SkipList {
	s := &SkipList{
		head: &skipListNode{next: make([]atomic.Pointer[skipListNode], maxHeight)},
		cmp:  cmp,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	s.level.Store(1)
	return s
}

// Len reports the number of stored entries.
func (s *SkipList) Len() int64 { return s.count.Load() }

// ApproxBytes reports the approximate heap footprint of stored keys and values.
func (s *SkipList) ApproxBytes() int64 { return s.bytes.Load() }

func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && s.rng.Intn(branching) == 0 {
		h++
	}
	return h
}

// Insert adds key/value, copying both so the caller retains ownership. If the
// key already exists its value is replaced.
func (s *SkipList) Insert(key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insert(cloneBytes(key), cloneBytes(value))
}

func (s *SkipList) insert(key, value []byte) {
	prev := make([]*skipListNode, maxHeight)
	x := s.head
	top := int(s.level.Load())
	for i := top - 1; i >= 0; i-- {
		for {
			n := x.next[i].Load()
			if n != nil && s.cmp(n.key, key) < 0 {
				x = n
				continue
			}
			break
		}
		prev[i] = x
	}

	if n := prev[0].next[0].Load(); n != nil && s.cmp(n.key, key) == 0 {
		s.bytes.Add(int64(len(value)) - int64(len(n.value)))
		n.value = value
		return
	}

	h := s.randomHeight()
	if h > top {
		for i := top; i < h; i++ {
			prev[i] = s.head
		}
		s.level.Store(int32(h))
	}

	node := &skipListNode{
		key:   key,
		value: value,
		next:  make([]atomic.Pointer[skipListNode], h),
	}
	// Link bottom-up so the level-0 pointer (the linearization point) is
	// published last among the levels a reader can reach.
	for i := 0; i < h; i++ {
		node.next[i].Store(prev[i].next[i].Load())
		prev[i].next[i].Store(node)
	}
	s.count.Add(1)
	s.bytes.Add(int64(len(key) + len(value)))
}

// Get returns the value for key and whether it was present.
func (s *SkipList) Get(key []byte) ([]byte, bool) {
	x := s.head
	for i := int(s.level.Load()) - 1; i >= 0; i-- {
		for {
			n := x.next[i].Load()
			if n != nil && s.cmp(n.key, key) < 0 {
				x = n
				continue
			}
			break
		}
	}
	if n := x.next[0].Load(); n != nil && s.cmp(n.key, key) == 0 {
		return n.value, true
	}
	return nil, false
}

// Iterator walks the list in key order. It is valid only for entries that were
// present when the iterator was created plus any node inserted afterwards with
// a greater key; it never observes removed entries because nodes are never
// removed.
type Iterator struct {
	list *SkipList
	node *skipListNode
}

// NewIterator returns an iterator positioned before the first entry.
func (s *SkipList) NewIterator() *Iterator {
	return &Iterator{list: s}
}

// SeekToFirst positions the iterator at the smallest key.
func (it *Iterator) SeekToFirst() {
	it.node = it.list.head.next[0].Load()
}

// Seek positions the iterator at the first key >= target.
func (it *Iterator) Seek(target []byte) {
	x := it.list.head
	top := int(it.list.level.Load())
	for i := top - 1; i >= 0; i-- {
		for {
			n := x.next[i].Load()
			if n != nil && it.list.cmp(n.key, target) < 0 {
				x = n
				continue
			}
			break
		}
	}
	it.node = x.next[0].Load()
}

// Next advances to the following entry.
func (it *Iterator) Next() {
	if it.node == nil {
		return
	}
	it.node = it.node.next[0].Load()
}

// Valid reports whether the iterator points at an entry.
func (it *Iterator) Valid() bool { return it.node != nil }

// Key returns the current key. The returned slice must not be mutated.
func (it *Iterator) Key() []byte { return it.node.key }

// Value returns the current value. The returned slice must not be mutated.
func (it *Iterator) Value() []byte { return it.node.value }

// Close is a no-op; it exists so *Iterator satisfies the iterator interface.
func (it *Iterator) Close() error { return nil }
