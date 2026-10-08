package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestSkipListInsertGet(t *testing.T) {
	s := NewSkipList()
	if _, ok := s.Get([]byte("missing")); ok {
		t.Fatal("expected miss on empty list")
	}

	s.Insert([]byte("b"), []byte("2"))
	s.Insert([]byte("a"), []byte("1"))
	s.Insert([]byte("c"), []byte("3"))

	for k, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, ok := s.Get([]byte(k))
		if !ok || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%q) = %q, %v; want %q, true", k, got, ok, want)
		}
	}
	if s.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", s.Len())
	}
}

func TestSkipListOverwrite(t *testing.T) {
	s := NewSkipList()
	s.Insert([]byte("k"), []byte("v1"))
	s.Insert([]byte("k"), []byte("v2"))
	if v, ok := s.Get([]byte("k")); !ok || string(v) != "v2" {
		t.Fatalf("Get = %q, %v; want v2, true", v, ok)
	}
	if s.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", s.Len())
	}
}

func TestSkipListIteratorOrderAndSeek(t *testing.T) {
	s := NewSkipList()
	for _, k := range []string{"d", "a", "c", "b", "e"} {
		s.Insert([]byte(k), []byte(k))
	}

	var order []string
	it := s.NewIterator()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		order = append(order, string(it.Key()))
	}
	want := []string{"a", "b", "c", "d", "e"}
	if fmt.Sprint(order) != fmt.Sprint(want) {
		t.Fatalf("iteration order = %v, want %v", order, want)
	}

	it.Seek([]byte("c"))
	if !it.Valid() || string(it.Key()) != "c" {
		t.Fatalf("Seek(c) landed on %q", it.Key())
	}
	it.Seek([]byte("cc"))
	if !it.Valid() || string(it.Key()) != "d" {
		t.Fatalf("Seek(cc) landed on %q, want d", it.Key())
	}
	it.Seek([]byte("z"))
	if it.Valid() {
		t.Fatalf("Seek(z) should be exhausted, got %q", it.Key())
	}
}

func TestSkipListConcurrentWritesAndReads(t *testing.T) {
	s := NewSkipList()
	const writers = 8
	const perWriter = 1500

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("k-%02d-%06d", g, i))
				s.Insert(key, key)
				s.Get(key)
			}
		}(g)
	}
	wg.Wait()

	if got := s.Len(); got != writers*perWriter {
		t.Fatalf("Len() = %d, want %d", got, writers*perWriter)
	}

	// Full traversal must be strictly sorted and complete.
	it := s.NewIterator()
	it.SeekToFirst()
	var prev []byte
	count := 0
	for it.Valid() {
		if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
			t.Fatalf("out of order: %q then %q", prev, it.Key())
		}
		prev = append(prev[:0], it.Key()...)
		count++
		it.Next()
	}
	if count != writers*perWriter {
		t.Fatalf("traversed %d entries, want %d", count, writers*perWriter)
	}
}
