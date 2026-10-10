package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

// buildTestSSTable writes count entries "keyNNN" at sequence 1 and returns the
// opened table.
func buildTestSSTable(t *testing.T, dir string, count int, blockSize int) *table {
	t.Helper()
	path := filepath.Join(dir, "test.sst"+tmpSuffix)
	b, err := newSSTableBuilder(path, blockSize)
	if err != nil {
		t.Fatalf("builder: %v", err)
	}
	for i := 0; i < count; i++ {
		uk := []byte(fmt.Sprintf("key%03d", i))
		if err := b.add(makeInternalKey(uk, 1, TypeValue), []byte("val"+string(uk[3:]))); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	tbl, err := b.finish(0, 1, 10)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	return tbl
}

func TestSSTableLookup(t *testing.T) {
	dir := t.TempDir()
	tbl := buildTestSSTable(t, dir, 100, 64)
	defer tbl.markRemoved()

	// Present keys (small block size forces many blocks / index entries).
	if len(tbl.index) < 2 {
		t.Fatalf("expected multiple blocks, got %d", len(tbl.index))
	}
	for _, i := range []int{0, 37, 99} {
		uk := []byte(fmt.Sprintf("key%03d", i))
		v, typ, ok := tbl.lookup(makeInternalKey(uk, 1, TypeDelete))
		if !ok || typ != TypeValue {
			t.Fatalf("lookup %q = %v, %v; want found value", uk, ok, typ)
		}
		if want := "val" + string(uk[3:]); string(v) != want {
			t.Fatalf("lookup %q = %q, want %q", uk, v, want)
		}
	}

	// Absent keys.
	for _, uk := range []string{"missing", "zzz", "key1000"} {
		if _, _, ok := tbl.lookup(makeInternalKey([]byte(uk), 1, TypeDelete)); ok {
			t.Fatalf("unexpected hit for %q", uk)
		}
	}

	// A snapshot below the only version must not see it.
	if _, _, ok := tbl.lookup(makeInternalKey([]byte("key050"), 0, TypeDelete)); ok {
		t.Fatal("version should be invisible at snapshot 0")
	}
}

func TestSSTableIterator(t *testing.T) {
	dir := t.TempDir()
	tbl := buildTestSSTable(t, dir, 50, 64)
	defer tbl.markRemoved()

	it := newTableIterator(tbl)
	it.SeekToFirst()
	var got []string
	for it.Valid() {
		got = append(got, string(userKeyOf(it.Key())))
		it.Next()
	}
	if len(got) != 50 {
		t.Fatalf("iterated %d entries, want 50", len(got))
	}
	for i := 1; i < len(got); i++ {
		if bytes.Compare([]byte(got[i-1]), []byte(got[i])) >= 0 {
			t.Fatalf("out of order at %d: %q then %q", i, got[i-1], got[i])
		}
	}

	it.Seek(makeInternalKey([]byte("key025"), 1, TypeDelete))
	if !it.Valid() || string(userKeyOf(it.Key())) != "key025" {
		t.Fatalf("Seek(key025) = %q", userKeyOf(it.Key()))
	}

	it.Seek(makeInternalKey([]byte("key999"), 1, TypeDelete))
	if it.Valid() {
		t.Fatalf("Seek past end should be invalid, got %q", userKeyOf(it.Key()))
	}
}
