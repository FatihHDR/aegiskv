package storage

import "testing"

func TestMemTableSnapshotVersions(t *testing.T) {
	m := NewMemTable()
	m.Add(1, TypeValue, []byte("k"), []byte("v1"))
	m.Add(2, TypeValue, []byte("k"), []byte("v2"))

	if v, ok := m.Get([]byte("k"), 1); !ok || string(v) != "v1" {
		t.Fatalf("snapshot@1 = %q, %v; want v1, true", v, ok)
	}
	if v, ok := m.Get([]byte("k"), 2); !ok || string(v) != "v2" {
		t.Fatalf("snapshot@2 = %q, %v; want v2, true", v, ok)
	}

	// A tombstone hides the key from that sequence onward, but older
	// snapshots still see the last live value.
	m.Add(3, TypeDelete, []byte("k"), nil)
	if _, ok := m.Get([]byte("k"), 3); ok {
		t.Fatal("key should be deleted at snapshot 3")
	}
	if v, ok := m.Get([]byte("k"), 2); !ok || string(v) != "v2" {
		t.Fatalf("snapshot@2 after delete = %q, %v; want v2, true", v, ok)
	}
}

func TestMemTableVariableLengthKeyOrdering(t *testing.T) {
	m := NewMemTable()
	// Keys chosen so a naive key+suffix byte comparison would interleave.
	for i, k := range []string{"a", "ab", "abc", "b"} {
		m.Add(uint64(i+1), TypeValue, []byte(k), []byte(k))
	}

	it := m.NewIterator()
	it.SeekToFirst()
	var got []string
	for it.Valid() {
		got = append(got, string(userKeyOf(it.Key())))
		it.Next()
	}
	want := []string{"a", "ab", "abc", "b"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
