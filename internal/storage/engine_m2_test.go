package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testOptions(dir string) Options {
	return Options{
		Dir:                 dir,
		SyncWrites:          true,
		MemTableSizeBytes:   256,
		BlockSizeBytes:      128,
		BloomBitsPerKey:     10,
		MaxFileSizeBytes:    512,
		L0CompactionTrigger: 2,
		BaseLevelSizeBytes:  1024,
		LevelSizeMultiplier: 4,
		MaxLevels:           5,
		DisableBackground:   true,
	}
}

func sstFileCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			n++
		}
	}
	return n
}

// countUserVersions returns how many stored versions exist for uk across all
// tables in the tree.
func countUserVersions(t *testing.T, e *Engine, uk []byte) int {
	t.Helper()
	e.mu.RLock()
	ver := e.ver
	ver.refAll()
	e.mu.RUnlock()
	defer ver.unrefAll()

	count := 0
	for _, lvl := range ver.levels {
		for _, tbl := range lvl {
			it := newTableIterator(tbl)
			it.Seek(makeInternalKey(uk, ^uint64(0), TypeDelete)) // smallest key for uk
			for it.Valid() && bytes.Equal(userKeyOf(it.Key()), uk) {
				count++
				it.Next()
			}
		}
	}
	return count
}

func TestEngineFlushCreatesSSTableAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 40; i++ {
		if err := eng.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if got := sstFileCount(t, dir); got == 0 {
		t.Fatal("flush produced no SSTable files")
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for i := 0; i < 40; i++ {
		want := fmt.Sprintf("val%03d", i)
		v, ok, err := reopened.Get([]byte(fmt.Sprintf("key%03d", i)))
		if err != nil || !ok || string(v) != want {
			t.Fatalf("key%03d = %q, %v, %v; want %q, true, nil", i, v, ok, err, want)
		}
	}
}

func TestEngineUpdateDeleteAcrossFlushBoundary(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	if err := eng.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Flush(); err != nil { // v1 now lives in an SSTable
		t.Fatal(err)
	}
	if v, ok, _ := eng.Get([]byte("k")); !ok || string(v) != "v1" {
		t.Fatalf("after flush Get = %q, %v; want v1, true", v, ok)
	}

	if err := eng.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := eng.Get([]byte("k")); !ok || string(v) != "v2" {
		t.Fatalf("after update Get = %q, %v; want v2, true", v, ok)
	}

	if err := eng.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := eng.Get([]byte("k")); ok {
		t.Fatal("key should be deleted")
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := eng.Get([]byte("k")); ok {
		t.Fatal("tombstone in SSTable did not hide older value")
	}
}

func TestEngineScanMergesLevels(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Spread writes across several flushes, updating some keys along the way.
	want := map[string]string{}
	for round := 0; round < 4; round++ {
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%02d", i)
			val := fmt.Sprintf("val-r%d-%02d", round, i)
			if err := eng.Put([]byte(key), []byte(val)); err != nil {
				t.Fatal(err)
			}
			want[key] = val
		}
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	kvs, err := eng.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != len(want) {
		t.Fatalf("scan returned %d keys, want %d", len(kvs), len(want))
	}
	var prev []byte
	for _, kv := range kvs {
		if prev != nil && bytes.Compare(prev, kv.Key) >= 0 {
			t.Fatalf("scan out of order: %q then %q", prev, kv.Key)
		}
		prev = kv.Key
		if want[string(kv.Key)] != string(kv.Value) {
			t.Fatalf("scan %q = %q, want %q", kv.Key, kv.Value, want[string(kv.Key)])
		}
	}
}

func TestEngineCompactionReducesL0AndPreservesData(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// One flush per key yields many overlapping L0 tables.
	for i := 0; i < 30; i++ {
		if err := eng.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i))); err != nil {
			t.Fatal(err)
		}
		if err := eng.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	l0Before := eng.Stats().Levels[0].Tables
	if l0Before < 2 {
		t.Fatalf("expected several L0 tables, got %d", l0Before)
	}

	if err := eng.Compact(); err != nil {
		t.Fatal(err)
	}
	st := eng.Stats()
	if st.Compactions == 0 {
		t.Fatal("no compaction ran")
	}
	if st.Levels[0].Tables >= l0Before {
		t.Fatalf("L0 not reduced: before=%d after=%d", l0Before, st.Levels[0].Tables)
	}

	for i := 0; i < 30; i++ {
		want := fmt.Sprintf("val%03d", i)
		v, ok, err := eng.Get([]byte(fmt.Sprintf("key%03d", i)))
		if err != nil || !ok || string(v) != want {
			t.Fatalf("after compaction key%03d = %q, %v, %v; want %q", i, v, ok, err, want)
		}
	}
}

func TestCompactionDropsObsoleteVersionsAndTombstones(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// key retains three versions across flushes; doomed is created and removed.
	if err := eng.Put([]byte("key"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("key"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("doomed"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("key"), []byte("v3")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Delete([]byte("doomed")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := eng.Compact(); err != nil {
		t.Fatal(err)
	}

	if n := countUserVersions(t, eng, []byte("key")); n != 1 {
		t.Fatalf("obsolete versions not reclaimed: key has %d versions, want 1", n)
	}
	if n := countUserVersions(t, eng, []byte("doomed")); n != 0 {
		t.Fatalf("tombstone not reclaimed at bottom level: doomed has %d versions, want 0", n)
	}
	if v, ok, _ := eng.Get([]byte("key")); !ok || string(v) != "v3" {
		t.Fatalf("key = %q, %v; want v3, true", v, ok)
	}
	if _, ok, _ := eng.Get([]byte("doomed")); ok {
		t.Fatal("doomed should remain deleted")
	}
}

func TestEngineRecoveryFromSSTableAndWAL(t *testing.T) {
	dir := t.TempDir()
	crashed, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}

	// Half the data is flushed to SSTables, half stays only in the WAL.
	for i := 0; i < 20; i++ {
		if err := crashed.Put([]byte(fmt.Sprintf("flushed%02d", i)), []byte("a")); err != nil {
			t.Fatal(err)
		}
	}
	if err := crashed.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := crashed.Put([]byte(fmt.Sprintf("walonly%02d", i)), []byte("b")); err != nil {
			t.Fatal(err)
		}
	}
	// No Close(): simulate a crash.

	recovered, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()

	for i := 0; i < 20; i++ {
		if v, ok, _ := recovered.Get([]byte(fmt.Sprintf("flushed%02d", i))); !ok || string(v) != "a" {
			t.Fatalf("flushed%02d lost after recovery: %q, %v", i, v, ok)
		}
		if v, ok, _ := recovered.Get([]byte(fmt.Sprintf("walonly%02d", i))); !ok || string(v) != "b" {
			t.Fatalf("walonly%02d lost after recovery: %q, %v", i, v, ok)
		}
	}
}

func TestEngineBackgroundFlushAndCompactionUnderLoad(t *testing.T) {
	dir := t.TempDir()
	opts := testOptions(dir)
	opts.DisableBackground = false
	eng, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	const writers = 6
	const perWriter = 300
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("w%02d-k%04d", g, i))
				if err := eng.Put(key, key); err != nil {
					t.Errorf("put: %v", err)
					return
				}
				if _, _, err := eng.Get(key); err != nil {
					t.Errorf("get: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// Drain any pending background work deterministically.
	if err := eng.Compact(); err != nil {
		t.Fatal(err)
	}

	for g := 0; g < writers; g++ {
		for i := 0; i < perWriter; i++ {
			key := fmt.Sprintf("w%02d-k%04d", g, i)
			v, ok, err := eng.Get([]byte(key))
			if err != nil || !ok || string(v) != key {
				t.Fatalf("%s = %q, %v, %v; want %q", key, v, ok, err, key)
			}
		}
	}

	// Reopen and confirm durability after all the background churn.
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if v, ok, _ := again.Get([]byte("w00-k0000")); !ok || string(v) != "w00-k0000" {
		t.Fatalf("post-reopen Get = %q, %v", v, ok)
	}
}

func TestEngineConcurrentReadersDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	for i := 0; i < 60; i++ {
		if err := eng.Put([]byte(fmt.Sprintf("key%03d", i)), []byte(fmt.Sprintf("val%03d", i))); err != nil {
			t.Fatal(err)
		}
		if i%3 == 0 {
			if err := eng.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				kvs, err := eng.Scan(nil, nil)
				if err != nil {
					t.Errorf("scan: %v", err)
					return
				}
				for _, kv := range kvs {
					if !bytes.HasPrefix(kv.Value, []byte("val")) {
						t.Errorf("unexpected value %q", kv.Value)
						return
					}
				}
			}
		}()
	}

	if err := eng.Compact(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()

	kvs, err := eng.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != 60 {
		t.Fatalf("scan returned %d keys, want 60", len(kvs))
	}
}

func TestEngineRemovesTmpFilesOnOpen(t *testing.T) {
	dir := t.TempDir()
	junk := filepath.Join(dir, "0-000099.sst"+tmpSuffix)
	if err := os.WriteFile(junk, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	eng, err := Open(testOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if _, err := os.Stat(junk); !os.IsNotExist(err) {
		t.Fatalf("stale tmp file not removed: %v", err)
	}
}
