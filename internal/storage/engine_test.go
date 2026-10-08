package storage

import (
	"fmt"
	"sync"
	"testing"
)

func openTestEngine(t *testing.T, dir string) *Engine {
	t.Helper()
	eng, err := Open(Options{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	return eng
}

func TestEnginePutGetDelete(t *testing.T) {
	eng := openTestEngine(t, t.TempDir())
	defer eng.Close()

	if err := eng.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	if v, ok, err := eng.Get([]byte("a")); err != nil || !ok || string(v) != "1" {
		t.Fatalf("Get(a) = %q, %v, %v; want 1, true, nil", v, ok, err)
	}
	if _, ok, _ := eng.Get([]byte("missing")); ok {
		t.Fatal("missing key reported found")
	}

	if err := eng.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := eng.Get([]byte("a")); err != nil || ok {
		t.Fatalf("Get(a) after delete = %v, %v; want false, nil", ok, err)
	}
	if v, ok, _ := eng.Get([]byte("b")); !ok || string(v) != "2" {
		t.Fatalf("Get(b) = %q, %v; want 2, true", v, ok)
	}
}

func TestEngineScanRangeAndTombstones(t *testing.T) {
	eng := openTestEngine(t, t.TempDir())
	defer eng.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		if err := eng.Put([]byte(k), []byte(k+"-val")); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	// Overwrite b; only the newest value should appear exactly once.
	if err := eng.Put([]byte("b"), []byte("b-new")); err != nil {
		t.Fatal(err)
	}

	all, err := eng.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"a": "a-val", "b": "b-new", "d": "d-val", "e": "e-val"}
	if len(all) != len(want) {
		t.Fatalf("scan returned %d entries, want %d: %+v", len(all), len(want), all)
	}
	for _, kv := range all {
		if want[string(kv.Key)] != string(kv.Value) {
			t.Fatalf("unexpected scan entry %s=%s", kv.Key, kv.Value)
		}
	}

	rng, err := eng.Scan([]byte("b"), []byte("e"))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, kv := range rng {
		keys = append(keys, string(kv.Key))
	}
	if fmt.Sprint(keys) != fmt.Sprint([]string{"b", "d"}) {
		t.Fatalf("range scan keys = %v, want [b d]", keys)
	}
}

func TestEnginePersistenceAcrossCleanReopen(t *testing.T) {
	dir := t.TempDir()
	eng := openTestEngine(t, dir)
	if err := eng.Put([]byte("persist"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}

	eng2 := openTestEngine(t, dir)
	defer eng2.Close()
	if v, ok, err := eng2.Get([]byte("persist")); err != nil || !ok || string(v) != "yes" {
		t.Fatalf("after reopen Get = %q, %v, %v; want yes, true, nil", v, ok, err)
	}
}

// TestEngineCrashRecovery simulates a process crash by abandoning the engine
// without closing it, then verifies a fresh engine replays the WAL.
func TestEngineCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	const n = 500

	crashed := openTestEngine(t, dir)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		val := []byte(fmt.Sprintf("val-%05d", i))
		if err := crashed.Put(key, val); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := crashed.Delete([]byte("key-00000")); err != nil {
		t.Fatal(err)
	}
	// No Close(): emulate an abrupt process death.

	recovered := openTestEngine(t, dir)
	defer recovered.Close()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		v, ok, err := recovered.Get(key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if i == 0 {
			if ok {
				t.Fatal("deleted key recovered as live")
			}
			continue
		}
		want := fmt.Sprintf("val-%05d", i)
		if !ok || string(v) != want {
			t.Fatalf("get %d = %q, %v; want %q, true", i, v, ok, want)
		}
	}

	// The recovered sequence must continue past the crashed engine's, so a
	// new write does not reuse a sequence number.
	if got := recovered.Stats().Seq; got < n+1 {
		t.Fatalf("recovered seq = %d, want >= %d", got, n+1)
	}
	if err := recovered.Put([]byte("key-00001"), []byte("updated")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := recovered.Get([]byte("key-00001")); !ok || string(v) != "updated" {
		t.Fatalf("post-recovery update = %q, %v; want updated, true", v, ok)
	}
}

func TestEngineConcurrentAccess(t *testing.T) {
	eng := openTestEngine(t, t.TempDir())
	defer eng.Close()

	const writers = 8
	const perWriter = 500
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("k-%02d-%04d", g, i))
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

	kvs, err := eng.Scan(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(kvs) != writers*perWriter {
		t.Fatalf("scan returned %d keys, want %d", len(kvs), writers*perWriter)
	}
}

func TestEngineOperationsAfterClose(t *testing.T) {
	eng := openTestEngine(t, t.TempDir())
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	if err := eng.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := eng.Put([]byte("x"), []byte("y")); err != ErrClosed {
		t.Fatalf("Put after close = %v, want ErrClosed", err)
	}
	if _, _, err := eng.Get([]byte("x")); err != ErrClosed {
		t.Fatalf("Get after close = %v, want ErrClosed", err)
	}
}
