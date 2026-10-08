package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	b1 := &WriteBatch{Seq: 1, Records: []BatchRecord{{Type: TypeValue, Key: []byte("a"), Value: []byte("1")}}}
	b2 := &WriteBatch{Seq: 2, Records: []BatchRecord{
		{Type: TypeValue, Key: []byte("b"), Value: []byte("2")},
		{Type: TypeDelete, Key: []byte("a")},
	}}
	if err := w.Append(b1, true); err != nil {
		t.Fatalf("append b1: %v", err)
	}
	if err := w.Append(b2, true); err != nil {
		t.Fatalf("append b2: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var seqs []uint64
	var recs []BatchRecord
	maxSeq, err := ReplayWAL(path, func(seq uint64, r []BatchRecord) error {
		seqs = append(seqs, seq)
		recs = append(recs, r...)
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if maxSeq != 2 {
		t.Fatalf("maxSeq = %d, want 2", maxSeq)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("seqs = %v, want [1 2]", seqs)
	}
	if len(recs) != 3 {
		t.Fatalf("records = %d, want 3", len(recs))
	}
	if recs[2].Type != TypeDelete || string(recs[2].Key) != "a" {
		t.Fatalf("unexpected last record: %+v", recs[2])
	}
}

func TestWALReplayMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.log")
	maxSeq, err := ReplayWAL(path, func(uint64, []BatchRecord) error { return nil })
	if err != nil {
		t.Fatalf("replay missing: %v", err)
	}
	if maxSeq != 0 {
		t.Fatalf("maxSeq = %d, want 0", maxSeq)
	}
}

func TestWALTornTailTolerated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := openWAL(path)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		b := &WriteBatch{Seq: i, Records: []BatchRecord{{
			Type: TypeValue, Key: []byte("k"), Value: []byte("value-payload"),
		}}}
		if err := w.Append(b, true); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	w.Close()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write by chopping off part of the final record.
	if err := os.Truncate(path, fi.Size()-4); err != nil {
		t.Fatal(err)
	}

	var seqs []uint64
	maxSeq, err := ReplayWAL(path, func(seq uint64, _ []BatchRecord) error {
		seqs = append(seqs, seq)
		return nil
	})
	if err != nil {
		t.Fatalf("replay after torn tail: %v", err)
	}
	if maxSeq != 2 {
		t.Fatalf("maxSeq = %d, want 2 (last record torn)", maxSeq)
	}
	if len(seqs) != 2 {
		t.Fatalf("recovered %d records, want 2", len(seqs))
	}
}
