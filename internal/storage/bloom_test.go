package storage

import (
	"fmt"
	"testing"
)

func TestBloomNoFalseNegatives(t *testing.T) {
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("present-%d", i))
	}
	b := buildBloom(keys, 10)
	for _, k := range keys {
		if !b.mayContain(k) {
			t.Fatalf("false negative for %q", k)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	keys := make([][]byte, 1000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("present-%d", i))
	}
	b := buildBloom(keys, 10)

	fp := 0
	const probes = 2000
	for i := 0; i < probes; i++ {
		if b.mayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.03 {
		t.Fatalf("false positive rate %.3f exceeds 3%%", rate)
	}
}

func TestBloomEncodeDecodeRoundTrip(t *testing.T) {
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	b := buildBloom(keys, 10)
	got := decodeBloom(b.encode())
	for _, k := range keys {
		if !got.mayContain(k) {
			t.Fatalf("round-trip lost key %q", k)
		}
	}
	if got.k != b.k {
		t.Fatalf("probe count changed: %d != %d", got.k, b.k)
	}
}
