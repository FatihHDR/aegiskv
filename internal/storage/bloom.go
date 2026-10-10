package storage

import "math"

// bloomFilter is a fixed-size probabilistic set used to avoid disk reads for
// keys a table cannot possibly contain. A false positive sends a lookup to
// disk; a false negative never happens.
type bloomFilter struct {
	bits []byte
	k    uint32
}

// bloomHash is a 32-bit multiplicative string hash (the LevelDB variant).
func bloomHash(data []byte) uint32 {
	const m = 0xc6a4a793
	h := uint32(0xbc9f1d34)
	for _, c := range data {
		h = (h ^ uint32(c)) * m
	}
	return h
}

// bloomProbes returns the optimal number of hash probes for a target of
// bitsPerKey bits per key, clamped to [1, 30].
func bloomProbes(bitsPerKey int) uint32 {
	k := int(float64(bitsPerKey) * math.Ln2)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return uint32(k)
}

// buildBloom constructs a filter over keys. bitsPerKey controls the trade-off
// between size and false-positive rate (10 is ~1%).
func buildBloom(keys [][]byte, bitsPerKey int) *bloomFilter {
	numBits := len(keys) * bitsPerKey
	if numBits < 64 {
		numBits = 64
	}
	numBytes := (numBits + 7) / 8
	bits := make([]byte, numBytes)
	numBits = numBytes * 8

	k := bloomProbes(bitsPerKey)
	for _, key := range keys {
		h := bloomHash(key)
		delta := h>>17 | h<<15
		for i := uint32(0); i < k; i++ {
			pos := h % uint32(numBits)
			bits[pos/8] |= 1 << (pos % 8)
			h += delta
		}
	}
	return &bloomFilter{bits: bits, k: k}
}

// encode serializes the filter as the bitset followed by the probe count byte.
func (b *bloomFilter) encode() []byte {
	out := make([]byte, len(b.bits)+1)
	copy(out, b.bits)
	out[len(b.bits)] = byte(b.k)
	return out
}

// decodeBloom parses the output of encode.
func decodeBloom(data []byte) *bloomFilter {
	if len(data) < 1 {
		return &bloomFilter{bits: []byte{0, 0}, k: 1}
	}
	k := data[len(data)-1]
	if k < 1 {
		k = 1
	}
	bits := make([]byte, len(data)-1)
	copy(bits, data[:len(data)-1])
	return &bloomFilter{bits: bits, k: uint32(k)}
}

// mayContain reports whether key might be present. false is definitive.
func (b *bloomFilter) mayContain(key []byte) bool {
	if len(b.bits) == 0 {
		return false
	}
	numBits := uint32(len(b.bits) * 8)
	h := bloomHash(key)
	delta := h>>17 | h<<15
	for i := uint32(0); i < b.k; i++ {
		pos := h % numBits
		if b.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
