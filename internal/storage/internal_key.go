package storage

import (
	"bytes"
	"encoding/binary"
)

// Record types stored inside internal keys and WAL batches.
const (
	TypeDelete byte = 0 // tombstone
	TypeValue  byte = 1 // live value
)

// internalSuffixLen is the fixed trailing size of an internal key:
// 8 bytes of inverted sequence number followed by 1 byte of record type.
const internalSuffixLen = 9

// makeInternalKey builds a key ordered by (userKey ASC, sequence DESC).
//
// The sequence number is stored bitwise-inverted and big-endian so that a
// higher sequence sorts *before* a lower one under plain lexicographic
// comparison of the suffix. Because the suffix has a fixed width and is only
// compared after the user keys are known to be equal (see compareInternal),
// variable-length user keys never interleave incorrectly.
func makeInternalKey(userKey []byte, seq uint64, typ byte) []byte {
	buf := make([]byte, len(userKey)+internalSuffixLen)
	copy(buf, userKey)
	binary.BigEndian.PutUint64(buf[len(userKey):len(userKey)+8], ^seq)
	buf[len(userKey)+8] = typ
	return buf
}

// userKeyOf strips the internal suffix, returning the original user key.
func userKeyOf(ik []byte) []byte {
	return ik[:len(ik)-internalSuffixLen]
}

// seqOf recovers the sequence number from an internal key.
func seqOf(ik []byte) uint64 {
	return ^binary.BigEndian.Uint64(ik[len(ik)-internalSuffixLen : len(ik)-1])
}

// typeOf recovers the record type from an internal key.
func typeOf(ik []byte) byte {
	return ik[len(ik)-1]
}

// compareInternal orders two internal keys by user key ascending, then by
// sequence descending (newest version first).
func compareInternal(a, b []byte) int {
	uka, ukb := userKeyOf(a), userKeyOf(b)
	if c := bytes.Compare(uka, ukb); c != 0 {
		return c
	}
	return bytes.Compare(a[len(uka):], b[len(ukb):])
}
