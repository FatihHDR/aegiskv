package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// maxRecordSize bounds a single WAL record body to guard against a corrupt
// length field causing a huge allocation.
const maxRecordSize = 64 << 20 // 64 MiB

// walHeaderSize is the fixed record header: crc32(4) + body length(4).
const walHeaderSize = 8

// Record frame types. M1 only emits recordFull; the remaining values are
// reserved so the on-disk format stays forward compatible with fragmentation.
const (
	recordFull   byte = 1
	recordFirst  byte = 2
	recordMiddle byte = 3
	recordLast   byte = 4
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// BatchRecord is one logical mutation inside a WriteBatch.
type BatchRecord struct {
	Type  byte // TypeValue or TypeDelete
	Key   []byte
	Value []byte
}

// WriteBatch is an atomic group of mutations sharing a single sequence number.
type WriteBatch struct {
	Seq     uint64
	Records []BatchRecord
}

// encode serializes the batch as:
//
//	seq(8) | count(4) | { type(1) | keyLen(4) | key | valLen(4) | value }*
func (b *WriteBatch) encode() []byte {
	size := 12
	for _, r := range b.Records {
		size += 1 + 4 + len(r.Key) + 4 + len(r.Value)
	}
	buf := make([]byte, 0, size)
	buf = binary.BigEndian.AppendUint64(buf, b.Seq)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(b.Records)))
	for _, r := range b.Records {
		buf = append(buf, r.Type)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Key)))
		buf = append(buf, r.Key...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Value)))
		buf = append(buf, r.Value...)
	}
	return buf
}

func decodeBatch(p []byte) (uint64, []BatchRecord, error) {
	if len(p) < 12 {
		return 0, nil, ErrCorrupt
	}
	seq := binary.BigEndian.Uint64(p[0:8])
	n := binary.BigEndian.Uint32(p[8:12])
	p = p[12:]
	recs := make([]BatchRecord, 0, n)
	for i := uint32(0); i < n; i++ {
		if len(p) < 1 {
			return 0, nil, ErrCorrupt
		}
		typ := p[0]
		p = p[1:]
		key, rest, err := readLenBytes(p)
		if err != nil {
			return 0, nil, err
		}
		val, rest2, err := readLenBytes(rest)
		if err != nil {
			return 0, nil, err
		}
		p = rest2
		recs = append(recs, BatchRecord{Type: typ, Key: cloneBytes(key), Value: cloneBytes(val)})
	}
	if len(p) != 0 {
		return 0, nil, ErrCorrupt
	}
	return seq, recs, nil
}

func readLenBytes(p []byte) ([]byte, []byte, error) {
	if len(p) < 4 {
		return nil, nil, ErrCorrupt
	}
	n := binary.BigEndian.Uint32(p[0:4])
	p = p[4:]
	if uint32(len(p)) < n {
		return nil, nil, ErrCorrupt
	}
	return p[:n], p[n:], nil
}

// encodeRecord frames a body (type byte + payload) as crc32(4) | len(4) | body.
func encodeRecord(typ byte, payload []byte) []byte {
	body := make([]byte, 1+len(payload))
	body[0] = typ
	copy(body[1:], payload)
	crc := crc32.Checksum(body, crcTable)
	buf := make([]byte, walHeaderSize+len(body))
	binary.LittleEndian.PutUint32(buf[0:4], crc)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(body)))
	copy(buf[walHeaderSize:], body)
	return buf
}

// readRecord reads one frame. It returns io.EOF at a clean end of file and
// io.ErrUnexpectedEOF for a record torn by a crash mid-write.
func readRecord(r *bufio.Reader) (byte, []byte, error) {
	var header [walHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	length := binary.LittleEndian.Uint32(header[4:8])
	if length < 1 || length > maxRecordSize {
		return 0, nil, ErrCorrupt
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, io.ErrUnexpectedEOF
	}
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(header[0:4]) {
		return 0, nil, ErrCorrupt
	}
	return body[0], body[1:], nil
}

// WAL is an append-only, crash-durable write log. Appends are serialized.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// openWAL opens (creating if needed) the log at path for appending.
func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{f: f, path: path}, nil
}

// Append writes the batch and, when sync is true, fsyncs before returning so
// the write is durable against process crash.
func (w *WAL) Append(b *WriteBatch, sync bool) error {
	rec := encodeRecord(recordFull, b.encode())
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	if sync {
		return w.f.Sync()
	}
	return nil
}

// Sync flushes buffered writes to stable storage.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Sync()
}

// Close syncs and closes the log.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// ReplayWAL streams every intact record in the log, invoking fn for each batch
// in order. A missing file is not an error. A torn or corrupt trailing record
// (an unacknowledged partial write) terminates replay without error, while the
// records that preceded it remain valid. It returns the highest sequence seen.
func ReplayWAL(path string, fn func(seq uint64, recs []BatchRecord) error) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1<<16)
	var maxSeq uint64
	for {
		typ, payload, rerr := readRecord(r)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			// io.ErrUnexpectedEOF and ErrCorrupt mark a torn/corrupt tail:
			// stop replay and keep everything read so far.
			break
		}
		if typ != recordFull {
			continue
		}
		seq, recs, derr := decodeBatch(payload)
		if derr != nil {
			return maxSeq, fmt.Errorf("wal: decode seq=%d: %w", seq, derr)
		}
		if err := fn(seq, recs); err != nil {
			return maxSeq, err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq, nil
}
