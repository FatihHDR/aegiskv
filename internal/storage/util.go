package storage

// cloneBytes returns a copy of b so stored data never aliases caller-owned
// buffers. A nil input yields a nil result.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
