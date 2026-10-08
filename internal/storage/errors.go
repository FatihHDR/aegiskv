package storage

import "errors"

var (
	// ErrClosed is returned when an operation is attempted on a closed engine.
	ErrClosed = errors.New("storage: engine is closed")
	// ErrCorrupt is returned when a WAL record or batch fails validation.
	ErrCorrupt = errors.New("storage: corrupt record")
)
