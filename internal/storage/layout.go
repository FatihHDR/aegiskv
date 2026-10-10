package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// walPath returns the path of WAL file number num, e.g. "000042.wal".
func (e *Engine) walPath(num uint64) string {
	return filepath.Join(e.opts.Dir, fmt.Sprintf("%06d.wal", num))
}

// sstPath returns the path of an SSTable, e.g. "1-000007.sst".
func (e *Engine) sstPath(level int, num uint64) string {
	return filepath.Join(e.opts.Dir, fmt.Sprintf("%d-%06d.sst", level, num))
}

// parseSSTName parses a "<level>-<number>.sst" file name.
func parseSSTName(name string) (level int, num uint64, ok bool) {
	if !strings.HasSuffix(name, ".sst") {
		return 0, 0, false
	}
	body := strings.TrimSuffix(name, ".sst")
	parts := strings.SplitN(body, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	lvl, err := strconv.Atoi(parts[0])
	if err != nil || lvl < 0 {
		return 0, 0, false
	}
	n, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return lvl, n, true
}

// parseWALName parses a "<number>.wal" file name.
func parseWALName(name string) (num uint64, ok bool) {
	if !strings.HasSuffix(name, ".wal") {
		return 0, false
	}
	n, err := strconv.ParseUint(strings.TrimSuffix(name, ".wal"), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// scanDir classifies the engine directory into SSTables and WAL files,
// removing any leftover ".tmp" files from an interrupted write.
func scanDir(dir string) (sstFiles []string, walNums []uint64, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		switch {
		case strings.HasSuffix(name, tmpSuffix):
			os.Remove(filepath.Join(dir, name))
		case strings.HasSuffix(name, ".sst"):
			sstFiles = append(sstFiles, name)
		case strings.HasSuffix(name, ".wal"):
			if n, ok := parseWALName(name); ok {
				walNums = append(walNums, n)
			}
		}
	}
	sort.Slice(walNums, func(i, j int) bool { return walNums[i] < walNums[j] })
	return sstFiles, walNums, nil
}
