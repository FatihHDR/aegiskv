// Command aegiskv is a minimal single-node front end for the AegisKV storage
// engine, used to exercise the Milestone 1 LSM core (memtable + WAL).
package main

import (
	"flag"
	"fmt"
	"os"

	"aegiskv/internal/storage"
)

func main() {
	dir := flag.String("dir", "aegiskv-data", "data directory")
	syncWrites := flag.Bool("sync", true, "fsync the WAL on every write")
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	eng, err := storage.Open(storage.Options{Dir: *dir, SyncWrites: *syncWrites})
	if err != nil {
		fatalf("open: %v", err)
	}
	defer eng.Close()

	switch args[0] {
	case "put":
		requireArgs(args, 3)
		if err := eng.Put([]byte(args[1]), []byte(args[2])); err != nil {
			fatalf("put: %v", err)
		}
	case "get":
		requireArgs(args, 2)
		v, ok, err := eng.Get([]byte(args[1]))
		if err != nil {
			fatalf("get: %v", err)
		}
		if !ok {
			fmt.Println("(not found)")
			return
		}
		os.Stdout.Write(v)
		fmt.Println()
	case "delete":
		requireArgs(args, 2)
		if err := eng.Delete([]byte(args[1])); err != nil {
			fatalf("delete: %v", err)
		}
	case "scan":
		requireArgs(args, 1)
		kvs, err := eng.Scan(nil, nil)
		if err != nil {
			fatalf("scan: %v", err)
		}
		for _, kv := range kvs {
			fmt.Printf("%s = %s\n", kv.Key, kv.Value)
		}
	case "stats":
		requireArgs(args, 1)
		s := eng.Stats()
		fmt.Printf("keys=%d seq=%d\n", s.Keys, s.Seq)
	default:
		usage()
		os.Exit(2)
	}
}

func requireArgs(args []string, n int) {
	if len(args) != n {
		usage()
		os.Exit(2)
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(1)
}

func usage() {
	fmt.Fprint(os.Stderr, `usage: aegiskv [-dir DIR] [-sync=true] <command> [args]

commands:
  put <key> <value>   store a value
  get <key>           retrieve a value
  delete <key>        delete a key
  scan                list all key/value pairs
  stats               print engine statistics
`)
}
