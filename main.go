package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ababo/mastore/store"
	logpkg "log"
	"os"
	"path/filepath"
	"strings"
)

func exeName() string {
	return filepath.Base(os.Args[0])
}

func printUsage() {
	usage := "Usage: %s (read|write|test) [options]\n"
	fmt.Fprintf(os.Stderr, usage, exeName())
	flag.PrintDefaults()
}

func main() {
	setInterruptHandler()

	flag.Usage = printUsage
	fconf := flag.String("config",
		exeName()+".config", "Path to config file")

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "read":
		read(fconf)
	case "write":
		write(fconf)
	case "test":
		test(fconf)
	default:
		printUsage()
		os.Exit(1)
	}

}

func readConfig(name string) (*store.Config, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var conf store.Config
	if err := json.NewDecoder(file).Decode(&conf); err != nil {
		return nil, err
	}

	return &conf, nil
}

func processCommonFlags(fconf *string) (*logpkg.Logger, *store.Store) {
	flag.CommandLine.Parse(os.Args[2:])

	conf, err := readConfig(*fconf)
	if err != nil {
		logpkg.Fatalf("failed to read configuration: %s", err)
	}

	log := logpkg.New(os.Stderr, "", logpkg.Ldate|logpkg.Ltime)
	st := store.New(conf, log)

	return log, st
}

func readCb(st *store.Store, val string) {
	checkInterrupted(nil)
	fmt.Println(val)
}

func read(fconf *string) {
	fkey := flag.String("key", "", "Key to read values for")
	fkeys := flag.Bool("keys", false, "Read all the keys")
	flag.CommandLine.Parse(os.Args[2:])
	_, st := processCommonFlags(fconf)

	if (*fkeys && !st.FindKeys(readCb)) ||
		(!*fkeys && !st.FindValues(*fkey, readCb)) {
		os.Exit(1)
	}
}

func write(fconf *string) {
	log, st := processCommonFlags(fconf)

	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		checkInterrupted(st)

		split := strings.SplitN(scan.Text(), "\t", 2)
		if len(split) != 2 {
			log.Println("key without value, ignored")
			continue
		}

		if !st.AddValue(split[0], split[1]) {
			os.Exit(1)
		}
	}

	st.Flush(true)

	if err := scan.Err(); err != nil {
		os.Exit(1)
	}
}

func test(fconf *string) {
	fkeys := flag.Int("keys", testKeys, "Total number of keys")
	fvals := flag.Int("values", testKeys, "Total number of values")
	log, st := processCommonFlags(fconf)

	if !doTest(log, st, *fkeys, *fvals) {
		os.Exit(1)
	}
}
