package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"mastore/store"
	"os"
	"path/filepath"
	"strings"
)

func exeName() string {
	return filepath.Base(os.Args[0])
}

func printUsage() {
	usage := "Usage %s: (read|write|test) [options]\n"
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

func processCommonFlags(fconf *string) (*log.Logger, *store.Store) {
	flag.CommandLine.Parse(os.Args[2:])

	conf, err := readConfig(*fconf)
	if err != nil {
		log.Fatalf("failed to read configuration: %s", err)
	}

	log_ := log.New(os.Stderr, "", log.Ldate|log.Ltime)
	st := store.New(conf, log_)

	return log_, st
}

func readCb(st *store.Store, entry string) {
	checkInterrupted(st)
	os.Stdout.WriteString(entry)
}

func read(fconf *string) {
	fkey := flag.String("key", "", "Key to read entries for")
	flag.CommandLine.Parse(os.Args[2:])
	_, st := processCommonFlags(fconf)

	if !st.FindEntries(*fkey, readCb) {
		os.Exit(1)
	}
}

func write(fconf *string) {
	log_, st := processCommonFlags(fconf)

	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		checkInterrupted(st)

		split := strings.SplitN(scan.Text(), "\t", 2)
		if len(split) != 2 {
			log_.Println("key without entry value, ignored")
			continue
		}

		if !st.AddEntry(split[0], split[1]) {
			os.Exit(1)
		}
	}

	if err := scan.Err(); err != nil {
		os.Exit(1)
	}
}

func test(fconf *string) {
	fkeys := flag.Int("keys", testKeys, "Total number of keys")
	fentries := flag.Int("entries", testKeys, "Total number of entries")
	log_, st := processCommonFlags(fconf)

	if !doTest(log_, st, *fkeys, *fentries) {
		os.Exit(1)
	}
}
