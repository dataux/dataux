package main

import (
	"flag"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/testdata"
)

/*

usage:

	go build

	./importgithub

	./importgithub --host="localhost:9201"

*/

var (
	eshost *string = flag.String("host", "localhost:9200", "Elasticsearch Server Host Address")
)

func init() {
	u.SetupLogging("debug")
	u.SetColorIfTerminal()
}

func main() {
	flag.Parse()
	testdata.LoadGithubToEsOnce(*eshost)
}
