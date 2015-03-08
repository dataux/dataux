package main

import (
	"flag"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/pkg/testutil"
)

/*

usage:

	go build && ./importgithub

*/

var (
	eshost *string = flag.String("host", "localhost", "Elasticsearch Server Host Address")
)

func init() {
	u.SetupLogging("debug")
	u.SetColorIfTerminal()
}

func main() {
	flag.Parse()
	testutil.LoadOnce(*eshost)
}
