package main

import (
	"flag"
	u "github.com/araddon/gou"
)

var (
	eshost *string = flag.String("host", "localhost", "Elasticsearch Server Host Address")
)

func main() {
	flag.Parse()
	u.SetupLogging("info")
	LoadGithubToEsOnce(*eshost)
}
