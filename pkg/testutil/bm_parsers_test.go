package testutil

import (
	"github.com/araddon/qlbridge/expr"
	"github.com/dataux/dataux/vendor/mixer/sqlparser"
	"testing"
)

/*
	Benchmark testing, mostly used to try out different runtime strategies for speed


BenchmarkVitessParser1		10000	    151674 ns/op
BenchmarkQlbridgeParser1	50000	     35837 ns/op



*/
// go test -bench="Parser"

func BenchmarkVitessParser1(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := sqlparser.Parse(`
			SELECT count(*), repository.name
			FROM github_watch
			GROUP BY repository.name, repository.language`)
		if err != nil {
			b.Fail()
		}
	}
}
func BenchmarkQlbridgeParser1(b *testing.B) {
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := expr.ParseSql(`
			SELECT count(*), repository.name
			FROM github_watch
			GROUP BY repository.name, repository.language`)
		if err != nil {
			b.Fail()
		}
	}
}
