package testutil

import (
	"encoding/json"
	"flag"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
)

var (
	setup sync.Once

	veryVerbose *bool   = flag.Bool("vv", false, "very verbose output")
	logLevel    *string = flag.String("logging", "debug", "Which log level: [debug,info,warn,error,fatal]")

	Articles = make([]*Article, 0)
	Users    = make([]*User, 0)
)

func init() {
	t := time.Now()
	ev := struct {
		Tag string
		ICt int
	}{"tag", 1}
	t1, _ := dateparse.ParseAny("2010-10-01")
	t2, _ := dateparse.ParseAny("2011-10-01")
	t3, _ := dateparse.ParseAny("2012-10-01")
	t4, _ := dateparse.ParseAny("2013-10-01")
	body := json.RawMessage([]byte(`{"name":"morestuff"}`))

	Articles = append(Articles, &Article{"article1", "aaron", 22, 75, false, []string{"news", "sports"}, t1, &t, 55.5, ev, &body})
	Articles = append(Articles, &Article{"article2", "james", 2, 64, true, []string{"news", "sports"}, t2, &t, 55.5, ev, &body})
	Articles = append(Articles, &Article{"article3", "bjorn", 55, 100, true, []string{"politics"}, t3, &t, 21.5, ev, &body})
	Articles = append(Articles, &Article{"listicle1", "bjorn", 7, 12, true, []string{"world"}, t4, &t, 21.5, ev, &body})
	// Users
	Users = append(Users, &User{"user123", "aaron", false, []string{"admin", "author"}, time.Now(), &t})
	Users = append(Users, &User{"user456", "james", true, []string{"admin", "author"}, time.Now().Add(-time.Hour * 100), &t})
	Users = append(Users, &User{"user789", "bjorn", true, []string{"author"}, time.Now().Add(-time.Hour * 220), &t})
}

func Setup() {
	setup.Do(func() {
		flag.Parse()
		if *veryVerbose {
			u.SetupLogging(*logLevel)
			u.SetColorOutput()
		} else {
			u.SetupLogging("warn")
			u.SetColorOutput()
		}
		u.SetColorIfTerminal()
	})
}

type Article struct {
	Title    string
	Author   string
	Count    int
	Count64  int64
	Deleted  bool
	Category []string
	Created  time.Time
	Updated  *time.Time
	F        float64
	Embedded struct {
		Tag string
		ICt int
	}
	Body *json.RawMessage
}

type User struct {
	Id      string
	Name    string
	Deleted bool
	Roles   []string
	Created time.Time
	Updated *time.Time
}
