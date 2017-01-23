package testutil

import (
	"database/sql/driver"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
)

var (
	setup sync.Once

	veryVerbose *bool   = flag.Bool("vv", false, "very verbose output")
	logLevel    *string = flag.String("logging", "debug", "Which log level: [debug,info,warn,error,fatal]")

	Articles = make([]*Article, 0)
	Users    = make([]*User, 0)
)

const (
	DbName = "datauxtest"
)

func init() {
	//t := time.Now()
	ev := struct {
		Tag string
		ICt int
	}{"tag", 1}
	t1, _ := dateparse.ParseAny("2010-10-01")
	t2, _ := dateparse.ParseAny("2011-10-01")
	t3, _ := dateparse.ParseAny("2012-10-01")
	t4, _ := dateparse.ParseAny("2013-10-01")
	ut, _ := dateparse.ParseAny("2016-01-01")
	body := json.RawMessage([]byte(`{"name":"morestuff"}`))

	Articles = append(Articles, &Article{"article1", "aaron", 22, 75, false, []string{"news", "sports"}, t1, &ut, 55.5, ev, &body})
	Articles = append(Articles, &Article{"qarticle2", "james", 2, 64, true, []string{"news", "sports"}, t2, &ut, 55.5, ev, &body})
	Articles = append(Articles, &Article{"zarticle3", "bjorn", 55, 100, true, []string{"politics"}, t3, &ut, 21.5, ev, &body})
	Articles = append(Articles, &Article{"listicle1", "bjorn", 7, 12, true, []string{"world"}, t4, &ut, 21.5, ev, &body})
	// Users
	Users = append(Users, &User{"user123", "aaron", false, []string{"admin", "author"}, time.Now(), &ut})
	Users = append(Users, &User{"user456", "james", true, []string{"admin", "author"}, time.Now().Add(-time.Hour * 100), &ut})
	Users = append(Users, &User{"user789", "bjorn", true, []string{"author"}, time.Now().Add(-time.Hour * 220), &ut})
}

func Setup() {
	setup.Do(func() {
		flag.Parse()
		if *veryVerbose {
			u.SetupLoggingLong(*logLevel)
			u.SetColorOutput()
		} else {
			u.SetupLogging("warn")
			u.SetColorOutput()
		}
	})
}

var (
	ArticleCsv = `title,author,count,deleted,created,updated,f
article1,aaron,22,false,2010-10-01 00:00:00 +0000 UTC,2016-01-01 00:00:00 +0000 UTC,55.5
qarticle2,james,2,true,2011-10-01 00:00:00 +0000 UTC,2016-01-01 00:00:00 +0000 UTC,55.5
zarticle3,bjorn,55,true,2012-10-01 00:00:00 +0000 UTC,2016-01-01 00:00:00 +0000 UTC,21.5
listicle1,bjorn,7,true,2013-10-01 00:00:00 +0000 UTC,2016-01-01 00:00:00 +0000 UTC,21.5`
)

type StructRow interface {
	Values() []driver.Value
	ValueI() []interface{}
	ColNames() []string
}
type Article struct {
	Title    string
	Author   string
	Count    int
	Count64  int64
	Deleted  bool
	Category datasource.StringArray
	Created  time.Time
	Updated  *time.Time
	F        float64
	Embedded struct {
		Tag string
		ICt int
	}
	Body *json.RawMessage
}

func (a *Article) Header() string {
	return "title,author,count,deleted,created,updated,f"
}
func (a *Article) Row() string {
	return fmt.Sprintf("%s,%s,%v,%v,%v,%v,%v", a.Title, a.Author, a.Count, a.Deleted, a.Created, a.Updated, a.F)
}
func (a *Article) ColNames() []string {
	return []string{"title", "author", "count", "count64", "deleted", "category",
		"created", "updated", "f", "body"}
}
func (a *Article) Values() []driver.Value {
	return []driver.Value{
		a.Title, a.Author, a.Count, a.Count64, a.Deleted, a.Category,
		a.Created, a.Updated, a.F, a.Body,
	}
}
func (a *Article) ValueI() []interface{} {
	return []interface{}{
		a.Title, a.Author, a.Count, a.Count64, a.Deleted, a.Category,
		a.Created, a.Updated, a.F, a.Body,
	}
}
func (a *Article) UrlMsg() url.Values {
	msg := url.Values{}
	msg.Add("title", a.Title)
	msg.Add("author", a.Author)
	msg.Add("count", fmt.Sprintf("%v", a.Count))
	msg.Add("deleted", fmt.Sprintf("%v", a.Deleted))
	msg.Add("created", fmt.Sprintf("%v", a.Created))
	msg.Add("updated", fmt.Sprintf("%v", a.Updated))
	msg.Add("f", fmt.Sprintf("%v", a.F))
	return msg
}

type User struct {
	Id      string
	Name    string
	Deleted bool
	Roles   datasource.StringArray
	Created time.Time
	Updated *time.Time
}

func (u *User) Header() string {
	return "id,name,deleted,created,updated"
}
func (u *User) Row() string {
	return fmt.Sprintf("%s,%s,%v,%v,%v", u.Id, u.Name, u.Deleted, u.Created, u.Updated)
}
func (a *User) ColNames() []string {
	return []string{"id", "name", "deleted", "roles", "created", "updated"}
}
func (u *User) Values() []driver.Value {
	return []driver.Value{
		u.Id, u.Name, u.Deleted, u.Roles, u.Created, u.Updated,
	}
}
func (u *User) ValueI() []interface{} {
	return []interface{}{
		u.Id, u.Name, u.Deleted, u.Roles, u.Created, u.Updated,
	}
}

type QuerySpec struct {
	Sql             string
	Exec            string
	Cols            []string
	ValidateRow     func([]interface{})
	ExpectRowCt     int
	ExpectColCt     int
	RowData         interface{}
	ValidateRowData func()
}

func ValidateQuery(t testing.TB, querySql string, expectCols []string, expectColCt, expectRowCt int, rowValidate func([]interface{})) {
	ValidateQuerySpec(t, QuerySpec{Sql: querySql,
		Cols:        expectCols,
		ExpectRowCt: expectRowCt, ExpectColCt: expectColCt,
		ValidateRow: rowValidate})
}

func ValidateQuerySpec(t testing.TB, testSpec QuerySpec) {

	switch tt := t.(type) {
	case *testing.T:
		testmysql.RunTestServer(tt)
	}

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", fmt.Sprintf("root@tcp(127.0.0.1:13307)/%s?parseTime=true", DbName))
	if err != nil {
		t.Errorf("Expected no err got %v", err)
	}
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	switch {
	case len(testSpec.Exec) > 0:
		result, err := dbx.Exec(testSpec.Exec)
		if err != nil {
			t.Errorf("Expected no err got %v", err)
		}
		u.Infof("result: %#v", result)
		if testSpec.ExpectRowCt > -1 {
			affected, err := result.RowsAffected()
			if err != nil {
				t.Errorf("Expected no err got %v", err)
			}
			if affected != int64(testSpec.ExpectRowCt) {
				t.Errorf("expected %v affected but got %v for %s", testSpec.ExpectRowCt, affected, testSpec.Exec)
			}
		}

	case len(testSpec.Sql) > 0:
		//u.Debugf("----ABOUT TO QUERY")
		rows, err := dbx.Queryx(testSpec.Sql)
		if err != nil {
			t.Errorf("Expected no err got %v", err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			t.Errorf("Expected no err got %v", err)
		}
		if len(testSpec.Cols) > 0 {
			for _, expectCol := range testSpec.Cols {
				found := false
				for _, colName := range cols {
					if colName == expectCol {
						found = true
					}
				}
				if !found {
					t.Errorf("Should have found column: %v", expectCol)
				}
			}
		}
		rowCt := 0
		for rows.Next() {
			if testSpec.RowData != nil {
				err = rows.StructScan(testSpec.RowData)
				//u.Infof("%#v rowVals: %#v", rows.Rows., testSpec.RowData)
				if err != nil {
					t.Errorf("Expected no err got %v for data: %+v", err, testSpec.RowData)
				}
				rowCt++
				if testSpec.ValidateRowData != nil {
					testSpec.ValidateRowData()
				}

			} else {
				// rowVals is an []interface{} of all of the column results
				rowVals, err := rows.SliceScan()
				//u.Infof("rowVals: %#v", rowVals)
				if err != nil {
					t.Errorf("Expected no err got %v", err)
				}
				if testSpec.ExpectColCt > 0 {
					if len(rowVals) != testSpec.ExpectColCt {
						t.Errorf("wanted %d cols but got %v", testSpec.ExpectColCt, len(rowVals))
					}
				}
				rowCt++
				if testSpec.ValidateRow != nil {
					testSpec.ValidateRow(rowVals)
				}
			}

		}

		if testSpec.ExpectRowCt > -1 {
			if rowCt != testSpec.ExpectRowCt {
				t.Errorf("expected %v rows but got %v", testSpec.ExpectRowCt, rowCt)
			}
		}
		if rows.Err() != nil {
			t.Errorf("Expected no err got %v", err)
		}
	}

}
