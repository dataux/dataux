package testutil

import (
	"encoding/json"
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/bmizerany/assert"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/dataux/dataux/pkg/frontends/testmysql"
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
			u.SetupLoggingLong(*logLevel)
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

type User struct {
	Id      string
	Name    string
	Deleted bool
	Roles   datasource.StringArray
	Created time.Time
	Updated *time.Time
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

func ValidateQuery(t *testing.T, querySql string, expectCols []string, expectColCt, expectRowCt int, rowValidate func([]interface{})) {
	ValidateQuerySpec(t, QuerySpec{Sql: querySql,
		Cols:        expectCols,
		ExpectRowCt: expectRowCt, ExpectColCt: expectColCt,
		ValidateRow: rowValidate})
}

func ValidateQuerySpec(t *testing.T, testSpec QuerySpec) {

	testmysql.RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", "root@tcp(127.0.0.1:13307)/datauxtest?parseTime=true")
	assert.Tf(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	switch {
	case len(testSpec.Exec) > 0:
		result, err := dbx.Exec(testSpec.Exec)
		assert.Tf(t, err == nil, "%v", err)
		//u.Infof("result: %Ev", result)
		if testSpec.ExpectRowCt > -1 {
			affected, err := result.RowsAffected()
			assert.Tf(t, err == nil, "%v", err)
			assert.Tf(t, affected == int64(testSpec.ExpectRowCt), "expected %v affected but got %v for %s", testSpec.ExpectRowCt, affected, testSpec.Exec)
		}

	case len(testSpec.Sql) > 0:
		rows, err := dbx.Queryx(testSpec.Sql)
		assert.Tf(t, err == nil, "%v", err)
		defer rows.Close()

		cols, err := rows.Columns()
		assert.Tf(t, err == nil, "%v", err)
		if len(testSpec.Cols) > 0 {
			for _, expectCol := range testSpec.Cols {
				found := false
				for _, colName := range cols {
					if colName == expectCol {
						found = true
					}
				}
				assert.Tf(t, found, "Should have found column: %v", expectCol)
			}
		}
		rowCt := 0
		for rows.Next() {
			if testSpec.RowData != nil {
				err = rows.StructScan(testSpec.RowData)
				//u.Infof("rowVals: %#v", testSpec.RowData)
				assert.Tf(t, err == nil, "data:%+v   err=%v", testSpec.RowData, err)
				rowCt++
				if testSpec.ValidateRowData != nil {
					testSpec.ValidateRowData()
				}

			} else {
				// rowVals is an []interface{} of all of the column results
				rowVals, err := rows.SliceScan()
				//u.Infof("rowVals: %#v", rowVals)
				assert.Tf(t, err == nil, "%v", err)
				assert.Tf(t, len(rowVals) == testSpec.ExpectColCt, "wanted cols but got %v", len(rowVals))
				rowCt++
				if testSpec.ValidateRow != nil {
					testSpec.ValidateRow(rowVals)
				}
			}

		}

		if testSpec.ExpectRowCt > -1 {
			assert.Tf(t, rowCt == testSpec.ExpectRowCt, "expected %v rows but got %v", testSpec.ExpectRowCt, rowCt)
		}

		assert.T(t, rows.Err() == nil)
		//u.Infof("rows: %v", cols)
	}

}
