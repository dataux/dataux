package mongo_test

import (
	"encoding/json"
	"flag"
	"testing"
	"time"

	_ "github.com/dataux/dataux/backends/elasticsearch"
	_ "github.com/dataux/dataux/backends/mongo"
	_ "github.com/go-sql-driver/mysql"

	"gopkg.in/mgo.v2"

	"github.com/araddon/dateparse"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/dataux/dataux/frontends/testmysql"
	"github.com/jmoiron/sqlx"
)

var (
	veryVerbose *bool   = flag.Bool("vv", false, "very verbose output")
	mongoHost   *string = flag.String("mgohost", "localhost", "mongo Server Host Address")
	mongoDb     *string = flag.String("mgodb", "mgo_datauxtest", "mongo database to use for testing")
	esHost      *string = flag.String("eshost", "localhost", "Elasticsearch Server Host Address")
	logging     *string = flag.String("logging", "debug", "very verbose output")
)

func init() {
	flag.Parse()
	if *veryVerbose {
		u.SetupLogging(*logging)
		u.SetColorOutput()
	} else {
		u.SetupLogging("warn")
		u.SetColorOutput()
	}
	loadTestData()
}

type article struct {
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

type user struct {
	Id      string
	Name    string
	Deleted bool
	Roles   []string
	Created time.Time
	Updated *time.Time
}

func loadTestData() {
	sess, _ := mgo.Dial(*mongoHost)
	userCol := sess.DB("mgo_datauxtest").C("user")
	userCol.DropCollection()
	articleColl := sess.DB("mgo_datauxtest").C("article")
	articleColl.DropCollection()
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

	articleColl.Insert(&article{"article1", "araddon", 22, 75, false, []string{"news", "sports"}, t1, &t, 55.5, ev, &body})
	articleColl.Insert(&article{"qarticle2", "james", 2, 64, true, []string{"news", "sports"}, t2, &t, 55.5, ev, &body})
	articleColl.Insert(&article{"zarticle3", "bjorn", 55, 100, true, []string{"politics"}, t3, &t, 21.5, ev, &body})
	articleColl.Insert(&article{"listicle1", "bjorn", 7, 12, true, []string{"world"}, t4, &t, 21.5, ev, &body})
	// Users
	userCol.Insert(&user{"user123", "araddon", false, []string{"admin", "author"}, time.Now(), &t})
	userCol.Insert(&user{"user456", "james", true, []string{"admin", "author"}, time.Now().Add(-time.Hour * 100), &t})
	userCol.Insert(&user{"user789", "bjorn", true, []string{"author"}, time.Now().Add(-time.Hour * 220), &t})
}

type QuerySpec struct {
	Sql             string
	Cols            []string
	ValidateRow     func([]interface{})
	ExpectRowCt     int
	ExpectColCt     int
	RowData         interface{}
	ValidateRowData func()
}

func validateQuery(t *testing.T, querySql string, expectCols []string, expectColCt, expectRowCt int, rowValidate func([]interface{})) {
	validateQuerySpec(t, QuerySpec{Sql: querySql,
		Cols:        expectCols,
		ExpectRowCt: expectRowCt, ExpectColCt: expectColCt,
		ValidateRow: rowValidate})
}

func validateQuerySpec(t *testing.T, testSpec QuerySpec) {
	testmysql.RunTestServer(t)
	dbName := "datauxtest"
	dbx, err := sqlx.Connect("mysql", "root@tcp(127.0.0.1:13307)/"+dbName)
	assert.Tf(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
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
			assert.Tf(t, err == nil, "%v", err)
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
	if testSpec.ExpectRowCt >= 0 {
		assert.Tf(t, rowCt == testSpec.ExpectRowCt, "expected %v rows but got %v", testSpec.ExpectRowCt, rowCt)
	}

	assert.T(t, rows.Err() == nil)
	//u.Infof("rows: %v", cols)
}

func TestMongoToEsJoin(t *testing.T) {

	//  - No sort (overall), or where, full scans
	sqlText := `
		SELECT 
			p.actor, p.repository.name, u.id
		FROM user AS u 
		INNER JOIN github_push AS p 
			ON p.actor = u.name
	`

	data := struct {
		Actor string
		Repo  string `db:"repository.name"`
		Id    string
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         sqlText,
		ExpectRowCt: 9,
		ValidateRowData: func() {
			switch data.Actor {
			case "araddon":
				u.Debugf("araddon:  %#v", data)
				assert.Tf(t, data.Repo == "qlparser" || data.Repo == "dateparse", "%#v", data)
			default:
				//assert.Tf(t, false, "Should not have found this column: %#v", data)
				u.Debugf("%#v", data)
			}

		},
		RowData: &data,
	})
}
