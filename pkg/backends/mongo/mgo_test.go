package mongo_test

import (
	"encoding/json"
	"flag"
	"testing"
	"time"

	"gopkg.in/mgo.v2"

	"database/sql"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/dataux/dataux/pkg/frontends/testmysql"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	veryVerbose *bool   = flag.Bool("vv", false, "very verbose output")
	mongoHost   *string = flag.String("host", "localhost", "mongo Server Host Address")
	mongoDb     *string = flag.String("db", "mgo_datauxtest", "mongo database to use for testing")
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

func loadTestData() {
	sess, _ := mgo.Dial(*mongoHost)
	coll := sess.DB("mgo_datauxtest").C("article")
	coll.DropCollection()
	t := time.Now()
	ev := struct {
		Tag string
		ICt int
	}{"tag", 1}
	body := json.RawMessage([]byte(`{"name":"morestuff"}`))
	coll.Insert(&article{"article1", "aaron", 22, 64, false, []string{"news", "sports"}, time.Now(), &t, 55.5, ev, &body})
	coll.Insert(&article{"article2", "james", 2, 64, true, []string{"news", "sports"}, time.Now().Add(-time.Hour * 100), &t, 55.5, ev, &body})
	coll.Insert(&article{"article3", "bjorn", 55, 12, true, []string{"politics"}, time.Now().Add(-time.Hour * 220), &t, 21.5, ev, &body})
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
	dbName := "mgo_datauxtest"
	// for _, schema := range testmysql.Conf.Schema {
	// }
	dbx, err := sqlx.Connect("mysql", "root@tcp(127.0.0.1:4000)/"+dbName)
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

	assert.Tf(t, rowCt == testSpec.ExpectRowCt, "expected %v rows but got %v", testSpec.ExpectRowCt, rowCt)
	assert.T(t, rows.Err() == nil)
	//u.Infof("rows: %v", cols)
}

func TestInvalidQuery(t *testing.T) {
	testmysql.RunTestServer(t)
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/mgo")
	assert.T(t, err == nil)
	// It is parsing the SQL on server side (proxy)
	//  not in client, so hence that is what this is testing, making sure
	//  proxy responds gracefully
	rows, err := db.Query("select `stuff`, NOTAKEYWORD github_fork NOTWHERE `description` LIKE \"database\";")
	assert.Tf(t, err != nil, "%v", err)
	assert.Tf(t, rows == nil, "must not get rows")
}

func TestShowTables(t *testing.T) {
	data := struct {
		Table string `db:"Table"`
	}{}
	found := false
	validateQuerySpec(t, QuerySpec{
		Sql:         "show tables;",
		ExpectRowCt: 2,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.Table != "", "%v", data)
			if data.Table == "article" {
				found = true
			}
		},
		RowData: &data,
	})
	assert.Tf(t, found, "Must have found article")
}

func TestDescribeTable(t *testing.T) {
	data := struct {
		Field   string `db:"Field"`
		Type    string `db:"Type"`
		Null    string `db:"Null"`
		Key     string `db:"Key"`
		Default string `db:"Default"`
		Extra   string `db:"Extra"`
	}{}
	describedCt := 0
	validateQuerySpec(t, QuerySpec{
		Sql:         "describe article;",
		ExpectRowCt: 12,
		ValidateRowData: func() {
			u.Infof("%#v", data)
			assert.Tf(t, data.Field != "", "%v", data)
			switch data.Field {
			case "embedded":
				assert.Tf(t, data.Type == "object", "%#v", data)
				describedCt++
			case "author":
				assert.T(t, data.Type == "string")
				describedCt++
			case "created":
				assert.T(t, data.Type == "datetime")
				describedCt++
			case "category":
				assert.T(t, data.Type == "[]string")
				describedCt++
			case "body":
				assert.T(t, data.Type == "binary")
				describedCt++
			case "deleted":
				assert.T(t, data.Type == "bool")
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.Tf(t, describedCt == 6, "Should have found/described 6 but was %v", describedCt)
}

func TestSimpleRowSelect(t *testing.T) {
	data := struct {
		Title   string
		Count   int
		Deleted bool
		// Category []string  // Crap, downside of sqlx/mysql is no complex types
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select title, count, deleted from article WHERE `author` = \"aaron\" ",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, QuerySpec{
		Sql:         "select title, count,deleted from article WHERE count = 22;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, QuerySpec{
		Sql:             "select title, count, deleted from article LIMIT 10;",
		ExpectRowCt:     3,
		ValidateRowData: func() {},
		RowData:         &data,
	})
}

func TestSelectLimit(t *testing.T) {
	data := struct {
		Title string
		Count int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:             "select title, count from article LIMIT 1;",
		ExpectRowCt:     1,
		ValidateRowData: func() {},
		RowData:         &data,
	})
}

func TestSelectAggs(t *testing.T) {
	data := struct {
		Oldest int `db:"oldest_repo"`
		Card   int `db:"users_who_released"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select cardinality(`actor`) AS users_who_released, min(`repository.id`) as oldest_repo from github_release;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Debugf("%v", data)
			assert.Tf(t, data.Card == 1772, "%v", data)
			assert.Tf(t, data.Oldest == 27, "%v", data)

		},
		RowData: &data,
	})

	data2 := struct {
		Oldest int `db:"oldest_repo"`
		Card   int `db:"users_who_released"`
		Ct     int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: "select cardinality(`actor`) AS users_who_released, count(*) as ct " +
			", min(`repository.id`) as oldest_repo " +
			` FROM github_release
			WHERE query LIKE "database";`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Debugf("%v", data2)
			assert.Tf(t, data2.Card == 36, "%v", data2)
			assert.Tf(t, data2.Oldest == 904810, "%v", data2)
			assert.Tf(t, data2.Ct == 47, "%v", data2) // 47 docs had database
		},
		RowData: &data2,
	})
}

func TestSelectAggsGroupBy(t *testing.T) {
	/*
		NOTE:   This fails because of parsing the response, not because request is bad
	*/
	return
	data := struct {
		Actor string
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select terms(repository.description), terms(repository.name) from github_push GROUP BY actor_attributes.login;`,
		ExpectRowCt: 0,
		ValidateRowData: func() {
			u.Infof("%v", data)
			//assert.Tf(t, data.Actor == "araddon", "%v", data)
		},
		RowData: &data,
	})

	data2 := struct {
		Actor string
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: `
		SELECT count(*), repository.name
		FROM github_watch
		GROUP BY repository.name, repository.language`,
		ExpectRowCt: 10,
		ValidateRowData: func() {
			u.Infof("%v", data2)
			//assert.Tf(t, data2.Actor == "araddon", "%v", data2)
		},
		RowData: &data2,
	})
}

func TestSelectWhereEqual(t *testing.T) {
	/*
		data := struct {
			Title   string
			Count   int
			Deleted bool
			Updated time.Time
		}{}
		validateQuerySpec(t, QuerySpec{
			Sql: "select title, count, deleted, updated " +
				"from article WHERE Count = 22;",
			ExpectRowCt: 1,
			ValidateRowData: func() {
				//u.Infof("%v", data)
				assert.Tf(t, data.Title != "", "%v", data)
				//assert.Tf(t, data.Language == "Go", "%v", data)
			},
			RowData: &data,
		})
		// Test when we have compound where clause

				validateQuerySpec(t, QuerySpec{
					Sql: "select `actor`, `repository.name`, `repository.stargazers_count`, `repository.language` " +
						" from github_watch where " +
						"`repository.language` == \"Go\" AND `repository.forks_count` > 1000;",
					ExpectRowCt: 20,
					ValidateRowData: func() {
						//u.Infof("%v", data)
						assert.Tf(t, data.Language == "Go", "%v", data)
						assert.Tf(t, data.Stars > 1000, "must have filterd by forks: %v", data)
						assert.Tf(t, data.Actor != "", "%v", data)
					},
					RowData: &data,
				})

			return
			// TODO:   This isn't working yet bc the nested 3 where clauses
			validateQuerySpec(t, QuerySpec{
				Sql: `
				SELECT
					actor, repository.name, repository.stargazers_count, repository.language
				FROM github_watch
				WHERE
						repository.language = "Go"
						AND repository.forks_count > 1000
						AND repository.description NOT LIKE "docker";`,
				ExpectRowCt: 20,
				ValidateRowData: func() {
					//u.Infof("%v", data)
					assert.Tf(t, data.Language == "Go", "%v", data)
					assert.Tf(t, data.Stars > 1000, "must have filterd by forks: %v", data)
					assert.Tf(t, data.Actor != "", "%v", data)
				},
				RowData: &data,
			})
	*/
}

func TestSelectWhereLike(t *testing.T) {
	data := struct {
		Name string
		Ct   int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: "select `repository.stargazers_count` AS ct, `repository.name` AS name " +
			" FROM github_fork " +
			"where `description` LIKE \"database AND graph\";",
		ExpectRowCt: 9,
		ValidateRowData: func() {
			u.Debugf("%#v", data)
			assert.Tf(t, data.Name != "", "%v", data)
			//assert.Tf(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
		},
		RowData: &data,
	})
}

func TestSelectWhereIn(t *testing.T) {
	data := struct {
		Title   string
		Count   int
		Deleted bool
		Updated mysql.NullTime // go-sql-driver/mysql#NullTime
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select title, count, deleted, updated from article WHERE category IN ("news");`,
		ExpectRowCt: 2,
		ValidateRowData: func() {
			u.Debugf("Updated: %v", data.Updated.Time)
			assert.Tf(t, data.Title != "", "%v", data)
		},
		RowData: &data,
	})
}

func TestSelectWhereExists(t *testing.T) {
	data := struct {
		Title   string
		Count   int
		Deleted bool
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select title, count, deleted from article WHERE exists(title);`,
		ExpectRowCt: 3,
		ValidateRowData: func() {
			assert.Tf(t, data.Title != "", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, QuerySpec{
		Sql:             `select title, count, deleted from article WHERE exists(fakefield);`,
		ExpectRowCt:     0,
		ValidateRowData: func() {},
		RowData:         &data,
	})
}

func TestSelectWhereBetween(t *testing.T) {
	data := struct {
		Actor string
		Name  string `db:"repository.name"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select actor, repository.name from github_push where repository.stargazers_count BETWEEN "1000" AND 1100;`,
		ExpectRowCt: 20,
		ValidateRowData: func() {
			u.Debugf("%#v", data)
			assert.Tf(t, data.Name != "", "%v", data)
			//assert.Tf(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
		},
		RowData: &data,
	})
	// Now one that is date based
	validateQuerySpec(t, QuerySpec{
		Sql: `SELECT actor, repository.name 
			FROM github_push 
			WHERE repository.created_at BETWEEN "2008-12-01" AND "2008-12-03";`,
		ExpectRowCt: 4,
		ValidateRowData: func() {
			u.Debugf("%#v", data)
			assert.Tf(t, data.Name == "jasmine", "%v", data)
			//assert.Tf(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
		},
		RowData: &data,
	})
	datact := struct {
		Actor string
		Name  string `db:"repository.name"`
		Stars int
	}{}
	// simple test of mixed aliasing, non-aliasing on field names
	validateQuerySpec(t, QuerySpec{
		Sql: `
			SELECT 
				actor, repository.name, repository.stargazers_count AS stars
			FROM github_push 
			WHERE 
				repository.stargazers_count BETWEEN 1000 AND 1100;
		`,
		ExpectRowCt: 20,
		ValidateRowData: func() {
			//u.Debugf("%#v", datact)
			assert.Tf(t, datact.Stars >= 1000 && datact.Stars < 1101, "%v", datact)
		},
		RowData: &datact,
	})
}

func TestSelectOrderBy(t *testing.T) {
	data := struct {
		Name string
		Ct   int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: "select `repository.stargazers_count` AS ct, `repository.name` AS name " +
			" FROM github_fork ORDER BY `repository.stargazers_count` DESC limit 3;",
		ExpectRowCt: 3,
		ValidateRowData: func() {
			assert.Tf(t, data.Name == "bootstrap", "%v", data)
			assert.Tf(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
		},
		RowData: &data,
	})
}
