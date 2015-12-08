package mongo_test

import (
	"database/sql"
	"flag"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/mgo.v2"

	"github.com/dataux/dataux/pkg/frontends/testmysql"
	"github.com/dataux/dataux/pkg/testutil"
)

var (
	mongoHost *string = flag.String("host", "localhost", "mongo Server Host Address")
	mongoDb   *string = flag.String("db", "mgo_datauxtest", "mongo database to use for testing")
)

func init() {
	testutil.Setup()
	loadTestData()
}

func loadTestData() {
	sess, _ := mgo.Dial(*mongoHost)
	userColl := sess.DB("mgo_datauxtest").C("user")
	userColl.DropCollection()
	articleColl := sess.DB("mgo_datauxtest").C("article")
	articleColl.DropCollection()
	for _, article := range testutil.Articles {
		articleColl.Insert(article)
	}
	for _, user := range testutil.Users {
		userColl.Insert(user)
	}
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

func TestSchemaQueries(t *testing.T) {
	found := false
	validateQuerySpec(t, QuerySpec{
		Sql: `
		select  
			@@session.auto_increment_increment as auto_increment_increment, 
  			@@character_set_client as character_set_client, 
  			@@character_set_connection as character_set_connection`,
		ExpectRowCt: 1,
		ValidateRow: func(row []interface{}) {
			u.Infof("%v", row)
		},
	})
	assert.Tf(t, found, "Must have found @@vaars")
}

func TestShowTables(t *testing.T) {
	data := struct {
		Table string `db:"Table"`
	}{}
	found := false
	validateQuerySpec(t, QuerySpec{
		Sql:         "show tables;",
		ExpectRowCt: -1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.Table != "", "%v", data)
			if data.Table == "article" {
				found = true
			}
		},
		RowData: &data,
	})
	assert.Tf(t, found, "Must have found article table with show")
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

	return
	validateQuerySpec(t, QuerySpec{
		Sql:         "select title, count, deleted from article WHERE `author` = \"aaron\" AND count = 22 ",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, QuerySpec{
		Sql:         "select title, count, deleted from article WHERE `author` = \"notarealname\" OR count = 22 ",
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
		ExpectRowCt:     4,
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

/*
func TestSelectAggsSimple(t *testing.T) {

	// TODO:  Not implemented
	t.Fail()
	return
	// db.article.aggregate([{"$group":{_id: null, count: {"$sum":1}}}]);
	// db.article.aggregate([{"$group":{_id: "$author", count: {"$sum":1}}}]);

	data := struct {
		Ct     int    `db:"article_ct"`
		Author string `db:"author"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select author, count(*) AS article_ct from article group by author;",
		ExpectRowCt: 3,
		ValidateRowData: func() {
			//u.Debugf("%v", data)
			switch data.Author {
			case "bjorn":
				assert.Tf(t, data.Ct == 2, "%v", data)
			}
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
	// TODO implement
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
*/
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
		Title string
		Ct    int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `SELECT title, count as ct from article WHERE title like "list%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, QuerySpec{
		Sql:         `SELECT title, count as ct from article WHERE title like "%stic%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
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
		ExpectRowCt: 4,
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
		Title  string
		Author string `db:"author"`
		Count  int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select title, count, author from article where count BETWEEN 5 AND 25;`,
		ExpectRowCt: 2,
		ValidateRowData: func() {
			u.Debugf("%#v", data)
			switch data.Title {
			case "article1":
				assert.Tf(t, data.Count == 22, "%v", data)
			case "listicle1":
				assert.Tf(t, data.Count == 7, "%v", data)
			default:
				t.Errorf("Should not be in results: %#v", data)
			}
		},
		RowData: &data,
	})

	// Now one that is date based
	validateQuerySpec(t, QuerySpec{
		Sql:         `select title, count, author from article where created BETWEEN todate("2011-08-01") AND todate("2013-08-03");`,
		ExpectRowCt: 2,
		ValidateRowData: func() {
			//u.Debugf("%#v", data)
			switch data.Title {
			case "article2":
				assert.Tf(t, data.Count == 2, "%v", data)
			case "article3":
				assert.Tf(t, data.Count == 55, "%v", data)
			default:
				t.Errorf("Should not be in results: %#v", data)
			}
		},
		RowData: &data,
	})

	// Now try that again but without todate
	// TODO - need to convert string -> date by virtue of schema knowing it is a date?
	// validateQuerySpec(t, QuerySpec{
	// 	Sql:         `select title, count, author from article where created BETWEEN "2011-08-01" AND "2013-08-03";`,
	// 	ExpectRowCt: 2,
	// 	ValidateRowData: func() {
	// 		u.Debugf("%#v", data)
	// 		switch data.Title {
	// 		case "article1":
	// 			assert.Tf(t, data.Count == 22, "%v", data)
	// 		case "listicle1":
	// 			assert.Tf(t, data.Count == 7, "%v", data)
	// 		default:
	// 			t.Errorf("Should not be in results: %#v", data)
	// 		}
	// 	},
	// 	RowData: &data,
	// })
}

func TestSelectOrderBy(t *testing.T) {
	data := struct {
		Title string
		Ct    int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select title, count64 AS ct FROM article ORDER BY count64 DESC LIMIT 1;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "article3", "%v", data)
			assert.Tf(t, data.Ct == 100, "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, QuerySpec{
		Sql:         "select title, count64 AS ct FROM article ORDER BY count64 ASC LIMIT 1;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
			assert.Tf(t, data.Ct == 12, "%v", data)
		},
		RowData: &data,
	})
}

func TestMongoToMongoJoin(t *testing.T) {
	//  - No sort (overall), or where, full scans
	sqlText := `
		SELECT 
			a.title, u.id
		FROM article AS a 
		INNER JOIN user AS u 
			ON u.name = a.author
	`

	data := struct {
		Title string
		Id    string
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         sqlText,
		ExpectRowCt: 4,
		ValidateRowData: func() {
			switch data.Title {
			case "listicle1":
				assert.Tf(t, data.Id == "user789", "%#v", data)
			case "article1":
				assert.Tf(t, data.Id == "user123", "%#v", data)
			case "article2":
				assert.Tf(t, data.Id == "user456", "%#v", data)
			case "article3":
				assert.Tf(t, data.Id == "user789", "%#v", data)
			default:
				assert.Tf(t, false, "Should not have found this column: %#v", data)
			}

		},
		RowData: &data,
	})

	/*
	   - Where Statement (rewrite query)
	*/
}
