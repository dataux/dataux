package mongo_test

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/go-sql-driver/mysql"
	"gopkg.in/mgo.v2"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	tu "github.com/dataux/dataux/testutil"
)

var (
	mongoHost           *string = flag.String("host", "localhost", "mongo Server Host Address")
	mongoDb             *string = flag.String("db", "mgo_datauxtest", "mongo database to use for testing")
	testServicesRunning bool
)

func init() {
	u.DiscardStandardLogger()
	tu.Setup()
	loadTestData()
	u.DiscardStandardLogger()
}

func loadTestData() {
	sess, _ := mgo.Dial(*mongoHost)
	userColl := sess.DB("mgo_datauxtest").C("user")
	userColl.DropCollection()
	articleColl := sess.DB("mgo_datauxtest").C("article")
	articleColl.DropCollection()
	for _, article := range tu.Articles {
		articleColl.Insert(article)
	}
	for _, user := range tu.Users {
		userColl.Insert(user)
	}
}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	ctx.Schema = testmysql.Schema
	//u.Warnf("jobMaker, going to do a full plan?")
	return planner.BuildExecutorUnPlanned(ctx, testmysql.ServerCtx.PlanGrid)
}

func RunTestServer(t *testing.T) {
	if !testServicesRunning {
		testServicesRunning = true
		planner.GridConf.JobMaker = jobMaker
		planner.GridConf.SchemaLoader = testmysql.SchemaLoader
		planner.GridConf.SupressRecover = testmysql.Conf.SupressRecover
		testmysql.RunTestServer(t)
		quit := make(chan bool)
		planner.RunWorkerNodes(quit, 2, testmysql.ServerCtx.Reg)
	}
}
func validateQuerySpec(t *testing.T, testSpec tu.QuerySpec) {
	RunTestServer(t)
	tu.ValidateQuerySpec(t, testSpec)
}

func TestInvalidQuery(t *testing.T) {
	RunTestServer(t)
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13307)/datauxtest")
	assert.T(t, err == nil)
	// It is parsing the SQL on server side (proxy)
	//  not in client, so hence that is what this is testing, making sure
	//  proxy responds gracefully with an error
	rows, err := db.Query("select `stuff`, NOTAKEYWORD github_fork NOTWHERE `description` LIKE \"database\";")
	assert.Tf(t, err != nil, "%v", err)
	assert.Tf(t, rows == nil, "must not get rows")
}

func TestSessionVarQueries(t *testing.T) {

	found := false
	data := struct {
		Max int64 `db:"@@max_allowed_packet"`
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql: `
		select @@max_allowed_packet`,
		ExpectRowCt: 1,
		RowData:     &data,
		ValidateRowData: func() {
			u.Infof("%v  T:%T", data, data.Max)
			assert.T(t, data.Max == 4194304)
			found = true
		},
	})
	assert.Tf(t, found, "Must have found @@vaars")

	found = false
	validateQuerySpec(t, tu.QuerySpec{
		Sql: `
		select  
			@@session.auto_increment_increment as auto_increment_increment, 
			@@character_set_client as character_set_client, 
			@@character_set_connection as character_set_connection`,
		ExpectRowCt: 1,
		ExpectColCt: 3,
		ValidateRow: func(row []interface{}) {
			u.Infof("%#v", row)
			assert.T(t, len(row) == 3)
			found = true
		},
	})
	assert.Tf(t, found, "Must have found @@vaars")

	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13307)/datauxtest")
	assert.T(t, err == nil)

	result, err := db.Exec(`SET @my_test_var = "hello world";`)
	assert.Tf(t, err == nil, "got error on SET? %v", err)
	assert.Tf(t, result != nil, "must get result")

	rows, err := db.Query("select @my_test_var;")
	assert.Tf(t, err == nil, "%v", err)
	assert.T(t, rows.Next(), "Must have a row")
	hw := ""
	err = rows.Scan(&hw)
	assert.T(t, hw == "hello world", "Should have found @var?")
}

func TestShowTables(t *testing.T) {

	found := false
	data := struct {
		Table string `db:"Table"`
	}{}
	validateQuerySpec(t, tu.QuerySpec{
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

	data2 := struct {
		Table  string `db:"Table"`
		Create string `db:"Create Table"`
	}{}
	found = false
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "SHOW CREATE TABLE `article`;",
		ExpectRowCt: -1,
		ValidateRowData: func() {
			u.Infof("\n%v", data2)
			assert.Tf(t, data2.Table != "", "%v", data2)
			if data2.Table == "article" {
				found = true
			}
			assert.Tf(t, len(data2.Create) > 10, "has create statement")
		},
		RowData: &data2,
	})
	assert.Tf(t, found, "Must have found article table with show")
}
func TestShowColumns(t *testing.T) {
	//[]string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	data := struct {
		Field      string         `db:"Field"`
		Type       string         `db:"Type"`
		Collation  sql.NullString `db:"Collation"`
		Null       string         `db:"Null"`
		Key        sql.NullString `db:"Key"`
		Default    interface{}    `db:"Default"`
		Extra      sql.NullString `db:"Extra"`
		Privileges sql.NullString `db:"Privileges"`
		Comment    sql.NullString `db:"Comment"`
	}{}
	describedCt := 0
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         fmt.Sprintf("show full columns from `article` from `%s` LIKE '%%'", tu.DbName),
		ExpectRowCt: 12,
		ValidateRowData: func() {
			u.Infof("%#v", data)
			assert.Tf(t, data.Field != "", "%v", data)
			switch data.Field {
			case "embedded":
				assert.Tf(t, data.Type == "text", "wanted text got %v", data.Type)
				describedCt++
			case "author":
				assert.Tf(t, data.Type == "varchar(255)", "wanted varchar(255) got %q", data.Type)
				describedCt++
			case "created":
				assert.Tf(t, data.Type == "datetime", "Wanted datetime, got %q")
				describedCt++
			case "category":
				assert.Tf(t, data.Type == "text", `wanted "text" got %q`, data.Type)
				describedCt++
			case "body":
				assert.Tf(t, data.Type == "text", "wanted text got %q")
				describedCt++
			case "deleted":
				assert.Tf(t, data.Type == "tinyint", "Wanted tinyint got? %q", data.Type)
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.Tf(t, describedCt == 6, "Should have found/described 6 but was %v", describedCt)
}

func TestDescribeTable(t *testing.T) {
	data := struct {
		Field   string         `db:"Field"`
		Type    string         `db:"Type"`
		Null    string         `db:"Null"`
		Key     sql.NullString `db:"Key"`
		Default interface{}    `db:"Default"`
		Extra   sql.NullString `db:"Extra"`
	}{}
	describedCt := 0
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "describe article;",
		ExpectRowCt: 12,
		ValidateRowData: func() {
			//u.Infof("%#v", data)
			assert.Tf(t, data.Field != "", "%v", data)
			switch data.Field {
			case "embedded":
				assert.Tf(t, data.Type == "text", "%#v", data)
				describedCt++
			case "author":
				assert.T(t, data.Type == "varchar(255)")
				describedCt++
			case "created":
				assert.T(t, data.Type == "datetime")
				describedCt++
			case "category":
				assert.T(t, data.Type == "text")
				describedCt++
			case "body":
				assert.T(t, data.Type == "text")
				describedCt++
			case "deleted":
				assert.T(t, data.Type == "tinyint", "type?", data.Type)
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.Tf(t, describedCt == 6, "Should have found/described 6 but was %v", describedCt)
}

func TestSelectStar(t *testing.T) {
	RunTestServer(t)
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13307)/datauxtest")
	assert.T(t, err == nil)
	rows, err := db.Query("select * from article;")
	assert.Tf(t, err == nil, "did not want err but got %v", err)
	cols, _ := rows.Columns()
	assert.Tf(t, len(cols) == 12, "want 12 cols but got %v", cols)
	assert.Tf(t, rows.Next(), "must get next row but couldn't")
	readCols := make([]interface{}, len(cols))
	writeCols := make([]string, len(cols))
	for i, _ := range writeCols {
		readCols[i] = &writeCols[i]
	}
	rows.Scan(readCols...)
	//assert.Tf(t, len(rows) == 12, "must get 12 rows but got %d", len(rows))
}
func TestSelectCountStar(t *testing.T) {
	data := struct {
		Count int `db:"count(*)"`
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select count(*) from article",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%#v", data.Count)
			//assert.Tf(t, data.Count == 4, "Not count right?? %v", data)
		},
		RowData: &data,
	})
}

func TestSelectDistributed(t *testing.T) {
	data := struct {
		Avg float64 `db:"title_avg"`
	}{}

	// We are going to use the WITH distributed=true to force distribution
	// which is a temporary hack
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "SELECT AVG(CHAR_LENGTH(CAST(`title` AS CHAR))) as title_avg from article WITH distributed=true, node_ct=2",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%#v", data.Avg)
			assert.Tf(t, data.Avg == 8.75, "Not avg right?? %v", data)
		},
		RowData: &data,
	})

	data2 := struct {
		Ct     int    `db:"ct"`
		Author string `db:"author"`
	}{}

	return

	// TODO:  fix me, this doesn't work because our distributed group-by planner/exec
	//  expects partial results , mgo_sql must add column count for each sum, avg

	found := false
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "SELECT author, count(*) as ct FROM article GROUP BY author WITH distributed=true, node_ct=2",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			switch data2.Author {
			case "bjorn":
				found = true
				assert.Tf(t, data2.Ct == 2, "Not ct right?? %v", data2)
			case "aaron":
				assert.Tf(t, data2.Ct == 1, "Not ct right?? %v", data2)
			}
			u.Infof("%#v", data2)
		},
		RowData: &data2,
	})
	assert.T(t, found == true)
}

func TestSelectAggAvg(t *testing.T) {
	data := struct {
		Avg float64 `db:"title_avg"`
	}{}

	// Note, this needs to be poly-filled (the avg(char_lenght)) as isn't natively suppported in mongo
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select AVG(CHAR_LENGTH(CAST(`title` AS CHAR))) as title_avg from article",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%#v", data.Avg)
			assert.Tf(t, data.Avg == 8.75, "Not avg right?? %v", data)
		},
		RowData: &data,
	})
	// Same test only with a left.right
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select AVG(CHAR_LENGTH(CAST(`article.title` AS CHAR))) as title_avg from article",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%#v", data.Avg)
			assert.Tf(t, data.Avg == 8.75, "Not avg right?? %v", data)
		},
		RowData: &data,
	})
}

func TestSimpleRowSelect(t *testing.T) {
	data := struct {
		Title   string
		Count   int
		Deleted bool
		//Category *datasource.StringArray
	}{}
	validateQuerySpec(t, tu.QuerySpec{
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

	// The problem here is ??  related to the mysql/mysqlx/type etc, the values are being written
	dataComplex := struct {
		Title    string
		Count    int
		Deleted  bool
		Category datasource.StringArray
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count, deleted, category from article WHERE `author` = \"aaron\" ",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", dataComplex)
			assert.Tf(t, dataComplex.Deleted == false, "Not deleted? %v", dataComplex)
			assert.Tf(t, dataComplex.Title == "article1", "%v", dataComplex)
		},
		RowData: &dataComplex,
	})

	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count, deleted from article WHERE `author` = \"aaron\" AND count = 22 ",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count, deleted from article WHERE `author` = \"notarealname\" OR count = 22 ",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count,deleted from article WHERE count = 22;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
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
		validateQuerySpec(t, tu.QuerySpec{
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

				validateQuerySpec(t, tu.QuerySpec{
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
			validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, count as ct from article WHERE title like "list%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `select title, count, deleted, updated from article WHERE category IN ("news");`,
		ExpectRowCt: 2,
		ValidateRowData: func() {
			//u.Debugf("Updated: %v", data.Updated.Time)
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `select title, count, deleted from article WHERE exists(title);`,
		ExpectRowCt: 4,
		ValidateRowData: func() {
			assert.Tf(t, data.Title != "", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `select title, count, author from article where count BETWEEN 5 AND 25;`,
		ExpectRowCt: 2,
		ValidateRowData: func() {
			//u.Debugf("%#v", data)
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `select title, count, author from article where created BETWEEN todate("2011-08-01") AND todate("2013-08-03");`,
		ExpectRowCt: 2,
		ValidateRowData: func() {
			//u.Debugf("%#v", data)
			switch data.Title {
			case "qarticle2":
				assert.Tf(t, data.Count == 2, "%v", data)
			case "zarticle3":
				assert.Tf(t, data.Count == 55, "%v", data)
			default:
				t.Errorf("Should not be in results: %#v", data)
			}
		},
		RowData: &data,
	})

	// Now try that again but without todate
	// TODO - need to convert string -> date by virtue of schema knowing it is a date?
	// validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count64 AS ct FROM article ORDER BY count64 DESC LIMIT 1;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "zarticle3", "%v", data)
			assert.Tf(t, data.Ct == 100, "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
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
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         sqlText,
		ExpectRowCt: 4,
		ValidateRowData: func() {
			switch data.Title {
			case "listicle1":
				assert.Tf(t, data.Id == "user789", "%#v", data)
			case "article1":
				assert.Tf(t, data.Id == "user123", "%#v", data)
			case "qarticle2":
				assert.Tf(t, data.Id == "user456", "%#v", data)
			case "zarticle3":
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
