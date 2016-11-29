package kubernetes

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"

	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	tu "github.com/dataux/dataux/testutil"
)

var (
	DbConn              = "root@tcp(127.0.0.1:13307)/datauxtest?parseTime=true"
	loadTestDataOnce    sync.Once
	now                 = time.Now()
	testServicesRunning bool
	_                   = json.RawMessage(nil)
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
	u.DiscardStandardLogger()
	flag.Parse()
	tu.Setup()
}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	ctx.Schema = testmysql.Schema
	return planner.BuildSqlJob(ctx, testmysql.ServerCtx.PlanGrid)
}

func RunTestServer(t *testing.T) func() {
	if !testServicesRunning {
		testServicesRunning = true

		loadTestData(t)

		planner.GridConf.JobMaker = jobMaker
		planner.GridConf.SchemaLoader = testmysql.SchemaLoader
		planner.GridConf.SupressRecover = testmysql.Conf.SupressRecover
		testmysql.RunTestServer(t)
		quit := make(chan bool)
		planner.RunWorkerNodes(quit, 2, testmysql.ServerCtx.Reg)
	}
	return func() {}
}

func validateQuerySpec(t *testing.T, testSpec tu.QuerySpec) {
	RunTestServer(t)
	tu.ValidateQuerySpec(t, testSpec)
}

func loadTestData(t *testing.T) {
	loadTestDataOnce.Do(func() {

		var sc *schema.ConfigSource
		for _, s := range testmysql.Conf.Sources {
			if s.SourceType == "kubernetes" {
				sc = s
			}
		}
		if sc == nil {
			panic("must have kubernetes source conf")
		}
		//u.Debugf("loading test data %#v", sc)

	})
}

func TestShowTables(t *testing.T) {

	RunTestServer(t)

	data := struct {
		Table string `db:"Table"`
	}{}
	found := false
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "show tables;",
		ExpectRowCt: len(endpoints),
		ValidateRowData: func() {
			u.Infof("%+v", data)
			assert.Tf(t, data.Table != "", "%v", data)
			if data.Table == strings.ToLower("pods") {
				found = true
			}
		},
		RowData: &data,
	})
	assert.Tf(t, found, "Must have found pods")
}

func TestBasic(t *testing.T) {

	RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", DbConn)
	assert.Tf(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	rows, err := dbx.Queryx(fmt.Sprintf("select * from pods"))
	assert.Equalf(t, err, nil, "%v", err)
	cols, _ := rows.Columns()
	assert.Tf(t, len(cols) > 5, "Should have columns %v", cols)
	defer rows.Close()

	_, err = dbx.Queryx(fmt.Sprintf("select kind, invalidcolumn from pods"))
	assert.NotEqual(t, err, nil, "Should have an error because invalid column")
}

func TestDescribeTable(t *testing.T) {

	loadTestData(t)

	data := struct {
		Field   string `db:"Field"`
		Type    string `db:"Type"`
		Null    string `db:"Null"`
		Key     string `db:"Key"`
		Default string `db:"Default"`
		Extra   string `db:"Extra"`
	}{}
	describedCt := 0
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         fmt.Sprintf("describe article;"),
		ExpectRowCt: 10,
		ValidateRowData: func() {
			//u.Infof("%s   %#v", data.Field, data)
			assert.Tf(t, data.Field != "", "%v", data)
			switch data.Field {
			case "embedded":
				assert.Tf(t, data.Type == "binary" || data.Type == "text", "%#v", data)
				describedCt++
			case "author":
				assert.Tf(t, data.Type == "varchar(255)", "data: %#v", data)
				describedCt++
			case "created":
				assert.Tf(t, data.Type == "datetime", "data: %#v", data)
				describedCt++
			case "category":
				assert.Tf(t, data.Type == "json", "data: %#v", data)
				describedCt++
			case "body":
				assert.Tf(t, data.Type == "json", "data: %#v", data)
				describedCt++
			case "deleted":
				assert.Tf(t, data.Type == "bool" || data.Type == "tinyint", "data: %#v", data)
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.Tf(t, describedCt == 5, "Should have found/described 5 but was %v", describedCt)
}

func TestSimpleRowSelect(t *testing.T) {
	loadTestData(t)
	data := struct {
		Kind              string
		Name              string
		Generation        int64
		CreationTimestamp time.Time
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select kind, name, generation, creationtimestamp from pods LIMIT 1",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.CreationTimestamp.IsZero() == false, "Should have timestamp? %v", data)
			assert.Tf(t, data.Kind == "pod", "expected pod got %v", data)
		},
		RowData: &data,
	})
	return
	// validateQuerySpec(t, tu.QuerySpec{
	// 	Sql:         "select title, count,deleted from article WHERE count = 22;",
	// 	ExpectRowCt: 1,
	// 	ValidateRowData: func() {
	// 		assert.Tf(t, data.Title == "article1", "%v", data)
	// 	},
	// 	RowData: &data,
	// })
	// validateQuerySpec(t, tu.QuerySpec{
	// 	Sql:             "select title, count, deleted from article LIMIT 10;",
	// 	ExpectRowCt:     4,
	// 	ValidateRowData: func() {},
	// 	RowData:         &data,
	// })
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

func TestSelectGroupBy(t *testing.T) {
	data := struct {
		Author string
		Ct     int
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select count(*) as ct, author from article GROUP BY author;",
		ExpectRowCt: 3,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			switch data.Author {
			case "aaron":
				assert.Tf(t, data.Ct == 1, "Should have found 1? %v", data)
			case "bjorn":
				assert.Tf(t, data.Ct == 2, "Should have found 2? %v", data)
			}
		},
		RowData: &data,
	})
}

func TestSelectWhereLike(t *testing.T) {

	// We are testing the LIKE clause doesn't exist in Cassandra so we are polyfillying
	data := struct {
		Title  string
		Author string
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, author from article WHERE title like "%stic%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, author from article WHERE title like "list%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
}

func TestSelectProjectionRewrite(t *testing.T) {

	data := struct {
		Title string
		Ct    int
	}{}
	// We are testing when we need to project twice (1: cassandra, 2: in dataux)
	// - the "count AS ct" alias needs to be rewritten to NOT be projected
	//      in cassandra and or be aware of it since we are projecting again
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, count AS ct from article WHERE title like "list%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
}

func TestSelectOrderBy(t *testing.T) {
	RunTestServer(t)

	data := struct {
		Title string
		Ct    int
	}{}
	// Try order by on primary partition key
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count64 AS ct FROM article ORDER BY title DESC LIMIT 1;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "zarticle3", "%v", data)
			assert.Tf(t, data.Ct == 100, "%v", data)
		},
		RowData: &data,
	})

	// try order by on some other keys

	// need to fix OrderBy for ints first
	return
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

func TestMutationInsertSimple(t *testing.T) {
	validateQuerySpec(t, tu.QuerySpec{
		Sql:             "select id, name from user;",
		ExpectRowCt:     3,
		ValidateRowData: func() {},
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `INSERT INTO user (id, name, deleted, created, updated) VALUES ("user814", "test_name",false, now(), now());`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec: `
		INSERT INTO user (id, name, deleted, created, updated) 
		VALUES 
			("user815", "test_name2",false, now(), now()),
			("user816", "test_name3",false, now(), now());
		`,
		ValidateRowData: func() {},
		ExpectRowCt:     2,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:             "select id, name from user;",
		ExpectRowCt:     6,
		ValidateRowData: func() {},
	})
}

func TestMutationDeleteSimple(t *testing.T) {
	data := struct {
		Id, Name string
	}{}
	ct := 0
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select id, name from user;",
		ExpectRowCt: -1, // don't evaluate row count
		ValidateRowData: func() {
			ct++
			u.Debugf("data: %+v  ct:%v", data, ct)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec: `
			INSERT INTO user (id, name, deleted, created, updated) 
			VALUES 
				("deleteuser123", "test_name",false, now(), now());`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:             "select id, name from user;",
		ExpectRowCt:     ct + 1,
		ValidateRowData: func() {},
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `DELETE FROM user WHERE id = "deleteuser123"`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec:        `SELECT * FROM user WHERE id = "deleteuser123"`,
		ExpectRowCt: 0,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select id, name from user;",
		ExpectRowCt: ct,
	})
}

func TestMutationUpdateSimple(t *testing.T) {
	data := struct {
		Id      string
		Name    string
		Deleted bool
		Roles   datasource.StringArray
		Created time.Time
		Updated time.Time
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Exec: `INSERT INTO user 
							(id, name, deleted, created, updated, roles) 
						VALUES 
							("update123", "test_name", false, todate("2014/07/04"), now(), ["admin","sysadmin"]);`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `select id, name, deleted, roles, created, updated from user WHERE id = "update123"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Id == "update123", "%v", data)
			assert.Tf(t, data.Name == "test_name", "%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
		},
		RowData: &data,
	})
	return
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT id, name, deleted, roles, created, updated FROM user WHERE id = "update123"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.Id == "update123", "%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
		},
		RowData: &data,
	})
	//u.Warnf("about to update")
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `UPDATE user SET name = "was_updated", [deleted] = true WHERE id = "update123"`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	//u.Warnf("about to final read")
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT id, name, deleted, roles, created, updated FROM user WHERE id = "user815"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.Id == "user815", "fr1 %v", data)
			assert.Tf(t, data.Name == "was_updated", "fr2 %v", data)
			assert.Tf(t, data.Deleted == true, "fr3 deleted? %v", data)
		},
		RowData: &data,
	})
}

func TestInvalidQuery(t *testing.T) {
	RunTestServer(t)
	db, err := sql.Open("mysql", DbConn)
	assert.T(t, err == nil)
	// It is parsing the SQL on server side (proxy) not in client
	//  so hence that is what this is testing, making sure proxy responds gracefully
	rows, err := db.Query("select `stuff`, NOTAKEYWORD fake_tablename NOTWHERE `description` LIKE \"database\";")
	assert.Tf(t, err != nil, "%v", err)
	assert.Tf(t, rows == nil, "must not get rows")
}
