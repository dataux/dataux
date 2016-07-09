package cassandra_test

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	"github.com/jmoiron/sqlx"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"

	_ "github.com/dataux/dataux/backends/cassandra"
	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	tu "github.com/dataux/dataux/testutil"
)

var (
	DbConn              = "root@tcp(127.0.0.1:13307)/datauxtest?parseTime=true"
	loadTestDataOnce    sync.Once
	now                 = time.Now()
	testServicesRunning bool
	cassHost            *string = flag.String("casshost", "localhost:9042", "Cassandra Host")
	session             *gocql.Session
	cassKeyspace        = "datauxtest"
	_                   = json.RawMessage(nil)
)

func init() {

	cass := os.Getenv("CASSANDRA_HOST")
	if len(cass) > 0 {
		*cassHost = cass
	}
	tu.Setup()
}

var testTables = []string{`
-- DROP TABLE IF EXISTS article;
CREATE TABLE IF NOT EXISTS article (
  author varchar,
  title varchar,
  count int,
  count64 bigint,
  category set<text>,
  deleted boolean,
  created timestamp,
  updated timestamp,
  f double,
  embedded blob,
  body blob,
  PRIMARY KEY (author)
);`,
	`CREATE TABLE IF NOT EXISTS user (
  id varchar,
  name varchar,
  deleted boolean,
  roles set<text>,
  created timestamp,
  updated timestamp,
  PRIMARY KEY (id)
);`}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	ctx.Schema = testmysql.Schema
	return planner.BuildSqlJob(ctx, testmysql.ServerCtx.Grid)
}

func RunTestServer(t *testing.T) func() {
	if !testServicesRunning {
		testServicesRunning = true
		planner.GridConf.JobMaker = jobMaker
		planner.GridConf.SchemaLoader = testmysql.SchemaLoader
		planner.GridConf.SupressRecover = testmysql.Conf.SupressRecover
		testmysql.RunTestServer(t)
		quit := make(chan bool)
		planner.RunWorkerNodes(quit, 2, testmysql.ServerCtx.Reg)

		loadTestData(t)
	}
	return func() {
		// placeholder
	}
}

func validateQuerySpec(t *testing.T, testSpec tu.QuerySpec) {
	RunTestServer(t)
	tu.ValidateQuerySpec(t, testSpec)
}

func loadTestData(t *testing.T) {
	loadTestDataOnce.Do(func() {
		u.Debugf("loading cassandra test data")
		cluster := gocql.NewCluster(*cassHost)
		cluster.Keyspace = cassKeyspace
		sess, err := cluster.CreateSession()
		assert.Tf(t, err == nil, "Must create cassandra session")
		session = sess
		cqlKeyspace := fmt.Sprintf(`
			CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };`, cassKeyspace)
		err = sess.Query(cqlKeyspace).Exec()
		time.Sleep(time.Millisecond * 30)
		assert.T(t, err == nil, "must create keyspace", err)
		for _, table := range testTables {
			err = sess.Query(table).Consistency(gocql.All).Exec()
			time.Sleep(time.Millisecond * 30)
			assert.Tf(t, err == nil, "failed to create dataux table: %v", err)
		}
		err = sess.Query("CREATE INDEX IF NOT EXISTS ON article (category);").Exec()
		assert.T(t, err == nil)

		/*
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

		*/

		// Articles = append(Articles, &Article{"article1", "aaron", 22, 75, false, []string{"news", "sports"}, t1, &ut, 55.5, ev, &body})
		for _, article := range tu.Articles {
			err = sess.Query(`
				INSERT INTO datauxtest.article 
					(title, author, count, count64, deleted, category, created, updated, f, body)
					VALUES (?, ? , ? , ? , ? , ? , ? , ? , ? , ?)
			`, article.Values()...).Exec()
			//u.Infof("insert: %v", article.Row())
			assert.Tf(t, err == nil, "must put but got err: %v", err)
		}
		/*
			// Now we are going to write the embeded?
			for i := 0; i < -1; i++ {
				n := time.Now()
				ev := struct {
					Tag string
					ICt int
				}{"tag", i}
				body := json.RawMessage([]byte(fmt.Sprintf(`{"name":"more %v"}`, i)))
				a := &tu.Article{fmt.Sprintf("article_%v", i), "auto", 22, 75, false, []string{"news", "sports"}, n, &n, 55.5, ev, &body}
				key, err := client.Put(ctx, articleKey(a.Title), &Article{a})
				//u.Infof("key: %v", key)
				assert.Tf(t, key != nil, "%v", key)
				assert.Tf(t, err == nil, "must put %v", err)
				//u.Warnf("made article: %v", a.Title)
			}
		*/
		for _, user := range tu.Users {
			err = sess.Query(`
				INSERT INTO datauxtest.user 
					(id, name, deleted, roles, created, updated)
					VALUES (?, ? , ? , ? , ? , ? )
			`, user.Values()...).Exec()
			//u.Infof("insert: %v", user.Row())
			assert.Tf(t, err == nil, "must put but got err: %v", err)
		}
	})
}

func TestShowTables(t *testing.T) {
	// By running testserver, we will load schema/config
	RunTestServer(t)

	data := struct {
		Table string `db:"Table"`
	}{}
	found := false
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "show tables;",
		ExpectRowCt: 2,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Table != "", "%v", data)
			if data.Table == strings.ToLower("article") {
				found = true
			}
		},
		RowData: &data,
	})
	assert.Tf(t, found, "Must have found article")
}

func TestBasic(t *testing.T) {

	// By running testserver, we will load schema/config
	RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", DbConn)
	assert.Tf(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	rows, err := dbx.Queryx(fmt.Sprintf("select * from article"))
	assert.Tf(t, err == nil, "%v", err)
	defer rows.Close()
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
		ExpectRowCt: 11,
		ValidateRowData: func() {
			//u.Infof("%s   %#v", data.Field, data)
			assert.Tf(t, data.Field != "", "%v", data)
			switch data.Field {
			case "embedded":
				assert.Tf(t, data.Type == "binary", "%#v", data)
				describedCt++
			case "author":
				assert.Tf(t, data.Type == "string", "data: %#v", data)
				describedCt++
			case "created":
				assert.Tf(t, data.Type == "datetime", "data: %#v", data)
				describedCt++
			case "category":
				assert.Tf(t, data.Type == "binary", "data: %#v", data)
				describedCt++
			case "body":
				assert.Tf(t, data.Type == "binary", "data: %#v", data)
				describedCt++
			case "deleted":
				assert.Tf(t, data.Type == "bool", "data: %#v", data)
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.Tf(t, describedCt == 6, "Should have found/described 6 but was %v", describedCt)
}

func TestSimpleRowSelect(t *testing.T) {
	loadTestData(t)
	data := struct {
		Title   string
		Count   int
		Deleted bool
		Author  string
		// Category []string  // Crap, downside of sqlx/mysql is no complex types
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count, deleted, author from article WHERE author = \"aaron\" LIMIT 1",
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
	// TODO:  poly fill this, as doesn't work in datastore
	// validateQuerySpec(t, tu.QuerySpec{
	// 	Sql:         `SELECT title, count as ct from article WHERE title like "%stic%"`,
	// 	ExpectRowCt: 1,
	// 	ValidateRowData: func() {
	// 		assert.Tf(t, data.Title == "listicle1", "%v", data)
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

func TestInsertSimple(t *testing.T) {
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `INSERT INTO DataUxTestUser (id, name, deleted, created, updated) VALUES ("user814", "test_name",false, now(), now());`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
}

func TestDeleteSimple(t *testing.T) {
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `INSERT INTO DataUxTestUser (id, name, deleted, created, updated) VALUES ("user814", "test_name",false, now(), now());`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `DELETE FROM DataUxTestUser WHERE id = "user814"`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `SELECT * FROM DataUxTestUser WHERE id = "user814"`,
		ValidateRowData: func() {},
		ExpectRowCt:     0,
	})
}

func TestUpdateSimple(t *testing.T) {
	data := struct {
		Id      string
		Name    string
		Deleted bool
		Roles   datasource.StringArray
		Created time.Time
		Updated time.Time
	}{}
	//u.Warnf("about to insert")
	validateQuerySpec(t, tu.QuerySpec{
		Exec: `INSERT INTO DataUxTestUser 
							(id, name, deleted, created, updated, roles) 
						VALUES 
							("user815", "test_name", false, todate("2014/07/04"), now(), ["admin","sysadmin"]);`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	//u.Warnf("about to test post update")
	//return
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `select id, name, deleted, roles, created, updated from DataUxTestUser WHERE id = "user815"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Id == "user815", "%v", data)
			assert.Tf(t, data.Name == "test_name", "%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT id, name, deleted, roles, created, updated FROM DataUxTestUser WHERE id = "user815"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.Id == "user815", "%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
		},
		RowData: &data,
	})
	//u.Warnf("about to update")
	validateQuerySpec(t, tu.QuerySpec{
		Exec:            `UPDATE DataUxTestUser SET name = "was_updated", [deleted] = true WHERE id = "user815"`,
		ValidateRowData: func() {},
		ExpectRowCt:     1,
	})
	//u.Warnf("about to final read")
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT id, name, deleted, roles, created, updated FROM DataUxTestUser WHERE id = "user815"`,
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
