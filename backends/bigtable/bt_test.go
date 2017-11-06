package bigtable_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	u "github.com/araddon/gou"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"

	"github.com/araddon/qlbridge/datasource"
	"github.com/araddon/qlbridge/plan"
	"github.com/araddon/qlbridge/schema"

	btbe "github.com/dataux/dataux/backends/bigtable"
	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	tu "github.com/dataux/dataux/testutil"
)

var (
	DbConn              = "root@tcp(127.0.0.1:13307)/datauxtest?parseTime=true"
	loadTestDataOnce    sync.Once
	now                 = time.Now()
	testServicesRunning bool
)

func init() {
	tu.Setup()
}

var testTables = []string{`
-- DROP TABLE IF EXISTS article;
CREATE TABLE IF NOT EXISTS article (
  title varchar,
  author varchar,
  count int,
  count64 bigint,
  category set<text>,
  deleted boolean,
  created timestamp,
  updated timestamp,
  f double,
  embedded blob,
  body blob,
  PRIMARY KEY (title)
);`,
	`
CREATE TABLE IF NOT EXISTS user (
  id varchar,
  name varchar,
  deleted boolean,
  roles set<text>,
  created timestamp,
  updated timestamp,
  PRIMARY KEY (id)
);`, `
-- Events Table
CREATE TABLE IF NOT EXISTS event (
  url varchar,
  ts timestamp,
  date text,
  jsondata text,
  PRIMARY KEY ((date, url), ts)
);
`,
	`truncate event`,
	`truncate user`,
	`truncate article`,
}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	ctx.Schema = testmysql.Schema
	return planner.BuildSqlJob(ctx, testmysql.ServerCtx.PlanGrid)
}

func RunTestServer(t *testing.T) func() {
	if !testServicesRunning {
		testServicesRunning = true

		// export BIGTABLE_EMULATOR_HOST="localhost:8600"
		os.Setenv("BIGTABLE_EMULATOR_HOST", "localhost:8600")

		reg := schema.DefaultRegistry()

		by := []byte(`{
					"name": "bt",
					"schema":"datauxtest",
					"type": "bigtable",
					"tables_to_load" : [ "datauxtest" , "article", "user", "event" ],
					"settings": {
						"instance": "bigtable0",
						"project": "lol"
					}
				  }`)

		conf := &schema.ConfigSource{}
		err := json.Unmarshal(by, conf)
		assert.Equal(t, nil, err)
		err = reg.SchemaAddFromConfig(conf)
		assert.Equal(t, nil, err)

		s, ok := reg.Schema("datauxtest")
		assert.Equal(t, true, ok)
		assert.NotEqual(t, nil, s)

		loadTestData(t, conf)

		planner.GridConf.JobMaker = jobMaker
		planner.GridConf.SchemaLoader = testmysql.SchemaLoader
		planner.GridConf.SupressRecover = testmysql.Conf.SupressRecover

		testmysql.RunTestServer(t)
	}
	return func() {}
}

func validateQuerySpec(t *testing.T, testSpec tu.QuerySpec) {
	RunTestServer(t)
	tu.ValidateQuerySpec(t, testSpec)
}

func loadTestData(t *testing.T, conf *schema.ConfigSource) {
	loadTestDataOnce.Do(func() {

		u.Debugf("loading bigtable test data %#v", conf)

		btInstance := conf.Settings.String("instance")
		gceProject := conf.Settings.String("project")

		ctx := context.Background()
		ac, err := bigtable.NewAdminClient(ctx, gceProject, btInstance)
		if err != nil {
			panic(fmt.Sprintf("admin client required but got err: %v", err))
		}
		ac.DeleteTable(ctx, "datauxtest")
		time.Sleep(time.Millisecond * 50)

		tables, err := ac.Tables(ctx)
		if err != nil {
			panic(fmt.Sprintf("Must be able to get tables %v", err))
		}
		foundTbl := false
		for _, table := range tables {
			if table == "datauxtest" {
				foundTbl = true
			}
		}
		if !foundTbl {
			err = ac.CreateTable(ctx, "datauxtest")
			if err != nil {
				panic(fmt.Sprintf("Must be able to create test table %v", err))
			}
			for _, cf := range []string{"user", "article", "event"} {
				err = ac.CreateColumnFamily(ctx, "datauxtest", cf)
				if err != nil {
					panic(fmt.Sprintf("Must be able to create test column-family %v", err))
				}
			}
		}

		client, err := bigtable.NewClient(ctx, gceProject, btInstance)
		if err != nil {
			panic(fmt.Sprintf("client required but got err: %v", err))
		}

		tbl := client.Open("datauxtest")

		//func (ac *AdminClient) DeleteTable(ctx context.Context, table string) error

		for _, article := range tu.Articles {

			mut := btbe.Mutation("article", article.Values(), article.ColNames())

			if err := tbl.Apply(ctx, article.Title, mut); err != nil {
				u.Errorf("Error Applying mutation: %v", err)
			}
		}

		for _, user := range tu.Users {
			mut := btbe.Mutation("user", user.Values(), user.ColNames())

			if err := tbl.Apply(ctx, user.Id, mut); err != nil {
				u.Errorf("Error Applying mutation: %v", err)
			}
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
		ExpectRowCt: 3,
		ValidateRowData: func() {
			//u.Infof("%+v", data)
			assert.True(t, data.Table != "", "%v", data)
			if data.Table == strings.ToLower("article") {
				found = true
			}
		},
		RowData: &data,
	})
	assert.True(t, found, "Must have found article")
}

func TestBasic(t *testing.T) {

	// By running testserver, we will load schema/config
	RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", DbConn)
	assert.True(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	rows, err := dbx.Queryx(fmt.Sprintf("select * from article"))
	assert.Equal(t, err, nil, "%v", err)
	defer rows.Close()
}

func TestDescribeTable(t *testing.T) {

	RunTestServer(t)

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
			assert.True(t, data.Field != "", "%v", data)
			switch data.Field {
			case "embedded":
				assert.True(t, data.Type == "binary" || data.Type == "text", "%#v", data)
				describedCt++
			case "author":
				assert.True(t, data.Type == "varchar(255)", "data: %#v", data)
				describedCt++
			case "created":
				assert.True(t, data.Type == "datetime", "data: %#v", data)
				describedCt++
			case "category":
				assert.True(t, data.Type == "json", "data: %#v", data)
				describedCt++
			case "body":
				assert.True(t, data.Type == "json", "data: %#v", data)
				describedCt++
			case "deleted":
				assert.True(t, data.Type == "bool" || data.Type == "tinyint", "data: %#v", data)
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.True(t, describedCt == 5, "Should have found/described 5 but was %v", describedCt)
}

func TestSimpleRowSelect(t *testing.T) {
	RunTestServer(t)
	data := struct {
		Title   string
		Count   int
		Deleted bool
		Author  string
		// Category []string  // Crap, downside of sqlx/mysql is no complex types
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count, deleted, author from article WHERE author = 'aaron' LIMIT 1",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.True(t, data.Deleted == false, "Not deleted? %v", data)
			assert.True(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count,deleted from article WHERE count = 22;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.True(t, data.Title == "article1", "%v", data)
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
	RunTestServer(t)
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
				assert.True(t, data.Ct == 1, "Should have found 1? %v", data)
			case "bjorn":
				assert.True(t, data.Ct == 2, "Should have found 2? %v", data)
			}
		},
		RowData: &data,
	})
}

func TestSelectWhereLike(t *testing.T) {

	// We are testing the LIKE clause doesn't exist in BigTable so we are polyfillying
	data := struct {
		Title  string
		Author string
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, author from article WHERE title like "%stic%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.True(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, author from article WHERE title like "list%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.True(t, data.Title == "listicle1", "%v", data)
		},
		RowData: &data,
	})
}

func TestSelectProjectionRewrite(t *testing.T) {

	data := struct {
		Title string
		Ct    int
	}{}
	// We are testing when we need to project twice (1: BigTable, 2: in dataux)
	// - the "count AS ct" alias needs to be rewritten to NOT be projected
	//      in BigTable and or be aware of it since we are projecting again
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT title, count AS ct from article WHERE title like "list%"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.True(t, data.Title == "listicle1", "%v", data)
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
			assert.True(t, data.Title == "zarticle3", "%v", data)
			assert.True(t, data.Ct == 100, "%v", data)
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
			assert.True(t, data.Title == "listicle1", "%v", data)
			assert.True(t, data.Ct == 12, "%v", data)
		},
		RowData: &data,
	})
}

func TestMutationInsertSimple(t *testing.T) {
	RunTestServer(t)
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

	// TODO:  Delete doesn't work
	return

	RunTestServer(t)
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
		Sql:         "select id, name from user;",
		ExpectRowCt: -1, // don't evaluate row count
		ValidateRowData: func() {
			u.Debugf("data: %+v", data)
		},
		RowData: &data,
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
	RunTestServer(t)
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
			assert.True(t, data.Id == "update123", "%v", data)
			assert.True(t, data.Name == "test_name", "%v", data)
			assert.True(t, data.Deleted == false, "Not deleted? %v", data)
		},
		RowData: &data,
	})
	return
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         `SELECT id, name, deleted, roles, created, updated FROM user WHERE id = "update123"`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.True(t, data.Id == "update123", "%v", data)
			assert.True(t, data.Deleted == false, "Not deleted? %v", data)
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
			assert.True(t, data.Id == "user815", "fr1 %v", data)
			assert.True(t, data.Name == "was_updated", "fr2 %v", data)
			assert.True(t, data.Deleted == true, "fr3 deleted? %v", data)
		},
		RowData: &data,
	})
}

func TestInvalidQuery(t *testing.T) {
	RunTestServer(t)
	db, err := sql.Open("mysql", DbConn)
	assert.Equal(t, nil, err)
	// It is parsing the SQL on server side (proxy) not in client
	// so hence that is what this is testing, making sure proxy responds gracefully
	rows, err := db.Query("select `stuff`, NOTAKEYWORD fake_tablename NOTWHERE `description` LIKE \"database\";")
	assert.NotEqual(t, nil, err)
	assert.True(t, rows == nil, "must not get rows")
}
