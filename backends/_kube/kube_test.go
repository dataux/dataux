package kubernetes

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	u "github.com/araddon/gou"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

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
			assert.True(t, data.Table != "", "%v", data)
			if data.Table == strings.ToLower("pods") {
				found = true
			}
		},
		RowData: &data,
	})
	assert.True(t, found, "Must have found pods")
}

func TestBasic(t *testing.T) {

	// This test is going to start the server
	// and run using lower level driver, not test harness

	RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", DbConn)
	assert.True(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	rows, err := dbx.Queryx(fmt.Sprintf("select * from pods"))
	assert.Equal(t, err, nil, "%v", err)
	cols, _ := rows.Columns()
	assert.True(t, len(cols) > 5, "Should have columns %v", cols)
	defer rows.Close()

	return
	// TODO:  allow strict enforcement of schema
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
		Sql:         fmt.Sprintf("describe pods;"),
		ExpectRowCt: 35,
		ValidateRowData: func() {
			//u.Infof("%s   %#v", data.Field, data)
			assert.True(t, data.Field != "", "%v", data)
			switch data.Field {
			case "name":
				assert.True(t, data.Type == "varchar(255)", "data: %#v", data)
				describedCt++
			case "kind":
				assert.True(t, data.Type == "varchar(255)", "data: %#v", data)
				describedCt++
			case "creationtimestamp":
				assert.True(t, data.Type == "datetime", "data: %#v", data)
				describedCt++
			case "category":
				assert.True(t, data.Type == "json", "data: %#v", data)
				describedCt++
			case "generation":
				assert.True(t, data.Type == "bigint" || data.Type == "integer", "data: %#v", data)
				describedCt++
			}
		},
		RowData: &data,
	})
	assert.True(t, describedCt == 4, "Should have found/described 4 but was %v", describedCt)
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
			assert.Equal(t, false, data.CreationTimestamp.IsZero(), "Should have timestamp? %v", data)
			assert.Equal(t, "pod", data.Kind, "expected pod got %v", data)
		},
		RowData: &data,
	})
}

func TestSelectLimit(t *testing.T) {
	data := struct {
		Kind string
		Name string
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:             "select kind, name from pods LIMIT 1;",
		ExpectRowCt:     1,
		ValidateRowData: func() {},
		RowData:         &data,
	})
}

func TestSelectGroupBy(t *testing.T) {
	data := struct {
		Namespace string
		Ct        int
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select count(*) as ct, namespace from pods GROUP BY namespace;",
		ExpectRowCt: 2,
		ValidateRowData: func() {
			u.Infof("%v", data)
			switch data.Namespace {
			case "default":
				assert.True(t, data.Ct >= 1, "Should have found at least 1? %v", data)
			case "kube-system":
				assert.True(t, data.Ct >= 3, "Should have found at least 3 %v", data)
			}
		},
		RowData: &data,
	})
}
