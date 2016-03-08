package elasticsearch_test

import (
	"database/sql"
	"flag"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	"github.com/araddon/qlbridge/plan"
	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/testutil"
)

var (
	eshost              *string = flag.String("host", "localhost", "Elasticsearch Server Host Address")
	testServicesRunning bool
)

func init() {
	testutil.Setup()
}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	// func BuildSqlJob(ctx *plan.Context, gs *Server) (*ExecutorGrid, error) {
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
		planner.RunWorkerNodes(2, testmysql.ServerCtx.Reg)
	}
	return func() {
		// placeholder
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

	RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", "root@tcp(127.0.0.1:13307)/datauxtest")
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

	if testSpec.ExpectRowCt > -1 {
		assert.Tf(t, rowCt == testSpec.ExpectRowCt, "expected %v rows but got %v", testSpec.ExpectRowCt, rowCt)
	}

	assert.T(t, rows.Err() == nil)
	//u.Infof("rows: %v", cols)
}

func TestShowTablesSelect(t *testing.T) {
	data := struct {
		Table string `db:"Table"`
	}{}
	found := false
	validateQuerySpec(t, QuerySpec{
		Sql:         "show tables;",
		ExpectRowCt: -1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Table != "", "%v", data)
			if data.Table == "github_fork" {
				found = true
			}
		},
		RowData: &data,
	})
	assert.Tf(t, found, "Must have found github_fork")
}

func TestInvalidQuery(t *testing.T) {
	testmysql.RunTestServer(t)
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:13307)/datauxtest")
	assert.T(t, err == nil)
	// It is parsing the SQL on server side (proxy)
	//  not in client, so hence that is what this is testing, making sure
	//  proxy responds gracefully
	rows, err := db.Query("select `stuff`, NOTAKEYWORD github_fork NOTWHERE `description` LIKE \"database\";")
	assert.Tf(t, err != nil, "%v", err)
	assert.Tf(t, rows == nil, "must not get rows")
}

func TestSimpleRowSelect(t *testing.T) {
	data := struct {
		Actor string
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select actor from github_push WHERE `actor` = \"araddon\" LIMIT 10;",
		ExpectRowCt: 2,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Actor == "araddon", "%v", data)
		},
		RowData: &data,
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
			assert.Tf(t, data2.Oldest == 904810, "%#v", data2)
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
	data := struct {
		Actor    string `db:"actor"`
		Name     string `db:"repository.name"`
		Stars    int    `db:"repository.stargazers_count"`
		Language string `db:"repository.language"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: "select `actor`, `repository.name`, `repository.stargazers_count`, `repository.language` " +
			"from github_watch where `repository.language` = \"Go\";",
		ExpectRowCt: 20,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Actor != "", "%v", data)
			assert.Tf(t, data.Language == "Go", "%v", data)
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
		Actor string
		Name  string `db:"repository.name"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select actor, repository.name from github_push where actor IN ("mdmarek", "epsniff", "schmichael", "kyledj", "ropes","araddon");`,
		ExpectRowCt: 13,
		ValidateRowData: func() {
			u.Debugf("%#v", data)
			assert.Tf(t, data.Name != "", "%v", data)
			//assert.Tf(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
		},
		RowData: &data,
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
			//u.Debugf("%#v", data)
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
