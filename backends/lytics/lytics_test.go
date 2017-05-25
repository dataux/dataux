package lytics_test

import (
	"database/sql"
	"os"
	"testing"

	u "github.com/araddon/gou"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/plan"
	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/testutil"
)

var (
	testServicesRunning bool
	_                   = u.EMPTY
	_                   = sql.Drivers()
)

func init() {
	testutil.Setup()
}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	ctx.Schema = testmysql.Schema
	return planner.BuildSqlJob(ctx, testmysql.ServerCtx.PlanGrid)
}

func RunTestServer(t *testing.T) func() {
	if !testServicesRunning {
		testServicesRunning = true
		planner.GridConf.JobMaker = jobMaker
		planner.GridConf.SchemaLoader = testmysql.SchemaLoader
		planner.GridConf.SupressRecover = testmysql.Conf.SupressRecover
		testmysql.RunTestServer(t)
	}
	return func() {}
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

	if os.Getenv("LIOKEY") == "" {
		t.Skip("No LIOKEY to run tests")
		return
	}
	RunTestServer(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", "root@tcp(127.0.0.1:13307)/datauxtest")
	assert.True(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	rows, err := dbx.Queryx(testSpec.Sql)
	assert.True(t, err == nil, "%v", err)
	defer rows.Close()

	cols, err := rows.Columns()
	assert.True(t, err == nil, "%v", err)
	if len(testSpec.Cols) > 0 {
		for _, expectCol := range testSpec.Cols {
			found := false
			for _, colName := range cols {
				if colName == expectCol {
					found = true
				}
			}
			assert.True(t, found, "Should have found column: %v", expectCol)
		}
	}
	rowCt := 0
	for rows.Next() {
		if testSpec.RowData != nil {
			err = rows.StructScan(testSpec.RowData)
			//u.Infof("rowVals: %#v", testSpec.RowData)
			assert.True(t, err == nil, "%v", err)
			rowCt++
			if testSpec.ValidateRowData != nil {
				testSpec.ValidateRowData()
			}

		} else {
			// rowVals is an []interface{} of all of the column results
			rowVals, err := rows.SliceScan()
			//u.Infof("rowVals: %#v", rowVals)
			assert.True(t, err == nil, "%v", err)
			assert.True(t, len(rowVals) == testSpec.ExpectColCt, "wanted cols but got %v", len(rowVals))
			rowCt++
			if testSpec.ValidateRow != nil {
				testSpec.ValidateRow(rowVals)
			}
		}

	}

	if testSpec.ExpectRowCt > -1 {
		assert.True(t, rowCt == testSpec.ExpectRowCt, "expected %v rows but got %v", testSpec.ExpectRowCt, rowCt)
	}

	assert.True(t, rows.Err() == nil)
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
			assert.True(t, data.Table != "", "%v", data)
			if data.Table == "user" {
				found = true
			}
		},
		RowData: &data,
	})
	assert.True(t, found, `Must have found "user"`)
}

func TestSimpleRowSelect(t *testing.T) {
	data := struct {
		Propensity float64 `db:"score_propensity"`
		UserId     string  `db:"user_id"`
		Paying     bool    `db:"paying_user"`
		Org        string  `db:"org"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select score_propensity, user_id, paying_user, org from user WHERE `user_id` = \"triggers123\" LIMIT 10;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.True(t, data.UserId == "triggers123", "%v", data)
		},
		RowData: &data,
	})
}

/*

func TestSelectAggs(t *testing.T) {
	data := struct {
		Oldest int `db:"oldest_repo"`
		Card   int `db:"users_who_watched"`
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         "select cardinality(`actor`) AS users_who_watched, min(`repository.id`) as oldest_repo from github_watch;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Debugf("%v", data)   {765 3448}
			assert.True(t, data.Card == 3448, "%v", data)
			assert.True(t, data.Oldest == 765, "%v", data)

		},
		RowData: &data,
	})

	data2 := struct {
		Oldest int `db:"oldest_repo"`
		Card   int `db:"users_who_watched"`
		Ct     int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: "select cardinality(`actor`) AS users_who_watched, count(*) as ct " +
			", min(`repository.id`) as oldest_repo " +
			` FROM github_watch
			WHERE repository.description LIKE "database";`,
		ExpectRowCt: 1,
		ValidateRowData: func() {
			u.Debugf("%+v", data2) //{108110 32 32}
			assert.True(t, data2.Card == 32, "%v", data2)
			assert.True(t, data2.Oldest == 108110, "%#v", data2)
			assert.True(t, data2.Ct == 32, "%v", data2) // 47 docs had database
		},
		RowData: &data2,
	})
}

func TestSelectAggsGroupBy(t *testing.T) {
	// NOTE:   This fails because of parsing the response, not because request is bad
	return
	data := struct {
		Actor string
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql:         `select terms(repository.description), terms(repository.name) from github_push GROUP BY actor_attributes.login;`,
		ExpectRowCt: 0,
		ValidateRowData: func() {
			u.Infof("%v", data)
			//assert.True(t, data.Actor == "araddon", "%v", data)
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
			//assert.True(t, data2.Actor == "araddon", "%v", data2)
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
		ExpectRowCt: 114,
		ValidateRowData: func() {
			//u.Infof("%+v", data)
			assert.True(t, data.Actor != "", "%+v", data)
			assert.True(t, data.Language == "Go", "%+v", data)
		},
		RowData: &data,
	})
	// Test when we have compound where clause
	validateQuerySpec(t, QuerySpec{
		Sql: "select `actor`, `repository.name`, `repository.stargazers_count`, `repository.language` " +
			" from github_watch where " +
			"`repository.language` == \"Go\" AND `repository.forks_count` > 1000;",
		ExpectRowCt: 5,
		ValidateRowData: func() {
			//u.Infof("%+v", data)
			assert.True(t, data.Language == "Go", "%v", data)
			assert.True(t, data.Stars > 1000, "must have filterd by forks: %v", data)
			assert.True(t, data.Actor != "", "%v", data)
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
			assert.True(t, data.Language == "Go", "%v", data)
			assert.True(t, data.Stars > 1000, "must have filterd by forks: %v", data)
			assert.True(t, data.Actor != "", "%v", data)
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
			" FROM github_watch " +
			"where `description` LIKE \"database AND graph\";",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Debugf("%#v", data)
			assert.True(t, data.Name == "flockdb", "%v", data)
			assert.True(t, data.Ct == 2348, "%v", data)
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
		Sql:         `select actor, repository.name from github_watch where repository.name IN ("node", "docker","d3","myicons", "bootstrap") limit 100;`,
		ExpectRowCt: 37,
		ValidateRowData: func() {
			//u.Debugf("%#v", data)
			assert.True(t, data.Name != "", "%v", data)
			//assert.True(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
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
		Sql:         `select actor, repository.name from github_watch where repository.stargazers_count BETWEEN "1000" AND 1100;`,
		ExpectRowCt: 132,
		ValidateRowData: func() {
			//u.Debugf("%#v", data)
			assert.True(t, data.Name != "", "%v", data)
			//assert.True(t, data.Ct == 74995 || data.Ct == 74994, "%v", data)
		},
		RowData: &data,
	})
	data2 := struct {
		Actor string
		Org   string `db:"org"`
	}{}
	// Now one that is date based
	// created_at: "2008-10-21T18:20:37Z
	// curl -s XGET localhost:9200/github_watch/_search -d '{"filter": { "range": { "repository.created_at": {"gte": "2008-10-21T17:20:37Z","lte": "2008-10-21T19:20:37Z"}}}}' | jq '.'
	validateQuerySpec(t, QuerySpec{
		Sql: `SELECT actor, repository.organization AS org
			FROM github_watch
			WHERE repository.created_at BETWEEN "2008-10-21T17:20:37Z" AND "2008-10-21T19:20:37Z";`,
		ExpectRowCt: 3,
		ValidateRowData: func() {
			u.Debugf("%#v", data2)
			assert.True(t, data2.Org == "android", "%v", data2)
		},
		RowData: &data2,
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
			FROM github_watch
			WHERE
				repository.stargazers_count BETWEEN 1000 AND 1100;
		`,
		ExpectRowCt: 132,
		ValidateRowData: func() {
			//u.Debugf("%#v", datact)
			assert.True(t, datact.Stars >= 1000 && datact.Stars < 1101, "%v", datact)
		},
		RowData: &datact,
	})
	data3 := struct {
		Actor string
		Org   sql.NullString `db:"org"`
	}{}
	// Try again but with missing field org to show it will return nil
	validateQuerySpec(t, QuerySpec{
		Sql: `SELECT actor, org
			FROM github_watch
			WHERE repository.created_at BETWEEN "2008-10-21T17:20:37Z" AND "2008-10-21T19:20:37Z";`,
		ExpectRowCt: 3,
		ValidateRowData: func() {
			u.Debugf("%#v", data3)
			assert.True(t, data3.Org.Valid == false)
			assert.True(t, data3.Org.String == "", "%v", data3.Org.String)
		},
		RowData: &data3,
	})
}

func TestSelectOrderBy(t *testing.T) {
	data := struct {
		Name string
		Ct   int
	}{}
	validateQuerySpec(t, QuerySpec{
		Sql: "select `repository.stargazers_count` AS ct, `repository.name` AS name " +
			" FROM github_watch ORDER BY `repository.stargazers_count` DESC limit 3;",
		ExpectRowCt: 3,
		ValidateRowData: func() {
			assert.True(t, data.Name == "bootstrap", "%v", data)
			assert.True(t, data.Ct == 74907 || data.Ct == 74906, "%v", data)
		},
		RowData: &data,
	})
}
*/
