package files_test

import (
	"bufio"
	"io/ioutil"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/jmoiron/sqlx"
	"github.com/lytics/cloudstorage"

	"github.com/araddon/qlbridge/plan"

	"github.com/dataux/dataux/backends/files"
	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	"github.com/dataux/dataux/testutil"
)

/*

# to run Google Cloud tests you must have
# 1)   have run "gcloud auth login"
# 2)   set flag

export TESTINT=1


*/
var (
	testServicesRunning bool
)

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
	testutil.Setup()
	loadTestData()
}

func loadTestData() {
	// for _, article := range testutil.Articles {
	// 	articleColl.Insert(article)
	// }
	// for _, user := range testutil.Users {
	// 	userColl.Insert(user)
	// }
}

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
		createTestData(t)
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
			u.Infof("rowVals: %#v", rowVals)
			assert.Tf(t, err == nil, "%v", err)
			if testSpec.ExpectColCt > 0 {
				assert.Tf(t, len(rowVals) == testSpec.ExpectColCt, "wanted cols but got %v", len(rowVals))
			}
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

func clearStore(t *testing.T, store cloudstorage.Store) {
	q := cloudstorage.NewQuery("")
	q.Sorted()
	objs, err := store.List(q)
	assert.T(t, err == nil)
	for _, o := range objs {
		u.Debugf("deleting %q", o.Name())
		store.Delete(o.Name())
	}

	// if os.Getenv("TESTINT") != "" {
	// 	//GCS is lazy about deletes...
	// 	time.Sleep(15 * time.Second)
	// }
}

func createTestData(t *testing.T) {
	store, err := files.CreateStore()
	assert.T(t, err == nil)
	clearStore(t, store)
	//defer clearStore(t, store)

	//Create a new object and write to it.
	obj, err := store.NewObject("tables/article/article1.csv")
	assert.T(t, err == nil)
	f, err := obj.Open(cloudstorage.ReadWrite)
	assert.T(t, err == nil)

	testcsv := "Year,Make,Model\n1997,Ford,E350\n2000,Mercury,Cougar\n"

	w := bufio.NewWriter(f)
	w.WriteString(testcsv)
	assert.T(t, err == nil)
	w.Flush()
	err = obj.Close()
	assert.T(t, err == nil)

	obj, _ = store.NewObject("tables/user/user1.csv")
	f, _ = obj.Open(cloudstorage.ReadWrite)
	w = bufio.NewWriter(f)
	w.WriteString(testcsv)
	w.Flush()
	obj.Close()

	//Read the object back out of the cloud storage.
	obj2, err := store.Get("tables/article/article1.csv")
	assert.T(t, err == nil)

	f2, err := obj2.Open(cloudstorage.ReadOnly)
	assert.T(t, err == nil)

	bytes, err := ioutil.ReadAll(f2)
	assert.T(t, err == nil)

	assert.Tf(t, testcsv == string(bytes), "Wanted equal got %s", bytes)
}

func TestShowTables(t *testing.T) {

	found := false
	data := struct {
		Table string `db:"Table"`
	}{}
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
