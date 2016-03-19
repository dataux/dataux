package files_test

import (
	"bufio"
	"io/ioutil"
	"os"
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
	"github.com/lytics/cloudstorage"
	"github.com/lytics/cloudstorage/logging"

	"github.com/araddon/qlbridge/plan"

	"github.com/dataux/dataux/frontends/mysqlfe/testmysql"
	"github.com/dataux/dataux/planner"
	tu "github.com/dataux/dataux/testutil"
)

/*

# to run Google Cloud tests you must have
# 1)   have run "gcloud auth login"
# 2)   set env variable "TESTINT=1"

export TESTINT=1


*/
var (
	testServicesRunning bool
)

var localconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "unittest",
	TokenSource:     cloudstorage.LocalFileSource,
	LocalFS:         "/tmp/mockcloud",
	TmpDir:          "/tmp/localcache",
}

var gcsIntconfig = &cloudstorage.CloudStoreContext{
	LogggingContext: "dataux-test",
	TokenSource:     cloudstorage.GCEDefaultOAuthToken,
	Project:         "lytics-dev",
	Bucket:          "lytics-dataux-tests",
	TmpDir:          "/tmp/localcache",
}

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
	tu.Setup()
}

func jobMaker(ctx *plan.Context) (*planner.ExecutorGrid, error) {
	ctx.Schema = testmysql.Schema
	return planner.BuildSqlJob(ctx, testmysql.ServerCtx.Grid)
}

func RunTestServer(t *testing.T) {
	if !testServicesRunning {
		testServicesRunning = true
		planner.GridConf.JobMaker = jobMaker
		planner.GridConf.SchemaLoader = testmysql.SchemaLoader
		planner.GridConf.SupressRecover = testmysql.Conf.SupressRecover
		//createTestData(t)
		testmysql.RunTestServer(t)
		planner.RunWorkerNodes(2, testmysql.ServerCtx.Reg)
	}
}

func createTestStore() (cloudstorage.Store, error) {

	cloudstorage.LogConstructor = func(prefix string) logging.Logger {
		return logging.NewStdLogger(true, logging.DEBUG, prefix)
	}

	var config *cloudstorage.CloudStoreContext
	if os.Getenv("TESTINT") == "" {
		//os.RemoveAll("/tmp/mockcloud")
		//os.RemoveAll("/tmp/localcache")
		config = localconfig
	} else {
		config = gcsIntconfig
	}
	return cloudstorage.NewStore(config)
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
	// 	// GCS is lazy about deletes...
	// 	time.Sleep(15 * time.Second)
	// }
}

func validateQuerySpec(t *testing.T, testSpec tu.QuerySpec) {
	RunTestServer(t)
	tu.ValidateQuerySpec(t, testSpec)
}

func createTestData(t *testing.T) {
	store, err := createTestStore()
	assert.T(t, err == nil)
	//clearStore(t, store)
	//defer clearStore(t, store)

	//Create a new object and write to it.
	obj, err := store.NewObject("tables/article/article1.csv")
	if err != nil {
		return // already created
	}
	assert.T(t, err == nil)
	f, err := obj.Open(cloudstorage.ReadWrite)
	assert.T(t, err == nil)

	w := bufio.NewWriter(f)
	w.WriteString(tu.Articles[0].Header())
	w.WriteByte('\n')
	lastIdx := len(tu.Articles) - 1
	for i, a := range tu.Articles {
		w.WriteString(a.Row())
		if i != lastIdx {
			w.WriteByte('\n')
		}
	}
	w.Flush()
	err = obj.Close()
	assert.T(t, err == nil)

	obj, _ = store.NewObject("tables/user/user1.csv")
	f, _ = obj.Open(cloudstorage.ReadWrite)
	w = bufio.NewWriter(f)
	w.WriteString(tu.Users[0].Header())
	w.WriteByte('\n')
	lastIdx = len(tu.Users) - 1
	for i, a := range tu.Users {
		w.WriteString(a.Row())
		if i != lastIdx {
			w.WriteByte('\n')
		}
	}
	w.Flush()
	obj.Close()

	//Read the object back out of the cloud storage.
	obj2, err := store.Get("tables/article/article1.csv")
	assert.T(t, err == nil)

	f2, err := obj2.Open(cloudstorage.ReadOnly)
	assert.T(t, err == nil)

	bytes, err := ioutil.ReadAll(f2)
	assert.T(t, err == nil)

	assert.Tf(t, tu.ArticleCsv == string(bytes), "Wanted equal got %s", bytes)
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
		Title   string
		Count   int
		Deleted bool
	}{}
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count, deleted from article WHERE `author` = \"aaron\" ",
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
