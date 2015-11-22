package datastore_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/datastore"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/datasource"
	"github.com/bmizerany/assert"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"

	gds "github.com/dataux/dataux/pkg/backends/datastore"
	"github.com/dataux/dataux/pkg/frontends/testmysql"
	tu "github.com/dataux/dataux/pkg/testutil"
)

var (
	ctx    context.Context
	client *datastore.Client

	DbConn = "root@tcp(127.0.0.1:13307)/datauxtest?parseTime=true"

	loadTestDataOnce sync.Once

	now = time.Now()

	// we are too lazy to type
	validateQuerySpec = tu.ValidateQuerySpec
	validateQuery     = tu.ValidateQuery
)

func init() {

	tu.Setup()

	if *gds.GoogleJwt == "" {
		u.Errorf("must have google oauth jwt")
		os.Exit(1)
	}
	if *gds.GoogleProject == "" {
		u.Errorf("must have google cloud project")
		os.Exit(1)
	}

	jsonKey, err := ioutil.ReadFile(*gds.GoogleJwt)
	if err != nil {
		u.Errorf("Could not open Google Auth Token JWT file %v", err)
		os.Exit(1)
	}
	ctx, client = loadAuth(jsonKey)
}

const (
	ArticleKind string = "DataUxTestArticle"
	UserKind    string = "DataUxTestUser"
)

func articleKey(title string) *datastore.Key {
	return datastore.NewKey(ctx, ArticleKind, title, 0, nil)
}

func userKey(id string) *datastore.Key {
	return datastore.NewKey(ctx, UserKind, id, 0, nil)
}

/*
type Article struct {
	Title    string
	Author   string
	Count    int
	Count64  int64
	Deleted  bool
	Category []string
	Created  time.Time
	Updated  *time.Time
	F        float64
	Embedded struct {
		Tag string
		ICt int
	}
	Body *json.RawMessage
}
*/
type Article struct {
	*tu.Article
}

func (m *Article) Load(props []datastore.Property) error {
	for _, p := range props {
		switch p.Name {
		default:
			u.Warnf("unmapped: %v  %T", p.Name, p.Value)
		}
	}
	return nil
}
func (m *Article) Save() ([]datastore.Property, error) {
	props := make([]datastore.Property, 11)
	props[0] = datastore.Property{Name: "title", Value: m.Title}
	props[1] = datastore.Property{Name: "author", Value: m.Author}
	props[2] = datastore.Property{Name: "count", Value: m.Count}
	props[3] = datastore.Property{Name: "count64", Value: m.Count64}
	props[4] = datastore.Property{Name: "deleted", Value: m.Deleted}
	cat, _ := json.Marshal(m.Category)
	props[5] = datastore.Property{Name: "category", Value: cat, NoIndex: true}
	props[6] = datastore.Property{Name: "created", Value: m.Created}
	props[7] = datastore.Property{Name: "updated", Value: *m.Updated}
	props[8] = datastore.Property{Name: "f", Value: m.F}
	embed, _ := json.Marshal(m.Embedded)
	props[9] = datastore.Property{Name: "embedded", Value: embed, NoIndex: true}
	if m.Body != nil {
		props[10] = datastore.Property{Name: "body", Value: []byte(*m.Body), NoIndex: true}
	} else {
		props[10] = datastore.Property{Name: "body", Value: []byte{}, NoIndex: true}
	}

	return props, nil
}

/*
type User struct {
	Id      string
	Name    string
	Deleted bool
	Roles   []string
	Created time.Time
	Updated *time.Time
}

*/
type User struct {
	*tu.User
}

func (m *User) Load(props []datastore.Property) error {
	for _, p := range props {
		switch p.Name {
		case "id":
			m.Id = p.Value.(string)
		default:
			u.Warnf("unmapped: %v  %T", p.Name, p.Value)
		}
	}
	return nil
}
func (m *User) Save() ([]datastore.Property, error) {
	props := make([]datastore.Property, 6)
	roles, _ := m.Roles.Value()
	u.Infof("roles: %T", roles)
	props[0] = datastore.Property{Name: "id", Value: m.Id}                    // Indexed
	props[1] = datastore.Property{Name: "name", Value: m.Id}                  // Indexed
	props[2] = datastore.Property{Name: "deleted", Value: m.Deleted}          // Indexed
	props[3] = datastore.Property{Name: "roles", Value: roles, NoIndex: true} // Not Indexed
	props[4] = datastore.Property{Name: "created", Value: m.Created}          // Indexed
	props[5] = datastore.Property{Name: "updated", Value: *m.Updated}         // Indexed
	return props, nil
}

func loadTestData(t *testing.T) {
	loadTestDataOnce.Do(func() {
		for _, article := range tu.Articles {
			key, err := client.Put(ctx, articleKey(article.Title), &Article{article})
			//u.Infof("key: %v", key)
			assert.Tf(t, key != nil, "%v", key)
			assert.Tf(t, err == nil, "must put %v", err)
		}
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
		for _, user := range tu.Users {
			key, err := client.Put(ctx, userKey(user.Id), &User{user})
			//u.Infof("key: %v", key)
			assert.Tf(t, err == nil, "must put %v", err)
			assert.Tf(t, key != nil, "%v", key)
		}
	})
}

func loadAuth(jsonKey []byte) (context.Context, *datastore.Client) {
	// Initialize an authorized context with Google Developers Console
	// JSON key. Read the google package examples to learn more about
	// different authorization flows you can use.
	// http://godoc.org/golang.org/x/oauth2/google
	conf, err := google.JWTConfigFromJSON(
		jsonKey,
		datastore.ScopeDatastore,
		datastore.ScopeUserEmail,
	)
	if err != nil {
		log.Fatal(err)
	}
	//ctx := cloud.NewContext(*gds.GoogleProject, conf.Client(oauth2.NoContext))
	ctx := context.Background()
	client, err := datastore.NewClient(ctx, *gds.GoogleProject, cloud.WithTokenSource(conf.TokenSource(ctx)))
	if err != nil {
		panic(err.Error())
	}
	return ctx, client
}

// We are testing that we can register this Google Datasource
// as a qlbridge-Datasource
func TestDataSourceInterface(t *testing.T) {

	// By running testserver, we will load schema/config
	testmysql.RunTestServer(t)
	loadTestData(t)

	// Now make sure that the datastore source has been registered
	// and meets api for qlbridge.DataSource
	ds, err := datasource.OpenConn(gds.DataSourceLabel, ArticleKind)
	assert.Tf(t, err == nil, "no error on conn: %v", err)
	assert.Tf(t, ds != nil, "Found datastore")
}

func TestInvalidQuery(t *testing.T) {
	testmysql.RunTestServer(t)
	db, err := sql.Open("mysql", DbConn)
	assert.T(t, err == nil)
	// It is parsing the SQL on server side (proxy) not in client
	//  so hence that is what this is testing, making sure proxy responds gracefully
	rows, err := db.Query("select `stuff`, NOTAKEYWORD fake_tablename NOTWHERE `description` LIKE \"database\";")
	assert.Tf(t, err != nil, "%v", err)
	assert.Tf(t, rows == nil, "must not get rows")
}

func TestShowTables(t *testing.T) {
	data := struct {
		Table string `db:"Table"`
	}{}
	found := false
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "show tables;",
		ExpectRowCt: -1,
		ValidateRowData: func() {
			u.Infof("%v", data)
			assert.Tf(t, data.Table != "", "%v", data)
			if data.Table == strings.ToLower(ArticleKind) {
				found = true
			}
		},
		RowData: &data,
	})
	assert.Tf(t, found, "Must have found %s", ArticleKind)
}

func TestBasic(t *testing.T) {

	// By running testserver, we will load schema/config
	testmysql.RunTestServer(t)
	loadTestData(t)

	// This is a connection to RunTestServer, which starts on port 13307
	dbx, err := sqlx.Connect("mysql", DbConn)
	assert.Tf(t, err == nil, "%v", err)
	defer dbx.Close()
	//u.Debugf("%v", testSpec.Sql)
	rows, err := dbx.Queryx(fmt.Sprintf("select * from %s", ArticleKind))
	assert.Tf(t, err == nil, "%v", err)
	defer rows.Close()

	/*
		aidAlias := fmt.Sprintf("%d-%s", 123, "query_users1")
		key := datastore.NewKey(ctx, QueryKind, aidAlias, 0, nil)
		qd := &QueryData{}
		err := datastore.Get(ctx, key, qd)
		assert.Tf(t, err == nil, "no error: %v", err)
		assert.Tf(t, qd.Alias == "query_users1", "has alias")
		queryMeta()
	*/
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
		Sql:         fmt.Sprintf("describe %s;", ArticleKind),
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
		Sql:         "select title, count, deleted, author from DataUxTestArticle WHERE author = \"aaron\" LIMIT 1",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			//u.Infof("%v", data)
			assert.Tf(t, data.Deleted == false, "Not deleted? %v", data)
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count,deleted from DataUxTestArticle WHERE count = 22;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "article1", "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:             "select title, count, deleted from DataUxTestArticle LIMIT 10;",
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
		Sql:             "select title, count from DataUxTestArticle LIMIT 1;",
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
		Sql:         `SELECT title, count as ct from DataUxTestArticle WHERE title like "list%"`,
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
		Sql:         "select title, count64 AS ct FROM DataUxTestArticle ORDER BY count64 DESC LIMIT 1;",
		ExpectRowCt: 1,
		ValidateRowData: func() {
			assert.Tf(t, data.Title == "article3", "%v", data)
			assert.Tf(t, data.Ct == 100, "%v", data)
		},
		RowData: &data,
	})
	validateQuerySpec(t, tu.QuerySpec{
		Sql:         "select title, count64 AS ct FROM DataUxTestArticle ORDER BY count64 ASC LIMIT 1;",
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
