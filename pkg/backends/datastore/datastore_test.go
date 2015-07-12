package datastore_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/expr"
	"github.com/bmizerany/assert"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/cloud"
	"google.golang.org/cloud/datastore"
)

var (
	ctx context.Context

	veryVerbose *bool   = flag.Bool("vv", false, "very verbose output")
	googleJwt   *string = flag.String("googlejwt", os.Getenv("GOOGLEJWT"), "Path to google JWT oauth token file")
	logLevel    *string = flag.String("logging", "debug", "Which log level: [debug,info,warn,error,fatal]")
)

func init() {

	flag.Parse()
	if *veryVerbose {
		u.SetupLogging(*logLevel)
		u.SetColorOutput()
	} else {
		u.SetupLogging("warn")
		u.SetColorOutput()
	}
	u.SetColorIfTerminal()

	if *googleJwt == "" {
		u.Errorf("must have google oauth jwt")
		os.Exit(1)
	}

	jsonKey, err := ioutil.ReadFile(*googleJwt)
	if err != nil {
		u.Errorf("Could not open Google Auth Token JWT file %v", err)
		os.Exit(1)
	}
	ctx = loadAuth(jsonKey)
}

const (
	QueryKind string = "Query"
)

type QueryData struct {
	Aid         int            `datastore:"aid"`
	Alias       string         `datastore:"alias"`
	Table       string         `datastore:"table"`
	Description string         `datastore:"description,noindex"`
	QueryText   string         `datastore:"query_text,noindex"`
	Author      *datastore.Key `datastore:"author,noindex"`
	Created     time.Time      `datastore:"created,noindex"`
	Updated     time.Time      `datastore:"updated,noindex"`
}

func (q *QueryData) Key() *datastore.Key {
	aidAlias := fmt.Sprintf("%d-%s", q.Aid, q.Alias)
	return datastore.NewKey(ctx, QueryKind, aidAlias, 0, nil)
}

func loadAuth(jsonKey []byte) context.Context {
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
	ctx := cloud.NewContext("lytics-dev", conf.Client(oauth2.NoContext))
	// Use the context (see other examples)
	return ctx
}

type MapDataStore struct {
	Vals  map[string]interface{}
	props []datastore.Property
}

func (m *MapDataStore) Load(props []datastore.Property) error {
	m.Vals = make(map[string]interface{}, len(props))
	//u.Infof("Load: %#v", props)
	for _, p := range props {
		u.Infof("prop: %#v", p)
		m.Vals[p.Name] = p.Value
	}
	return nil
}

func (m *MapDataStore) Save() ([]datastore.Property, error) {
	return nil, nil
}

func testDataSourceTables(t *testing.T) {
	queryPut(t, `
		SELECT 
		    fname
		FROM mystream 
		WHERE 
		   ne(event,"stuff") AND ge(party, 1)
		ALIAS query_users1;
	`)
	aidAlias := fmt.Sprintf("%d-%s", 123, "query_users1")
	key := datastore.NewKey(ctx, QueryKind, aidAlias, 0, nil)
	qd := &QueryData{}
	err := datastore.Get(ctx, key, qd)
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, qd.Alias == "query_users1", "has alias")
}

func TestBasic(t *testing.T) {
	queryPut(t, `
		SELECT 
		    fname
		    , lname AS last_name
		    , count(host(_ses)) IF contains(_ses,"google.com")
		    , now() AS created_ts
		    , name          -- comment 
		    , valuect(event) 
		    , todate(reg_date)
		    , todate(`+"`field xyz $%`"+`)
		FROM mystream 
		WHERE 
		   ne(event,"stuff") AND ge(party, 1)
		ALIAS query_users1;
	`)
	aidAlias := fmt.Sprintf("%d-%s", 123, "query_users1")
	key := datastore.NewKey(ctx, QueryKind, aidAlias, 0, nil)
	qd := &QueryData{}
	err := datastore.Get(ctx, key, qd)
	assert.Tf(t, err == nil, "no error: %v", err)
	assert.Tf(t, qd.Alias == "query_users1", "has alias")

	queryMeta()
}
func pageQuery(iter *datastore.Iterator) {
	for {
		mv := MapDataStore{}
		if key, err := iter.Next(&mv); err != nil {
			if err == datastore.Done {
				break
			}
			u.Errorf("error: %v", err)
			break
		} else {
			u.Infof("\n\tkey:\t%#v\n\tvals:\t%#v", key, mv.Vals)
		}
	}
}

func queryMeta() {
	u.Infof("getting __namespace__")
	pageQuery(datastore.NewQuery("__namespace__").Run(ctx))
	u.Infof("getting __kind__")
	pageQuery(datastore.NewQuery("__kind__").Run(ctx))
	u.Infof("getting __property__")
	pageQuery(datastore.NewQuery("__property__").Limit(1000).Run(ctx))
	u.Infof("getting Query")
	pageQuery(datastore.NewQuery("Query").Limit(1000).Run(ctx))
}

func queryPut(t *testing.T, query string) {
	q, err := expr.ParseSql(query)
	assert.Tf(t, err == nil, "must parse %v", err)
	sel, ok := q.(*expr.SqlSelect)
	assert.T(t, ok)

	n := time.Now()
	qd := &QueryData{
		Aid:         123,
		Alias:       sel.Alias,
		Description: "description",
		QueryText:   sel.Raw,
		Author:      datastore.NewKey(ctx, "Author", "aaron", 0, nil),
		Created:     n,
		Updated:     n,
	}
	//newKey := datastore.NewIncompleteKey(ctx, "Query", nil)
	key, err := datastore.Put(ctx, qd.Key(), qd)
	u.Infof("key: %v", key)
	assert.Tf(t, err == nil, "must put %v", err)
}

func ExampleDelete() {

	key := datastore.NewKey(ctx, "Article", "articled1", 0, nil)
	if err := datastore.Delete(ctx, key); err != nil {
		log.Fatal(err)
	}
}

type Post struct {
	Title       string
	PublishedAt time.Time
	Comments    int
}

func ExampleGetMulti() {

	keys := []*datastore.Key{
		datastore.NewKey(ctx, "Post", "post1", 0, nil),
		datastore.NewKey(ctx, "Post", "post2", 0, nil),
		datastore.NewKey(ctx, "Post", "post3", 0, nil),
	}
	posts := make([]Post, 3)
	if err := datastore.GetMulti(ctx, keys, posts); err != nil {
		log.Println(err)
	}
}

func ExamplePutMulti_slice() {

	keys := []*datastore.Key{
		datastore.NewKey(ctx, "Post", "post1", 0, nil),
		datastore.NewKey(ctx, "Post", "post2", 0, nil),
	}

	// PutMulti with a Post slice.
	posts := []*Post{
		{Title: "Post 1", PublishedAt: time.Now()},
		{Title: "Post 2", PublishedAt: time.Now()},
	}
	if _, err := datastore.PutMulti(ctx, keys, posts); err != nil {
		log.Fatal(err)
	}
}

func ExamplePutMulti_interfaceSlice() {

	keys := []*datastore.Key{
		datastore.NewKey(ctx, "Post", "post1", 0, nil),
		datastore.NewKey(ctx, "Post", "post2", 0, nil),
	}

	// PutMulti with an empty interface slice.
	posts := []interface{}{
		&Post{Title: "Post 1", PublishedAt: time.Now()},
		&Post{Title: "Post 2", PublishedAt: time.Now()},
	}
	if _, err := datastore.PutMulti(ctx, keys, posts); err != nil {
		log.Fatal(err)
	}
}

func ExampleQuery() {

	// Count the number of the post entities.
	n, err := datastore.NewQuery("Post").Count(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("There are %d posts.", n)

	// List the posts published since yesterday.
	yesterday := time.Now().Add(-24 * time.Hour)

	it := datastore.NewQuery("Post").Filter("PublishedAt >", yesterday).Run(ctx)
	// Use the iterator.
	_ = it

	// Order the posts by the number of comments they have recieved.
	datastore.NewQuery("Post").Order("-Comments")

	// Start listing from an offset and limit the results.
	datastore.NewQuery("Post").Offset(20).Limit(10)
}
