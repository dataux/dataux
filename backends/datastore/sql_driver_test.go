package datastore_test

import (
	"database/sql"
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"

	"github.com/araddon/qlbridge/exec"
	"github.com/araddon/qlbridge/testutil"
	"github.com/dataux/dataux/backends/datastore"
)

func init() {
	testutil.Setup()
	time.Sleep(time.Second * 1)
	exec.RegisterSqlDriver()
	exec.DisableRecover()
}

func TestDatastoreSelectSqlDriver(t *testing.T) {

	sqlText := `select title, count, deleted, author from DataUxTestArticle WHERE author = "aaron" LIMIT 1`
	db, err := sql.Open("qlbridge", datastore.SourceLabel)
	assert.Equal(t, nil, err, "no error: %v", err)
	assert.NotEqual(t, nil, db, "has conn: ", db)

	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Should not error on close: %v", err)
		}
	}()

	rows, err := db.Query(sqlText)
	assert.True(t, err == nil, "no error: %v", err)
	defer rows.Close()
	assert.True(t, rows != nil, "has results: %v", rows)
	cols, err := rows.Columns()
	assert.True(t, err == nil, "no error: %v", err)
	assert.True(t, len(cols) == 4, "4 cols: %v", cols)
	articles := make([]Article, 0)
	for rows.Next() {
		a := NewArticle()
		err = rows.Scan(&a.Title, &a.Count, &a.Deleted, &a.Author)
		assert.True(t, err == nil, "no error: %v", err)
		u.Debugf("article=%+v", a)
		articles = append(articles, a)
	}
	assert.True(t, rows.Err() == nil, "no error: %v", err)
	assert.True(t, len(articles) == 1, "has 1 articles row: %+v", articles)

	a1 := articles[0]
	assert.Equal(t, "article1", a1.Title)
	assert.True(t, a1.Author == "aaron")
}
