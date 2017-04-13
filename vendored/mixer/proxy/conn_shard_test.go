package proxy

import (
	"fmt"
	"reflect"
	"testing"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/vendored/mixer/mysql"
	//"github.com/stretchr/testify/assert"
)

var _ = u.EMPTY

func setupShardDeleteAll(t *testing.T, table string) {
	conn := newTestDBConn(t)
	r, _ := conn.Execute(fmt.Sprintf(`delete from %s`, table))
	u.Warnf("shard cleanup delete %v", r)
}

func testShardInsert(t *testing.T, table string, node string, id int, str string) {
	conn := newTestDBConn(t)

	s := fmt.Sprintf(`insert into %s (id, str) values (%d, "%s")`, table, id, str)
	if r, err := conn.Execute(s); err != nil {
		t.Fatal(s, err)
	} else if r.AffectedRows != 1 {
		t.Fatal(r.AffectedRows)
	}
	s = fmt.Sprintf(`select str from %s where id = %d`, table, id)

	n := newTestServer(t).nodes[node]
	c, err := n.getMasterConn()
	if err != nil {
		t.Fatal(s, err)
	} else {
		if r, err := c.Execute(s); err != nil {
			t.Fatal(s, err)
		} else if v, _ := r.GetString(0, 0); v != str {
			t.Fatal(s, v)
		}
	}

	if r, err := conn.Execute(s); err != nil {
		t.Fatal(s, err)
	} else if v, _ := r.GetString(0, 0); v != str {
		t.Fatal(s, v)
	}
}

func testShardSelect(t *testing.T, table string, where string, strs ...string) {
	sql := fmt.Sprintf("select str from %s where %s", table, where)
	conn := newTestDBConn(t)

	r, err := conn.Execute(sql)
	if err != nil {
		t.Fatal(sql, err)
	} else if r.RowNumber() != len(strs) {
		t.Fatal(sql, r.RowNumber(), len(strs))
	}

	m := map[string]struct{}{}
	for _, s := range strs {
		m[s] = struct{}{}
	}

	for i := 0; i < r.RowNumber(); i++ {
		if v, err := r.GetString(i, 0); err != nil {
			t.Fatal(sql, err)
		} else if _, ok := m[v]; !ok {
			t.Fatal(sql, v, "no in check strs")
		} else {
			delete(m, v)
		}
	}

	if len(m) != 0 {
		t.Fatal(sql, "invalid select")
	}
}

func testShardStmtInsert(t *testing.T, table string, node string, id int, str string) {
	conn := newTestDBConn(t)

	s := fmt.Sprintf(`insert into %s (id, str) values (?, ?)`, table)
	if r, err := conn.Execute(s, id, str); err != nil {
		t.Fatal(s, err)
	} else if r.AffectedRows != 1 {
		t.Fatal(r.AffectedRows)
	}
	s = fmt.Sprintf(`select str from %s where id = ?`, table)

	n := newTestServer(t).nodes[node]
	c, err := n.getMasterConn()
	if err != nil {
		t.Fatal(s, err)
	} else {
		if r, err := c.Execute(s, id); err != nil {
			t.Fatal(s, err)
		} else if v, _ := r.GetString(0, 0); v != str {
			t.Fatal(s, v)
		}
	}

	if r, err := conn.Execute(s, id); err != nil {
		t.Fatal(s, err)
	} else if v, _ := r.GetString(0, 0); v != str {
		t.Fatal(s, v)
	}
}

func testShardStmtSelect(t *testing.T, table string, where string, args []interface{}, strs ...string) {
	sql := fmt.Sprintf("select str from %s where %s", table, where)
	conn := newTestDBConn(t)

	r, err := conn.Execute(sql, args...)
	if err != nil {
		t.Fatal(sql, err)
	} else if r.RowNumber() != len(strs) {
		t.Fatal(sql, r.RowNumber(), len(strs))
	}

	m := map[string]struct{}{}
	for _, s := range strs {
		m[s] = struct{}{}
	}

	for i := 0; i < r.RowNumber(); i++ {
		if v, err := r.GetString(i, 0); err != nil {
			t.Fatal(sql, err)
		} else if _, ok := m[v]; !ok {
			t.Fatal(sql, v, "no in check strs")
		} else {
			delete(m, v)
		}
	}

	if len(m) != 0 {
		t.Fatal(sql, "invalid select")
	}
}

func TestShardDeleteHashTable(t *testing.T) {
	s := `drop table if exists mixer_test_shard_hash`

	server := newTestServer(t)

	for _, n := range server.nodes {
		if n.String() != "node2" && n.String() != "node3" {
			continue
		}
		c, err := n.getMasterConn()
		if err != nil {
			t.Fatal(err)
		}

		c.UseDB("mixer")
		defer c.Close()
		if _, err := c.Execute(s); err != nil {
			t.Fatal(err)
		}

	}
}

func TestShardCreateHashTable(t *testing.T) {
	s := `CREATE TABLE IF NOT EXISTS mixer_test_shard_hash (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	server := newTestServer(t)

	for _, n := range server.nodes {
		if n.String() != "node2" && n.String() != "node3" {
			continue
		}
		c, err := n.getMasterConn()
		if err != nil {
			t.Fatal(err)
		}

		c.UseDB("mixer")
		defer c.Close()
		if _, err := c.Execute(s); err != nil {
			t.Fatal(err)
		}
	}
}

func TestShardHashDefault(t *testing.T) {

	table := "mixer_test_shard_hash"

	setupShardDeleteAll(t, table)

	testShardInsert(t, table, "node2", 0, "a")
	testShardInsert(t, table, "node3", 1, "b")
	testShardInsert(t, table, "node2", 2, "c")
	testShardInsert(t, table, "node3", 3, "d")

	testShardSelect(t, table, "id = 2", "c")
	testShardSelect(t, table, "id = 2 or id = 3", "c", "d")
	testShardSelect(t, table, "id = 2 and id = 3")
	testShardSelect(t, table, "id in (0, 1, 3)", "a", "b", "d")

	testShardStmtInsert(t, table, "node2", 10, "a")
	testShardStmtInsert(t, table, "node3", 11, "b")
	testShardStmtInsert(t, table, "node2", 12, "c")
	testShardStmtInsert(t, table, "node3", 13, "d")

	testShardStmtSelect(t, table, "id = ?", []interface{}{12}, "c")
	testShardStmtSelect(t, table, "id = ? or id = ?", []interface{}{12, 13}, "c", "d")
	testShardStmtSelect(t, table, "id = ? and id = ?", []interface{}{12, 13})
	testShardStmtSelect(t, table, "id in (?, ?, ?)", []interface{}{10, 11, 13}, "a", "b", "d")
}

func testExecute(t *testing.T, sql string) *mysql.Result {
	conn := newTestDBConn(t)

	r, err := conn.Execute(sql)
	if err != nil {
		t.Fatal(err)
	}

	return r
}

func testSharedSelectOrderBy(t *testing.T, table string, where string, v [][]interface{}) {
	sql := fmt.Sprintf("select id, str from %s where %s", table, where)

	r := testExecute(t, sql)

	if !reflect.DeepEqual(r.Values, v) {
		t.Fatal(fmt.Sprintf("%v != %v", r.Values, v))
	}
}

func TestShardHashOrderByLimit(t *testing.T) {
	table := "mixer_test_shard_hash"

	setupShardDeleteAll(t, table)

	testShardInsert(t, table, "node2", 4, "a")
	testShardInsert(t, table, "node3", 5, "a")
	testShardInsert(t, table, "node2", 6, "b")
	testShardInsert(t, table, "node3", 7, "b")

	var v [][]interface{}
	v = [][]interface{}{
		[]interface{}{uint64(7), []byte("b")},
		[]interface{}{uint64(6), []byte("b")},
		[]interface{}{uint64(5), []byte("a")},
		[]interface{}{uint64(4), []byte("a")},
	}

	testSharedSelectOrderBy(t, table, "id in (4,5,6,7) order by id desc", v)

	v = [][]interface{}{
		[]interface{}{uint64(6), []byte("b")},
		[]interface{}{uint64(7), []byte("b")},
		[]interface{}{uint64(4), []byte("a")},
		[]interface{}{uint64(5), []byte("a")},
	}

	testSharedSelectOrderBy(t, table, "id in (4,5,6,7) order by str desc, id asc", v)

	v = [][]interface{}{
		[]interface{}{uint64(6), []byte("b")},
		[]interface{}{uint64(7), []byte("b")},
	}

	testSharedSelectOrderBy(t, table, "id in (4,5,6,7) order by str desc, id asc limit 0, 2", v)

	v = [][]interface{}{
		[]interface{}{uint64(5), []byte("a")},
	}

	testSharedSelectOrderBy(t, table, "id in (4,5,6,7) order by str desc, id asc limit 1, 2", v)

}

func TestShardDeleteRangeTable(t *testing.T) {
	s := `drop table if exists mixer_test_shard_range`

	server := newTestServer(t)

	for _, n := range server.nodes {
		if n.String() != "node2" && n.String() != "node3" {
			continue
		}
		c, err := n.getMasterConn()
		if err != nil {
			t.Fatal(err)
		}

		c.UseDB("mixer")
		defer c.Close()
		if _, err := c.Execute(s); err != nil {
			t.Fatal(err)
		}

	}
}

func TestShardCreateRangeTable(t *testing.T) {
	s := `CREATE TABLE IF NOT EXISTS mixer_test_shard_range (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	server := newTestServer(t)

	for _, n := range server.nodes {
		if n.String() != "node2" && n.String() != "node3" {
			continue
		}
		c, err := n.getMasterConn()
		if err != nil {
			t.Fatal(err)
		}

		c.UseDB("mixer")
		defer c.Close()
		if _, err := c.Execute(s); err != nil {
			t.Fatal(err)
		}

	}
}

func TestShardRange(t *testing.T) {
	table := "mixer_test_shard_range"
	testShardInsert(t, table, "node2", 0, "a")
	testShardInsert(t, table, "node3", 10000, "b")
	testShardStmtInsert(t, table, "node2", 2, "c")
	testShardStmtInsert(t, table, "node3", 10001, "d")

	testShardSelect(t, table, "id = 2", "c")
	testShardSelect(t, table, "id = 2 or id = 10001", "c", "d")
	testShardSelect(t, table, "id = 2 and id = 10001")
	testShardSelect(t, table, "id in (0, 10000, 10001)", "a", "b", "d")
	testShardSelect(t, table, "id < 1 or id >= 10000", "a", "b", "d")
	testShardSelect(t, table, "id > 1 and id <= 10000", "b", "c")
	testShardSelect(t, table, "id < 1 and id >= 10000")

	testShardStmtSelect(t, table, "id = ?", []interface{}{2}, "c")
	testShardStmtSelect(t, table, "id = ? or id = ?", []interface{}{2, 10001}, "c", "d")
	testShardStmtSelect(t, table, "id = ? and id = ?", []interface{}{2, 10001})
	testShardStmtSelect(t, table, "id in (?, ?, ?)", []interface{}{0, 10000, 10001}, "a", "b", "d")
	testShardStmtSelect(t, table, "id < ? or id >= ?", []interface{}{1, 10000}, "a", "b", "d")
	testShardStmtSelect(t, table, "id > ? and id <= ?", []interface{}{1, 10000}, "b", "c")
	testShardStmtSelect(t, table, "id < ? and id >= ?", []interface{}{1, 10000})
}
