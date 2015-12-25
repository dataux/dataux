package mysql

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestResultsetSort(t *testing.T) {
	r1 := new(Resultset)
	r2 := new(Resultset)

	r1.Values = [][]driver.Value{
		[]driver.Value{int64(1), "a", []byte("aa")},
		[]driver.Value{int64(2), "a", []byte("bb")},
		[]driver.Value{int64(3), "c", []byte("bb")},
	}

	r1.RowDatas = []RowData{
		RowData([]byte("1")),
		RowData([]byte("2")),
		RowData([]byte("3")),
	}

	s := new(resultsetSorter)

	s.Resultset = r1

	s.sk = []SortKey{
		SortKey{column: 0, Direction: SortDesc},
	}

	sort.Sort(s)

	r2.Values = [][]driver.Value{
		[]driver.Value{int64(3), "c", []byte("bb")},
		[]driver.Value{int64(2), "a", []byte("bb")},
		[]driver.Value{int64(1), "a", []byte("aa")},
	}

	r2.RowDatas = []RowData{
		RowData([]byte("3")),
		RowData([]byte("2")),
		RowData([]byte("1")),
	}

	if !reflect.DeepEqual(r1, r2) {
		t.Fatal(fmt.Sprintf("%v %v", r1, r2))
	}

	s.sk = []SortKey{
		SortKey{column: 1, Direction: SortAsc},
		SortKey{column: 2, Direction: SortDesc},
	}

	sort.Sort(s)

	r2.Values = [][]driver.Value{
		[]driver.Value{int64(2), "a", []byte("bb")},
		[]driver.Value{int64(1), "a", []byte("aa")},
		[]driver.Value{int64(3), "c", []byte("bb")},
	}

	r2.RowDatas = []RowData{
		RowData([]byte("2")),
		RowData([]byte("1")),
		RowData([]byte("3")),
	}

	if !reflect.DeepEqual(r1, r2) {
		t.Fatal(fmt.Sprintf("%v %v", r1, r2))
	}

	s.sk = []SortKey{
		SortKey{column: 1, Direction: SortAsc},
		SortKey{column: 2, Direction: SortAsc},
	}

	sort.Sort(s)

	r2.Values = [][]driver.Value{
		[]driver.Value{int64(1), "a", []byte("aa")},
		[]driver.Value{int64(2), "a", []byte("bb")},
		[]driver.Value{int64(3), "c", []byte("bb")},
	}

	r2.RowDatas = []RowData{
		RowData([]byte("1")),
		RowData([]byte("2")),
		RowData([]byte("3")),
	}

	if !reflect.DeepEqual(r1, r2) {
		t.Fatal(fmt.Sprintf("%v %v", r1, r2))
	}

}
