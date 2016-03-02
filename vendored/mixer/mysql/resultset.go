package mysql

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/vendored/mixer/hack"
)

type Result struct {
	Status       uint16
	InsertId     uint64
	AffectedRows uint64
	*Resultset
}

type Resultset struct {
	Fields     []*Field         // List of fields in this result set
	FieldNames map[string]int   // List of name, to position in column list
	Values     [][]driver.Value // row-results
	RowDatas   []RowData        // The serialized mysql byte value of a row
}

func NewResult() *Result {
	return &Result{
		Status: 0,
	}
}
func NewResultSet() *Resultset {
	return &Resultset{
		FieldNames: make(map[string]int),
	}
}

type RowData []byte

func (p RowData) Parse(f []*Field, binary bool) ([]driver.Value, error) {
	if binary {
		return p.ParseBinary(f)
	} else {
		return p.ParseText(f)
	}
}

func ValuesToRowData(values []driver.Value, fields []*Field) (RowData, error) {

	var buf bytes.Buffer
	var err error
	//var isNull, isUnsigned bool
	if len(values) != len(fields) {
		for _, fld := range fields {
			u.Debugf("field:  %v %v", fld.FieldName, fld.Name)
		}
		//panic("wtf")
		u.LogTracef(u.ERROR, "Number of values doesn't match number of fields:  fields:%v vals:%v   \n%#v", len(fields), len(values), values)
		//return nil, fmt.Errorf("Number of values doesn't match number of fields:  fields:%v vals:%v", len(fields), len(values))
	}

	for i, _ := range fields {

		//isUnsigned = (f.Flag&UNSIGNED_FLAG > 0)
		// switch f.Type {
		// case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
		// 	MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
		// 	//if isUnsigned {
		// 	buf.Write(PutLengthEncodedInt(values[i].(int64)))
		// }
		//u.Debugf("i:%d T:%T\t", i, values[i])
		switch v := values[i].(type) {
		case int:
			leby := PutLengthEncodedString([]byte(strconv.FormatInt(int64(v), 10)))
			buf.Write(leby)
		case int32:
			leby := PutLengthEncodedString([]byte(strconv.FormatInt(int64(v), 10)))
			buf.Write(leby)
		case int64:
			leby := PutLengthEncodedString([]byte(strconv.FormatInt(v, 10)))
			buf.Write(leby)
		case uint32:
			leby := PutLengthEncodedString([]byte(strconv.FormatUint(uint64(v), 10)))
			buf.Write(leby)
		case uint64:
			leby := PutLengthEncodedString([]byte(strconv.FormatUint(v, 10)))
			buf.Write(leby)
		case float64:
			leby := PutLengthEncodedString([]byte(strconv.FormatFloat(v, 'f', -1, 64)))
			buf.Write(leby)
		case float32:
			leby := PutLengthEncodedString([]byte(strconv.FormatFloat(float64(v), 'f', -1, 64)))
			buf.Write(leby)
		case string:
			leby := PutLengthEncodedString([]byte(v))
			buf.Write(leby)
		case []byte:
			//u.Debugf("write string: %v", string(v))
			buf.Write(PutLengthEncodedString(v))
		case time.Time:
			//  "YYYY-MM-DD HH:MM:SS.MMMMMM"
			//val := v.Format("2006-01-02 15:04:05.999999")
			val := v.Format("2006-01-02 15:04:05")
			leby := PutLengthEncodedString([]byte(val))
			buf.Write(leby)
		case bool:
			if v {
				//buf.Write(PutLengthEncodedInt(1))
				leby := PutLengthEncodedString([]byte(strconv.FormatInt(1, 10)))
				buf.Write(leby)
			} else {
				//buf.Write(PutLengthEncodedInt(0))
				leby := PutLengthEncodedString([]byte(strconv.FormatInt(0, 10)))
				buf.Write(leby)
			}
		default:
			if v == nil {
				buf.WriteByte(0xfb)
			} else {
				//u.Warnf("type not implemented: T:%T v:%v", v, v)
				by, err := json.Marshal(v)
				if err != nil {
					u.Warnf("could not json marshall err=%v  v=%#v", err, v)
				} else {
					buf.Write(by)
				}
			}
		}

		if err != nil {
			return nil, err
		}

	}
	//u.Infof("rowdata: %s", buf.Bytes())
	return RowData(buf.Bytes()), nil
}

func (p RowData) ParseText(f []*Field) ([]driver.Value, error) {
	data := make([]driver.Value, len(f))

	var err error
	var v []byte
	var isNull, isUnsigned bool
	var pos int = 0
	var n int = 0

	for i := range f {
		v, isNull, n, err = LengthEnodedString(p[pos:])
		if err != nil {
			return nil, err
		}

		pos += n

		if isNull {
			data[i] = nil
		} else {
			isUnsigned = (f[i].Flag&UNSIGNED_FLAG > 0)

			switch f[i].Type {
			case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_INT24,
				MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
				if isUnsigned {
					data[i], err = strconv.ParseUint(string(v), 10, 64)
				} else {
					data[i], err = strconv.ParseInt(string(v), 10, 64)
				}
			case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
				data[i], err = strconv.ParseFloat(string(v), 64)
			default:
				data[i] = v
			}

			if err != nil {
				return nil, err
			}
		}
	}

	return data, nil
}

func (p RowData) ParseBinary(f []*Field) ([]driver.Value, error) {
	data := make([]driver.Value, len(f))

	if p[0] != OK_HEADER {
		return nil, ErrMalformPacket
	}

	pos := 1 + ((len(f) + 7 + 2) >> 3)

	nullBitmap := p[1:pos]

	var isUnsigned bool
	var isNull bool
	var n int
	var err error
	var v []byte
	for i := range data {
		if nullBitmap[(i+2)/8]&(1<<(uint(i+2)%8)) > 0 {
			data[i] = nil
			continue
		}

		isUnsigned = f[i].Flag&UNSIGNED_FLAG > 0

		switch f[i].Type {
		case MYSQL_TYPE_NULL:
			data[i] = nil
			continue

		case MYSQL_TYPE_TINY:
			if isUnsigned {
				data[i] = uint64(p[pos])
			} else {
				data[i] = int64(p[pos])
			}
			pos++
			continue

		case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
			if isUnsigned {
				data[i] = uint64(binary.LittleEndian.Uint16(p[pos : pos+2]))
			} else {
				data[i] = int64((binary.LittleEndian.Uint16(p[pos : pos+2])))
			}
			pos += 2
			continue

		case MYSQL_TYPE_INT24, MYSQL_TYPE_LONG:
			if isUnsigned {
				data[i] = uint64(binary.LittleEndian.Uint32(p[pos : pos+4]))
			} else {
				data[i] = int64(binary.LittleEndian.Uint32(p[pos : pos+4]))
			}
			pos += 4
			continue

		case MYSQL_TYPE_LONGLONG:
			if isUnsigned {
				data[i] = binary.LittleEndian.Uint64(p[pos : pos+8])
			} else {
				data[i] = int64(binary.LittleEndian.Uint64(p[pos : pos+8]))
			}
			pos += 8
			continue

		case MYSQL_TYPE_FLOAT:
			data[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(p[pos : pos+4])))
			pos += 4
			continue

		case MYSQL_TYPE_DOUBLE:
			data[i] = math.Float64frombits(binary.LittleEndian.Uint64(p[pos : pos+8]))
			pos += 8
			continue

		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_VARCHAR,
			MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_TINY_BLOB,
			MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB,
			MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING, MYSQL_TYPE_GEOMETRY:
			v, isNull, n, err = LengthEnodedString(p[pos:])
			pos += n
			if err != nil {
				return nil, err
			}

			if !isNull {
				data[i] = v
				continue
			} else {
				data[i] = nil
				continue
			}
		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i] = nil
				continue
			}

			data[i], err = FormatBinaryDate(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_DATETIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i] = nil
				continue
			}

			data[i], err = FormatBinaryDateTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		case MYSQL_TYPE_TIME:
			var num uint64
			num, isNull, n = LengthEncodedInt(p[pos:])

			pos += n

			if isNull {
				data[i] = nil
				continue
			}

			data[i], err = FormatBinaryTime(int(num), p[pos:])
			pos += int(num)

			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("Stmt Unknown FieldType %d %s", f[i].Type, f[i].Name)
		}
	}

	return data, nil
}

func (r *Resultset) RowNumber() int {
	return len(r.Values)
}

func (r *Resultset) AddRowValues(values []driver.Value) {
	r.Values = append(r.Values, values)
	rowData, _ := ValuesToRowData(values, r.Fields)
	r.RowDatas = append(r.RowDatas, rowData)
}
func (r *Resultset) ColumnNumber() int {
	return len(r.Fields)
}

func (r *Resultset) GetValue(row, column int) (interface{}, error) {
	if row >= len(r.Values) || row < 0 {
		return nil, fmt.Errorf("invalid row index %d", row)
	}

	if column >= len(r.Fields) || column < 0 {
		return nil, fmt.Errorf("invalid column index %d", column)
	}

	return r.Values[row][column], nil
}

func (r *Resultset) NameIndex(name string) (int, error) {
	if column, ok := r.FieldNames[name]; ok {
		return column, nil
	} else {
		return 0, fmt.Errorf("invalid field name %s", name)
	}
}

func (r *Resultset) GetValueByName(row int, name string) (interface{}, error) {
	if column, err := r.NameIndex(name); err != nil {
		return nil, err
	} else {
		return r.GetValue(row, column)
	}
}

func (r *Resultset) IsNull(row, column int) (bool, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return false, err
	}

	return d == nil, nil
}

func (r *Resultset) IsNullByName(row int, name string) (bool, error) {
	if column, err := r.NameIndex(name); err != nil {
		return false, err
	} else {
		return r.IsNull(row, column)
	}
}

func (r *Resultset) GetUint(row, column int) (uint64, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return 0, err
	}

	switch v := d.(type) {
	case uint64:
		return v, nil
	case int64:
		return uint64(v), nil
	case float64:
		return uint64(v), nil
	case string:
		return strconv.ParseUint(v, 10, 64)
	case []byte:
		return strconv.ParseUint(string(v), 10, 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetUintByName(row int, name string) (uint64, error) {
	if column, err := r.NameIndex(name); err != nil {
		return 0, err
	} else {
		return r.GetUint(row, column)
	}
}

func (r *Resultset) GetInt(row, column int) (int64, error) {
	v, err := r.GetUint(row, column)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *Resultset) GetIntByName(row int, name string) (int64, error) {
	v, err := r.GetUintByName(row, name)
	if err != nil {
		return 0, err
	}

	return int64(v), nil
}

func (r *Resultset) GetFloat(row, column int) (float64, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return 0, err
	}

	switch v := d.(type) {
	case float64:
		return v, nil
	case uint64:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetFloatByName(row int, name string) (float64, error) {
	if column, err := r.NameIndex(name); err != nil {
		return 0, err
	} else {
		return r.GetFloat(row, column)
	}
}

func (r *Resultset) GetString(row, column int) (string, error) {
	d, err := r.GetValue(row, column)
	if err != nil {
		return "", err
	}

	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return hack.String(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("data type is %T", v)
	}
}

func (r *Resultset) GetStringByName(row int, name string) (string, error) {
	if column, err := r.NameIndex(name); err != nil {
		return "", err
	} else {
		return r.GetString(row, column)
	}
}
