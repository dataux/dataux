package proxy

import (
	"fmt"
	"strconv"

	u "github.com/araddon/gou"
	"github.com/dataux/dataux/pkg/models"
	"github.com/dataux/dataux/vendor/mixer/hack"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

var _ = u.EMPTY

func formatValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int16:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int64:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case int:
		return strconv.AppendInt(nil, int64(v), 10), nil
	case uint8:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint16:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint64:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(v), 10), nil
	case float32:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.AppendFloat(nil, float64(v), 'f', -1, 64), nil
	case []byte:
		return v, nil
	case string:
		return hack.Slice(v), nil
	default:
		return nil, fmt.Errorf("invalid type %T", value)
	}
}

func formatField(field *mysql.Field, value interface{}) error {
	switch value.(type) {
	case int8, int16, int32, int64, int:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONGLONG
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG
	case uint8, uint16, uint32, uint64, uint:
		field.Charset = 63
		field.Type = mysql.MYSQL_TYPE_LONGLONG
		field.Flag = mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG | mysql.UNSIGNED_FLAG
	case string, []byte:
		field.Charset = 33
		field.Type = mysql.MYSQL_TYPE_VAR_STRING
	default:
		return fmt.Errorf("unsupport type %T for resultset", value)
	}
	return nil
}

func buildResultset(names []string, values [][]interface{}) (*mysql.Resultset, error) {
	r := new(mysql.Resultset)

	r.Fields = make([]*mysql.Field, len(names))

	var b []byte
	var err error

	for i, vs := range values {
		if len(vs) != len(r.Fields) {
			return nil, fmt.Errorf("row %d has %d column not equal %d", i, len(vs), len(r.Fields))
		}

		var row []byte
		for j, value := range vs {
			if i == 0 {
				field := &mysql.Field{}
				r.Fields[j] = field
				field.Name = hack.Slice(names[j])

				if err = formatField(field, value); err != nil {
					return nil, err
				}
			}
			b, err = formatValue(value)

			if err != nil {
				return nil, err
			}

			row = append(row, mysql.PutLengthEncodedString(b)...)
		}

		r.RowDatas = append(r.RowDatas, row)
	}

	return r, nil
}

func (c *Conn) WriteResult(r models.Result) error {
	//u.Warnf("in WriteResult():  %T %#v", r, r)
	switch resVal := r.(type) {
	case *mysql.Resultset:
		return c.WriteHandlerResult(c.Status, resVal)
	case *mysql.Result:
		return c.WriteOK(resVal)
	}
	u.Errorf("unknown result type?:  T:%T   v:%v", r, r)
	return fmt.Errorf("Unknown result type: %T", r)
}

func (c *Conn) WriteResultset(status uint16, r *mysql.Resultset) error {
	return c.WriteHandlerResult(status, r)
}

func (c *Conn) WriteHandlerResult(status uint16, r *mysql.Resultset) error {
	c.affectedRows = int64(-1)

	//u.Debugf("write result: status=%v", status)
	columnLen := mysql.PutLengthEncodedInt(uint64(len(r.Fields)))

	data := make([]byte, 4, 1024)

	data = append(data, columnLen...)
	if err := c.WritePacket(data); err != nil {
		u.Warn(err)
		return err
	}

	for _, v := range r.Fields {
		data = data[0:4]
		data = append(data, v.Dump()...)
		//u.Infof("field; %v", v.String())
		//u.Debugf("data: %s", data)
		if err := c.WritePacket(data); err != nil {
			u.Warn(err)
			return err
		}
	}

	if err := c.WriteEOF(status); err != nil {
		u.Warn(err)
		return err
	}

	for _, v := range r.RowDatas {
		data = data[0:4]
		data = append(data, v...)
		if err := c.WritePacket(data); err != nil {
			u.Warn(err)
			return err
		}
	}

	if err := c.WriteEOF(status); err != nil {
		u.Warn(err)
		return err
	}

	return nil
}
