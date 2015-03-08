package proxy

import (
	"github.com/dataux/dataux/vendor/mixer/hack"
	"github.com/dataux/dataux/vendor/mixer/mysql"
)

func (c *Conn) BuildSimpleShowResultset(values []interface{}, name string) (*mysql.Resultset, error) {

	r := new(mysql.Resultset)

	field := &mysql.Field{}

	field.Name = hack.Slice(name)
	field.Charset = 33
	field.Type = mysql.MYSQL_TYPE_VAR_STRING

	r.Fields = []*mysql.Field{field}

	var row []byte
	var err error

	for _, value := range values {
		row, err = formatValue(value)
		if err != nil {
			return nil, err
		}
		r.RowDatas = append(r.RowDatas, mysql.PutLengthEncodedString(row))
	}

	return r, nil
}

func (c *Conn) HandleShowProxyConfig() (*mysql.Resultset, error) {
	var names []string = []string{"Section", "Key", "Value"}
	var rows [][]string
	const (
		Column = 3
	)

	rows = append(rows, []string{"Global_Config", "Addr", c.listener.feconf.Addr})
	rows = append(rows, []string{"Global_Config", "User", c.listener.feconf.User})
	rows = append(rows, []string{"Global_Config", "Password", c.listener.feconf.Password})
	rows = append(rows, []string{"Global_Config", "LogLevel", c.listener.cfg.LogLevel})
	//rows = append(rows, []string{"Global_Config", "Schemas_Count", fmt.Sprintf("%d", len(c.listener.schemas))})
	//rows = append(rows, []string{"Global_Config", "Nodes_Count", fmt.Sprintf("%d", len(c.listener.nodes))})

	// TODO:  re-enable
	// for db, schema := range c.listener.schemas {
	// 	rows = append(rows, []string{"Schemas", "DB", db})

	// 	var nodeNames []string
	// 	var nodeRows [][]string
	// 	for name, node := range schema.nodes {
	// 		nodeNames = append(nodeNames, name)
	// 		var nodeSection = fmt.Sprintf("Schemas[%s]-Node[ %v ]", db, name)

	// 		if node.master != nil {
	// 			nodeRows = append(nodeRows, []string{nodeSection, "Master", node.master.String()})
	// 		}

	// 		if node.slave != nil {
	// 			nodeRows = append(nodeRows, []string{nodeSection, "Slave", node.slave.String()})
	// 		}
	// 		nodeRows = append(nodeRows, []string{nodeSection, "Last_Master_Ping", fmt.Sprintf("%v", time.Unix(node.lastMasterPing, 0))})

	// 		nodeRows = append(nodeRows, []string{nodeSection, "Last_Slave_Ping", fmt.Sprintf("%v", time.Unix(node.lastSlavePing, 0))})

	// 		nodeRows = append(nodeRows, []string{nodeSection, "down_after_noalive", fmt.Sprintf("%v", node.downAfterNoAlive)})

	// 	}
	// 	rows = append(rows, []string{fmt.Sprintf("Schemas[%s]", db), "Nodes_List", strings.Join(nodeNames, ",")})

	// 	var defaultRule = schema.rule.DefaultRule
	// 	if defaultRule.DB == db {
	// 		if defaultRule.DB == db {
	// 			rows = append(rows, []string{fmt.Sprintf("Schemas[%s]_Rule_Default", db),
	// 				"Default_Table", defaultRule.String()})
	// 		}
	// 	}
	// 	for tb, r := range schema.rule.Rules {
	// 		if r.DB == db {
	// 			rows = append(rows, []string{fmt.Sprintf("Schemas[%s]_Rule_Table", db),
	// 				fmt.Sprintf("Table[ %s ]", tb), r.String()})
	// 		}
	// 	}

	// 	rows = append(rows, nodeRows...)
	// }

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, Column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	return buildResultset(names, values)
}
