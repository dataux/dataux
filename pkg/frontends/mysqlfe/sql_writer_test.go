package mysqlfe

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"

	"github.com/araddon/qlbridge/schema"
	"github.com/araddon/qlbridge/value"
)

var _ = u.EMPTY

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
}

/*

mysql> describe pet;
+---------+-------------+------+-----+---------+-------+
| Field   | Type        | Null | Key | Default | Extra |
+---------+-------------+------+-----+---------+-------+
| name    | varchar(20) | YES  |     | NULL    |       |
| owner   | varchar(20) | YES  |     | NULL    |       |
| species | varchar(20) | YES  |     | NULL    |       |
| sex     | char(1)     | YES  |     | NULL    |       |
| birth   | date        | YES  |     | NULL    |       |
| death   | date        | YES  |     | NULL    |       |
+---------+-------------+------+-----+---------+-------+
6 rows in set (0.00 sec)

CREATE TABLE pet (name VARCHAR(20), owner VARCHAR(20),
  species VARCHAR(20), sex CHAR(1), birth DATE, death DATE);


column2 datatype [ NULL | NOT NULL ]
                   [ DEFAULT default_value ]
                   [ AUTO_INCREMENT ]
                   [ UNIQUE KEY | PRIMARY KEY ]
                   [ COMMENT 'string' ],

CREATE TABLE `test` (
  `id` bigint DEFAULT NULL COMMENT,
  `name` varchar(20) DEFAULT NULL,
  `species` varchar(20) DEFAULT NULL,
  `sex` char(1) DEFAULT NULL,
  `birth` date DEFAULT NULL,
  `death` date DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `equipment` (
  `id` int(8) NOT NULL AUTO_INCREMENT,
  `type` varchar(50) DEFAULT NULL,
  `install_date` date DEFAULT NULL,
  `color` varchar(20) DEFAULT NULL,
  `working` tinyint(1) DEFAULT NULL,
  `location` varchar(250) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

mysql> describe equipment;
+--------------+--------------+------+-----+---------+----------------+
| Field        | Type         | Null | Key | Default | Extra          |
+--------------+--------------+------+-----+---------+----------------+
| equip_id     | int(5)       | NO   | PRI | NULL    | auto_increment |
| type         | varchar(50)  | YES  |     | NULL    |                |
| install_date | date         | YES  |     | NULL    |                |
| color        | varchar(20)  | YES  |     | NULL    |                |
| working      | tinyint(1)   | YES  |     | NULL    |                |
| location     | varchar(250) | YES  |     | NULL    |                |
+--------------+--------------+------+-----+---------+----------------+
6 rows in set (0.00 sec)

ALTER TABLE "table_name"
ADD "column_name" "Data Type";
*/
// Test Table Create Statement
func TestSqlCreate(t *testing.T) {
	t.Parallel()

	ss := schema.NewSourceSchema("test", "test")
	tbl := schema.NewTable("equipment", ss)
	tbl.AddField(schema.NewField("id", value.IntType, 64, "Id is auto-generated random uuid"))
	tbl.AddField(schema.NewField("name", value.StringType, 20, ""))
	tbl.AddField(schema.NewField("installed", value.TimeType, 0, "When was this installed?"))
	tbl.AddField(schema.NewField("jsondata", value.ByteSliceType, 0, "Json Data"))

	expected := "CREATE TABLE `equipment` (\n" +
		"    `id` bigint DEFAULT NULL COMMENT \"Id is auto-generated random uuid\",\n" +
		"    `name` varchar(20) DEFAULT NULL,\n" +
		"    `installed` datetime DEFAULT NULL COMMENT \"When was this installed?\",\n" +
		"    `jsondata` text DEFAULT NULL COMMENT \"Json Data\"\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;"

	createStmt, err := TableCreate(tbl)
	assert.T(t, err == nil)
	for i, _ := range expected {
		if expected[i] != createStmt[i] {
			end := i + 20
			if end > len(expected) {
				end = len(expected)
			}
			t.Errorf("failed at %d  %v", i, expected[i:end])
			u.Warn("\n\n", expected, "\n\n expected but got\n\n", createStmt)
			return
		}
	}
	assert.Tf(t, expected == createStmt, "%s", createStmt)
}
