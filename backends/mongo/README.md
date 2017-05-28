Mongo Data Source
--------------------------------------

Provides SQL Access to Mongo via the DataUX Mysql Proxy Service.


![mongo dataux](https://cloud.githubusercontent.com/assets/7269/26533001/e96cea54-43c5-11e7-942e-bb214fb6761f.png)


```sql
mysql -h 127.0.0.1 -P4000


-- Create a new schema = "dbx1" with one source being
-- a mongo database called "mgo_datauxtest"

CREATE source mgo_datauxtest WITH {
  "type":"mongo",
  "schema":"dbx1",
  "hosts": ["localhost:28017"]
};

-- DB_NAME = existing database in mongo

-- WITH Properties:
-- "schema":  Name of schema to attach this source to
-- "type":  Source type, most be datasource registered in registry (mongo, bigtable, etc)

CREATE source DB_NAME WITH json_properties


use dbx1;

show tables;

describe article;

select * from article;



```