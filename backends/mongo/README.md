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

-- Syntax:  CREATE source DB_NAME WITH json_properties
-- DB_NAME = existing database in mongo, in this example "mgo_datauxtest"

-- WITH Properties:
-- "schema":  Name of schema to attach this source to
-- "type":  Source type, most be datasource registered in registry (mongo, bigtable, etc)
-- "hosts": Array of hosts:port



use dbx1;

show tables;

describe article;

select * from article;



```

SQL -> Mongo
----------------------------------

Mongo | SQL Query  
----- | -------
`show collections`                                                                | `show tables;`
na, -- runtime inspection                                                         | `describe mytable;`
`db.accounts.find({},{created:{"$gte":"1/1/2016"}}).count();`                     | `select count(*) from accounts WHERE created > "1/1/2016";`
`db.article.find({"year":{"$in": [2013,2014,2015] }},{}}`                         | `select * from article WHERE year IN (2015,2014,2013);`
`db.article.find({"created":{"$gte": new Date('Aug 01, 2011'), "$lte": new Date('Aug 03, 2013') },{title:1,count:1,author:1}}` |  `SELECT title, count, author FROM article WHERE created BETWEEN todate(\"2011-08-01\") AND todate(\"2013-08-03\")
`db.article.find({"title":{"Pattern":"^list","Options":"i"}},{title:1,count:1})`  | `SELECT title, count AS ct FROM article WHERE title like \"list%\"`
na, not avail in mongo (polyfill in dataux)                                       |  `SELECT avg(CHAR_LENGTH(CAST(`title`, \"AS\", \"CHAR\"))) AS title_avg FROM article;`
need to document ...                                                              | `select min(year), max(year), avg(year), sum(year) from table WHERE exists(a);`



