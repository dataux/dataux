
Google BigTable Data source
--------------------------------------

Provides SQL Access to Google BigTable

![Dataux BigTable](https://cloud.githubusercontent.com/assets/7269/20776318/a5711ad4-b714-11e6-96bf-408506158cbf.png)

* https://cloud.google.com/bigtable/docs/go/cbt-reference
* https://cloud.google.com/bigtable/docs/emulator
* https://github.com/spotify/docker-bigtable

SQL -> BigTable
----------------------------------

* *key* Big-Table primarily has a single index based on key.  


BigTable API | SQL Query  
----- | -------
Tables                  | `show tables;`
Column Families         | `describe mytable;`  
`WHERE`                 | `select count(*) from table WHERE exists(a);`  Some of these are pushed down to big-table if avaialble.
filter:   terms         | `select * from table WHERE year IN (2015,2014,2013);`
filter: gte, range      | `select * from table WHERE year BETWEEN 2012 AND 2014`
aggs min, max, avg, sum | `select min(year), max(year), avg(year), sum(year) from table WHERE exists(a);`   These are poly-filled in the distributed query engine.


**Details**

* Table vs Column Familes:   Data is stored very, very different in
  bigtable compared to traditional table oriented db's.  Different column-familes (data-types)
  can share a key.  Think of these as a group of tables in sql that you would 
  have a foreign-key shared.