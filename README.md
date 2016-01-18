
Sql Query Proxy to Elasticsearch, Mongo, Etc
----------------------------------------------
Mysql tcp proxy to Elasticsearch, Mongo, Mysql backend sources, including join.

This is an early prototype, not production ready.  It is wire compatible with
Mysql by implementing a relational algebra engine to map the sql queries to one or more
backends.

**Why?** An experiement to see if it is possible, as more and more databases for more and more
specialized needs seems to be the norm and this is an attempt to translate back into a
cohesive data model.

SQL -> Mongo
----------------------------------

Mongo | SQL Query  
----- | -------
`show collections`                     | `show tables;`
na, -- runtime inspection                  | `describe mytable;`
`db.accounts.find({},{created:{"$gte":"1/1/2016"}}).count();`          | `select count(*) from accounts WHERE created > "1/1/2016";`
 | `select min(year), max(year), avg(year), sum(year) from table WHERE exists(a);`
          | `select * from table WHERE year IN (2015,2014,2013);`
       | `select * from table WHERE year BETWEEN 2012 AND 2014`


SQL -> Elasticsearch Api
----------------------------------

ES API | SQL Query  
----- | -------
Aliases                 | `show tables;`
Mapping                 | `describe mytable;`
hits.total  for filter  | `select count(*) from table WHERE exists(a);`
aggs min, max, avg, sum | `select min(year), max(year), avg(year), sum(year) from table WHERE exists(a);`
filter:   terms         | `select * from table WHERE year IN (2015,2014,2013);`
filter: gte, range      | `select * from table WHERE year BETWEEN 2012 AND 2014`


* see **tools/importgithub** for tool to import 2 days of github data for examples above.

```sh
# to run queries below, run test data import

go get -u github.com/dataux/dataux
cd $GOPATH/src/github.com/dataux/dataux/tools/importgithub
go build
./importgithub  ## will import ~200k plus docs from Github archive

```

Other Projects, Database Proxies & Multi-Data QL
-------------------------------------------------------
* ***Data-Accessability*** Making it easier to query, access, share, and use data.   Protocol shifting (for accessibility).  Sharing/Replication between db types.
* ***Scalability/Sharding*** Implement sharding, connection sharing

Name | Scaling | Ease Of Access (sql, etc) | Comments
---- | ------- | ----------------------------- | ---------
***[Couchbase N1QL](https://github.com/couchbaselabs/query)***          | Y | Y | sql interface to couchbase k/v (and full-text-index)
***[prestodb](http://prestodb.io/)***                                   |   | Y | not really a proxy more of query front end
***[cratedb](https://crate.io/)***                                      | Y | Y | all-in-one db, not a proxy, sql to es
***[Vitess](https://github.com/youtube/vitess)***                       | Y |   | for scaling (sharding), very mature
***[twemproxy](https://github.com/twitter/twemproxy)***                 | Y |   | for scaling memcache
***[codis](https://github.com/wandoulabs/codis)***                      | Y |   | for scaling redis
***[MariaDB MaxScale](https://github.com/mariadb-corporation/MaxScale)***  | Y |   | for scaling mysql/mariadb (sharding) mature
***[Netflix Dynomite](https://github.com/Netflix/dynomite)***           | Y |   | not really sql, just multi-store k/v 
***[redishappy](https://github.com/mdevilliers/redishappy)***           | Y |   | for scaling redis, haproxy
***[mixer](https://github.com/siddontang/mixer)***                      | Y |   | simple mysql sharding 

We use more and more databases, flatfiles, message queues, etc.
For db's the primary reader/writer is fine but secondary readers 
such as investigating ad-hoc issues means we might be accessing 
and learning many different query languages.  

Credit to [mixer](https://github.com/siddontang/mixer), derived mysql connection pieces from it (which was forked from vitess).

Roadmap(ish)
------------------------------
* ***Elasticsearch***  Make elasticsearch more accessible through SQL
* ***Mongo*** Extend SQL with search specfiic syntax
* [ ] **Backends**: Redis, CSV, Json-FlatFiles, Kafka
* Cross-DB Join
* Event-Bus for Writes

Inspiration/Other works
--------------------------
* https://github.com/linkedin/databus, 
* [ql.io](http://www.ebaytechblog.com/2011/11/30/announcing-ql-io/), [yql](https://developer.yahoo.com/yql/)
* [dockersql](https://github.com/crosbymichael/dockersql), [q -python](http://harelba.github.io/q/), [textql](https://github.com/dinedal/textql), [GitQL](https://github.com/cloudson/gitql)


> In Internet architectures, data systems are typically categorized
> into source-of-truth systems that serve as primary stores 
> for the user-generated writes, and derived data stores or 
> indexes which serve reads and other complex queries. The data 
> in these secondary stores is often derived from the primary data 
> through custom transformations, sometimes involving complex processing 
> driven by business logic. Similarly data in caching tiers is derived 
> from reads against the primary data store, but needs to get 
> invalidated or refreshed when the primary data gets mutated. 
> A fundamental requirement emerging from these kinds of data 
> architectures is the need to reliably capture, 
> flow and process primary data changes.

from [Databus](https://github.com/linkedin/databus)
