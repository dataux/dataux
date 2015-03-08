
DataUX a query proxy
---------------------------------
Mysql compatible querying of elasticsearch.  Making data more usable by
opening SQL access patterns to data.


Elasticsearch QL -> ES Api
----------------------------------

ES API | SQL Query  
----- | -------
Aliases                 | `show tables;`
Mapping                 | `describe mytable;`
hits.total              | `select count(*) from table WHERE exists(a);`
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
***[GitQL](https://github.com/cloudson/gitql)***                        |   | Y | SQL to http api 
***[cratedb](https://crate.io/docs/current/sql/index.html)***           | Y | Y | all-in-one db, not a proxy, sql to es
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

This is a tool to facilitate non-primary ad-hoc investigation 
of elasticsearch, redis, csv, etc via a proxy.   
Used small bits of a forked [mixer](https://github.com/siddontang/mixer), the mysql connection pieces.

Roadmap(ish)
------------------------------
* ***Elasticsearch***  Make elasticsearch more accessible through SQL
* ***Elasticsearch BQL*** Extend SQL with search specfiic syntax
* Mongo, Redis, CSV, Json-FlatFiles, Kafka Backends
* Cross-DB Join
* Event-Bus for Writes

Inspiration/Other works
--------------------------
* https://github.com/linkedin/databus
* [ql.io](http://ql.io/), [yql](https://developer.yahoo.com/yql/)
* [dockersql](https://github.com/crosbymichael/dockersql), [q -python](http://harelba.github.io/q/), [textql](https://github.com/dinedal/textql)


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
