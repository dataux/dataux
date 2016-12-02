
##  Sql Query Proxy to Elasticsearch, Mongo, Kuberntes, BigTable, etc.

Unify disparate data sources, files into a single Virtual or Federated
view of your data. 


Mysql compatible federated query engine to Elasticsearch, Mongo, 
Google Datastore, Cassandra, Google BigTable, Kuberntes, File-based backend sources, including join.
This query engine hosts a mysql protocol listener, 
which then rewrites sql queries to native (elasticsearch, mongo, cassandra, kuberntes-rest-api, bigtable).
It works by implementing a full relational algebra layer 
to run sql queries and poly-fill missing features
from underlying sources.  So, a backend key-value storage such as cassandra
can now have complete `WHERE` clause support as well as aggregate functions etc.

Most similar to [prestodb](http://prestodb.io/) but in Golang, and focused on
easy to add custom data sources.


## Features

* *Distributed*  run queries across multiple servers
* *Hackable Sources*  Very easy to add a new Source for your custom data, files, json, csv, storage.
* *Hackable Functions* Add custom go functions to extend the sql language.
* *Joins* Get join functionality between heterogeneous sources.
* *Frontends* currently only MySql protocol is supported but RethinkDB (for real-time api) is planned, and are pluggable.
* *Backends*  Elasticsearch, Google-Datastore, Mongo, Cassandra, BigTable, Kubernetes currently implemented.  Csv, Json files, and custom formats (protobuf) are in progress.

## Status
* NOT Production ready.  Currently supporting a few non-critical use-cases (ad-hoc queries, support tool) in production.


## Backends

* [Kubernetes](https://github.com/dataux/dataux/tree/master/backends/kubernetes) An example of REST api backend.
* [Big Table](https://github.com/dataux/dataux/tree/master/backends/bigtable) SQL against big-table.
* [Elasticsearch](https://github.com/dataux/dataux/tree/master/backends/elasticseearch) Simplify access to Elasticsearch.
* [Mongo](https://github.com/dataux/dataux/tree/master/backends/mongo) Translate SQL into mongo.
* [Google Cloud Storage / (csv, json files)](https://github.com/dataux/dataux/tree/master/backends/files) An example of REST api backends (list of files), as well as the file contents themselves are tables.
* [Cassandra](https://github.com/dataux/dataux/tree/master/backends/cassandra) SQL against cassandra.  Adds sql features that are missing.


## Try it Out
This example imports a couple hours worth of historical data
from  https://www.githubarchive.org/ into a local
elasticsearch server for example.  Requires [nats.io](http://nats.io)
and etcd server running local as well. Docker setup coming soon.
```sh

cd tools/importgithub
# assuming elasticsearch on localhost elase --host=myeshost
go build && ./importgithub

# using dataux.conf from root of this project
go build
./dataux --config=dataux.conf

# now that dataux is running use mysql-client to connect

mysql -h 127.0.0.1 -P 4000
```
now run some queries
```sql
use datauxtest;

show tables;

describe github_watch;

select cardinality(`actor`) AS users_who_watched, min(`repository.id`) as oldest_repo from github_watch;

SELECT actor, `repository.name`, `repository.stargazers_count`, `repository.language`
FROM github_watch where `repository.language` = "Go";

select actor, repository.name from github_watch where repository.stargazers_count BETWEEN "1000" AND 1100;

SELECT actor, repository.organization AS org
FROM github_watch 
WHERE repository.created_at BETWEEN "2008-10-21T17:20:37Z" AND "2008-10-21T19:20:37Z";

select actor, repository.name from github_watch where repository.name IN ("node", "docker","d3","myicons", "bootstrap") limit 100;

select cardinality(`actor`) AS users_who_watched, count(*) as ct, min(`repository.id`) as oldest_repo
FROM github_watch
WHERE repository.description LIKE "database";

# to add other data sources see dataux.conf example

```

Roadmap(ish)
------------------------------
* Sources
  * Cassandra
  * Big-Query
  * Big-Table
  * Json-Files
* Writes
  * write propogation:  inbound insert/update gets written multiple places.
  * write lambda functions:  allow arbitray functions to get nats.io pub/sub of write events.



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

SQL -> Elasticsearch
----------------------------------

ES API | SQL Query  
----- | -------
Aliases                 | `show tables;`
Mapping                 | `describe mytable;`
hits.total  for filter  | `select count(*) from table WHERE exists(a);`
aggs min, max, avg, sum | `select min(year), max(year), avg(year), sum(year) from table WHERE exists(a);`
filter:   terms         | `select * from table WHERE year IN (2015,2014,2013);`
filter: gte, range      | `select * from table WHERE year BETWEEN 2012 AND 2014`




**Hacking**

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
***[Vitess](https://github.com/youtube/vitess)***                          | Y |   | for scaling (sharding), very mature
***[twemproxy](https://github.com/twitter/twemproxy)***                    | Y |   | for scaling memcache
***[Couchbase N1QL](https://github.com/couchbaselabs/query)***             | Y | Y | sql interface to couchbase k/v (and full-text-index)
***[prestodb](http://prestodb.io/)***                                      |   | Y | query front end to multiple backends, distributed
***[cratedb](https://crate.io/)***                                         | Y | Y | all-in-one db, not a proxy, sql to es
***[codis](https://github.com/wandoulabs/codis)***                         | Y |   | for scaling redis
***[MariaDB MaxScale](https://github.com/mariadb-corporation/MaxScale)***  | Y |   | for scaling mysql/mariadb (sharding) mature
***[Netflix Dynomite](https://github.com/Netflix/dynomite)***              | Y |   | not really sql, just multi-store k/v 
***[redishappy](https://github.com/mdevilliers/redishappy)***              | Y |   | for scaling redis, haproxy
***[mixer](https://github.com/siddontang/mixer)***                         | Y |   | simple mysql sharding 

We use more and more databases, flatfiles, message queues, etc.
For db's the primary reader/writer is fine but secondary readers 
such as investigating ad-hoc issues means we might be accessing 
and learning many different query languages.  

Credit to [mixer](https://github.com/siddontang/mixer), derived mysql connection pieces from it (which was forked from vitess).

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
