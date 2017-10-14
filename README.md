
##  Sql Query Proxy to Elasticsearch, Mongo, Kubernetes, BigTable, etc.

Unify disparate data sources and files into a single Federated
view of your data and query with SQL without copying into datawarehouse.


Mysql compatible federated query engine to Elasticsearch, Mongo, 
Google Datastore, Cassandra, Google BigTable, Kubernetes, file-based sources.
This query engine hosts a mysql protocol listener, 
which rewrites sql queries to native (elasticsearch, mongo, cassandra, kuberntes-rest-api, bigtable).
It works by implementing a full relational algebra distributed execution engine
to run sql queries and poly-fill missing features
from underlying sources.  So, a backend key-value storage such as cassandra
can now have complete `WHERE` clause support as well as aggregate functions etc.

Most similar to [prestodb](http://prestodb.io/) but in Golang, and focused on
easy to add custom data sources as well as REST api sources.

## Storage Sources

* [Google Big Table](https://github.com/dataux/dataux/tree/master/backends/bigtable) SQL against big-table [Bigtable](https://cloud.google.com/bigtable/).
* [Elasticsearch](https://github.com/dataux/dataux/tree/master/backends/elasticsearch) Simplify access to Elasticsearch.
* [Mongo](https://github.com/dataux/dataux/tree/master/backends/mongo) Translate SQL into mongo.
* [Google Cloud Storage / (csv, json files)](https://github.com/dataux/dataux/tree/master/backends/files) An example of REST api backends (list of files), as well as the file contents themselves are tables.
* [Cassandra](https://github.com/dataux/dataux/tree/master/backends/cassandra) SQL against cassandra.  Adds sql features that are missing.
* [Lytics](https://github.com/dataux/dataux/tree/master/backends/lytics) SQL against [Lytics REST Api's](https://www.getlytics.com)
* [Kubernetes](https://github.com/dataux/dataux/tree/master/backends/_kube) An example of REST api backend.
* [Google Big Query](https://github.com/dataux/dataux/tree/master/backends/bigquery) MYSQL against worlds best analytics datawarehouse [BigQuery](https://cloud.google.com/bigquery/).
* [Google Datastore](https://github.com/dataux/dataux/tree/master/backends/datastore) MYSQL against [Datastore](https://cloud.google.com/datastore/).


## Features

* *Distributed*  run queries across multiple servers
* *Hackable Sources*  Very easy to add a new Source for your custom data, files, json, csv, storage.
* *Hackable Functions* Add custom go functions to extend the sql language.
* *Joins* Get join functionality between heterogeneous sources.
* *Frontends* currently only MySql protocol is supported but RethinkDB (for real-time api) is planned, and are pluggable.
* *Backends*  Elasticsearch, Google-Datastore, Mongo, Cassandra, BigTable, Kubernetes currently implemented.  Csv, Json files, and custom formats (protobuf) are in progress.

## Status
* NOT Production ready.  Currently supporting a few non-critical use-cases (ad-hoc queries, support tool) in production.


## Try it Out
This example imports a couple hours worth of historical data
from  https://www.githubarchive.org/ into a local
elasticsearch server for example.  Requires etcd server running local as well. 
Docker setup coming soon.
```sh

# assuming you are running local, if you are instead in Google Cloud, or Google Container Engine
# you don't need the credentials or volume mount
docker run -e "GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json" \
  -e "LOGGING=debug" \
  --rm -it \
  -p 4000:4000 \
  -v ~/.config/gcloud:/.config/gcloud \
  gcr.io/dataux-io/dataux:latest

# now that dataux is running use mysql-client to connect
mysql -h 127.0.0.1 -P 4000
```
now run some queries
```sql
-- we have a default CSV dataset that we have copied to the local
-- docker drive from http://seanlahman.com/baseball-archive/statistics/ 

show databases;

use baseball;

show tables;

describe appearances

select count(*) from appearances;

select * from appearances limit 10;


-- Lets add a bigquery datasource
CREATE source `datauxtest` WITH {
    "type":"bigquery",
    "schema":"bqsf_bikes",
    "table_aliases" : {
       "bikeshare_stations" : "bigquery-public-data:san_francisco.bikeshare_stations"
    },
    "settings" : {
      "billing_project" : "your-google-cloud-project",
      "data_project" : "bigquery-public-data",
      "dataset" : "san_francisco"
    }
};

use bqsf_bikes;

show tables;

describe film_locations;

select * from film_locations limit 10;

```

Roadmap(ish)
------------------------------
* Sources
  * Big-Query
  * Big-Table
  * Json-Files
* Writes
  * write pub/sub:  inbound insert/update are available as pub-sub messages.
  * write propogation:  inbound insert/update gets written multiple places.
  * write lambda functions:  allow arbitray functions to get nats.io pub/sub of write events.






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
* [dockersql](https://github.com/crosbymichael/dockersql), [q -python](http://harelba.github.io/q/), [textql](https://github.com/dinedal/textql),[GitQL/GitQL](https://github.com/gitql/gitql), [GitQL](https://github.com/cloudson/gitql)


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
