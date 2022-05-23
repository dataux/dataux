##  SQL Query Proxy to Elasticsearch, MongoDB, Kubernetes, Bigtable, etc.

Unify disparate data sources and files into a single Federated
view of your data and query with SQL without copying into datawarehouse.


MySQL compatible federated query engine to Elasticsearch, MongoDB,
Google Datastore, Cassandra, Google BigTable, Kubernetes, file-based sources.
This query engine hosts a MySQL protocol listener, which rewrites SQL queries
to native (Elasticsearch, MongoDB, Cassandra, kuberntes-rest-api, Bigtable).
It works by implementing a full relational algebra distributed execution engine
to run SQL queries and poly-fill missing features from underlying sources.
So, a backend key-value storage such as Cassandra can now have complete `WHERE`
clause support as well as aggregate functions etc.

Most similar to [Presto](http://prestodb.io/) but in Golang, and focused on
easy to add custom data sources as well as REST API sources.

## Storage Sources

* [Google Bigtable](https://github.com/dataux/dataux/tree/master/backends/bigtable) SQL against big-table [Bigtable](https://cloud.google.com/bigtable/).
* [Elasticsearch](https://github.com/dataux/dataux/tree/master/backends/elasticsearch) Simplify access to Elasticsearch.
* [MongoDB](https://github.com/dataux/dataux/tree/master/backends/mongo) Translate SQL into MongoDB.
* [Google Cloud Storage / (CSV, JSON files)](https://github.com/dataux/dataux/tree/master/backends/files) An example of REST API backends (list of files), as well as the file contents themselves are tables.
* [Cassandra](https://github.com/dataux/dataux/tree/master/backends/cassandra) SQL against cassandra. Adds SQL features that are missing.
* [Lytics](https://github.com/dataux/dataux/tree/master/backends/lytics) SQL against [Lytics REST API's](https://www.getlytics.com)
* [Kubernetes](https://github.com/dataux/dataux/tree/master/backends/_kube) An example of REST API backend.
* [Google BigQuery](https://github.com/dataux/dataux/tree/master/backends/bigquery) MySQL against worlds best analytics datawarehouse [BigQuery](https://cloud.google.com/bigquery/).
* [Google Datastore](https://github.com/dataux/dataux/tree/master/backends/datastore) MySQL against [Datastore](https://cloud.google.com/datastore/).


## Features

* *Distributed*  Run queries across multiple servers.
* *Hackable Sources*  Very easy to add a new Source for your custom data, files, JSON, CSV, storage.
* *Hackable Functions* Add custom go functions to extend the SQL language.
* *Joins* Get join functionality between heterogeneous sources.
* *Frontends* currently only MySQL protocol is supported but RethinkDB (for real-time API) is planned, and are pluggable.
* *Backends*  Elasticsearch, Google Datastore, MongoDB, Cassandra, Bigtable, Kubernetes currently implemented. CSV, JSON files, and custom formats (protobuf) are in progress.

## Status
* NOT Production ready. Currently supporting a few non-critical use-cases (ad-hoc queries, support tool) in production.


## Try it Out
These examples are:
1. We are going to create a CSV `database` of Baseball data from http://seanlahman.com/baseball-archive/statistics/
2. Connect to Google BigQuery public datasets (you will need a project, but the free quota will probably keep it free).


```sh
# download files to local /tmp
mkdir -p /tmp/baseball
cd /tmp/baseball
curl -Ls http://seanlahman.com/files/database/baseballdatabank-2017.1.zip > bball.zip
unzip bball.zip

mv baseball*/core/*.csv .
rm bball.zip
rm -rf baseballdatabank-*

# run a docker container locally
docker run -e "LOGGING=debug" --rm -it -p 4000:4000 \
  -v /tmp/baseball:/tmp/baseball \
  gcr.io/dataux-io/dataux:latest
```

In another Console open MySQL:
```sql
# connect to the docker container you just started
mysql -h 127.0.0.1 -P4000


-- Now create a new Source
CREATE source baseball WITH {
  "type":"cloudstore", 
  "schema":"baseball", 
  "settings" : {
     "type": "localfs",
     "format": "csv",
     "path": "baseball/",
     "localpath": "/tmp"
  }
};

show databases;

use baseball;

show tables;

describe appearances;

select count(*) from appearances;

select * from appearances limit 10;
```

BigQuery Example
----------------

```sh
# assuming you are running local, if you are instead in Google Cloud or Google Container Engine
# you don't need any credentials or volume mount
docker run --rm -it \
  -e "GOOGLE_APPLICATION_CREDENTIALS=/.config/gcloud/application_default_credentials.json" \
  -e "LOGGING=debug" \
  -p 4000:4000 \
  -v ~/.config/gcloud:/.config/gcloud \
  gcr.io/dataux-io/dataux:latest

# now that dataux is running, use mysql-client to connect
mysql -h 127.0.0.1 -P 4000
```
Now run some queries
```sql
-- add a bigquery datasource
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


**Hacking**

For now, the goal is to allow this to be used for library, so the 
`vendor` is not checked in. Use the Docker or `dep` for now.

```sh
# run dep ensure
dep ensure -v 
```

Related Projects, Database Proxies & Multi-Data QL
-------------------------------------------------------
* ***Data-Accessability*** Making it easier to query, access, share, and use data. Protocol shifting (for accessibility). Sharing/Replication between db types.
* ***Scalability/Sharding*** Implement sharding, connection sharing

Name | Scaling | Ease Of Access (SQL, etc) | Comments
---- | ------- | ----------------------------- | ---------
***[Vitess](https://github.com/youtube/vitess)***                          | Y |   | for scaling (sharding), very mature
***[twemproxy](https://github.com/twitter/twemproxy)***                    | Y |   | for scaling memcache
***[Couchbase N1QL](https://github.com/couchbaselabs/query)***             | Y | Y | SQL interface to couchbase k/v (and full-text-index)
***[prestodb](http://prestodb.io/)***                                      |   | Y | query front end to multiple backends, distributed
***[cratedb](https://crate.io/)***                                         | Y | Y | all-in-one db, not a proxy, sql to es
***[codis](https://github.com/wandoulabs/codis)***                         | Y |   | for scaling Redis
***[MariaDB MaxScale](https://github.com/mariadb-corporation/MaxScale)***  | Y |   | for scaling MySQL/MariaDB (sharding) mature
***[Netflix Dynomite](https://github.com/Netflix/dynomite)***              | Y |   | not really SQL, just multi-store k/v 
***[redishappy](https://github.com/mdevilliers/redishappy)***              | Y |   | for scaling redis, haproxy
***[mixer](https://github.com/siddontang/mixer)***                         | Y |   | simple MySQL sharding 

We use more and more databases, flatfiles, message queues, etc.
For db's the primary reader/writer is fine but secondary readers 
such as investigating ad-hoc issues means we might be accessing 
and learning many different query languages.

Credit to [mixer](https://github.com/siddontang/mixer), derived MySQL connection pieces from it (which was forked from Vitess).

Inspiration/Other works
-----------------------
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


Building
--------------------------
I plan on getting the `vendor` getting checked in soon so the build will work. However,
I am currently trying to figure out how to organize packages to allow use as both a library
as well as a daemon (see how minimal [main.go](./main.go) is, to encourage your own builtins and datasources).

```sh
# for just docker

# ensure /vendor has correct versions
dep ensure -update 

# build binary
./.build

# build docker
docker build -t gcr.io/dataux-io/dataux:v0.15.1 .
```
