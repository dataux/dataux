
* ???   Kubernetes source https://github.com/dataux/dataux/pull/31
* 11/27/16 BigTable Data Source https://github.com/dataux/dataux/pull/30
* 11/10/16 upgrade grid to newer version, rework the gnats, etcd apis
* 9/8/16 New balancer to schedule work more evently across nodes https://github.com/dataux/dataux/pull/26
* 8/28/16 massive schema improvements https://github.com/dataux/dataux/pull/25
* 8/14/16 Cassandra Data Source https://github.com/dataux/dataux/pull/21
* 6/27/16 Update data-types to ensure it works with metabase, first real use-case https://github.com/dataux/dataux/pull/17
* 04/27/16 Distributed runtime https://github.com/dataux/dataux/pull/16

## v 0.13  March 27th 2016

* Cloud Based Files datasource (csv, json, etc) uses distributed query engine.  Use Google Storage
   files as datasource.  Includes pluggable scanner-parser, so custom file formats can easily
   have an adapter added.
* improve the distributed execution engine, with cleaner start, stop, finish mechanics including
  better flushing, ensuring each task is drained appropriately.
* **QLBridge Improvements**
  *Better internal Schema Query planning, system (SHOW, DESCRIBE) with schemadb https://github.com/araddon/qlbridge/pull/68
    * introspect csv files for types
    * convert `SHOW`, `DESCRIBE` into `SELECT` statements
    * better internal data-source registry

## v 0.12  February 2016

* Distributed Query runtime using Actors and Nats.io message passing.
  * Distributed Queries required new implementation of query planner
  * Utilize Metafora for Task Scheduler https://github.com/lytics/metafora
  * Utilize Grid for Distributed Work primitives https://github.com/lytics/grid:
    * actors
    * message passing/hashing/rings via Nats.io
