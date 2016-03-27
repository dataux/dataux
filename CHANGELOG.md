

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
