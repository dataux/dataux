


## v 0.12  February 2016

* Distributed Query runtime using Actors and Nats.io message passing.
  * Distributed Queries required new implementation of query planner
  * Utilize Metafora for Task Scheduler https://github.com/lytics/metafora
  * Utilize Grid for Distributed Work primitives https://github.com/lytics/grid:
    * actors
    * message passing/hashing/rings via Nats.io
