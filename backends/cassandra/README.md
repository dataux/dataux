



```
# start docker container for testing

docker run --name dataux-cass-test -d cassandra:2


# log onto container to get a cqlsh
docker run -it --rm --net container:lioenv_cass_1 cassandra:2 cqlsh

INSERT INTO user (id, name, deleted, created, updated) VALUES ('user814', 'test_name', false, '2016-07-24','2016-07-24 23:46:01')

cqlsh:datauxtest> select * from system.schema_columnfamilies WHERE keyspace_name = "datauxtest";
SyntaxException: <ErrorMessage code=2000 [Syntax error in CQL query] message="line 1:65 no viable alternative at input 'datauxtest' (...system.schema_columnfamilies WHERE keyspace_name = ["datauxtes]...)">

cqlsh:datauxtest> select * from system.schema_columnfamilies WHERE keyspace_name = 'datauxtest';


 keyspace_name | columnfamily_name | bloom_filter_fp_chance | caching                                     | cf_id                                | column_aliases | comment | compaction_strategy_class                                       | compaction_strategy_options | comparator                                                                                                                                                                                                                                         | compression_parameters                                                   | default_time_to_live | default_validator                         | dropped_columns | gc_grace_seconds | index_interval | is_dense | key_aliases    | key_validator                                                                                                                    | local_read_repair_chance | max_compaction_threshold | max_index_interval | memtable_flush_period_in_ms | min_compaction_threshold | min_index_interval | read_repair_chance | speculative_retry | subcomparator | type     | value_alias
---------------+-------------------+------------------------+---------------------------------------------+--------------------------------------+----------------+---------+-----------------------------------------------------------------+-----------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------+----------------------+-------------------------------------------+-----------------+------------------+----------------+----------+----------------+----------------------------------------------------------------------------------------------------------------------------------+--------------------------+--------------------------+--------------------+-----------------------------+--------------------------+--------------------+--------------------+-------------------+---------------+----------+-------------
    datauxtest |           article |                   0.01 | {"keys":"ALL", "rows_per_partition":"NONE"} | bfd51db0-4628-11e6-a329-4f04994f1f6a |             [] |         | org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy |                          {} | org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(63617465676f7279:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))) | {"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"} |                    0 | org.apache.cassandra.db.marshal.BytesType |            null |           864000 |           null |    False |     ["author"] |                                                                                         org.apache.cassandra.db.marshal.UTF8Type |                      0.1 |                       32 |               2048 |                           0 |                        4 |                128 |                  0 |    99.0PERCENTILE |          null | Standard |        null
    datauxtest |             event |                   0.01 | {"keys":"ALL", "rows_per_partition":"NONE"} | 5d461c00-46cb-11e6-a329-4f04994f1f6a |         ["ts"] |         | org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy |                          {} |                                                                                                              org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.TimestampType,org.apache.cassandra.db.marshal.UTF8Type) | {"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"} |                    0 | org.apache.cassandra.db.marshal.BytesType |            null |           864000 |           null |    False | ["date","url"] | org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type) |                      0.1 |                       32 |               2048 |                           0 |                        4 |                128 |                  0 |    99.0PERCENTILE |          null | Standard |        null
    datauxtest |              user |                   0.01 | {"keys":"ALL", "rows_per_partition":"NONE"} | bfe91ae0-4628-11e6-a329-4f04994f1f6a |             [] |         | org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy |                          {} |       org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(726f6c6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))) | {"sstable_compression":"org.apache.cassandra.io.compress.LZ4Compressor"} |                    0 | org.apache.cassandra.db.marshal.BytesType |            null |           864000 |           null |    False |         ["id"] |                                                                                         org.apache.cassandra.db.marshal.UTF8Type |                      0.1 |                       32 |               2048 |                           0 |                        4 |                128 |                  0 |    99.0PERCENTILE |          null | Standard |        null

select columnfamily_name AS cf, key_aliases from system.schema_columnfamilies WHERE keyspace_name = 'datauxtest';

-- Events Table
CREATE TABLE event (
  url varchar,
  ts timestamp,
  date text,
  jsondata text,
  PRIMARY KEY ((date, url), ts)
);

cqlsh:datauxtest> describe table datauxtest.event;

CREATE TABLE datauxtest.event (
    date text,
    url text,
    ts timestamp,
    jsondata text,
    PRIMARY KEY ((date, url), ts)
) WITH CLUSTERING ORDER BY (ts ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';



# this is from a cassandra 2.2.6
cqlsh:system> describe table system.schema_columnfamilies;

CREATE TABLE system.schema_columnfamilies (
    keyspace_name text,
    columnfamily_name text,
    bloom_filter_fp_chance double,
    caching text,
    cf_id uuid,
    comment text,
    compaction_strategy_class text,
    compaction_strategy_options text,
    comparator text,
    compression_parameters text,
    default_time_to_live int,
    default_validator text,
    dropped_columns map<text, bigint>,
    gc_grace_seconds int,
    is_dense boolean,
    key_validator text,
    local_read_repair_chance double,
    max_compaction_threshold int,
    max_index_interval int,
    memtable_flush_period_in_ms int,
    min_compaction_threshold int,
    min_index_interval int,
    read_repair_chance double,
    speculative_retry text,
    subcomparator text,
    type text,
    PRIMARY KEY (keyspace_name, columnfamily_name)
) WITH CLUSTERING ORDER BY (columnfamily_name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = 'table definitions'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 604800
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

# this is from cassandra 2.1.8

cqlsh:datauxtest> describe table system.schema_columnfamilies;

CREATE TABLE system.schema_columnfamilies (
    keyspace_name text,
    columnfamily_name text,
    bloom_filter_fp_chance double,
    caching text,
    cf_id uuid,
    column_aliases text,
    comment text,
    compaction_strategy_class text,
    compaction_strategy_options text,
    comparator text,
    compression_parameters text,
    default_time_to_live int,
    default_validator text,
    dropped_columns map<text, bigint>,
    gc_grace_seconds int,
    index_interval int,
    is_dense boolean,
    key_aliases text,
    key_validator text,
    local_read_repair_chance double,
    max_compaction_threshold int,
    max_index_interval int,
    memtable_flush_period_in_ms int,
    min_compaction_threshold int,
    min_index_interval int,
    read_repair_chance double,
    speculative_retry text,
    subcomparator text,
    type text,
    value_alias text,
    PRIMARY KEY (keyspace_name, columnfamily_name)
) WITH CLUSTERING ORDER BY (columnfamily_name ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = 'ColumnFamily definitions'
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 604800
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 3600000
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

```
