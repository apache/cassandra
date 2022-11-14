# Memtable API

[CEP-11](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-11%3A+Pluggable+memtable+implementations) 
/ [CASSANDRA-17034](https://issues.apache.org/jira/browse/CASSANDRA-17034)

## Configuration specification

Memtable types and options are specified using memtable "configurations", which specify an implementation class
and its parameters. 

The memtable configurations are defined in `cassandra.yaml`, using the following format:

```yaml
memtable:
    configurations:
        〈configuration name〉:
          class_name: 〈class〉
          inherits: 〈configuration name〉
          parameters:
            〈parameters〉
```

Configurations can copy the properties from others, including being full copies of another, which can be useful for
easily remapping one name to another configuration.

The default memtable configuration is named `default`. It can be overridden if the yaml specifies it (including
using inheritance to copy another configuration), and it can be inherited, even if it is not explicitly defined in
the yaml (e.g. to change some parameter but not the memtable class).

Examples:

```yaml
memtable:
    configurations:
        more_shards:
          inherits: default
          parameters:
             shards: 32
```

```yaml
memtable:
    configurations:
        skiplist:
          class_name: SkipListMemtable
        sharded:
          class_name: ShardedSkipListMemtable
        trie:
          class_name: TrieMemtable
        default:
          inherits: trie
```

Note that the database will only validate the memtable class and its parameters when a configuration needs to be
instantiated for a table.

## Implementations provided

Cassandra currently comes with three memtable implementations:

- `SkipListMemtable` is the default and matches the memtable format of Cassandra versions up to 4.1. It organizes
  partitions into a single concurrent skip list.
- `ShardedSkipListMemtable` splits the partition skip-list into several independent skip-lists each covering a roughly
  equal part of the token space served by this node. This reduces congestion of the skip-list from concurrent writes and
  can lead to improved write throughput. Its configuration takes two parameters:
  - `shards`: the number of shards to split into, defaulting to the number of CPU cores on the machine.
  - `serialize_writes`: if false (default), each shard may serve multiple writes in parallel; if true, writes to each
    shard are synchronized.
- `TrieMemtable` is a novel solution that organizes partitions into an in-memory trie which places the partition
  indexing structure in a buffer, off-heap if desired, which significantly improves garbage collection efficiency. It
  also improves the memtable's space efficiency and lookup performance. Its configuration can take a single parameter
  `shards` as above.

## Memtable selection

Once a configuration has been defined, it can be used by specifying it in the `memtable` parameter of a `CREATE TABLE`
or `ALTER TABLE` statement, for example:

```
CREATE TABLE ... WITH ... AND memtable = 'trie';
```
or
```
ALTER TABLE ... WITH memtable = 'skiplist';
```

If a memtable is not specified, the configuration `default` will be used. To reset a table to the default memtable,
use
```
ALTER TABLE ... WITH memtable = 'default';
```

The memtable configuration selection is per table, i.e. it will be propagated to all nodes in the cluster. If some nodes
do not have a definition for that configuration or cannot instantiate the class, they will log an error and fall 
back to the default memtable configuration to avoid schema disagreements. However, if some nodes are still on a version 
of Cassandra before 4.1, they will reject the schema change. We therefore recommend using a separate `ALTER` statement 
to change a table's memtable implementation; upgrading all nodes to 4.1 or later is required to use the API.

As additional safety when first deploying an alternative implementation to a production cluster, one may consider
first deploying a remapped `default` configuration to all nodes in the cluster, switching the schema to reference
it, and then changing the implementation by modifying the configuration one node at a time.

For example, a remapped default can be specified with this:
```yaml
memtable:
    configurations:
        better_memtable:
            inherits: default
```
selected via
```
ALTER TABLE production_table WITH memtable = 'better_memtable';
```
and later switched one node at a time to
```yaml
memtable:
    configurations:
        our_memtable:
            class_name: ...
        better_memtable:
            inherits: our_memtable
```

## Memtable implementation

A memtable implementation is an implementation of the `Memtable` interface. The main work of the class will be
performed by the `put`, `rowIterator` and `partitionIterator` methods, used to write and read information to/from the
memtable. In addition to this, the implementation must support retrieval of the content in a form suitable for flushing 
(via `getFlushSet`), memory use and statistics tracking, mechanics for triggering a flush for reasons
controlled by the memtable (e.g. exhaustion of the given memory allocation), and finally mechanisms for tracking the
commit log spans covered by a memtable.

Abstract classes that provide the latter parts of the functionality (expected to be shared by most
implementations) are provided as the `AbstractMemtable` (statistics tracking), `AbstractMemtableWithCommitlog` (adds
commit log span tracking) and `AbstractAllocatorMemtable` (adds memory management via the `Allocator` class, together
with flush triggering on memory use and time interval expiration).

The memtable API also gives the memtable some control over flushing and the functioning of the commit log. The former
is there to permit memtables that operate long-term and/or can handle some events internally, without a need to flush.
The latter enables memtables that have an internal durability mechanism, such as ones using persistent memory or a
tightly integrated commit log (e.g. using the commit log buffers for memtable data storage).

The memtable implementation must also provide a mechanism for memtable construction called a memtable "factory" 
(the `Memtable.Factory` interface). Some features of the implementation may be needed before an instance is created or
where the memtable instance is not accessible. To make working with them more straightforward, the following 
memtable-controlled options are implemented on the factory:

- `boolean writesAreDurable()` should return true if the memtable has its own persistence mechanism and does not want
  the commitlog to provide persistence. In this case the commit log can still store the writes for changed-data-capture (CDC)
  or point-in-time restore (PITR), but it need not keep them for replay until the memtable is flushed.
- `boolean writesShouldSkipCommitLog()` should return true if the memtable does not want the commit log to store any of
  its data. The expectation for this flag is that a persistent memtable will take a configuration parameter to turn this
  option on to improve performance. Enabling this flag is not compatible with CDC or PITR.
- `boolean streamToMemtable()` and `boolean streamFromMemtable()` should return true if the memtable is long-lived and 
  cannot flush to facilitate streaming. In this case the streaming code will implement the process in a way that 
  retrieves data in the memtable before sending, and applies received data in the memtable instead of directly creating 
  an sstable.

### Instantiation and configuration

The memtables are instantiated by the factory, which is constructed via reflection on creating a `ColumnFamilyStore` or
altering the table's configuration.

Memtable classes must either contain a static `FACTORY` field (if they take no arguments other than class), or implement 
a `factory(Map<String, String>)` method, which is called using the configuration `parameters`. For validation, the
latter should consume any further options (using `map.remove`).

The `MemtableParams` class will look for the specified class name (prefixed with `org.apache.cassandra.db.memtable.`
if only a short name was given), then look for a `factory` method. If it finds one, it will call it with a copy of the 
supplied parameters; if it does not, it will look for the `FACTORY` field and use its value if found. It will error out
if the class was not found, if neither the method or field was found, or if the user supplied parameters that did not 
get consumed.

Because multiple configurations and tables may use the same parameters, it is expected that the factory method will
store and reuse constructed factories to avoid wasting space for duplicate objects (this is typical for configuration 
objects in Cassandra).

At this time many of the configuration parameters for memtables are still configured using top-level parameters like
`memtable_allocation_type` in `cassandra.yaml` and `memtable_flush_period_in_ms` in the table schema.


### Sample implementation

The API comes with a proof-of-concept implementation, a sharded skip-list memtable implemented by the 
`ShardedSkipListMemtable` class. The difference between this and the default memtable is that the sharded version breaks 
the token space served by the node into roughly equal regions and uses a separate skip-list for each shard. Sharding
spreads the write concurrency among these independent skip lists, reducing congestion and can lead to significantly
improved write throughput.

This implementation takes two parameters, `shards` which specifies the number of shards to split into (by default, the
number of CPU threads available to the process) and `serialize_writes`, which, if set to `true` causes writes to the
memtable to be synchronized. The latter can be useful to minimize space and time wasted for unsuccesful lockless 
partition modification where a new copy of the partition would be prepared but not used due to concurrent modification.
Regardless of the setting, reads can always execute in parallel, including concurrently with writes.

Please note that sharding cannot be used with non-hashing partitioners (i.e. `ByteOrderPartitioner` or 
`OrderPreservingPartitioner`).