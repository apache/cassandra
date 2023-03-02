# SSTable API

[CEP-17](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-17%3A+SSTable+format+API) 
/ [CASSANDRA-17056](https://issues.apache.org/jira/browse/CASSANDRA-17056)

## Configuration specification

SSTable formats and options are specified under the `sstable_formats` key in the _cassandra.yaml_ configuration file.
By default, the first sstable format implementation specified is the default one, which means that newly produced
sstables will use that implementation. Other implementations are used to read the sstables.

```yaml
sstable_formats:
  - class_name: 〈class name of the default SSTableFormat implementation〉
    parameters:
      id: 〈unique integer identifier of the implementation〉
      name: 〈unique string identifier of the implementation〉
      param1: 〈format specific parameter 1〉
      param2: 〈format specific parameter 2〉
      # ...
  - class_name: 〈class name of additional SSTableFormat implementation〉
    parameters:
      id: 〈unique integer identifier of the implementation〉
      name: 〈unique string identifier of the implementation〉
      param1: 〈format specific parameter 1〉
      param2: 〈format specific parameter 2〉
      # ...
  # ...      
```

Each sstable format definition includes the class name of the class implementing [`SSTableFormat`](format/SSTableFormat.java)
interface and a map of parameters. Two parameters are mandatory - _id_ and _name_, and they have to be unique across
all the defined formats. Additionally, they need to obey the following rules:
- _id_ must be an integer between 0 and 127
- _name_ must contain only lowercase ASCII letters

The _name_ parameter is the string put in the sstable file name. Therefore, it is the way Cassandra chooses the sstable 
format implementation when loading an sstable. The _id_ parameter is used in saved cache files. Both parameters should 
remain the same for the same format when upgrading. In particular, previous Cassandra versions have only 
[_big_ format](format/big/BigFormat.java) implementation, thus when upgrading from the old version, the configuration 
should keep _id_ set to _0_ and _name_ set to _big_ for that implementation (this is the default configuration), and any 
new implementations should use different values for those parameters.

The remaining parameters are optional and passed as a map to the `setup` method of a class implementing `SSTableFormat`.

Default configuration:
```yaml
sstable_formats:
  - class_name: org.apache.cassandra.io.sstable.format.big.BigFormat
    parameters:
      id: 0
      name: big
```

Example configuration which uses `BtiFormat` as the default one keeping the right parameters for the `BigFormat` 
implementation.
```yaml
sstable_formats:
  - class_name: org.apache.cassandra.io.sstable.format.bti.BtiFormat
    parameters:
      id: 1
      name: bti
  - class_name: org.apache.cassandra.io.sstable.format.big.BigFormat
    parameters:
      id: 0
      name: big
```

## Components

Each sstable consists of a set of components - required and optional. A component constitutes an identifier required 
to obtain the exact file with an sstable descriptor. Components are grouped by type. A type may define either 
a singleton component (for example, _stats_ component) or a non-singleton component (for example, _secondary index_ 
component).

A set of generic types of components that are thought of as common to all the sstable implementations is defined in the 
[`SSTableFormat.Components`](format/SSTableFormat.java) class. They include singleton types like `DATA`, 
`COMPRESSION_INFO`, `STATS`, `FILTER`, `DIGEST`, `CRC`, and `TOC`, which comes with predefined singleton component 
instances, as well as non-singleton types like `SECONDARY_INDEX` and `CUSTOM`.

Apart from the generic components, each sstable format implementation may describe its specific component types.
For example, the _big table_ format describes additionally `PRIMARY_INDEX` and `SUMMARY` singleton types and 
the corresponding singleton components (see [`BigFormat.Components`](format/big/BigFormat.java)).

Custom types can be created with one of the `Component.Type.create(name, repr, formatClass)`,
`Component.Type.createSingleton(name, repr, formatClass)` methods. Each created type is registered in a global types'
registry. Types registry is hierarchical which means that an sstable implementation may use types defined for its
format class and for all parent format classes (for example, the types defined for the `BigFormat` class extend the set
of types defined for the `SSTableFormat` interface).

For example, types defined for `BigFormat`:

```java
public static class Types extends SSTableFormat.Components.Types
{
    public static final Component.Type PRIMARY_INDEX = Component.Type.createSingleton("PRIMARY_INDEX", "Index.db", BigFormat.class);
    public static final Component.Type SUMMARY = Component.Type.createSingleton("SUMMARY", "Summary.db", BigFormat.class);
}
```

Singleton components are immediately associated with the singleton types and retrieved with the `<type>.getSingleton()` 
method:

```java
public static class Components extends AbstractSSTableFormat.Components
{
    public final static Component PRIMARY_INDEX = Types.PRIMARY_INDEX.getSingleton();
    public final static Component SUMMARY = Types.SUMMARY.getSingleton();
}
```

Non-singleton components are created explicitly as follows:

```java
Component idx1 = Types.SECONDARY_INDEX.createComponent("SI_idx1.db");
```

## Implementation

We strongly suggest the main format class to extend [`AbstractSSTableFormat`](format/AbstractSSTableFormat.java) because 
it includes the expected implementation of a couple of methods that should not be reimplemented differently.

### Initialization 

Cassandra either initializes the sstable format class as a singleton by calling its constructor or obtains the instance 
by accessing a static field called `instance` in the class. As a part of the initialization, Cassandra calls the `setup` 
method and provides the configuration parameters. Right after initialization, Cassandra calls the `allComponents` method 
to confirm all the components defined for the format are initialized and usable.

### Predefined sets of components

SSTable format defines a couple of collections of components. You should declare those collections as constant and 
immutable sets.

### Reader

#### Construction

An sstable reader ([`SSTableReader`](format/SSTableReader.java)) is responsible for reading the data from an sstable. 
It is created by a _simple builder_ ([`SSTableReader.Builder`](format/SSTableReader.java)) or a _loading 
builder_ ([`SSTableReaderLoadingBuilder`](format/SSTableReaderLoadingBuilder.java)). The builders are supplied by 
a _reader factory_ ([`SSTableFormat.SSTableReaderFactory`](format/SSTableFormat.java)).

The constructor of a particular `SSTableReader` implementation should accept two parameters - one is the format-specific
_simple builder_ and the other one is an sstable owner (usually a `ColumnFamilyStore` instance, but it can be null 
either). The constructor should be simple and not do anything but assign internal fields with values from the builder.

A simple builder does not perform any logic except basic validation - it only stores the provided values the reader 
constructor can access. A new reader implementation should include a public static simple builder inner class that 
extends the `SSTableReader.Builder` generic reader builder (or `SSTableReaderWithFilter.Builder`, see [below](#filter)).

In contrast to the simple builder, a loading builder can perform additional operations like more complex validation, 
opening resources, loading caches, indexes, filters, etc. It internally creates a simple builder and eventually 
instantiates a reader.

#### General notes

Note that if the builder carries some closeable resources to the reader, they should be returned by the `setupInstance` 
method.

You will find some `cloneXXX` methods to implement - remember to create a reader clone in a lambda passed to 
the `runWithLock()` method.

#### Unbuilding
It is convenient to implement the `unbuildTo` method, which takes a _simple builder_ and initializes it so that
the builder can produce the same reader. The method should also take the `sharedCopy` boolean argument denoting whether
it should copy the fields referencing closeable resources to the builder directly or as (shared) copies. The convention
also requires copying the resources only if they are unset in the builder (the field is null). The method should call
the `super.unbuildTo` method as a first step so that all the fields managed by the parent class are copied and in
the actual implementation only the fields specific to this format have to be assigned.

For example, the implementation of that method in a reader for the _big table_ format is as follows:

```java
protected final Builder unbuildTo(Builder builder, boolean sharedCopy)
{
    Builder b = super.unbuildTo(builder, sharedCopy);
    if (builder.getIndexFile() == null)
        b.setIndexFile(sharedCopy ? sharedCopyOrNull(ifile) : ifile);
    if (builder.getIndexSummary() == null)
        b.setIndexSummary(sharedCopy ? sharedCopyOrNull(indexSummary) : indexSummary);

    b.setKeyCache(keyCache);

    return b;
}
```

#### Filter

If the sstable includes a _filter_, the reader class should extend 
the [`SSTableReaderWithFilter`](format/SSTableReaderWithFilter.java) abstract reader (and its _simple builder_ should 
extend the `SSTableReaderWithFilter.SSTableReaderWithFilterBuilder` builder).

The reader with filter provides the `isPresentInFilter` method for extending implementation. It also implements other 
filter-specific methods the system relies on if the implemented reader extends that class.

Note that if the implemented reader extends the `SSTableReaderWithFilter` class, it should include the `FILTER` 
component in the appropriate component sets.

The reader with filter implementation comes with additional [metrics](filter/BloomFilterMetrics.java) - read more about custom
metrics support [here](#metrics).

#### Index summary

Some sstable format implementations, such as _big table_ format, may use _index summaries_. If a reader uses _index 
summaries_ it should implement the [`IndexSummarySupport`](indexsummary/IndexSummarySupport.java) interface. 

The support for _index summaries_ comes with additional [metrics](indexsummary/IndexSummaryMetrics.java) - read more 
about custom metrics support [here](#metrics).

#### Key cache

If an sstable format implementation uses row key cache, it should implement 
the[`KeyCacheSupport`](keycache/KeyCacheSupport.java) interface. In particular, it should store a `KeyCache` instance 
and return it with the `getKeyCache()` method. The interface has the default implementations of several methods 
the system relies on if the reader implements the `KeyCacheSupport` interface.

The interface comes with additional [metrics](keycache/KeyCacheMetrics.java) - read more about custom metrics support 
[here](#metrics).

#### Metrics

A custom sstable format implementation may provide additional metrics on a table, keyspace, and global level. Those 
metrics are accessible via JMX. The `SSTableFormat` implementation exposes the additional metrics by implementing the
`SSTableFormat.getFormatSpecificMetricsProviders` method. The method should return a singleton object implementing the
[`MetricsProviders`](MetricsProviders.java) interface. Currently, there is only support for custom gauges, but it can be
extended when needed.

Each custom metric (gauge) is an implementation of the [GaugeProvider](GaugeProvider.java) abstract class. Although the
class expects the implementation to provide a gauge for each level of aggregation, there is a helper class -
[SimpleGaugeProvider](SimpleGaugeProvider.java) - which does that automatically with a supplied reduction lambda. There 
is [`AbstractMetricsProviders`](AbstractMetricsProviders.java) class which is a partial implementation of the
`MetricsProviders` interface and leverages `SimpleGaugeProvider` in the offered methods.

Example - additional metrics for sstables supporting index summaries (see 
[`IndexSummaryMetrics`](indexsummary/IndexSummaryMetrics.java) for a full example):
```java
private final GaugeProvider<Long> indexSummaryOffHeapMemoryUsed = newGaugeProvider("IndexSummaryOffHeapMemoryUsed",
                                                                                   0L,
                                                                                   r -> r.getIndexSummary().getOffHeapSize(),
                                                                                   Long::sum);
```

### Writer

#### Construction

An sstable writer ([`SSTableWriter`](format/SSTableWriter.java)) is responsible for writing the data to sstable files.
It is created by a _builder_ ([`SSTableWriter.Builder`](format/SSTableWriter.java)). The builder is supplied by
a _writer factory_ ([`SSTableFormat.SSTableWriterFactory`](format/SSTableFormat.java)).

#### SortedTableWriter

There are not many methods to be implemented in the writer. The most notable one is `append` which should write 
the provided partition to disk. However, there is a generic default implementation 
[`SortedTableWriter`](format/SortedTableWriter.java) which handles things like writing to the data file using 
the default serializers, generic support for a partition index, notifications, metadata collection, and building 
a filter. The writer triggers fine-grained events when data are added and those methods can be overridden in 
the subclasses to apply specific behaviours (for example, `onPartitionStart`, `onRow`, `onStaticRow`, etc.).
Eventually it calls an abstract `createRowIndexEntry` method which should be implemented in the subclass. 

### Scrubber and verifier

A custom sstable format should also come with its own verifier and scrubber implementing [`IVerifier`](IVerifier.java)
and [`IScrubber`](IScrubber.java) interfaces correspondingly. A generic partial implementation is provided in 
[`SortedTableVerifier`](format/SortedTableVerifier.java) and [`SortedTableScrubber`](format/SortedTableScrubber.java).
