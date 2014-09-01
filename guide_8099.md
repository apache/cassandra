Overview of CASSANDRA-8099 changes
==================================

The goal of this document is to provide an overview of the main changes done in
CASSANDRA-8099 so it's easier to dive into the patch. This assumes knowledge of
the pre-existing code.

CASSANDRA-8099 refactors the abstractions used by the storage engine and as
such impact most of the code of said engine. The changes can be though of as
following two main guidelines:
1. the new abstractions are much more iterator-based (i.e. it tries harder to
   avoid materializing everything in memory),
2. and they are closer to the CQL representation of things (i.e. the storage
   engine is aware of more structure, making it able to optimize accordingly).
Note that while those changes have heavy impact on the actual code, the basic
mechanisms of the read and write paths are largely unchanged.

In the following, I'll start by describe the new abstractions introduced by the
patch. I'll then provide a quick reference of existing class to what it becomes
in the patch, after which I'll discuss how the refactor handles a number of
more specific points. Lastly, the patch introduces some change to the on-wire
and on-disk format so I'll discuss those quickly.


Main new abstractions
---------------------

### Atom: Row and RangeTombstoneMarker

Where the existing storage engine is mainly handling cells, the new engine
groups cells into rows, and rows becomes the more central building block. A
`Row` is identified by the value of it's clustering columns which are stored in
a `Clustering` object (see below), and it associate a number of cells to each
of its non-PK non-static columns (we'll discuss static columns more
specifically later).

The patch distinguishes 2 kind of columns: simple and complex ones. The
_simple_ columns can have only 1 cell associated to them (or none), while the
_complex_ ones will have an arbitrary number of cells associated.  Currently,
the complex columns are only the non frozen collections (but we'll have
non-frozen udt at some point and who knows what in the future).

Like before, we also have to deal with range tombstones. However, instead of
dealing with full range tombstones, we generally deal with
`RangeTombstoneMarker` which is just one of the bound of the range tombstone
(so that a range tombstone is composed of 2 "marker" in practice, its start and
its end). I'll discuss the reasoning for this a bit more later. A
`RangeTombstoneMarker` is identified by a `Slice.Bound` (which is to RT markers
what the `Clustering` is to `Row`) and simply store its deletion information.

The engine thus mainly work with rows and range tombstone markers, and they are
both grouped under the common `Atom` interface. An "unfiltered" is thus just that:
either a row or a range tombstone marker.

> Side Note: the "Atom" naming is pretty bad. I've reused it mainly because it
> plays a similar role to the existing OnDiskAtom, but it's arguably crappy now
> because a row is definitively not "indivisible". Anyway, renaming suggestions
> are more than welcome. The only alternative I've come up so far are "Block"
> or "Element" but I'm not entirely convinced by either.

### ClusteringPrefix, Clustering, Slice.Bound and ClusteringComparator

Atoms are sorted (within a partition). They are ordered by their
`ClusteringPrefix`, which is mainly a common interface for the `Clustering` of
`Row`, and the `Slice.Bound` of `RangeTombstoneMarker`. More generally, a
`ClusteringPrefix` is a prefix of the clustering values for the clustering
columns of the table involved, with a `Clustering` being the special case where
all values are provided. A `Slice.Bound` can be a true prefix however, having
only some of the clustering values. Further, a `Slice.Bound` can be either a
start or end bound, and it can be inclusive or exclusive. Sorting make sure that
a start bound is before anything it "selects" (and conversely for the end). A
`Slice` is then just 2 `Slice.Bound`: a start and a end, and selects anything
that sorts between those.

`ClusteringPrefix` are compared through the table `ClusteringComparator`, which
is like our existing table comparator except that it only include comparators
for the clustering column values. In particular, it includes neither the
comparator for the column names themselves, nor the post-column-name comparator
for collections (the latter being handled through the `CellPath`, see below).
There is a also a `Clusterable` interface that `Atom` implements and that
simply marks object that can be compared by a `ClusteringComparator`, i.e.
objects that have a `ClusteringPrefix`.

### Cell

A cell holds the informations on a single value. It corresponds to a (CQL)
column, has a value, a timestamp and optional ttl and local deletion time.
Further, as said above, complex columns (collections) will have multiple
associated cells. Those cells are distinguished by their `CellPath`, which are
compared through a comparator that depends on the column type. The cells of
simple columns just have a `null` cell path.

### AtomIterator and PartitionIterator

As often as possible, atoms are manipulated through iterators. And this through
`AtomIterator`, which is an iterator over the atoms of a single partition, and
`PartitionIterator`, which is an iterator of `AtomIterator`, i.e. an iterator
over multiple partitions. In other words a single partition query fundamentally
returns an `AtomIterator`, while a range query returns a `PartitionIterator`.
Those iterators are closeable and the code has to be make sure to always close
them as they will often have resources to clean (like an OpOrder.Group to close
or files to release).

The read path mainly consists in getting unfiltered and partition iterators from
sstables and memtable and merging, filtering and transforming them. There is a
number of functions to do just that (merging, filtering and transforming) in
`AtomIterators` and `PartitionIterators`, but there is also a number of classes
(`RowFilteringAtomIterator`, `CountingAtomIterator`, ...) that wraps one of
those iterator type to apply some filtering/transformation.

`AtomIterator` and `PartitionIterator` also have their doppelgänger
`RowIterator` and `DataIterator` which exists for the sake of making it easier
for the upper layers (StorageProxy and above) to deal with deletion. We'll
discuss those later.

### Partition: PartitionUpdate, AtomicBTreePartition and CachedPartition

While we avoid materializing partitions in memory as much as possible (favoring
the iterative approach), there is cases where we need/want to hold some subpart
of a partition in memory and we have a generic `Partition` interface for those.
A `Partition` basically corresponds to materializing an `AtomIterator` and
is thus somewhat equivalent to the existing
`ColumnFamily` (but again, many existing usage of `ColumnFamily` simply use
`AtomIterator`). `Partition` is mainly used through the following
implementations:
* `PartitionUpdate`: this is what a `Mutation` holds and is used to gather
  update and apply them to memtables.
* `AtomicBTreePartition`: this is the direct counterpart of AtomicBTreeColumns.
  The difference being that the BTree holds rows instead of cells. On updates, we
  merge the rows together to create a new one.
* `CachedPartition`: this is used by the row cache.

### Read commands

The `ReadCommand` class still exists, but instead of being just for single
partition reads, it's now a common abstract class for both single partition and
range reads. It then has 2 subclass: `SinglePartitionReadCommand` and
`PartitionRangeReadCommand`, the former of which has 2 subclasses itself:
`SinglePartitionSliceCommand` and `SinglePartitionNamesCommand`. All `ReadCommand`,
have a `ColumnFilter` and a `DataLimits` (see below). `PartitionRangeReadCommand`
additionally has a `DataRange`, which is mostly just a range of partition key
with a `PartitionFilter`, while `SinglePartitionReadCommand` has a partition
key and a `PartitionFilter` (see below too).

The code to execute those queries locally, which used to be in `ColumnFamilyStore`
and `CollationController` is now in those `ReadCommand` classes. For instance, the
`CollationController` code for names queries is in `SinglePartitionNamesCommand`,
and the code to decide if we use a 2ndary index or not is directly in
`ReadCommand.executeLocally()`.

Note that because they share a common class, all `ReadCommand` actually
return a `PartitionIterator` (an iterator over partitions), even single partition
ones (that iterator will just return one or zero result). It actually allows to
generalize (and simplify) the "response resolver". Instead of having separate resolver
for range and single partition queries, we only have `DataResolver` and
`DigestResolver` that work for any read command. This does mean that the patch
fixes CASSANDRA-2986, and that we could use digest queries for range if we
wanted to (not necessarily saying it's a good idea).

### ColumnFilter

`ColumnFilter` is the new `List<IndexExpression>`. It holds those column restrictions that
can't be directly fulfilled by the `PartitionFilter`, i.e. those that require either a
2ndary index, or filtering.

### PartitionFilter

`PartitionFilter` is the new `IDiskAtomFilter`/`QueryFilter`. There is still 2 variants:
`SlicePartitionFilter` and `NamesPartitionFilter`. Both variant includes the actual columns
that are queried (as we don't return full CQL rows anymore), and both can be
reversed. A names filter queries a bunch of rows by names, i.e. has a set of
`Clustering`. A slice filter queries one or more slice of rows. A slice filter
does not however include a limit since that is dealt with by `DataLimits` which
is in `ReadCommand` directly.


### DataLimits

`DataLimits` implement the limits on a query. This is meant to abstract the differences between
how we count for thrift and for CQL. Further, for CQL, this allow to have a limit per partition,
which clean up how DISTINCT queries are handled and allow for CASSANDRA-7017 (the patch doesn't
add support for the PER PARTITION LIMIT syntax of that ticket, but handle it
internally otherwise).

### SliceableAtomIterator

The code also use the `SliceableAtomIterator` abstraction. A
`SliceableAtomIterator` is an `AtomIterator` for which we basically know how to seek
into efficiently. In particular, we have a `SSTableIterator` which is a
`SliceableAtomIterator`. That `SSTableIterator` replaces both the existing
`SSTableNamesIterator` and `SSTableSliceIterator`, and the respective
`PartitionFilter` uses that `SliceableAtomIterator` interface to query what
they want exactly.


What did that become again?
---------------------------

For quick reference, here's the rough correspondence of old classes to new classes:
* `ColumnFamily`: for writes, this is handled by `PartitionUpdate` and
 `AtomicBTreePartition`. For reads, this is replaced by `AtomIterator` (or
 `RowIterator`, see the parts on tombstones below). For the row cache, there is
 a specific `CachedPartition` (which is actually an interface, the implementing
 class being `ArrayBackedPartition`)
* `Cell`: there is still a `Cell` class, which is roughly the same thing than
  the old one, except that instead of having a cell name, cells are now in a
  row and correspond to a column.
* `QueryFilter`: doesn't really exists anymore. What it was holding is now in
  `SinglePartitionReadCommand`.
* `AbstractRangeCommand` is now `PartitionRangeReadCommand`.
* `IDiskAtomFilter` is now `PartitionFilter`.
* `List<IndexExpression>` is now `ColumnFilter`.
* `RowDataResolver`, `RowDigestResolver` and `RangeSliceResponseResolver` are
  now `DataResolver` and `DigestResolver`.
* `Row` is now `AtomIterator`.
* `List<Row>` is now `PartitionIterator` (or `DataIterator`, see the part about
  tombstones below).
* `AbstractCompactedRow` and`LazilyCompactedRow` are not really needed anymore.
  Their corresponding code is in `CompactionIterable`.


Noteworthy points
-----------------

### Dealing with tombstones and shadowed cells

There is a few aspects worth noting regarding the handling of deleted and
shadowed data:
1. it's part of the contract of an `AtomIterator` that it must not shadow it's
   own data. In other words, it should not return a cell that is deleted by one
   of its own range tombstone, or by its partition level deletion. In practice
   this means that we get rid of shadowed data quickly (a good thing) and that
   there is a limited amount of places that have to deal with shadowing
   (merging being one).
2. Upper layer of the code (anything above StorageProxy) don't care about
   deleted data (and deletion informations in general). Basically, as soon as
   we've merge results for the replica, we don't need tombstones anymore.
   So, instead of requiring all those upper layer to filter tombstones
   themselves (which is error prone), we get rid of them as soon as we can,
   i.e. as soon as we've resolved replica responses (so in the
   `ResponseResolver`). To do that and to make it clear when tombstones have
   been filtered (and make the code cleaner), we transform an `AtomIterator`
   into a `RowIterator`. Both being essentially the same thing, except that a
   `RowIterator` only return live stuffs. Which mean in particular that it's an
   iterator of `Row` (since an unfiltered is either a row or a range tombstone and
   we've filtered tombstones). Similarly, a `PartitionIterator` becomes a
   `DataIterator`, which is just an iterator of `RowIterator`.
3. In the existing code a CQL row deletion involves a range tombstone. But as
   row deletions are pretty frequent and range tombstone have inherent
   inefficiencies, the patch adds the notion of row deletion, which is just
   some optional deletion information on the `Row`. This can be though as just
   an optimization of range tombstones that span only a single row.
4. As mentioned at the beginning of this document, the code splits range
   tombstones into 2 range tombstone marker, one for each bound. The problem
   with storing full range tombstone as we currently do is that it makes it
   harder to merge them efficiently, and in particular to "normalize"
   overlapping ones. In practice, a given `AtomIterator` guarantees that there
   is only one range tombstone to worry about at any given time. In other
   words, at any point of the iterator, either there is a single open range
   tombstone or there is none, which makes things easier and more efficient.
   It's worth noting that we still have the `RangeTombstone` class because for
   in-memory structures (`PartitionUpdate`, `AtomicBTreePartition`, ...) we
   currently use the existing `RangeTombstoneList` out of convenience. This is
   kind of an implementation detail that could change in the future.

### statics

Static columns are handled separately from the rest of the rows. In practice,
each `Partition`/`AtomIterator` has a separate "static row" that holds the
value for all the static columns (that row can of course be empty). That row
doesn't really correspond to any CQL row, it's content is simply "merged" to
the result set in `SelectStatement` (very much like we already do).

### Row liveness

In CQL, a row can exists even if only its PK columns have values. In other
words, a `Row` can be live even if it doesn't have any cells. Currently, we
handle this through the "row marker" (i.e. through a specific cell). The patch
makes this slightly less hacky by adding a timestamp (and potentially a ttl) to
each row, which can be though as the timestamp (and ttl) of the PK columns.

Further, when we query only some specific columns in a row, we need to add a
row (with nulls) in the result set if a row is live (it has live cells or the
timestamp we described above) even if it has no actual values for the queried
columns.  We currently deal with that by querying entire row all the time, but
the patch change that. This does mean that even when we query only some
columns, we still need to have the information on whether the row itself is
live or not. And because we'll merge results from different sources (which can
include deletions), a boolean is not enough, so we include in a `Row` object
the maximum live timestamp known for this row. Which currently mean that in
practice, we do scan the full row on disk but filter cells we're not interested
by right away (and in fact, we don't even deserialize the value of such cells).
This might be optimizable later on, but expiring data makes that harder (we
typically can't just pre-compute that max live timestamp when writing the
sstable since it can depend on the time of the query).

### Flyweight pattern

The patch makes relatively heavy use of a "flyweight"-like pattern. Typically,
`Row` and `Cell` data are stored in arrays, and a given `AtomIterator` will
only use a single `Row` object (and `Cell` object) that points in those arrays
(and thus change at each call to `hasNext()/next()`). This does mean that the
objects returned that way shouldn't be aliased (taken a reference of) without
care. The patch uses an `Aliasable` interface to mark object that may use this
pattern and should thus potentially be copied in the rare cases where a
reference should be kept on them.

### Reversed queries

The patch slightly change the way reversed queries are handled. First, while
a reverse query should currently reverse its slices in `SliceQueryFilter`, this
is not the case anymore: `SlicePartitionFilter` always keep its slices in
clustering order and simply handles those from the end to the beginning on
reversed queries. Further, if a query is reversed, the results are returned in
this reverse order all the way through. Which differs from the current code
where `ColumnFamily` actually always holds cells in forward order (making it a
mess for paging and forcing us to re-reverse everything in the result set).

### Compaction

As the storage engine works iteratively, compaction simply has to get iterators
on the sstables, merge them and write the result back, along with a simple
filter that skip purgeable tombstones in the process. So there isn't really a
need for `AbstractCompactedRow`/`LazilyCompactedRow` anymore and the
compaction/purging code is now in `CompactionIterable` directly.

### Short reads

The current way to handle short reads would require us to consume the whole
result before deciding on a retry (and we currently retry the whole command),
which doesn't work too well in an iterative world.  So the patch moves read
protection in `DataResolver` (where it kind of belong anyway) and we don't
retry the full command anymore. Instead, if we realize that a given node has a
short read while its result is consumed, we simply query this node (and this
node only) for a continuation of the result. On top of avoiding the retry of
the whole read (and limiting the number of node queried on the retry), this
also make it trivial to solve CASSANDRA-8933.


Storage format (on-disk and on-wire)
------------------------------------

Given that the main abstractions are changed, the existing on-wire
`ColumnFamily` format is not appropriate anymore and the patch switches to a
new format. The same can be told of the on-disk format, and while it is not an
objective of CASSANDRA-8099 to get fancy on the on-disk format, using the
on-wire format as on-disk format was actually relatively simple (until more
substantial changes land with CASSANDRA-7447) and the patch does that too.

For a given partition, the format simply serialize rows one after another
(atoms in practice). For the on-disk format, this means that it is now rows
that are indexed, not cells. The format uses a header that is written at the
beginning of each partition for the on-wire format (in
`AtomIteratorSerializer`) and is kept as a new sstable `Component` for
sstables. The details of the format are described in the javadoc of
`AtomIteratorSerializer` (only used by the on-wire format) and of
`AtomSerializer`, so let me just point the following differences compared to
the current format:
* Clustering values are only serialized once per row (we even skip serializing the
  number of elements since that is fixed for a given table).
* Column names are not written for every row. Instead they are written once in
  the header. For a given row, we support two small variant: dense and sparse.
  When dense, cells come in the order the columns have in the header, meaning
  that if a row doesn't have a particular column, this column will still use
  a byte. When sparse, we don't have anything if the column doesn't have a cell,
  but each cell has an additional 2 bytes which points into the header (so with
  vint it should rarely take more than 1 byte in practice). The variant used
  is automatically decided based on stats on how many columns set a row has on
  average for the source we serialize.
* Values for fixed-width cell values are serialized without a size.
* If a cell has the same timestamp than its row, that timestamp is not repeated
  for the cell. Same for the ttl (if applicable).
* Timestamps, ttls and local deletion times are delta encoded so that they are
  ripe for vint encoding. The current version of the patch does not yet
  activate vint encoding however (neither for on-wire or on-disk).

