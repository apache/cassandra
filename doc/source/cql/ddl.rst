.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: sql

.. _data-definition:

Data Definition
---------------

CQL stores data in *tables*, whose schema defines the layout of said data in the table, and those tables are grouped in
*keyspaces*. A keyspace defines a number of options that applies to all the tables it contains, most prominently of
which is the :ref:`replication strategy <replication-strategy>` used by the keyspace. It is generally encouraged to use
one keyspace by *application*, and thus many cluster may define only one keyspace.

This section describes the statements used to create, modify, and remove those keyspace and tables.

Common definitions
^^^^^^^^^^^^^^^^^^

The names of the keyspaces and tables are defined by the following grammar:

.. productionlist::
   keyspace_name: `name`
   table_name: [ `keyspace_name` '.' ] `name`
   name: `unquoted_name` | `quoted_name`
   unquoted_name: re('[a-zA-Z_0-9]{1, 48}')
   quoted_name: '"' `unquoted_name` '"'

Both keyspace and table name should be comprised of only alphanumeric characters, cannot be empty and are limited in
size to 48 characters (that limit exists mostly to avoid filenames (which may include the keyspace and table name) to go
over the limits of certain file systems). By default, keyspace and table names are case insensitive (``myTable`` is
equivalent to ``mytable``) but case sensitivity can be forced by using double-quotes (``"myTable"`` is different from
``mytable``).

Further, a table is always part of a keyspace and a table name can be provided fully-qualified by the keyspace it is
part of. If is is not fully-qualified, the table is assumed to be in the *current* keyspace (see :ref:`USE statement
<use-statement>`).

We also define the notion of statement options for use in the following section:

.. productionlist::
   options: `option` ( AND `option` )*
   option: `identifier` '=' ( `identifier` | `constant` | `map_literal` )

.. _create-keyspace-statement:

CREATE KEYSPACE
^^^^^^^^^^^^^^^

A keyspace is created using a ``CREATE KEYSPACE`` statement:

.. productionlist::
   create_keyspace_statement: CREATE KEYSPACE [ IF NOT EXISTS ] `keyspace_name` WITH `options`

For instance::

    CREATE KEYSPACE Excelsior
               WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

    CREATE KEYSPACE Excalibur
               WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 1, 'DC2' : 3}
                AND durable_writes = false;


.. _create-keyspace-options:
The supported ``options`` are:

=================== ========== =========== ========= ===================================================================
name                 kind       mandatory   default   description
=================== ========== =========== ========= ===================================================================
``replication``      *map*      yes                   The replication strategy and options to use for the keyspace (see
                                                      details below).
``durable_writes``   *simple*   no          true      Whether to use the commit log for updates on this keyspace
                                                      (disable this option at your own risk!).
=================== ========== =========== ========= ===================================================================

The ``replication`` property is mandatory and must at least contains the ``'class'`` sub-option which defines the
:ref:`replication strategy <replication-strategy>` class to use. The rest of the sub-options depends on what replication
strategy is used. By default, Cassandra support the following ``'class'``:

- ``'SimpleStrategy'``: A simple strategy that defines a replication factor for the whole cluster. The only sub-options
  supported is ``'replication_factor'`` to define that replication factor and is mandatory.
- ``'NetworkTopologyStrategy'``: A replication strategy that allows to set the replication factor independently for
  each data-center. The rest of the sub-options are key-value pairs where a key is a data-center name and its value is
  the associated replication factor.

Attempting to create a keyspace that already exists will return an error unless the ``IF NOT EXISTS`` option is used. If
it is used, the statement will be a no-op if the keyspace already exists.

.. _use-statement:

USE
^^^

The ``USE`` statement allows to change the *current* keyspace (for the *connection* on which it is executed). A number
of objects in CQL are bound to a keyspace (tables, user-defined types, functions, ...) and the current keyspace is the
default keyspace used when those objects are referred without a fully-qualified name (that is, without being prefixed a
keyspace name). A ``USE`` statement simply takes the keyspace to use as current as argument:

.. productionlist::
   use_statement: USE `keyspace_name`

.. _alter-keyspace-statement:

ALTER KEYSPACE
^^^^^^^^^^^^^^

An ``ALTER KEYSPACE`` statement allows to modify the options of a keyspace:

.. productionlist::
   alter_keyspace_statement: ALTER KEYSPACE `keyspace_name` WITH `options`

For instance::

    ALTER KEYSPACE Excelsior
              WITH replication = {’class’: ‘SimpleStrategy’, ‘replication_factor’ : 4};

The supported options are the same than for :ref:`creating a keyspace <create-keyspace-options>`.

.. _drop-keyspace-statement:

DROP KEYSPACE
^^^^^^^^^^^^^

Dropping a keyspace can be done using the ``DROP KEYSPACE`` statement:

.. productionlist::
   drop_keyspace_statement: DROP KEYSPACE [ IF EXISTS ] `keyspace_name`

For instance::

    DROP KEYSPACE Excelsior;

Dropping a keyspace results in the immediate, irreversible removal of that keyspace, including all the tables, UTD and
functions in it, and all the data contained in those tables.

If the keyspace does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case the
operation is a no-op.

.. _create-table-statement:

CREATE TABLE
^^^^^^^^^^^^

Creating a new table uses the ``CREATE TABLE`` statement:

.. productionlist::
   create_table_statement: CREATE TABLE [ IF NOT EXISTS ] `table_name`
                         : '('
                         :     `column_definition`
                         :     ( ',' `column_definition` )*
                         :     [ ',' PRIMARY KEY '(' `primary_key` ')' ]
                         : ')' [ WITH `table_options` ]
   column_definition: `identifier` `cql_type` [ STATIC ] [ PRIMARY KEY]
   primary_key: `partition_key` [ ',' `clustering_columns` ]
   partition_key: `identifier`
                : | '(' `identifier` ( ',' `identifier` )* ')'
   clustering_columns: `identifier` ( ',' `identifier` )*
   table_options: COMPACT STORAGE [ AND `table_options` ]
                   : | CLUSTERING ORDER BY '(' `clustering_order` ')' [ AND `table_options` ]
                   : | `options`
   clustering_order: `identifier` (ASC | DESC) ( ',' `identifier` (ASC | DESC) )*

For instance::

    CREATE TABLE monkeySpecies (
        species text PRIMARY KEY,
        common_name text,
        population varint,
        average_size int
    ) WITH comment=‘Important biological records’
       AND read_repair_chance = 1.0;

    CREATE TABLE timeline (
        userid uuid,
        posted_month int,
        posted_time uuid,
        body text,
        posted_by text,
        PRIMARY KEY (userid, posted_month, posted_time)
    ) WITH compaction = { ‘class’ : ‘LeveledCompactionStrategy’ };

    CREATE TABLE loads (
        machine inet,
        cpu int,
        mtime timeuuid,
        load float,
        PRIMARY KEY ((machine, cpu), mtime)
    ) WITH CLUSTERING ORDER BY (mtime DESC);

A CQL table has a name and is composed of a set of *rows*. Creating a table amounts to defining which :ref:`columns
<column-definition>` the rows will be composed, which of those columns compose the :ref:`primary key <primary-key>`, as
well as optional :ref:`options <create-table-options>` for the table.

Attempting to create an already existing table will return an error unless the ``IF NOT EXISTS`` directive is used. If
it is used, the statement will be a no-op if the table already exists.


.. _column-definition:

Column definitions
~~~~~~~~~~~~~~~~~~

Every rows in a CQL table has a set of predefined columns defined at the time of the table creation (or added later
using an :ref:`alter statement<alter-table-statement>`).

A :token:`column_definition` is primarily comprised of the name of the column defined and it's :ref:`type <data-types>`,
which restrict which values are accepted for that column. Additionally, a column definition can have the following
modifiers:

``STATIC``
    it declares the column as being a :ref:`static column <static-columns>`.

``PRIMARY KEY``
    it declares the column as being the sole component of the :ref:`primary key <primary-key>` of the table.

.. _static-columns:

Static columns
``````````````
Some columns can be declared as ``STATIC`` in a table definition. A column that is static will be “shared” by all the
rows belonging to the same partition (having the same :ref:`partition key <partition-key>`). For instance::

    CREATE TABLE t (
        pk int,
        t int,
        v text,
        s text static,
        PRIMARY KEY (pk, t)
    );

    INSERT INTO t (pk, t, v, s) VALUES (0, 0, 'val0', 'static0');
    INSERT INTO t (pk, t, v, s) VALUES (0, 1, 'val1', 'static1');

    SELECT * FROM t;
       pk | t | v      | s
      ----+---+--------+-----------
       0  | 0 | 'val0' | 'static1'
       0  | 1 | 'val1' | 'static1'

As can be seen, the ``s`` value is the same (``static1``) for both of the row in the partition (the partition key in
that example being ``pk``, both rows are in that same partition): the 2nd insertion has overridden the value for ``s``.

The use of static columns as the following restrictions:

- tables with the ``COMPACT STORAGE`` option (see below) cannot use them.
- a table without clustering columns cannot have static columns (in a table without clustering columns, every partition
  has only one row, and so every column is inherently static).
- only non ``PRIMARY KEY`` columns can be static.

.. _primary-key:

The Primary key
~~~~~~~~~~~~~~~

Within a table, a row is uniquely identified by its ``PRIMARY KEY``, and hence all table **must** define a PRIMARY KEY
(and only one). A ``PRIMARY KEY`` definition is composed of one or more of the columns defined in the table.
Syntactically, the primary key is defined the keywords ``PRIMARY KEY`` followed by comma-separated list of the column
names composing it within parenthesis, but if the primary key has only one column, one can alternatively follow that
column definition by the ``PRIMARY KEY`` keywords. The order of the columns in the primary key definition matter.

A CQL primary key is composed of 2 parts:

- the :ref:`partition key <partition-key>` part. It is the first component of the primary key definition. It can be a
  single column or, using additional parenthesis, can be multiple columns. A table always have at least a partition key,
  the smallest possible table definition is::

      CREATE TABLE t (k text PRIMARY KEY);

- the :ref:`clustering columns <clustering-columns>`. Those are the columns after the first component of the primary key
  definition, and the order of those columns define the *clustering order*.

Some example of primary key definition are:

- ``PRIMARY KEY (a)``: ``a`` is the partition key and there is no clustering columns.
- ``PRIMARY KEY (a, b, c)`` : ``a`` is the partition key and ``b`` and ``c`` are the clustering columns.
- ``PRIMARY KEY ((a, b), c)`` : ``a`` and ``b`` compose the partition key (this is often called a *composite* partition
  key) and ``c`` is the clustering column.


.. _partition-key:

The partition key
`````````````````

Within a table, CQL defines the notion of a *partition*. A partition is simply the set of rows that share the same value
for their partition key. Note that if the partition key is composed of multiple columns, then rows belong to the same
partition only they have the same values for all those partition key column. So for instance, given the following table
definition and content::

    CREATE TABLE t (
        a int,
        b int,
        c int,
        d int,
        PRIMARY KEY ((a, b), c, d)
    );

    SELECT * FROM t;
       a | b | c | d
      ---+---+---+---
       0 | 0 | 0 | 0    // row 1
       0 | 0 | 1 | 1    // row 2
       0 | 1 | 2 | 2    // row 3
       0 | 1 | 3 | 3    // row 4
       1 | 1 | 4 | 4    // row 5

``row 1`` and ``row 2`` are in the same partition, ``row 3`` and ``row 4`` are also in the same partition (but a
different one) and ``row 5`` is in yet another partition.

Note that a table always has a partition key, and that if the table has no :ref:`clustering columns
<clustering-columns>`, then every partition of that table is only comprised of a single row (since the primary key
uniquely identifies rows and the primary key is equal to the partition key if there is no clustering columns).

The most important property of partition is that all the rows belonging to the same partition are guarantee to be stored
on the same set of replica nodes. In other words, the partition key of a table defines which of the rows will be
localized together in the Cluster, and it is thus important to choose your partition key wisely so that rows that needs
to be fetch together are in the same partition (so that querying those rows together require contacting a minimum of
nodes).

Please note however that there is a flip-side to this guarantee: as all rows sharing a partition key are guaranteed to
be stored on the same set of replica node, a partition key that groups too much data can create a hotspot.

Another useful property of a partition is that when writing data, all the updates belonging to a single partition are
done *atomically* and in *isolation*, which is not the case across partitions.

The proper choice of the partition key and clustering columns for a table is probably one of the most important aspect
of data modeling in Cassandra, and it largely impact which queries can be performed, and how efficiently they are.


.. _clustering-columns:

The clustering columns
``````````````````````

The clustering columns of a table defines the clustering order for the partition of that table. For a given
:ref:`partition <partition-key>`, all the rows are physically ordered inside Cassandra by that clustering order. For
instance, given::

    CREATE TABLE t (
        a int,
        b int,
        c int,
        PRIMARY KEY (a, c, d)
    );

    SELECT * FROM t;
       a | b | c
      ---+---+---
       0 | 0 | 4     // row 1
       0 | 1 | 9     // row 2
       0 | 2 | 2     // row 3
       0 | 3 | 3     // row 4

then the rows (which all belong to the same partition) are all stored internally in the order of the values of their
``b`` column (the order they are displayed above). So where the partition key of the table allows to group rows on the
same replica set, the clustering columns controls how those rows are stored on the replica. That sorting allows the
retrieval of a range of rows within a partition (for instance, in the example above, ``SELECT * FROM t WHERE a = 0 AND b
> 1 and b <= 3``) very efficient.


.. _create-table-options

Table options
~~~~~~~~~~~~~

A CQL table has a number of options that can be set at creation (and, for most of them, :ref:`altered
<alter-table-statement>` later). These options are specified after the ``WITH`` keyword.

Amongst those options, two important ones cannot be changed after creation and influence which queries can be done
against the table: the ``COMPACT STORAGE`` option and the ``CLUSTERING ORDER`` option. Those, as well as the other
options of a table are described in the following sections.

.. _compact-storage:

Compact tables
``````````````

.. warning:: Since Cassandra 3.0, compact tables have the exact same layout internally than non compact ones (for the
   same schema obviously), and declaring a table compact **only** creates artificial limitations on the table definition
   and usage that are necessary to ensure backward compatibility with the deprecated Thrift API. And as ``COMPACT
   STORAGE`` cannot, as of Cassandra |3.8|, be removed, it is strongly discouraged to create new table with the
   ``COMPACT STORAGE`` option.

A *compact* table is one defined with the ``COMPACT STORAGE`` option. This option is mainly targeted towards backward
compatibility for definitions created before CQL version 3 (see `www.datastax.com/dev/blog/thrift-to-cql3
<http://www.datastax.com/dev/blog/thrift-to-cql3>`__ for more details) and shouldn't be used for new tables. Declaring a
table with this option creates limitations for the table which are largely arbitrary but necessary for backward
compatibility with the (deprecated) Thrift API. Amongst those limitation:

- a compact table cannot use collections nor static columns.
- if a compact table has at least one clustering column, then it must have *exactly* one column outside of the primary
  key ones. This imply you cannot add or remove columns after creation in particular.
- a compact table is limited in the indexes it can create, and no materialized view can be created on it.

.. _clustering-order:

Reversing the clustering order
``````````````````````````````

The clustering order of a table is defined by the :ref:`clustering columns <clustering-columns>` of that table. By
default, that ordering is based on natural order of those clustering order, but the ``CLUSTERING ORDER`` allows to
change that clustering order to use the *reverse* natural order for some (potentially all) of the columns.

The ``CLUSTERING ORDER`` option takes the comma-separated list of the clustering column, each with a ``ASC`` (for
*ascendant*, e.g. the natural order) or ``DESC`` (for *descendant*, e.g. the reverse natural order). Note in particular
that the default (if the ``CLUSTERING ORDER`` option is not used) is strictly equivalent to using the option with all
clustering columns using the ``ASC`` modifier.

Note that this option is basically a hint for the storage engine to change the order in which it stores the row but it
has 3 visible consequences:

# it limits which ``ORDER BY`` clause are allowed for :ref:`selects <select-statement>` on that table. You can only
  order results by the clustering order or the reverse clustering order. Meaning that if a table has 2 clustering column
  ``a`` and ``b`` and you defined ``WITH CLUSTERING ORDER (a DESC, b ASC)``, then in queries you will be allowed to use
  ``ORDER BY (a DESC, b ASC)`` and (reverse clustering order) ``ORDER BY (a ASC, b DESC)`` but **not** ``ORDER BY (a
  ASC, b ASC)`` (nor ``ORDER BY (a DESC, b DESC)``).
# it also change the default order of results when queried (if no ``ORDER BY`` is provided). Results are always returned
  in clustering order (within a partition).
# it has a small performance impact on some queries as queries in reverse clustering order are slower than the one in
  forward clustering order. In practice, this means that if you plan on querying mostly in the reverse natural order of
  your columns (which is common with time series for instance where you often want data from the newest to the oldest),
  it is an optimization to declare a descending clustering order.

.. _create-table-general-options:

Other table options
```````````````````

.. todo:: review (misses cdc if nothing else) and link to proper categories when appropriate (compaction for instance)

A table supports the following options:

+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                           | kind       | default       | description                                                                                                                                                                                                                     |
+==================================+============+===============+=================================================================================================================================================================================================================================+
| ``comment``                      | *simple*   | none          | A free-form, human-readable comment.                                                                                                                                                                                            |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``read_repair_chance``           | *simple*   | 0.1           | The probability with which to query extra nodes (e.g. more nodes than required by the consistency level) for the purpose of read repairs.                                                                                       |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``dclocal_read_repair_chance``   | *simple*   | 0             | The probability with which to query extra nodes (e.g. more nodes than required by the consistency level) belonging to the same data center than the read coordinator for the purpose of read repairs.                           |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``gc_grace_seconds``             | *simple*   | 864000        | Time to wait before garbage collecting tombstones (deletion markers).                                                                                                                                                           |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bloom_filter_fp_chance``       | *simple*   | 0.00075       | The target probability of false positive of the sstable bloom filters. Said bloom filters will be sized to provide the provided probability (thus lowering this value impact the size of bloom filters in-memory and on-disk)   |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``default_time_to_live``         | *simple*   | 0             | The default expiration time (“TTL”) in seconds for a table.                                                                                                                                                                     |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compaction``                   | *map*      | *see below*   | Compaction options, see “below”:#compactionOptions.                                                                                                                                                                             |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compression``                  | *map*      | *see below*   | Compression options, see “below”:#compressionOptions.                                                                                                                                                                           |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``caching``                      | *map*      | *see below*   | Caching options, see “below”:#cachingOptions.                                                                                                                                                                                   |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Compaction options
##################

The ``compaction`` property must at least define the ``'class'``
sub-option, that defines the compaction strategy class to use. The
default supported class are ``'SizeTieredCompactionStrategy'``,
``'LeveledCompactionStrategy'``, ``'DateTieredCompactionStrategy'`` and
``'TimeWindowCompactionStrategy'``. Custom strategy can be provided by
specifying the full class name as a `string constant <#constants>`__.
The rest of the sub-options depends on the chosen class. The sub-options
supported by the default classes are:

+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                               | supported compaction strategy   | default        | description                                                                                                                                                                                                                                                                                                                            |
+======================================+=================================+================+========================================================================================================================================================================================================================================================================================================================================+
| ``enabled``                          | *all*                           | true           | A boolean denoting whether compaction should be enabled or not.                                                                                                                                                                                                                                                                        |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tombstone_threshold``              | *all*                           | 0.2            | A ratio such that if a sstable has more than this ratio of gcable tombstones over all contained columns, the sstable will be compacted (with no other sstables) for the purpose of purging those tombstones.                                                                                                                           |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tombstone_compaction_interval``    | *all*                           | 1 day          | The minimum time to wait after an sstable creation time before considering it for “tombstone compaction”, where “tombstone compaction” is the compaction triggered if the sstable has more gcable tombstones than ``tombstone_threshold``.                                                                                             |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``unchecked_tombstone_compaction``   | *all*                           | false          | Setting this to true enables more aggressive tombstone compactions - single sstable tombstone compactions will run without checking how likely it is that they will be successful.                                                                                                                                                     |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``min_sstable_size``                 | SizeTieredCompactionStrategy    | 50MB           | The size tiered strategy groups SSTables to compact in buckets. A bucket groups SSTables that differs from less than 50% in size. However, for small sizes, this would result in a bucketing that is too fine grained. ``min_sstable_size`` defines a size threshold (in bytes) below which all SSTables belong to one unique bucket   |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``min_threshold``                    | SizeTieredCompactionStrategy    | 4              | Minimum number of SSTables needed to start a minor compaction.                                                                                                                                                                                                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``max_threshold``                    | SizeTieredCompactionStrategy    | 32             | Maximum number of SSTables processed by one minor compaction.                                                                                                                                                                                                                                                                          |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bucket_low``                       | SizeTieredCompactionStrategy    | 0.5            | Size tiered consider sstables to be within the same bucket if their size is within [average\_size \* ``bucket_low``, average\_size \* ``bucket_high`` ] (i.e the default groups sstable whose sizes diverges by at most 50%)                                                                                                           |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bucket_high``                      | SizeTieredCompactionStrategy    | 1.5            | Size tiered consider sstables to be within the same bucket if their size is within [average\_size \* ``bucket_low``, average\_size \* ``bucket_high`` ] (i.e the default groups sstable whose sizes diverges by at most 50%).                                                                                                          |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``sstable_size_in_mb``               | LeveledCompactionStrategy       | 5MB            | The target size (in MB) for sstables in the leveled strategy. Note that while sstable sizes should stay less or equal to ``sstable_size_in_mb``, it is possible to exceptionally have a larger sstable as during compaction, data for a given partition key are never split into 2 sstables                                            |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timestamp_resolution``             | DateTieredCompactionStrategy    | MICROSECONDS   | The timestamp resolution used when inserting data, could be MILLISECONDS, MICROSECONDS etc (should be understandable by Java TimeUnit) - don’t change this unless you do mutations with USING TIMESTAMP (or equivalent directly in the client)                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``base_time_seconds``                | DateTieredCompactionStrategy    | 60             | The base size of the time windows.                                                                                                                                                                                                                                                                                                     |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``max_sstable_age_days``             | DateTieredCompactionStrategy    | 365            | SSTables only containing data that is older than this will never be compacted.                                                                                                                                                                                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timestamp_resolution``             | TimeWindowCompactionStrategy    | MICROSECONDS   | The timestamp resolution used when inserting data, could be MILLISECONDS, MICROSECONDS etc (should be understandable by Java TimeUnit) - don’t change this unless you do mutations with USING TIMESTAMP (or equivalent directly in the client)                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compaction_window_unit``           | TimeWindowCompactionStrategy    | DAYS           | The Java TimeUnit used for the window size, set in conjunction with ``compaction_window_size``. Must be one of DAYS, HOURS, MINUTES                                                                                                                                                                                                    |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compaction_window_size``           | TimeWindowCompactionStrategy    | 1              | The number of ``compaction_window_unit`` units that make up a time window.                                                                                                                                                                                                                                                             |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Compression options
###################

For the ``compression`` property, the following sub-options are
available:

+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                 | default         | description                                                                                                                                                                                                                                                                                                                                                                                                           |
+========================+=================+=======================================================================================================================================================================================================================================================================================================================================================================================================================+
| ``class``              | LZ4Compressor   | The compression algorithm to use. Default compressor are: LZ4Compressor, SnappyCompressor and DeflateCompressor. Use ``'enabled' : false`` to disable compression. Custom compressor can be provided by specifying the full class name as a “string constant”:#constants.                                                                                                                                             |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``enabled``            | true            | By default compression is enabled. To disable it, set ``enabled`` to ``false``                                                                                                                                                                                                                                                                                                                                        |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|`` chunk_length_in_kb``  | 64KB            | On disk SSTables are compressed by block (to allow random reads). This defines the size (in KB) of said block. Bigger values may improve the compression rate, but increases the minimum size of data to be read from disk for a read                                                                                                                                                                                 |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``crc_check_chance``   | 1.0             | When compression is enabled, each compressed block includes a checksum of that block for the purpose of detecting disk bitrot and avoiding the propagation of corruption to other replica. This option defines the probability with which those checksums are checked during read. By default they are always checked. Set to 0 to disable checksum checking and to 0.5 for instance to check them every other read   |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Caching options
###############

For the ``caching`` property, the following sub-options are available:

+--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                   | default   | description                                                                                                                                                                                                                                                                |
+==========================+===========+============================================================================================================================================================================================================================================================================+
| ``keys``                 | ALL       | Whether to cache keys (“key cache”) for this table. Valid values are: ``ALL`` and ``NONE``.                                                                                                                                                                                |
+--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``rows_per_partition``   | NONE      | The amount of rows to cache per partition (“row cache”). If an integer ``n`` is specified, the first ``n`` queried rows of a partition will be cached. Other possible options are ``ALL``, to cache all rows of a queried partition, or ``NONE`` to disable row caching.   |
+--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Other considerations:
#####################

-  When `inserting <#insertStmt>`__ / `updating <#updateStmt>`__ a given
   row, not all columns needs to be defined (except for those part of
   the key), and missing columns occupy no space on disk. Furthermore,
   adding new columns (see \ ``ALTER TABLE``\ ) is a constant time
   operation. There is thus no need to try to anticipate future usage
   (or to cry when you haven’t) when creating a table.

ALTER TABLE
^^^^^^^^^^^

Altering an existing table uses the ``ALTER TABLE`` statement:

.. productionlist::
   alter_table_statement: ALTER TABLE `table_name` `alter_table_instruction`
   alter_table_instruction: ALTER `identifier` TYPE `cql_type`
                          : | ADD `identifier` `cql_type` ( ',' `identifier` `cql_type` )*
                          : | DROP `identifier` ( `identifier` )*
                          : | WITH `options`

For instance::

    ALTER TABLE addamsFamily ALTER lastKnownLocation TYPE uuid;

    ALTER TABLE addamsFamily ADD gravesite varchar;

    ALTER TABLE addamsFamily
           WITH comment = ‘A most excellent and useful table’
           AND read_repair_chance = 0.2;

The ``ALTER TABLE`` statement can:

- Change the type of one of the column in the table (through the ``ALTER`` instruction). Note that the type of a column
  cannot be changed arbitrarily. The change of type should be such that any value of the previous type should be a valid
  value of the new type. Further, for :ref:`clustering columns <clustering-columns>` and columns on which a secondary
  index is defined, the new type must sort values in the same way the previous type does. See the :ref:`type
  compatibility table <alter-table-type-compatibility>` below for detail on which type changes are accepted.
- Add new column(s) to the table (through the ``ADD`` instruction). Note that the primary key of a table cannot be
  changed and thus newly added column will, by extension, never be part of the primary key. Also note that :ref:`compact
  tables <compact-tables>` have restrictions regarding column addition.
- Remove column(s) from the table. This drops both the column and all its content, but note that while the column
  becomes immediately unavailable, its content is only removed lazily during compaction. Please also see the warnings
  below.
- Change some of the table options (through the ``WITH`` instruction). The :ref:`supported options
  <create-table-options>` are the same that when creating a table (outside of ``COMPACT STORAGE`` and ``CLUSTERING
  ORDER`` that cannot be changed after creation). Note that setting any ``compaction`` sub-options has the effect of
  erasing all previous ``compaction`` options, so you need to re-specify all the sub-options if you want to keep them.
  The same note applies to the set of ``compression`` sub-options.

.. warning:: Dropping a column assumes that the timestamps used for the value of this column are "real" timestamp in
   microseconds. Using "real" timestamps in microseconds is the default is and is **strongly** recommended but as
   Cassandra allows the client to provide any timestamp on any table it is theoretically possible to use another
   convention. Please be aware that if you do so, dropping a column will not work correctly.

.. warning:: Once a column is dropped, it is allowed to re-add a column with the same name than the dropped one
   **unless* the type of the dropped column was a (non-frozen) column (due to an internal technical limitation).

.. _alter-table-type-compatibility:

CQL type compatibility:
~~~~~~~~~~~~~~~~~~~~~~~

CQL data types may be converted only as the following table.

+-------------------------------------------------------+--------------------+
| Existing type                                         | Can be altered to: |
+=======================================================+====================+
| timestamp                                             | bigint             |
+-------------------------------------------------------+--------------------+
| ascii, bigint, boolean, date, decimal, double, float, | blob               |
| inet, int, smallint, text, time, timestamp, timeuuid, |                    |
| tinyint, uuid, varchar, varint                        |                    |
+-------------------------------------------------------+--------------------+
| int                                                   | date               |
+-------------------------------------------------------+--------------------+
| ascii, varchar                                        | text               |
+-------------------------------------------------------+--------------------+
| bigint                                                | time               |
+-------------------------------------------------------+--------------------+
| bigint                                                | timestamp          |
+-------------------------------------------------------+--------------------+
| timeuuid                                              | uuid               |
+-------------------------------------------------------+--------------------+
| ascii, text                                           | varchar            |
+-------------------------------------------------------+--------------------+
| bigint, int, timestamp                                | varint             |
+-------------------------------------------------------+--------------------+

Clustering columns have stricter requirements, only the following conversions are allowed:

+------------------------+-------------------+
| Existing type          | Can be altered to |
+========================+===================+
| ascii, text, varchar   | blob              |
+------------------------+-------------------+
| ascii, varchar         | text              |
+------------------------+-------------------+
| ascii, text            | varchar           |
+------------------------+-------------------+

DROP TABLE
^^^^^^^^^^

*Syntax:*

bc(syntax). ::= DROP TABLE ( IF EXISTS )?

*Sample:*

bc(sample). DROP TABLE worldSeriesAttendees;

The ``DROP TABLE`` statement results in the immediate, irreversible
removal of a table, including all data contained in it. As for table
creation, ``DROP COLUMNFAMILY`` is allowed as an alias for
``DROP TABLE``.

If the table does not exist, the statement will return an error, unless
``IF EXISTS`` is used in which case the operation is a no-op.

TRUNCATE
^^^^^^^^

*Syntax:*

bc(syntax). ::= TRUNCATE ( TABLE \| COLUMNFAMILY )?

*Sample:*

bc(sample). TRUNCATE superImportantData;

The ``TRUNCATE`` statement permanently removes all data from a table.

