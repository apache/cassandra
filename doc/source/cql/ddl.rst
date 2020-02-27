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

.. highlight:: cql

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

Further, the valid names for columns is simply defined as:

.. productionlist::
   column_name: `identifier`

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

    CREATE KEYSPACE excelsior
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 1, 'DC2' : 3}
        AND durable_writes = false;

Attempting to create a keyspace that already exists will return an error unless the ``IF NOT EXISTS`` option is used. If
it is used, the statement will be a no-op if the keyspace already exists.

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

.. _replication-strategy:

``SimpleStrategy``
""""""""""""""""""

A simple strategy that defines a replication factor for data to be spread
across the entire cluster. This is generally not a wise choice for production
because it does not respect datacenter layouts and can lead to wildly varying
query latency. For a production ready strategy, see
``NetworkTopologyStrategy``. ``SimpleStrategy`` supports a single mandatory argument:

========================= ====== ======= =============================================
sub-option                 type   since   description
========================= ====== ======= =============================================
``'replication_factor'``   int    all     The number of replicas to store per range
========================= ====== ======= =============================================

``NetworkTopologyStrategy``
"""""""""""""""""""""""""""

A production ready replication strategy that allows to set the replication
factor independently for each data-center. The rest of the sub-options are
key-value pairs where a key is a data-center name and its value is the
associated replication factor. Options:

===================================== ====== ====== =============================================
sub-option                             type   since  description
===================================== ====== ====== =============================================
``'<datacenter>'``                     int    all    The number of replicas to store per range in
                                                     the provided datacenter.
``'replication_factor'``               int    4.0    The number of replicas to use as a default
                                                     per datacenter if not specifically provided.
                                                     Note that this always defers to existing
                                                     definitions or explicit datacenter settings.
                                                     For example, to have three replicas per
                                                     datacenter, supply this with a value of 3.
===================================== ====== ====== =============================================

Note that when ``ALTER`` ing keyspaces and supplying ``replication_factor``,
auto-expansion will only *add* new datacenters for safety, it will not alter
existing datacenters or remove any even if they are no longer in the cluster.
If you want to remove datacenters while still supplying ``replication_factor``,
explicitly zero out the datacenter you want to have zero replicas.

An example of auto-expanding datacenters with two datacenters: ``DC1`` and ``DC2``::

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3}

    DESCRIBE KEYSPACE excalibur
        CREATE KEYSPACE excalibur WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3', 'DC2': '3'} AND durable_writes = true;


An example of auto-expanding and overriding a datacenter::

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3, 'DC2': 2}

    DESCRIBE KEYSPACE excalibur
        CREATE KEYSPACE excalibur WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3', 'DC2': '2'} AND durable_writes = true;

An example that excludes a datacenter while using ``replication_factor``::

    CREATE KEYSPACE excalibur
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 3, 'DC2': 0} ;

    DESCRIBE KEYSPACE excalibur
        CREATE KEYSPACE excalibur WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': '3'} AND durable_writes = true;

If transient replication has been enabled, transient replicas can be configured for both
``SimpleStrategy`` and ``NetworkTopologyStrategy`` by defining replication factors in the format ``'<total_replicas>/<transient_replicas>'``

For instance, this keyspace will have 3 replicas in DC1, 1 of which is transient, and 5 replicas in DC2, 2 of which are transient::

    CREATE KEYSPACE some_keysopace
               WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : '3/1'', 'DC2' : '5/2'};

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
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 4};

The supported options are the same than for :ref:`creating a keyspace <create-keyspace-statement>`.

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
   column_definition: `column_name` `cql_type` [ STATIC ] [ PRIMARY KEY]
   primary_key: `partition_key` [ ',' `clustering_columns` ]
   partition_key: `column_name`
                : | '(' `column_name` ( ',' `column_name` )* ')'
   clustering_columns: `column_name` ( ',' `column_name` )*
   table_options: COMPACT STORAGE [ AND `table_options` ]
                   : | CLUSTERING ORDER BY '(' `clustering_order` ')' [ AND `table_options` ]
                   : | `options`
   clustering_order: `column_name` (ASC | DESC) ( ',' `column_name` (ASC | DESC) )*

For instance::

    CREATE TABLE monkeySpecies (
        species text PRIMARY KEY,
        common_name text,
        population varint,
        average_size int
    ) WITH comment='Important biological records';

    CREATE TABLE timeline (
        userid uuid,
        posted_month int,
        posted_time uuid,
        body text,
        posted_by text,
        PRIMARY KEY (userid, posted_month, posted_time)
    ) WITH compaction = { 'class' : 'LeveledCompactionStrategy' };

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
        PRIMARY KEY (a, b, c)
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
> 1 and b <= 3``) to be very efficient.


.. _create-table-options:

Table options
~~~~~~~~~~~~~

A CQL table has a number of options that can be set at creation (and, for most of them, :ref:`altered
<alter-table-statement>` later). These options are specified after the ``WITH`` keyword.

Amongst those options, two important ones cannot be changed after creation and influence which queries can be done
against the table: the ``COMPACT STORAGE`` option and the ``CLUSTERING ORDER`` option. Those, as well as the other
options of a table are described in the following sections.

.. _compact-tables:

Compact tables
``````````````

.. warning:: Since Cassandra 3.0, compact tables have the exact same layout internally than non compact ones (for the
   same schema obviously), and declaring a table compact **only** creates artificial limitations on the table definition
   and usage. It only exists for historical reason and is preserved for backward compatibility And as ``COMPACT
   STORAGE`` cannot, as of Cassandra |version|, be removed, it is strongly discouraged to create new table with the
   ``COMPACT STORAGE`` option.

A *compact* table is one defined with the ``COMPACT STORAGE`` option. This option is only maintained for backward
compatibility for definitions created before CQL version 3 and shouldn't be used for new tables. Declaring a
table with this option creates limitations for the table which are largely arbitrary (and exists for historical
reasons). Amongst those limitation:

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

+--------------------------------+----------+-------------+-----------------------------------------------------------+
| option                         | kind     | default     | description                                               |
+================================+==========+=============+===========================================================+
| ``comment``                    | *simple* | none        | A free-form, human-readable comment.                      |
| ``speculative_retry``          | *simple* | 99PERCENTILE| :ref:`Speculative retry options                           |
|                                |          |             | <speculative-retry-options>`.                             |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``cdc``                        | *boolean*| false       | Create a Change Data Capture (CDC) log on the table.      |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``additional_write_policy``    | *simple* | 99PERCENTILE| :ref:`Speculative retry options                           |
|                                |          |             | <speculative-retry-options>`.                             |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``gc_grace_seconds``           | *simple* | 864000      | Time to wait before garbage collecting tombstones         |
|                                |          |             | (deletion markers).                                       |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``bloom_filter_fp_chance``     | *simple* | 0.00075     | The target probability of false positive of the sstable   |
|                                |          |             | bloom filters. Said bloom filters will be sized to provide|
|                                |          |             | the provided probability (thus lowering this value impact |
|                                |          |             | the size of bloom filters in-memory and on-disk)          |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``default_time_to_live``       | *simple* | 0           | The default expiration time (“TTL”) in seconds for a      |
|                                |          |             | table.                                                    |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``compaction``                 | *map*    | *see below* | :ref:`Compaction options <cql-compaction-options>`.       |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``compression``                | *map*    | *see below* | :ref:`Compression options <cql-compression-options>`.     |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``caching``                    | *map*    | *see below* | :ref:`Caching options <cql-caching-options>`.             |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``memtable_flush_period_in_ms``| *simple* | 0           | Time (in ms) before Cassandra flushes memtables to disk.  |
+--------------------------------+----------+-------------+-----------------------------------------------------------+
| ``read_repair``                | *simple* | BLOCKING    | Sets read repair behavior (see below)                     |
+--------------------------------+----------+-------------+-----------------------------------------------------------+

.. _speculative-retry-options:

Speculative retry options
#########################

By default, Cassandra read coordinators only query as many replicas as necessary to satisfy
consistency levels: one for consistency level ``ONE``, a quorum for ``QUORUM``, and so on.
``speculative_retry`` determines when coordinators may query additional replicas, which is useful
when replicas are slow or unresponsive.  Speculative retries are used to reduce the latency.  The speculative_retry option may be
used to configure rapid read protection with which a coordinator sends more requests than needed to satisfy the Consistency level.

Pre-4.0 speculative Retry Policy takes a single string as a parameter, this can be ``NONE``, ``ALWAYS``, ``99PERCENTILE`` (PERCENTILE), ``50MS`` (CUSTOM).

Examples of setting speculative retry are:

::

  ALTER TABLE users WITH speculative_retry = '10ms';


Or,

::

  ALTER TABLE users WITH speculative_retry = '99PERCENTILE';

The problem with these settings is when a single host goes into an unavailable state this drags up the percentiles. This means if we
are set to use ``p99`` alone, we might not speculate when we intended to to because the value at the specified percentile has gone so high.
As a fix 4.0 adds  support for hybrid ``MIN()``, ``MAX()`` speculative retry policies (`CASSANDRA-14293
<https://issues.apache.org/jira/browse/CASSANDRA-14293>`_). This means if the normal ``p99`` for the
table is <50ms, we will still speculate at this value and not drag the tail latencies up... but if the ``p99th`` goes above what we know we
should never exceed we use that instead.

In 4.0 the values (case-insensitive) discussed in the following table are supported:

============================ ======================== =============================================================================
 Format                       Example                  Description
============================ ======================== =============================================================================
 ``XPERCENTILE``             90.5PERCENTILE           Coordinators record average per-table response times for all replicas.
                                                      If a replica takes longer than ``X`` percent of this table's average
                                                      response time, the coordinator queries an additional replica.
                                                      ``X`` must be between 0 and 100.
 ``XP``                      90.5P                    Synonym for ``XPERCENTILE``
 ``Yms``                     25ms                     If a replica takes more than ``Y`` milliseconds to respond,
                                                      the coordinator queries an additional replica.
 ``MIN(XPERCENTILE,YMS)``    MIN(99PERCENTILE,35MS)   A hybrid policy that will use either the specified percentile or fixed
                                                      milliseconds depending on which value is lower at the time of calculation.
                                                      Parameters are ``XPERCENTILE``, ``XP``, or ``Yms``.
                                                      This is helpful to help protect against a single slow instance; in the
                                                      happy case the 99th percentile is normally lower than the specified
                                                      fixed value however, a slow host may skew the percentile very high
                                                      meaning the slower the cluster gets, the higher the value of the percentile,
                                                      and the higher the calculated time used to determine if we should
                                                      speculate or not. This allows us to set an upper limit that we want to
                                                      speculate at, but avoid skewing the tail latencies by speculating at the
                                                      lower value when the percentile is less than the specified fixed upper bound.
 ``MAX(XPERCENTILE,YMS)``    MAX(90.5P,25ms)          A hybrid policy that will use either the specified percentile or fixed
                                                      milliseconds depending on which value is higher at the time of calculation.
 ``ALWAYS``                                           Coordinators always query all replicas.
 ``NEVER``                                            Coordinators never query additional replicas.
============================ =================== =============================================================================

As of version 4.0 speculative retry allows more friendly params (`CASSANDRA-13876
<https://issues.apache.org/jira/browse/CASSANDRA-13876>`_). The ``speculative_retry`` is more flexible with case. As an example a
value does not have to be ``NONE``, and the following are supported alternatives.

::

  alter table users WITH speculative_retry = 'none';
  alter table users WITH speculative_retry = 'None';

The text component is case insensitive and for ``nPERCENTILE`` version 4.0 allows ``nP``, for instance ``99p``.
In a hybrid value for speculative retry, one of the two values must be a fixed millisecond value and the other a percentile value.

Some examples:

::

 min(99percentile,50ms)
 max(99p,50MS)
 MAX(99P,50ms)
 MIN(99.9PERCENTILE,50ms)
 max(90percentile,100MS)
 MAX(100.0PERCENTILE,60ms)

Two values of the same kind cannot be specified such as ``min(90percentile,99percentile)`` as it wouldn’t be a hybrid value.
This setting does not affect reads with consistency level ``ALL`` because they already query all replicas.

Note that frequently reading from additional replicas can hurt cluster performance.
When in doubt, keep the default ``99PERCENTILE``.


``additional_write_policy`` specifies the threshold at which a cheap quorum write will be upgraded to include transient replicas.

.. _cql-compaction-options:

Compaction options
##################

The ``compaction`` options must at least define the ``'class'`` sub-option, that defines the compaction strategy class
to use. The supported class are ``'SizeTieredCompactionStrategy'`` (:ref:`STCS <STCS>`),
``'LeveledCompactionStrategy'`` (:ref:`LCS <LCS>`) and ``'TimeWindowCompactionStrategy'`` (:ref:`TWCS <TWCS>`) (the
``'DateTieredCompactionStrategy'`` is also supported but is deprecated and ``'TimeWindowCompactionStrategy'`` should be
preferred instead). The default is ``'SizeTieredCompactionStrategy'``. Custom strategy can be provided by specifying the full class name as a :ref:`string constant
<constants>`.

All default strategies support a number of :ref:`common options <compaction-options>`, as well as options specific to
the strategy chosen (see the section corresponding to your strategy for details: :ref:`STCS <stcs-options>`, :ref:`LCS
<lcs-options>` and :ref:`TWCS <TWCS>`).

.. _cql-compression-options:

Compression options
###################

The ``compression`` options define if and how the sstables of the table are compressed. Compression is configured on a per-table
basis as an optional argument to ``CREATE TABLE`` or ``ALTER TABLE``. The following sub-options are
available:

========================= =============== =============================================================================
 Option                    Default         Description
========================= =============== =============================================================================
 ``class``                 LZ4Compressor   The compression algorithm to use. Default compressor are: LZ4Compressor,
                                           SnappyCompressor, DeflateCompressor and ZstdCompressor. Use ``'enabled' : false`` to disable
                                           compression. Custom compressor can be provided by specifying the full class
                                           name as a “string constant”:#constants.

 ``enabled``               true            Enable/disable sstable compression. If the ``enabled`` option is set to ``false`` no other
                                           options must be specified.

 ``chunk_length_in_kb``    64              On disk SSTables are compressed by block (to allow random reads). This
                                           defines the size (in KB) of said block. Bigger values may improve the
                                           compression rate, but increases the minimum size of data to be read from disk
                                           for a read. The default value is an optimal value for compressing tables. Chunk length must
                                           be a power of 2 because so is assumed so when computing the chunk number from an uncompressed
                                           file offset.  Block size may be adjusted based on read/write access patterns such as:

                                             - How much data is typically requested at once
                                             - Average size of rows in the table

 ``crc_check_chance``      1.0             Determines how likely Cassandra is to verify the checksum on each compression chunk during
                                           reads.

  ``compression_level``    3               Compression level. It is only applicable for ``ZstdCompressor`` and accepts values between
                                           ``-131072`` and ``22``.
========================= =============== =============================================================================


For instance, to create a table with LZ4Compressor and a chunk_lenth_in_kb of 4KB::

   CREATE TABLE simple (
      id int,
      key text,
      value text,
      PRIMARY KEY (key, value)
   ) with compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 4};


.. _cql-caching-options:

Caching options
###############

Caching optimizes the use of cache memory of a table. The cached data is weighed by size and access frequency. The ``caching``
options allows to configure both the *key cache* and the *row cache* for the table. The following
sub-options are available:

======================== ========= ====================================================================================
 Option                   Default   Description
======================== ========= ====================================================================================
 ``keys``                 ALL       Whether to cache keys (“key cache”) for this table. Valid values are: ``ALL`` and
                                    ``NONE``.
 ``rows_per_partition``   NONE      The amount of rows to cache per partition (“row cache”). If an integer ``n`` is
                                    specified, the first ``n`` queried rows of a partition will be cached. Other
                                    possible options are ``ALL``, to cache all rows of a queried partition, or ``NONE``
                                    to disable row caching.
======================== ========= ====================================================================================


For instance, to create a table with both a key cache and 10 rows per partition::

    CREATE TABLE simple (
    id int,
    key text,
    value text,
    PRIMARY KEY (key, value)
    ) WITH caching = {'keys': 'ALL', 'rows_per_partition': 10};


Read Repair options
###################

The ``read_repair`` options configures the read repair behavior to allow tuning for various performance and
consistency behaviors. Two consistency properties are affected by read repair behavior.

- Monotonic Quorum Reads: Provided by ``BLOCKING``. Monotonic quorum reads prevents reads from appearing to go back
  in time in some circumstances. When monotonic quorum reads are not provided and a write fails to reach a quorum of
  replicas, it may be visible in one read, and then disappear in a subsequent read.
- Write Atomicity: Provided by ``NONE``. Write atomicity prevents reads from returning partially applied writes.
  Cassandra attempts to provide partition level write atomicity, but since only the data covered by a SELECT statement
  is repaired by a read repair, read repair can break write atomicity when data is read at a more granular level than it
  is written. For example read repair can break write atomicity if you write multiple rows to a clustered partition in a
  batch, but then select a single row by specifying the clustering column in a SELECT statement.

The available read repair settings are:

Blocking
````````
The default setting. When ``read_repair`` is set to ``BLOCKING``, and a read repair is triggered, the read will block
on writes sent to other replicas until the CL is reached by the writes. Provides monotonic quorum reads, but not partition
level write atomicity

None
````

When ``read_repair`` is set to ``NONE``, the coordinator will reconcile any differences between replicas, but will not
attempt to repair them. Provides partition level write atomicity, but not monotonic quorum reads.


Other considerations:
#####################

- Adding new columns (see ``ALTER TABLE`` below) is a constant time operation. There is thus no need to try to
  anticipate future usage when creating a table.

.. _alter-table-statement:

ALTER TABLE
^^^^^^^^^^^

Altering an existing table uses the ``ALTER TABLE`` statement:

.. productionlist::
   alter_table_statement: ALTER TABLE `table_name` `alter_table_instruction`
   alter_table_instruction: ADD `column_name` `cql_type` ( ',' `column_name` `cql_type` )*
                          : | DROP `column_name` ( `column_name` )*
                          : | WITH `options`

For instance::

    ALTER TABLE addamsFamily ADD gravesite varchar;

    ALTER TABLE addamsFamily
           WITH comment = 'A most excellent and useful table';

The ``ALTER TABLE`` statement can:

- Add new column(s) to the table (through the ``ADD`` instruction). Note that the primary key of a table cannot be
  changed and thus newly added column will, by extension, never be part of the primary key. Also note that :ref:`compact
  tables <compact-tables>` have restrictions regarding column addition. Note that this is constant (in the amount of
  data the cluster contains) time operation.
- Remove column(s) from the table. This drops both the column and all its content, but note that while the column
  becomes immediately unavailable, its content is only removed lazily during compaction. Please also see the warnings
  below. Due to lazy removal, the altering itself is a constant (in the amount of data removed or contained in the
  cluster) time operation.
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
   **unless** the type of the dropped column was a (non-frozen) column (due to an internal technical limitation).


.. _drop-table-statement:

DROP TABLE
^^^^^^^^^^

Dropping a table uses the ``DROP TABLE`` statement:

.. productionlist::
   drop_table_statement: DROP TABLE [ IF EXISTS ] `table_name`

Dropping a table results in the immediate, irreversible removal of the table, including all data it contains.

If the table does not exist, the statement will return an error, unless ``IF EXISTS`` is used in which case the
operation is a no-op.

.. _truncate-statement:

TRUNCATE
^^^^^^^^

A table can be truncated using the ``TRUNCATE`` statement:

.. productionlist::
   truncate_statement: TRUNCATE [ TABLE ] `table_name`

Note that ``TRUNCATE TABLE foo`` is allowed for consistency with other DDL statements but tables are the only object
that can be truncated currently and so the ``TABLE`` keyword can be omitted.

Truncating a table permanently removes all existing data from the table, but without removing the table itself.
