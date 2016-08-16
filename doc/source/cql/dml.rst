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

.. _data-manipulation:

Data Manipulation
-----------------

This section describes the statements supported by CQL to insert, update, delete and query data.

.. _select-statement:

SELECT
^^^^^^

Querying data from data is done using a ``SELECT`` statement:

.. productionlist::
   select_statement: SELECT [ JSON | DISTINCT ] ( `select_clause` | '*' )
                   : FROM `table_name`
                   : [ WHERE `where_clause` ]
                   : [ GROUP BY `group_by_clause` ]
                   : [ ORDER BY `ordering_clause` ]
                   : [ PER PARTITION LIMIT (`integer` | `bind_marker`) ]
                   : [ LIMIT (`integer` | `bind_marker`) ]
                   : [ ALLOW FILTERING ]
   select_clause: `selector` [ AS `identifier` ] ( ',' `selector` [ AS `identifier` ] )
   selector: `column_name`
           : | `term`
           : | CAST '(' `selector` AS `cql_type` ')'
           : | `function_name` '(' [ `selector` ( ',' `selector` )* ] ')'
           : | COUNT '(' '*' ')'
   where_clause: `relation` ( AND `relation` )*
   relation: `column_name` `operator` `term`
           : '(' `column_name` ( ',' `column_name` )* ')' `operator` `tuple_literal`
           : TOKEN '(' `column_name` ( ',' `column_name` )* ')' `operator` `term`
   operator: '=' | '<' | '>' | '<=' | '>=' | '!=' | IN | CONTAINS | CONTAINS KEY
   group_by_clause: `column_name` ( ',' `column_name` )*
   ordering_clause: `column_name` [ ASC | DESC ] ( ',' `column_name` [ ASC | DESC ] )*

For instance::

    SELECT name, occupation FROM users WHERE userid IN (199, 200, 207);
    SELECT JSON name, occupation FROM users WHERE userid = 199;
    SELECT name AS user_name, occupation AS user_occupation FROM users;

    SELECT time, value
    FROM events
    WHERE event_type = 'myEvent'
      AND time > '2011-02-03'
      AND time <= '2012-01-01'

    SELECT COUNT (*) AS user_count FROM users;

The ``SELECT`` statements reads one or more columns for one or more rows in a table. It returns a result-set of the rows
matching the request, where each row contains the values for the selection corresponding to the query. Additionally,
:ref:`functions <cql-functions>` including :ref:`aggregation <aggregate-functions>` ones can be applied to the result.

A ``SELECT`` statement contains at least a :ref:`selection clause <selection-clause>` and the name of the table on which
the selection is on (note that CQL does **not** joins or sub-queries and thus a select statement only apply to a single
table). In most case, a select will also have a :ref:`where clause <where-clause>` and it can optionally have additional
clauses to :ref:`order <ordering-clause>` or :ref:`limit <limit-clause>` the results. Lastly, :ref:`queries that require
filtering <allow-filtering>` can be allowed if the ``ALLOW FILTERING`` flag is provided.

.. _selection-clause:

Selection clause
~~~~~~~~~~~~~~~~

The :token:`select_clause` determines which columns needs to be queried and returned in the result-set, as well as any
transformation to apply to this result before returning. It consists of a comma-separated list of *selectors* or,
alternatively, of the wildcard character (``*``) to select all the columns defined in the table.

Selectors
`````````

A :token:`selector` can be one of:

- A column name of the table selected, to retrieve the values for that column.
- A term, which is usually used nested inside other selectors like functions (if a term is selected directly, then the
  corresponding column of the result-set will simply have the value of this term for every row returned).
- A casting, which allows to convert a nested selector to a (compatible) type.
- A function call, where the arguments are selector themselves. See the section on :ref:`functions <cql-functions>` for
  more details.
- The special call ``COUNT(*)`` to the :ref:`COUNT function <count-function>`, which counts all non-null results.

Aliases
```````

Every *top-level* selector can also be aliased (using `AS`). If so, the name of the corresponding column in the result
set will be that of the alias. For instance::

    // Without alias
    SELECT intAsBlob(4) FROM t;

    //  intAsBlob(4)
    // --------------
    //  0x00000004

    // With alias
    SELECT intAsBlob(4) AS four FROM t;

    //  four
    // ------------
    //  0x00000004

.. note:: Currently, aliases aren't recognized anywhere else in the statement where they are used (not in the ``WHERE``
   clause, not in the ``ORDER BY`` clause, ...). You must use the orignal column name instead.


``WRITETIME`` and ``TTL`` function
```````````````````````````````````

Selection supports two special functions (that aren't allowed anywhere else): ``WRITETIME`` and ``TTL``. Both function
take only one argument and that argument *must* be a column name (so for instance ``TTL(3)`` is invalid).

Those functions allow to retrieve meta-information that are stored internally for each column, namely:

- the timestamp of the value of the column for ``WRITETIME``.
- the remaining time to live (in seconds) for the value of the column if it set to expire (and ``null`` otherwise).

.. _where-clause:

The ``WHERE`` clause
~~~~~~~~~~~~~~~~~~~~

The ``WHERE`` clause specifies which rows must be queried. It is composed of relations on the columns that are part of
the ``PRIMARY KEY`` and/or have a `secondary index <#createIndexStmt>`__ defined on them.

Not all relations are allowed in a query. For instance, non-equal relations (where ``IN`` is considered as an equal
relation) on a partition key are not supported (but see the use of the ``TOKEN`` method below to do non-equal queries on
the partition key). Moreover, for a given partition key, the clustering columns induce an ordering of rows and relations
on them is restricted to the relations that allow to select a **contiguous** (for the ordering) set of rows. For
instance, given::

    CREATE TABLE posts (
        userid text,
        blog_title text,
        posted_at timestamp,
        entry_title text,
        content text,
        category int,
        PRIMARY KEY (userid, blog_title, posted_at)
    )

The following query is allowed::

    SELECT entry_title, content FROM posts
     WHERE userid = 'john doe'
       AND blog_title='John''s Blog'
       AND posted_at >= '2012-01-01' AND posted_at < '2012-01-31'

But the following one is not, as it does not select a contiguous set of rows (and we suppose no secondary indexes are
set)::

    // Needs a blog_title to be set to select ranges of posted_at
    SELECT entry_title, content FROM posts
     WHERE userid = 'john doe'
       AND posted_at >= '2012-01-01' AND posted_at < '2012-01-31'

When specifying relations, the ``TOKEN`` function can be used on the ``PARTITION KEY`` column to query. In that case,
rows will be selected based on the token of their ``PARTITION_KEY`` rather than on the value. Note that the token of a
key depends on the partitioner in use, and that in particular the RandomPartitioner won't yield a meaningful order. Also
note that ordering partitioners always order token values by bytes (so even if the partition key is of type int,
``token(-1) > token(0)`` in particular). Example::

    SELECT * FROM posts
     WHERE token(userid) > token('tom') AND token(userid) < token('bob')

Moreover, the ``IN`` relation is only allowed on the last column of the partition key and on the last column of the full
primary key.

It is also possible to “group” ``CLUSTERING COLUMNS`` together in a relation using the tuple notation. For instance::

    SELECT * FROM posts
     WHERE userid = 'john doe'
       AND (blog_title, posted_at) > ('John''s Blog', '2012-01-01')

will request all rows that sorts after the one having “John's Blog” as ``blog_tile`` and '2012-01-01' for ``posted_at``
in the clustering order. In particular, rows having a ``post_at <= '2012-01-01'`` will be returned as long as their
``blog_title > 'John''s Blog'``, which would not be the case for::

    SELECT * FROM posts
     WHERE userid = 'john doe'
       AND blog_title > 'John''s Blog'
       AND posted_at > '2012-01-01'

The tuple notation may also be used for ``IN`` clauses on clustering columns::

    SELECT * FROM posts
     WHERE userid = 'john doe'
       AND (blog_title, posted_at) IN (('John''s Blog', '2012-01-01'), ('Extreme Chess', '2014-06-01'))

The ``CONTAINS`` operator may only be used on collection columns (lists, sets, and maps). In the case of maps,
``CONTAINS`` applies to the map values. The ``CONTAINS KEY`` operator may only be used on map columns and applies to the
map keys.

.. _group-by-clause:

Grouping results
~~~~~~~~~~~~~~~~

The ``GROUP BY`` option allows to condense into a single row all selected rows that share the same values for a set
of columns.

Using the ``GROUP BY`` option, it is only possible to group rows at the partition key level or at a clustering column
level. By consequence, the ``GROUP BY`` option only accept as arguments primary key column names in the primary key
order. If a primary key column is restricted by an equality restriction it is not required to be present in the
``GROUP BY`` clause.

Aggregate functions will produce a separate value for each group. If no ``GROUP BY`` clause is specified,
aggregates functions will produce a single value for all the rows.

If a column is selected without an aggregate function, in a statement with a ``GROUP BY``, the first value encounter
in each group will be returned.

.. _ordering-clause:

Ordering results
~~~~~~~~~~~~~~~~

The ``ORDER BY`` clause allows to select the order of the returned results. It takes as argument a list of column names
along with the order for the column (``ASC`` for ascendant and ``DESC`` for descendant, omitting the order being
equivalent to ``ASC``). Currently the possible orderings are limited by the :ref:`clustering order <clustering-order>`
defined on the table:

- if the table has been defined without any specific ``CLUSTERING ORDER``, then then allowed orderings are the order
  induced by the clustering columns and the reverse of that one.
- otherwise, the orderings allowed are the order of the ``CLUSTERING ORDER`` option and the reversed one.

.. _limit-clause:

Limiting results
~~~~~~~~~~~~~~~~

The ``LIMIT`` option to a ``SELECT`` statement limits the number of rows returned by a query, while the ``PER PARTITION
LIMIT`` option limits the number of rows returned for a given partition by the query. Note that both type of limit can
used in the same statement.

.. _allow-filtering:

Allowing filtering
~~~~~~~~~~~~~~~~~~

By default, CQL only allows select queries that don't involve “filtering” server side, i.e. queries where we know that
all (live) record read will be returned (maybe partly) in the result set. The reasoning is that those “non filtering”
queries have predictable performance in the sense that they will execute in a time that is proportional to the amount of
data **returned** by the query (which can be controlled through ``LIMIT``).

The ``ALLOW FILTERING`` option allows to explicitly allow (some) queries that require filtering. Please note that a
query using ``ALLOW FILTERING`` may thus have unpredictable performance (for the definition above), i.e. even a query
that selects a handful of records **may** exhibit performance that depends on the total amount of data stored in the
cluster.

For instance, considering the following table holding user profiles with their year of birth (with a secondary index on
it) and country of residence::

    CREATE TABLE users (
        username text PRIMARY KEY,
        firstname text,
        lastname text,
        birth_year int,
        country text
    )

    CREATE INDEX ON users(birth_year);

Then the following queries are valid::

    SELECT * FROM users;
    SELECT * FROM users WHERE birth_year = 1981;

because in both case, Cassandra guarantees that these queries performance will be proportional to the amount of data
returned. In particular, if no users are born in 1981, then the second query performance will not depend of the number
of user profile stored in the database (not directly at least: due to secondary index implementation consideration, this
query may still depend on the number of node in the cluster, which indirectly depends on the amount of data stored.
Nevertheless, the number of nodes will always be multiple number of magnitude lower than the number of user profile
stored). Of course, both query may return very large result set in practice, but the amount of data returned can always
be controlled by adding a ``LIMIT``.

However, the following query will be rejected::

    SELECT * FROM users WHERE birth_year = 1981 AND country = 'FR';

because Cassandra cannot guarantee that it won't have to scan large amount of data even if the result to those query is
small. Typically, it will scan all the index entries for users born in 1981 even if only a handful are actually from
France. However, if you “know what you are doing”, you can force the execution of this query by using ``ALLOW
FILTERING`` and so the following query is valid::

    SELECT * FROM users WHERE birth_year = 1981 AND country = 'FR' ALLOW FILTERING;

.. _insert-statement:

INSERT
^^^^^^

Inserting data for a row is done using an ``INSERT`` statement:

.. productionlist::
   insert_statement: INSERT INTO `table_name` ( `names_values` | `json_clause` )
                   : [ IF NOT EXISTS ]
                   : [ USING `update_parameter` ( AND `update_parameter` )* ]
   names_values: `names` VALUES `tuple_literal`
   json_clause: JSON `string` [ DEFAULT ( NULL | UNSET ) ]
   names: '(' `column_name` ( ',' `column_name` )* ')'

For instance::

    INSERT INTO NerdMovies (movie, director, main_actor, year)
                    VALUES ('Serenity', 'Joss Whedon', 'Nathan Fillion', 2005)
          USING TTL 86400;

    INSERT INTO NerdMovies JSON '{"movie": "Serenity",
                                  "director": "Joss Whedon",
                                  "year": 2005}';

The ``INSERT`` statement writes one or more columns for a given row in a table. Note that since a row is identified by
its ``PRIMARY KEY``, at least the columns composing it must be specified. The list of columns to insert to must be
supplied when using the ``VALUES`` syntax. When using the ``JSON`` syntax, they are optional. See the
section on :ref:`JSON support <cql-json>` for more detail.

Note that unlike in SQL, ``INSERT`` does not check the prior existence of the row by default: the row is created if none
existed before, and updated otherwise. Furthermore, there is no mean to know which of creation or update happened.

It is however possible to use the ``IF NOT EXISTS`` condition to only insert if the row does not exist prior to the
insertion. But please note that using ``IF NOT EXISTS`` will incur a non negligible performance cost (internally, Paxos
will be used) so this should be used sparingly.

All updates for an ``INSERT`` are applied atomically and in isolation.

Please refer to the :ref:`UPDATE <update-parameters>` section for informations on the :token:`update_parameter`.

Also note that ``INSERT`` does not support counters, while ``UPDATE`` does.

.. _update-statement:

UPDATE
^^^^^^

Updating a row is done using an ``UPDATE`` statement:

.. productionlist::
   update_statement: UPDATE `table_name`
                   : [ USING `update_parameter` ( AND `update_parameter` )* ]
                   : SET `assignment` ( ',' `assignment` )*
                   : WHERE `where_clause`
                   : [ IF ( EXISTS | `condition` ( AND `condition` )*) ]
   update_parameter: ( TIMESTAMP | TTL ) ( `integer` | `bind_marker` )
   assignment: `simple_selection` '=' `term`
             :| `column_name` '=' `column_name` ( '+' | '-' ) `term`
             :| `column_name` '=' `list_literal` '+' `column_name`
   simple_selection: `column_name`
                   :| `column_name` '[' `term` ']'
                   :| `column_name` '.' `field_name
   condition: `simple_selection` `operator` `term`

For instance::

    UPDATE NerdMovies USING TTL 400
       SET director   = 'Joss Whedon',
           main_actor = 'Nathan Fillion',
           year       = 2005
     WHERE movie = 'Serenity';

    UPDATE UserActions
       SET total = total + 2
       WHERE user = B70DE1D0-9908-4AE3-BE34-5573E5B09F14
         AND action = 'click';

The ``UPDATE`` statement writes one or more columns for a given row in a table. The :token:`where_clause` is used to
select the row to update and must include all columns composing the ``PRIMARY KEY``. Non primary key columns are then
set using the ``SET`` keyword.

Note that unlike in SQL, ``UPDATE`` does not check the prior existence of the row by default (except through ``IF``, see
below): the row is created if none existed before, and updated otherwise. Furthermore, there are no means to know
whether a creation or update occurred.

It is however possible to use the conditions on some columns through ``IF``, in which case the row will not be updated
unless the conditions are met. But, please note that using ``IF`` conditions will incur a non-negligible performance
cost (internally, Paxos will be used) so this should be used sparingly.

In an ``UPDATE`` statement, all updates within the same partition key are applied atomically and in isolation.

Regarding the :token:`assignment`:

- ``c = c + 3`` is used to increment/decrement counters. The column name after the '=' sign **must** be the same than
  the one before the '=' sign. Note that increment/decrement is only allowed on counters, and are the *only* update
  operations allowed on counters. See the section on :ref:`counters <counters>` for details.
- ``id = id + <some-collection>`` and ``id[value1] = value2`` are for collections, see the :ref:`relevant section
  <collections>` for details.
- ``id.field = 3`` is for setting the value of a field on a non-frozen user-defined types. see the :ref:`relevant section
  <udts>` for details.

.. _update-parameters:

Update parameters
~~~~~~~~~~~~~~~~~

The ``UPDATE``, ``INSERT`` (and ``DELETE`` and ``BATCH`` for the ``TIMESTAMP``) statements support the following
parameters:

- ``TIMESTAMP``: sets the timestamp for the operation. If not specified, the coordinator will use the current time (in
  microseconds) at the start of statement execution as the timestamp. This is usually a suitable default.
- ``TTL``: specifies an optional Time To Live (in seconds) for the inserted values. If set, the inserted values are
  automatically removed from the database after the specified time. Note that the TTL concerns the inserted values, not
  the columns themselves. This means that any subsequent update of the column will also reset the TTL (to whatever TTL
  is specified in that update). By default, values never expire. A TTL of 0 is equivalent to no TTL. If the table has a
  default_time_to_live, a TTL of 0 will remove the TTL for the inserted or updated values. A TTL of ``null`` is equivalent
  to inserting with a TTL of 0.

.. _delete_statement:

DELETE
^^^^^^

Deleting rows or parts of rows uses the ``DELETE`` statement:

.. productionlist::
   delete_statement: DELETE [ `simple_selection` ( ',' `simple_selection` ) ]
                   : FROM `table_name`
                   : [ USING `update_parameter` ( AND `update_parameter` )* ]
                   : WHERE `where_clause`
                   : [ IF ( EXISTS | `condition` ( AND `condition` )*) ]

For instance::

    DELETE FROM NerdMovies USING TIMESTAMP 1240003134
     WHERE movie = 'Serenity';

    DELETE phone FROM Users
     WHERE userid IN (C73DE1D3-AF08-40F3-B124-3FF3E5109F22, B70DE1D0-9908-4AE3-BE34-5573E5B09F14);

The ``DELETE`` statement deletes columns and rows. If column names are provided directly after the ``DELETE`` keyword,
only those columns are deleted from the row indicated by the ``WHERE`` clause. Otherwise, whole rows are removed.

The ``WHERE`` clause specifies which rows are to be deleted. Multiple rows may be deleted with one statement by using an
``IN`` operator. A range of rows may be deleted using an inequality operator (such as ``>=``).

``DELETE`` supports the ``TIMESTAMP`` option with the same semantics as in :ref:`updates <update-parameters>`.

In a ``DELETE`` statement, all deletions within the same partition key are applied atomically and in isolation.

A ``DELETE`` operation can be conditional through the use of an ``IF`` clause, similar to ``UPDATE`` and ``INSERT``
statements. However, as with ``INSERT`` and ``UPDATE`` statements, this will incur a non-negligible performance cost
(internally, Paxos will be used) and so should be used sparingly.

.. _batch_statement:

BATCH
^^^^^

Multiple ``INSERT``, ``UPDATE`` and ``DELETE`` can be executed in a single statement by grouping them through a
``BATCH`` statement:

.. productionlist::
   batch_statement: BEGIN [ UNLOGGED | COUNTER ] BATCH
                   : [ USING `update_parameter` ( AND `update_parameter` )* ]
                   : `modification_statement` ( ';' `modification_statement` )*
                   : APPLY BATCH
   modification_statement: `insert_statement` | `update_statement` | `delete_statement`

For instance::

    BEGIN BATCH
       INSERT INTO users (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');
       UPDATE users SET password = 'ps22dhds' WHERE userid = 'user3';
       INSERT INTO users (userid, password) VALUES ('user4', 'ch@ngem3c');
       DELETE name FROM users WHERE userid = 'user1';
    APPLY BATCH;

The ``BATCH`` statement group multiple modification statements (insertions/updates and deletions) into a single
statement. It serves several purposes:

- It saves network round-trips between the client and the server (and sometimes between the server coordinator and the
  replicas) when batching multiple updates.
- All updates in a ``BATCH`` belonging to a given partition key are performed in isolation.
- By default, all operations in the batch are performed as *logged*, to ensure all mutations eventually complete (or
  none will). See the notes on :ref:`UNLOGGED batches <unlogged-batches>` for more details.

Note that:

- ``BATCH`` statements may only contain ``UPDATE``, ``INSERT`` and ``DELETE`` statements (not other batches for instance).
- Batches are *not* a full analogue for SQL transactions.
- If a timestamp is not specified for each operation, then all operations will be applied with the same timestamp
  (either one generated automatically, or the timestamp provided at the batch level). Due to Cassandra's conflict
  resolution procedure in the case of `timestamp ties <http://wiki.apache.org/cassandra/FAQ#clocktie>`__, operations may
  be applied in an order that is different from the order they are listed in the ``BATCH`` statement. To force a
  particular operation ordering, you must specify per-operation timestamps.

.. _unlogged-batches:

``UNLOGGED`` batches
~~~~~~~~~~~~~~~~~~~~

By default, Cassandra uses a batch log to ensure all operations in a batch eventually complete or none will (note
however that operations are only isolated within a single partition).

There is a performance penalty for batch atomicity when a batch spans multiple partitions. If you do not want to incur
this penalty, you can tell Cassandra to skip the batchlog with the ``UNLOGGED`` option. If the ``UNLOGGED`` option is
used, a failed batch might leave the patch only partly applied.

``COUNTER`` batches
~~~~~~~~~~~~~~~~~~~

Use the ``COUNTER`` option for batched counter updates. Unlike other
updates in Cassandra, counter updates are not idempotent.
