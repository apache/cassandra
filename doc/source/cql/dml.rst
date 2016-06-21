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

.. _data-manipulation:

Data Manipulation
-----------------

SELECT
^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= SELECT ( JSON )? 
|  FROM 
|  ( WHERE )?
|  ( ORDER BY )?
|  ( PER PARTITION LIMIT )?
|  ( LIMIT )?
|  ( ALLOW FILTERING )?

|  ::= DISTINCT? 
|  \| COUNT ‘(’ ( ‘\*’ \| ‘1’ ) ‘)’ (AS )?

|  ::= (AS )? ( ‘,’ (AS )? )\*
|  \| ‘\*’

|  ::= 
|  \| WRITETIME ‘(’ ‘)’
|  \| TTL ‘(’ ‘)’
|  \| CAST ‘(’ AS ‘)’
|  \| ‘(’ ( (‘,’ )\*)? ‘)’

 ::= ( AND )\*

|  ::= 
|  \| ‘(’ (‘,’ )\* ‘)’ 
|  \| IN ‘(’ ( ( ‘,’ )\* )? ‘)’
|  \| ‘(’ (‘,’ )\* ‘)’ IN ‘(’ ( ( ‘,’ )\* )? ‘)’
|  \| TOKEN ‘(’ ( ‘,’ )\* ‘)’ 

|  ::= ‘=’ \| ‘<’ \| ‘>’ \| ‘<=’ \| ‘>=’ \| CONTAINS \| CONTAINS KEY
|  ::= ( ‘,’ )\*
|  ::= ( ASC \| DESC )?
|  ::= ‘(’ (‘,’ )\* ‘)’
| p.
| *Sample:*

| bc(sample)..
| SELECT name, occupation FROM users WHERE userid IN (199, 200, 207);

SELECT JSON name, occupation FROM users WHERE userid = 199;

SELECT name AS user\_name, occupation AS user\_occupation FROM users;

| SELECT time, value
| FROM events
| WHERE event\_type = ‘myEvent’
|  AND time > ‘2011-02-03’
|  AND time <= ‘2012-01-01’

SELECT COUNT (\*) FROM users;

SELECT COUNT (\*) AS user\_count FROM users;

The ``SELECT`` statements reads one or more columns for one or more rows
in a table. It returns a result-set of rows, where each row contains the
collection of columns corresponding to the query. If the ``JSON``
keyword is used, the results for each row will contain only a single
column named “json”. See the section on
```SELECT JSON`` <#selectJson>`__ for more details.

``<select-clause>``
~~~~~~~~~~~~~~~~~~~

The ``<select-clause>`` determines which columns needs to be queried and
returned in the result-set. It consists of either the comma-separated
list of or the wildcard character (``*``) to select all the columns
defined for the table.

A ``<selector>`` is either a column name to retrieve or a ``<function>``
of one or more ``<term>``\ s. The function allowed are the same as for
``<term>`` and are described in the `function section <#functions>`__.
In addition to these generic functions, the ``WRITETIME`` (resp.
``TTL``) function allows to select the timestamp of when the column was
inserted (resp. the time to live (in seconds) for the column (or null if
the column has no expiration set)) and the ```CAST`` <#castFun>`__
function can be used to convert one data type to another.

Any ``<selector>`` can be aliased using ``AS`` keyword (see examples).
Please note that ``<where-clause>`` and ``<order-by>`` clause should
refer to the columns by their original names and not by their aliases.

The ``COUNT`` keyword can be used with parenthesis enclosing ``*``. If
so, the query will return a single result: the number of rows matching
the query. Note that ``COUNT(1)`` is supported as an alias.

``<where-clause>``
~~~~~~~~~~~~~~~~~~

The ``<where-clause>`` specifies which rows must be queried. It is
composed of relations on the columns that are part of the
``PRIMARY KEY`` and/or have a `secondary index <#createIndexStmt>`__
defined on them.

Not all relations are allowed in a query. For instance, non-equal
relations (where ``IN`` is considered as an equal relation) on a
partition key are not supported (but see the use of the ``TOKEN`` method
below to do non-equal queries on the partition key). Moreover, for a
given partition key, the clustering columns induce an ordering of rows
and relations on them is restricted to the relations that allow to
select a **contiguous** (for the ordering) set of rows. For instance,
given

| bc(sample).
| CREATE TABLE posts (
|  userid text,
|  blog\_title text,
|  posted\_at timestamp,
|  entry\_title text,
|  content text,
|  category int,
|  PRIMARY KEY (userid, blog\_title, posted\_at)
| )

The following query is allowed:

| bc(sample).
| SELECT entry\_title, content FROM posts WHERE userid=‘john doe’ AND
  blog\_title=‘John’‘s Blog’ AND posted\_at >= ‘2012-01-01’ AND
  posted\_at < ‘2012-01-31’

But the following one is not, as it does not select a contiguous set of
rows (and we suppose no secondary indexes are set):

| bc(sample).
| // Needs a blog\_title to be set to select ranges of posted\_at
| SELECT entry\_title, content FROM posts WHERE userid=‘john doe’ AND
  posted\_at >= ‘2012-01-01’ AND posted\_at < ‘2012-01-31’

When specifying relations, the ``TOKEN`` function can be used on the
``PARTITION KEY`` column to query. In that case, rows will be selected
based on the token of their ``PARTITION_KEY`` rather than on the value.
Note that the token of a key depends on the partitioner in use, and that
in particular the RandomPartitioner won’t yield a meaningful order. Also
note that ordering partitioners always order token values by bytes (so
even if the partition key is of type int, ``token(-1) > token(0)`` in
particular). Example:

| bc(sample).
| SELECT \* FROM posts WHERE token(userid) > token(‘tom’) AND
  token(userid) < token(‘bob’)

Moreover, the ``IN`` relation is only allowed on the last column of the
partition key and on the last column of the full primary key.

It is also possible to “group” ``CLUSTERING COLUMNS`` together in a
relation using the tuple notation. For instance:

| bc(sample).
| SELECT \* FROM posts WHERE userid=‘john doe’ AND (blog\_title,
  posted\_at) > (‘John’‘s Blog’, ‘2012-01-01’)

will request all rows that sorts after the one having “John’s Blog” as
``blog_tile`` and ‘2012-01-01’ for ``posted_at`` in the clustering
order. In particular, rows having a ``post_at <= '2012-01-01'`` will be
returned as long as their ``blog_title > 'John''s Blog'``, which
wouldn’t be the case for:

| bc(sample).
| SELECT \* FROM posts WHERE userid=‘john doe’ AND blog\_title >
  ‘John’‘s Blog’ AND posted\_at > ‘2012-01-01’

The tuple notation may also be used for ``IN`` clauses on
``CLUSTERING COLUMNS``:

| bc(sample).
| SELECT \* FROM posts WHERE userid=‘john doe’ AND (blog\_title,
  posted\_at) IN ((‘John’‘s Blog’, ‘2012-01-01), (’Extreme Chess’,
  ‘2014-06-01’))

The ``CONTAINS`` operator may only be used on collection columns (lists,
sets, and maps). In the case of maps, ``CONTAINS`` applies to the map
values. The ``CONTAINS KEY`` operator may only be used on map columns
and applies to the map keys.

``<order-by>``
~~~~~~~~~~~~~~

The ``ORDER BY`` option allows to select the order of the returned
results. It takes as argument a list of column names along with the
order for the column (``ASC`` for ascendant and ``DESC`` for descendant,
omitting the order being equivalent to ``ASC``). Currently the possible
orderings are limited (which depends on the table
```CLUSTERING ORDER`` <#createTableOptions>`__ ):

-  if the table has been defined without any specific
   ``CLUSTERING ORDER``, then then allowed orderings are the order
   induced by the clustering columns and the reverse of that one.
-  otherwise, the orderings allowed are the order of the
   ``CLUSTERING ORDER`` option and the reversed one.

``LIMIT`` and ``PER PARTITION LIMIT``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``LIMIT`` option to a ``SELECT`` statement limits the number of rows
returned by a query, while the ``PER PARTITION LIMIT`` option limits the
number of rows returned for a given partition by the query. Note that
both type of limit can used in the same statement.

``ALLOW FILTERING``
~~~~~~~~~~~~~~~~~~~

By default, CQL only allows select queries that don’t involve
“filtering” server side, i.e. queries where we know that all (live)
record read will be returned (maybe partly) in the result set. The
reasoning is that those “non filtering” queries have predictable
performance in the sense that they will execute in a time that is
proportional to the amount of data **returned** by the query (which can
be controlled through ``LIMIT``).

The ``ALLOW FILTERING`` option allows to explicitly allow (some) queries
that require filtering. Please note that a query using
``ALLOW FILTERING`` may thus have unpredictable performance (for the
definition above), i.e. even a query that selects a handful of records
**may** exhibit performance that depends on the total amount of data
stored in the cluster.

For instance, considering the following table holding user profiles with
their year of birth (with a secondary index on it) and country of
residence:

| bc(sample)..
| CREATE TABLE users (
|  username text PRIMARY KEY,
|  firstname text,
|  lastname text,
|  birth\_year int,
|  country text
| )

| CREATE INDEX ON users(birth\_year);
| p.

Then the following queries are valid:

| bc(sample).
| SELECT \* FROM users;
| SELECT firstname, lastname FROM users WHERE birth\_year = 1981;

because in both case, Cassandra guarantees that these queries
performance will be proportional to the amount of data returned. In
particular, if no users are born in 1981, then the second query
performance will not depend of the number of user profile stored in the
database (not directly at least: due to secondary index implementation
consideration, this query may still depend on the number of node in the
cluster, which indirectly depends on the amount of data stored.
Nevertheless, the number of nodes will always be multiple number of
magnitude lower than the number of user profile stored). Of course, both
query may return very large result set in practice, but the amount of
data returned can always be controlled by adding a ``LIMIT``.

However, the following query will be rejected:

| bc(sample).
| SELECT firstname, lastname FROM users WHERE birth\_year = 1981 AND
  country = ‘FR’;

because Cassandra cannot guarantee that it won’t have to scan large
amount of data even if the result to those query is small. Typically, it
will scan all the index entries for users born in 1981 even if only a
handful are actually from France. However, if you “know what you are
doing”, you can force the execution of this query by using
``ALLOW FILTERING`` and so the following query is valid:

| bc(sample).
| SELECT firstname, lastname FROM users WHERE birth\_year = 1981 AND
  country = ‘FR’ ALLOW FILTERING;

INSERT
^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= INSERT INTO 
|  ( ( VALUES )
|  \| ( JSON ))
|  ( IF NOT EXISTS )?
|  ( USING ( AND )\* )?

 ::= ‘(’ ( ‘,’ )\* ‘)’

 ::= ‘(’ ( ‘,’ )\* ‘)’

|  ::= 
|  \| 

|  ::= TIMESTAMP 
|  \| TTL 
| p.
| *Sample:*

| bc(sample)..
| INSERT INTO NerdMovies (movie, director, main\_actor, year)
|  VALUES (‘Serenity’, ‘Joss Whedon’, ‘Nathan Fillion’, 2005)
| USING TTL 86400;

| INSERT INTO NerdMovies JSON ‘{`movie <>`__ “Serenity”, `director <>`__
  “Joss Whedon”, `year <>`__ 2005}’
| p.
| The ``INSERT`` statement writes one or more columns for a given row in
  a table. Note that since a row is identified by its ``PRIMARY KEY``,
  at least the columns composing it must be specified. The list of
  columns to insert to must be supplied when using the ``VALUES``
  syntax. When using the ``JSON`` syntax, they are optional. See the
  section on ```INSERT JSON`` <#insertJson>`__ for more details.

Note that unlike in SQL, ``INSERT`` does not check the prior existence
of the row by default: the row is created if none existed before, and
updated otherwise. Furthermore, there is no mean to know which of
creation or update happened.

It is however possible to use the ``IF NOT EXISTS`` condition to only
insert if the row does not exist prior to the insertion. But please note
that using ``IF NOT EXISTS`` will incur a non negligible performance
cost (internally, Paxos will be used) so this should be used sparingly.

All updates for an ``INSERT`` are applied atomically and in isolation.

Please refer to the ```UPDATE`` <#updateOptions>`__ section for
information on the ``<option>`` available and to the
`collections <#collections>`__ section for use of
``<collection-literal>``. Also note that ``INSERT`` does not support
counters, while ``UPDATE`` does.

UPDATE
^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= UPDATE 
|  ( USING ( AND )\* )?
|  SET ( ‘,’ )\*
|  WHERE 
|  ( IF ( AND condition )\* )?

|  ::= ‘=’ 
|  \| ‘=’ (‘+’ \| ‘-’) ( \| \| )
|  \| ‘=’ ‘+’ 
|  \| ‘[’ ‘]’ ‘=’ 
|  \| ‘.’ ‘=’ 

|  ::= 
|  \| IN 
|  \| ‘[’ ‘]’ 
|  \| ‘[’ ‘]’ IN 
|  \| ‘.’ 
|  \| ‘.’ IN 

|  ::= ‘<’ \| ‘<=’ \| ‘=’ \| ‘!=’ \| ‘>=’ \| ‘>’
|  ::= ( \| ‘(’ ( ( ‘,’ )\* )? ‘)’)

 ::= ( AND )\*

|  ::= ‘=’ 
|  \| ‘(’ (‘,’ )\* ‘)’ ‘=’ 
|  \| IN ‘(’ ( ( ‘,’ )\* )? ‘)’
|  \| IN 
|  \| ‘(’ (‘,’ )\* ‘)’ IN ‘(’ ( ( ‘,’ )\* )? ‘)’
|  \| ‘(’ (‘,’ )\* ‘)’ IN 

|  ::= TIMESTAMP 
|  \| TTL 
| p.
| *Sample:*

| bc(sample)..
| UPDATE NerdMovies USING TTL 400
| SET director = ‘Joss Whedon’,
|  main\_actor = ‘Nathan Fillion’,
|  year = 2005
| WHERE movie = ‘Serenity’;

| UPDATE UserActions SET total = total + 2 WHERE user =
  B70DE1D0-9908-4AE3-BE34-5573E5B09F14 AND action = ‘click’;
| p.
| The ``UPDATE`` statement writes one or more columns for a given row in
  a table. The ``<where-clause>`` is used to select the row to update
  and must include all columns composing the ``PRIMARY KEY``. Other
  columns values are specified through ``<assignment>`` after the
  ``SET`` keyword.

Note that unlike in SQL, ``UPDATE`` does not check the prior existence
of the row by default (except through the use of ``<condition>``, see
below): the row is created if none existed before, and updated
otherwise. Furthermore, there are no means to know whether a creation or
update occurred.

It is however possible to use the conditions on some columns through
``IF``, in which case the row will not be updated unless the conditions
are met. But, please note that using ``IF`` conditions will incur a
non-negligible performance cost (internally, Paxos will be used) so this
should be used sparingly.

In an ``UPDATE`` statement, all updates within the same partition key
are applied atomically and in isolation.

The ``c = c + 3`` form of ``<assignment>`` is used to
increment/decrement counters. The identifier after the ‘=’ sign **must**
be the same than the one before the ‘=’ sign (Only increment/decrement
is supported on counters, not the assignment of a specific value).

The ``id = id + <collection-literal>`` and ``id[value1] = value2`` forms
of ``<assignment>`` are for collections. Please refer to the `relevant
section <#collections>`__ for more details.

The ``id.field = <term>`` form of ``<assignemt>`` is for setting the
value of a single field on a non-frozen user-defined types.

``<options>``
~~~~~~~~~~~~~

The ``UPDATE`` and ``INSERT`` statements support the following options:

-  ``TIMESTAMP``: sets the timestamp for the operation. If not
   specified, the coordinator will use the current time (in
   microseconds) at the start of statement execution as the timestamp.
   This is usually a suitable default.
-  ``TTL``: specifies an optional Time To Live (in seconds) for the
   inserted values. If set, the inserted values are automatically
   removed from the database after the specified time. Note that the TTL
   concerns the inserted values, not the columns themselves. This means
   that any subsequent update of the column will also reset the TTL (to
   whatever TTL is specified in that update). By default, values never
   expire. A TTL of 0 is equivalent to no TTL. If the table has a
   default\_time\_to\_live, a TTL of 0 will remove the TTL for the
   inserted or updated values.

DELETE
^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= DELETE ( ( ‘,’ )\* )?
|  FROM 
|  ( USING TIMESTAMP )?
|  WHERE 
|  ( IF ( EXISTS \| ( ( AND )\*) ) )?

|  ::= 
|  \| ‘[’ ‘]’
|  \| ‘.’ 

 ::= ( AND )\*

|  ::= 
|  \| ‘(’ (‘,’ )\* ‘)’ 
|  \| IN ‘(’ ( ( ‘,’ )\* )? ‘)’
|  \| IN 
|  \| ‘(’ (‘,’ )\* ‘)’ IN ‘(’ ( ( ‘,’ )\* )? ‘)’
|  \| ‘(’ (‘,’ )\* ‘)’ IN 

|  ::= ‘=’ \| ‘<’ \| ‘>’ \| ‘<=’ \| ‘>=’
|  ::= ( \| ‘(’ ( ( ‘,’ )\* )? ‘)’)

|  ::= ( \| ‘!=’) 
|  \| IN 
|  \| ‘[’ ‘]’ ( \| ‘!=’) 
|  \| ‘[’ ‘]’ IN 
|  \| ‘.’ ( \| ‘!=’) 
|  \| ‘.’ IN 

*Sample:*

| bc(sample)..
| DELETE FROM NerdMovies USING TIMESTAMP 1240003134 WHERE movie =
  ‘Serenity’;

| DELETE phone FROM Users WHERE userid IN
  (C73DE1D3-AF08-40F3-B124-3FF3E5109F22,
  B70DE1D0-9908-4AE3-BE34-5573E5B09F14);
| p.
| The ``DELETE`` statement deletes columns and rows. If column names are
  provided directly after the ``DELETE`` keyword, only those columns are
  deleted from the row indicated by the ``<where-clause>``. The
  ``id[value]`` syntax in ``<selection>`` is for non-frozen collections
  (please refer to the `collection section <#collections>`__ for more
  details). The ``id.field`` syntax is for the deletion of non-frozen
  user-defined types. Otherwise, whole rows are removed. The
  ``<where-clause>`` specifies which rows are to be deleted. Multiple
  rows may be deleted with one statement by using an ``IN`` clause. A
  range of rows may be deleted using an inequality operator (such as
  ``>=``).

``DELETE`` supports the ``TIMESTAMP`` option with the same semantics as
the ```UPDATE`` <#updateStmt>`__ statement.

In a ``DELETE`` statement, all deletions within the same partition key
are applied atomically and in isolation.

A ``DELETE`` operation can be conditional through the use of an ``IF``
clause, similar to ``UPDATE`` and ``INSERT`` statements. However, as
with ``INSERT`` and ``UPDATE`` statements, this will incur a
non-negligible performance cost (internally, Paxos will be used) and so
should be used sparingly.

BATCH
^^^^^

*Syntax:*

| bc(syntax)..
|  ::= BEGIN ( UNLOGGED \| COUNTER ) BATCH
|  ( USING ( AND )\* )?
|  ( ‘;’ )\*
|  APPLY BATCH

|  ::= 
|  \| 
|  \| 

|  ::= TIMESTAMP 
| p.
| *Sample:*

| bc(sample).
| BEGIN BATCH
|  INSERT INTO users (userid, password, name) VALUES (‘user2’,
  ‘ch@ngem3b’, ‘second user’);
|  UPDATE users SET password = ‘ps22dhds’ WHERE userid = ‘user3’;
|  INSERT INTO users (userid, password) VALUES (‘user4’, ‘ch@ngem3c’);
|  DELETE name FROM users WHERE userid = ‘user1’;
| APPLY BATCH;

The ``BATCH`` statement group multiple modification statements
(insertions/updates and deletions) into a single statement. It serves
several purposes:

#. It saves network round-trips between the client and the server (and
   sometimes between the server coordinator and the replicas) when
   batching multiple updates.
#. All updates in a ``BATCH`` belonging to a given partition key are
   performed in isolation.
#. By default, all operations in the batch are performed as ``LOGGED``,
   to ensure all mutations eventually complete (or none will). See the
   notes on ```UNLOGGED`` <#unloggedBatch>`__ for more details.

Note that:

-  ``BATCH`` statements may only contain ``UPDATE``, ``INSERT`` and
   ``DELETE`` statements.
-  Batches are *not* a full analogue for SQL transactions.
-  If a timestamp is not specified for each operation, then all
   operations will be applied with the same timestamp. Due to
   Cassandra’s conflict resolution procedure in the case of `timestamp
   ties <http://wiki.apache.org/cassandra/FAQ#clocktie>`__, operations
   may be applied in an order that is different from the order they are
   listed in the ``BATCH`` statement. To force a particular operation
   ordering, you must specify per-operation timestamps.

``UNLOGGED``
~~~~~~~~~~~~

By default, Cassandra uses a batch log to ensure all operations in a
batch eventually complete or none will (note however that operations are
only isolated within a single partition).

There is a performance penalty for batch atomicity when a batch spans
multiple partitions. If you do not want to incur this penalty, you can
tell Cassandra to skip the batchlog with the ``UNLOGGED`` option. If the
``UNLOGGED`` option is used, a failed batch might leave the patch only
partly applied.

``COUNTER``
~~~~~~~~~~~

Use the ``COUNTER`` option for batched counter updates. Unlike other
updates in Cassandra, counter updates are not idempotent.

``<option>``
~~~~~~~~~~~~

``BATCH`` supports both the ``TIMESTAMP`` option, with similar semantic
to the one described in the ```UPDATE`` <#updateOptions>`__ statement
(the timestamp applies to all the statement inside the batch). However,
if used, ``TIMESTAMP`` **must not** be used in the statements within the
batch.
