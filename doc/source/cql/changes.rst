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

Changes
-------

The following describes the changes in each version of CQL.

3.4.2
^^^^^

-  ```INSERT/UPDATE options`` <#updateOptions>`__ for tables having a
   default\_time\_to\_live specifying a TTL of 0 will remove the TTL
   from the inserted or updated values
-  ```ALTER TABLE`` <#alterTableStmt>`__ ``ADD`` and ``DROP`` now allow
   mutiple columns to be added/removed
-  New ```PER PARTITION LIMIT`` <#selectLimit>`__ option (see
   `CASSANDRA-7017 <https://issues.apache.org/jira/browse/CASSANDRA-7017)>`__.
-  `User-defined functions <#udfs>`__ can now instantiate ``UDTValue``
   and ``TupleValue`` instances via the new ``UDFContext`` interface
   (see
   `CASSANDRA-10818 <https://issues.apache.org/jira/browse/CASSANDRA-10818)>`__.
-  “User-defined types”#createTypeStmt may now be stored in a non-frozen
   form, allowing individual fields to be updated and deleted in
   ```UPDATE`` statements <#updateStmt>`__ and ```DELETE``
   statements <#deleteStmt>`__, respectively.
   (`CASSANDRA-7423 <https://issues.apache.org/jira/browse/CASSANDRA-7423)>`__

3.4.1
^^^^^

-  Adds ``CAST`` functions. See ```Cast`` <#castFun>`__.

3.4.0
^^^^^

-  Support for `materialized views <#createMVStmt>`__
-  ```DELETE`` <#deleteStmt>`__ support for inequality expressions and
   ``IN`` restrictions on any primary key columns
-  ```UPDATE`` <#updateStmt>`__ support for ``IN`` restrictions on any
   primary key columns

3.3.1
^^^^^

-  The syntax ``TRUNCATE TABLE X`` is now accepted as an alias for
   ``TRUNCATE X``

3.3.0
^^^^^

-  Adds new `aggregates <#aggregates>`__
-  User-defined functions are now supported through
   ```CREATE FUNCTION`` <#createFunctionStmt>`__ and
   ```DROP FUNCTION`` <#dropFunctionStmt>`__.
-  User-defined aggregates are now supported through
   ```CREATE AGGREGATE`` <#createAggregateStmt>`__ and
   ```DROP AGGREGATE`` <#dropAggregateStmt>`__.
-  Allows double-dollar enclosed strings literals as an alternative to
   single-quote enclosed strings.
-  Introduces Roles to supercede user based authentication and access
   control
-  ```Date`` <#usingdates>`__ and ```Time`` <usingtime>`__ data types
   have been added
-  ```JSON`` <#json>`__ support has been added
-  ``Tinyint`` and ``Smallint`` data types have been added
-  Adds new time conversion functions and deprecate ``dateOf`` and
   ``unixTimestampOf``. See ```Time conversion functions`` <#timeFun>`__

3.2.0
^^^^^

-  User-defined types are now supported through
   ```CREATE TYPE`` <#createTypeStmt>`__,
   ```ALTER TYPE`` <#alterTypeStmt>`__, and
   ```DROP TYPE`` <#dropTypeStmt>`__
-  ```CREATE INDEX`` <#createIndexStmt>`__ now supports indexing
   collection columns, including indexing the keys of map collections
   through the ``keys()`` function
-  Indexes on collections may be queried using the new ``CONTAINS`` and
   ``CONTAINS KEY`` operators
-  Tuple types were added to hold fixed-length sets of typed positional
   fields (see the section on `types <#types>`__ )
-  ```DROP INDEX`` <#dropIndexStmt>`__ now supports optionally
   specifying a keyspace

3.1.7
^^^^^

-  ``SELECT`` statements now support selecting multiple rows in a single
   partition using an ``IN`` clause on combinations of clustering
   columns. See `SELECT WHERE <#selectWhere>`__ clauses.
-  ``IF NOT EXISTS`` and ``IF EXISTS`` syntax is now supported by
   ``CREATE USER`` and ``DROP USER`` statmenets, respectively.

3.1.6
^^^^^

-  A new ```uuid`` method <#uuidFun>`__ has been added.
-  Support for ``DELETE ... IF EXISTS`` syntax.

3.1.5
^^^^^

-  It is now possible to group clustering columns in a relatiion, see
   `SELECT WHERE <#selectWhere>`__ clauses.
-  Added support for ``STATIC`` columns, see `static in CREATE
   TABLE <#createTableStatic>`__.

3.1.4
^^^^^

-  ``CREATE INDEX`` now allows specifying options when creating CUSTOM
   indexes (see `CREATE INDEX reference <#createIndexStmt>`__ ).

3.1.3
^^^^^

-  Millisecond precision formats have been added to the timestamp parser
   (see `working with dates <#usingtimestamps>`__ ).

3.1.2
^^^^^

-  ``NaN`` and ``Infinity`` has been added as valid float contants. They
   are now reserved keywords. In the unlikely case you we using them as
   a column identifier (or keyspace/table one), you will noew need to
   double quote them (see `quote identifiers <#identifiers>`__ ).

3.1.1
^^^^^

-  ``SELECT`` statement now allows listing the partition keys (using the
   ``DISTINCT`` modifier). See
   `CASSANDRA-4536 <https://issues.apache.org/jira/browse/CASSANDRA-4536>`__.
-  The syntax ``c IN ?`` is now supported in ``WHERE`` clauses. In that
   case, the value expected for the bind variable will be a list of
   whatever type ``c`` is.
-  It is now possible to use named bind variables (using ``:name``
   instead of ``?``).

3.1.0
^^^^^

-  `ALTER TABLE <#alterTableStmt>`__ ``DROP`` option has been reenabled
   for CQL3 tables and has new semantics now: the space formerly used by
   dropped columns will now be eventually reclaimed (post-compaction).
   You should not readd previously dropped columns unless you use
   timestamps with microsecond precision (see
   `CASSANDRA-3919 <https://issues.apache.org/jira/browse/CASSANDRA-3919>`__
   for more details).
-  ``SELECT`` statement now supports aliases in select clause. Aliases
   in WHERE and ORDER BY clauses are not supported. See the `section on
   select <#selectStmt>`__ for details.
-  ``CREATE`` statements for ``KEYSPACE``, ``TABLE`` and ``INDEX`` now
   supports an ``IF NOT EXISTS`` condition. Similarly, ``DROP``
   statements support a ``IF EXISTS`` condition.
-  ``INSERT`` statements optionally supports a ``IF NOT EXISTS``
   condition and ``UPDATE`` supports ``IF`` conditions.

3.0.5
^^^^^

-  ``SELECT``, ``UPDATE``, and ``DELETE`` statements now allow empty
   ``IN`` relations (see
   `CASSANDRA-5626 <https://issues.apache.org/jira/browse/CASSANDRA-5626)>`__.

3.0.4
^^^^^

-  Updated the syntax for custom `secondary
   indexes <#createIndexStmt>`__.
-  Non-equal condition on the partition key are now never supported,
   even for ordering partitioner as this was not correct (the order was
   **not** the one of the type of the partition key). Instead, the
   ``token`` method should always be used for range queries on the
   partition key (see `WHERE clauses <#selectWhere>`__ ).

3.0.3
^^^^^

-  Support for custom `secondary indexes <#createIndexStmt>`__ has been
   added.

3.0.2
^^^^^

-  Type validation for the `constants <#constants>`__ has been fixed.
   For instance, the implementation used to allow ``'2'`` as a valid
   value for an ``int`` column (interpreting it has the equivalent of
   ``2``), or ``42`` as a valid ``blob`` value (in which case ``42`` was
   interpreted as an hexadecimal representation of the blob). This is no
   longer the case, type validation of constants is now more strict. See
   the `data types <#types>`__ section for details on which constant is
   allowed for which type.
-  The type validation fixed of the previous point has lead to the
   introduction of `blobs constants <#constants>`__ to allow inputing
   blobs. Do note that while inputing blobs as strings constant is still
   supported by this version (to allow smoother transition to blob
   constant), it is now deprecated (in particular the `data
   types <#types>`__ section does not list strings constants as valid
   blobs) and will be removed by a future version. If you were using
   strings as blobs, you should thus update your client code ASAP to
   switch blob constants.
-  A number of functions to convert native types to blobs have also been
   introduced. Furthermore the token function is now also allowed in
   select clauses. See the `section on functions <#functions>`__ for
   details.

3.0.1
^^^^^

-  `Date strings <#usingtimestamps>`__ (and timestamps) are no longer
   accepted as valid ``timeuuid`` values. Doing so was a bug in the
   sense that date string are not valid ``timeuuid``, and it was thus
   resulting in `confusing
   behaviors <https://issues.apache.org/jira/browse/CASSANDRA-4936>`__.
   However, the following new methods have been added to help working
   with ``timeuuid``: ``now``, ``minTimeuuid``, ``maxTimeuuid`` ,
   ``dateOf`` and ``unixTimestampOf``. See the `section dedicated to
   these methods <#timeuuidFun>`__ for more detail.
-  “Float constants”#constants now support the exponent notation. In
   other words, ``4.2E10`` is now a valid floating point value.

Versioning
^^^^^^^^^^

Versioning of the CQL language adheres to the `Semantic
Versioning <http://semver.org>`__ guidelines. Versions take the form
X.Y.Z where X, Y, and Z are integer values representing major, minor,
and patch level respectively. There is no correlation between Cassandra
release versions and the CQL language version.

+-----------+-------------------------------------------------------------------------------------------------------------------+
| version   | description                                                                                                       |
+===========+===================================================================================================================+
| Major     | The major version *must* be bumped when backward incompatible changes are introduced. This should rarely occur.   |
+-----------+-------------------------------------------------------------------------------------------------------------------+
| Minor     | Minor version increments occur when new, but backward compatible, functionality is introduced.                    |
+-----------+-------------------------------------------------------------------------------------------------------------------+
| Patch     | The patch version is incremented when bugs are fixed.                                                             |
+-----------+-------------------------------------------------------------------------------------------------------------------+
