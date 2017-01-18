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

Changes
-------

The following describes the changes in each version of CQL.

3.4.4
^^^^^

- ``ALTER TABLE`` ``ALTER`` has been removed; a column's type may not be changed after creation (:jira:`12443`).
- ``ALTER TYPE`` ``ALTER`` has been removed; a field's type may not be changed after creation (:jira:`12443`).

3.4.3
^^^^^

- Adds a new ``duration `` :ref:`data types <data-types>` (:jira:`11873`).
- Support for ``GROUP BY`` (:jira:`10707`).
- Adds a ``DEFAULT UNSET`` option for ``INSERT JSON`` to ignore omitted columns (:jira:`11424`).
- Allows ``null`` as a legal value for TTL on insert and update. It will be treated as equivalent to
inserting a 0 (:jira:`12216`).

3.4.2
^^^^^

- If a table has a non zero ``default_time_to_live``, then explicitly specifying a TTL of 0 in an ``INSERT`` or
  ``UPDATE`` statement will result in the new writes not having any expiration (that is, an explicit TTL of 0 cancels
  the ``default_time_to_live``). This wasn't the case before and the ``default_time_to_live`` was applied even though a
  TTL had been explicitly set.
- ``ALTER TABLE`` ``ADD`` and ``DROP`` now allow multiple columns to be added/removed.
- New ``PER PARTITION LIMIT`` option for ``SELECT`` statements (see `CASSANDRA-7017
  <https://issues.apache.org/jira/browse/CASSANDRA-7017)>`__.
- :ref:`User-defined functions <cql-functions>` can now instantiate ``UDTValue`` and ``TupleValue`` instances via the
  new ``UDFContext`` interface (see `CASSANDRA-10818 <https://issues.apache.org/jira/browse/CASSANDRA-10818)>`__.
- :ref:`User-defined types <udts>` may now be stored in a non-frozen form, allowing individual fields to be updated and
  deleted in ``UPDATE`` statements and ``DELETE`` statements, respectively. (`CASSANDRA-7423
  <https://issues.apache.org/jira/browse/CASSANDRA-7423)>`__).

3.4.1
^^^^^

- Adds ``CAST`` functions.

3.4.0
^^^^^

- Support for :ref:`materialized views <materialized-views>`.
- ``DELETE`` support for inequality expressions and ``IN`` restrictions on any primary key columns.
- ``UPDATE`` support for ``IN`` restrictions on any primary key columns.

3.3.1
^^^^^

- The syntax ``TRUNCATE TABLE X`` is now accepted as an alias for ``TRUNCATE X``.

3.3.0
^^^^^

- :ref:`User-defined functions and aggregates <cql-functions>` are now supported.
- Allows double-dollar enclosed strings literals as an alternative to single-quote enclosed strings.
- Introduces Roles to supersede user based authentication and access control
- New ``date``, ``time``, ``tinyint`` and ``smallint`` :ref:`data types <data-types>` have been added.
- :ref:`JSON support <cql-json>` has been added
- Adds new time conversion functions and deprecate ``dateOf`` and ``unixTimestampOf``.

3.2.0
^^^^^

- :ref:`User-defined types <udts>` supported.
- ``CREATE INDEX`` now supports indexing collection columns, including indexing the keys of map collections through the
  ``keys()`` function
- Indexes on collections may be queried using the new ``CONTAINS`` and ``CONTAINS KEY`` operators
- :ref:`Tuple types <tuples>` were added to hold fixed-length sets of typed positional fields.
- ``DROP INDEX`` now supports optionally specifying a keyspace.

3.1.7
^^^^^

- ``SELECT`` statements now support selecting multiple rows in a single partition using an ``IN`` clause on combinations
  of clustering columns.
- ``IF NOT EXISTS`` and ``IF EXISTS`` syntax is now supported by ``CREATE USER`` and ``DROP USER`` statements,
  respectively.

3.1.6
^^^^^

- A new ``uuid()`` method has been added.
- Support for ``DELETE ... IF EXISTS`` syntax.

3.1.5
^^^^^

- It is now possible to group clustering columns in a relation, see :ref:`WHERE <where-clause>` clauses.
- Added support for :ref:`static columns <static-columns>`.

3.1.4
^^^^^

- ``CREATE INDEX`` now allows specifying options when creating CUSTOM indexes.

3.1.3
^^^^^

- Millisecond precision formats have been added to the :ref:`timestamp <timestamps>` parser.

3.1.2
^^^^^

- ``NaN`` and ``Infinity`` has been added as valid float constants. They are now reserved keywords. In the unlikely case
  you we using them as a column identifier (or keyspace/table one), you will now need to double quote them.

3.1.1
^^^^^

- ``SELECT`` statement now allows listing the partition keys (using the ``DISTINCT`` modifier). See `CASSANDRA-4536
  <https://issues.apache.org/jira/browse/CASSANDRA-4536>`__.
- The syntax ``c IN ?`` is now supported in ``WHERE`` clauses. In that case, the value expected for the bind variable
  will be a list of whatever type ``c`` is.
- It is now possible to use named bind variables (using ``:name`` instead of ``?``).

3.1.0
^^^^^

- ``ALTER TABLE`` ``DROP`` option added.
- ``SELECT`` statement now supports aliases in select clause. Aliases in WHERE and ORDER BY clauses are not supported.
- ``CREATE`` statements for ``KEYSPACE``, ``TABLE`` and ``INDEX`` now supports an ``IF NOT EXISTS`` condition.
  Similarly, ``DROP`` statements support a ``IF EXISTS`` condition.
- ``INSERT`` statements optionally supports a ``IF NOT EXISTS`` condition and ``UPDATE`` supports ``IF`` conditions.

3.0.5
^^^^^

- ``SELECT``, ``UPDATE``, and ``DELETE`` statements now allow empty ``IN`` relations (see `CASSANDRA-5626
  <https://issues.apache.org/jira/browse/CASSANDRA-5626)>`__.

3.0.4
^^^^^

- Updated the syntax for custom :ref:`secondary indexes <secondary-indexes>`.
- Non-equal condition on the partition key are now never supported, even for ordering partitioner as this was not
  correct (the order was **not** the one of the type of the partition key). Instead, the ``token`` method should always
  be used for range queries on the partition key (see :ref:`WHERE clauses <where-clause>`).

3.0.3
^^^^^

- Support for custom :ref:`secondary indexes <secondary-indexes>` has been added.

3.0.2
^^^^^

- Type validation for the :ref:`constants <constants>` has been fixed. For instance, the implementation used to allow
  ``'2'`` as a valid value for an ``int`` column (interpreting it has the equivalent of ``2``), or ``42`` as a valid
  ``blob`` value (in which case ``42`` was interpreted as an hexadecimal representation of the blob). This is no longer
  the case, type validation of constants is now more strict. See the :ref:`data types <data-types>` section for details
  on which constant is allowed for which type.
- The type validation fixed of the previous point has lead to the introduction of blobs constants to allow the input of
  blobs. Do note that while the input of blobs as strings constant is still supported by this version (to allow smoother
  transition to blob constant), it is now deprecated and will be removed by a future version. If you were using strings
  as blobs, you should thus update your client code ASAP to switch blob constants.
- A number of functions to convert native types to blobs have also been introduced. Furthermore the token function is
  now also allowed in select clauses. See the :ref:`section on functions <cql-functions>` for details.

3.0.1
^^^^^

- Date strings (and timestamps) are no longer accepted as valid ``timeuuid`` values. Doing so was a bug in the sense
  that date string are not valid ``timeuuid``, and it was thus resulting in `confusing behaviors
  <https://issues.apache.org/jira/browse/CASSANDRA-4936>`__. However, the following new methods have been added to help
  working with ``timeuuid``: ``now``, ``minTimeuuid``, ``maxTimeuuid`` ,
  ``dateOf`` and ``unixTimestampOf``.
- Float constants now support the exponent notation. In other words, ``4.2E10`` is now a valid floating point value.

Versioning
^^^^^^^^^^

Versioning of the CQL language adheres to the `Semantic Versioning <http://semver.org>`__ guidelines. Versions take the
form X.Y.Z where X, Y, and Z are integer values representing major, minor, and patch level respectively. There is no
correlation between Cassandra release versions and the CQL language version.

========= =============================================================================================================
 version   description
========= =============================================================================================================
 Major     The major version *must* be bumped when backward incompatible changes are introduced. This should rarely
           occur.
 Minor     Minor version increments occur when new, but backward compatible, functionality is introduced.
 Patch     The patch version is incremented when bugs are fixed.
========= =============================================================================================================
