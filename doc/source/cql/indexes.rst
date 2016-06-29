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

.. _secondary-indexes:

Secondary Indexes
-----------------

CQL supports creating secondary indexes on tables, allowing queries on the table to use those indexes. A secondary index
is identified by a name defined by:

.. productionlist::
   index_name: re('[a-zA-Z_0-9]+')



.. _create-index-statement:

CREATE INDEX
^^^^^^^^^^^^

Creating a secondary index on a table uses the ``CREATE INDEX`` statement:

.. productionlist::
   create_index_statement: CREATE [ CUSTOM ] INDEX [ IF NOT EXISTS ] [ `index_name` ]
                         :     ON `table_name` '(' `index_identifier` ')'
                         :     [ USING `string` [ WITH OPTIONS = `map_literal` ] ]
   index_identifier: `column_name`
                   :| ( KEYS | VALUES | ENTRIES | FULL ) '(' `column_name` ')'

For instance::

    CREATE INDEX userIndex ON NerdMovies (user);
    CREATE INDEX ON Mutants (abilityId);
    CREATE INDEX ON users (keys(favs));
    CREATE CUSTOM INDEX ON users (email) USING 'path.to.the.IndexClass';
    CREATE CUSTOM INDEX ON users (email) USING 'path.to.the.IndexClass' WITH OPTIONS = {'storage': '/mnt/ssd/indexes/'};

The ``CREATE INDEX`` statement is used to create a new (automatic) secondary index for a given (existing) column in a
given table. A name for the index itself can be specified before the ``ON`` keyword, if desired. If data already exists
for the column, it will be indexed asynchronously. After the index is created, new data for the column is indexed
automatically at insertion time.

Attempting to create an already existing index will return an error unless the ``IF NOT EXISTS`` option is used. If it
is used, the statement will be a no-op if the index already exists.

Indexes on Map Keys
~~~~~~~~~~~~~~~~~~~

When creating an index on a :ref:`maps <maps>`, you may index either the keys or the values. If the column identifier is
placed within the ``keys()`` function, the index will be on the map keys, allowing you to use ``CONTAINS KEY`` in
``WHERE`` clauses. Otherwise, the index will be on the map values.

.. _drop-index-statement:

DROP INDEX
^^^^^^^^^^

Dropping a secondary index uses the ``DROP INDEX`` statement:

.. productionlist::
   drop_index_statement: DROP INDEX [ IF EXISTS ] `index_name`

The ``DROP INDEX`` statement is used to drop an existing secondary index. The argument of the statement is the index
name, which may optionally specify the keyspace of the index.

If the index does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case the
operation is a no-op.
