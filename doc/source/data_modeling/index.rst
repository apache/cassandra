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

Data Modeling
=============

The data model of Apache Cassandra is not relational and orients on Google's Big Table.
It belongs to the type of column-structures or column-family-structures of the NoSQL-databases.
The database consists of columns, column families and the keyspace.
The data-model has no logical structure and no fixed schema what makes it dynamically changeable and supports the removal and addition of columns during runtime.

.. _column:
Column
^^^^^^^

The column is the smallest Element and by that the basic element of the data structure.
It consists of a key, a value and a timestamp. The key corresponds to the column-name. The value is the content of the column and the timestamp includes the time when the column was written.
The timestamp is used internally to solve conflicts with redundancies or replics and to identify the latest element.

.. _column families:
Column Families
^^^^^^^^^^^^^^

In column families several columns are grouped with a unique key. The amount of columns is hereby not fixed and so it can vary between rows.
Furthermore columns of a row can be extended or reduced afterwards what makes the column family not having a fixed schema.
A unique name identifies the table in which the columns are stored in an ordered and grouped way.
Also, combinations of keys is supported. With their help columns within a row can be grouped.
This concept is also known as "wide rows". By that a row becomes horizontal extendable.
Within a row different columns can get connected via their timestamp and grouped unambiguous.

.. _keyspace:
Keyspace
^^^^^^^^

The outermost unit is a keyspace which unites one or more colum families under a unique name.
At the creation of a keyspace 2 attributes need to be established: the replication factor and the replica replacement strategy.
The replication factor indicates how many copies of the column families are created in the cluster.
In the replica replacement strategy is defined how the distribution of the copies is done.
Within a cluster any numbers of keyspaces can be established to define the redundancy for different column families individually.  