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

.. _cql-triggers:

Triggers
--------

Triggers are identified by a name defined by:

.. productionlist::
   trigger_name: `identifier`


.. _create-trigger-statement:

CREATE TRIGGER
^^^^^^^^^^^^^^

Creating a new trigger uses the ``CREATE TRIGGER`` statement:

.. productionlist::
   create_trigger_statement: CREATE TRIGGER [ IF NOT EXISTS ] `trigger_name`
                           :     ON `table_name`
                           :     USING `string`

For instance::

    CREATE TRIGGER myTrigger ON myTable USING 'org.apache.cassandra.triggers.InvertedIndex';

The actual logic that makes up the trigger can be written in any Java (JVM) language and exists outside the database.
You place the trigger code in a ``lib/triggers`` subdirectory of the Cassandra installation directory, it loads during
cluster startup, and exists on every node that participates in a cluster. The trigger defined on a table fires before a
requested DML statement occurs, which ensures the atomicity of the transaction.

.. _drop-trigger-statement:

DROP TRIGGER
^^^^^^^^^^^^

Dropping a trigger uses the ``DROP TRIGGER`` statement:

.. productionlist::
   drop_trigger_statement: DROP TRIGGER [ IF EXISTS ] `trigger_name` ON `table_name`

For instance::

    DROP TRIGGER myTrigger ON myTable;
