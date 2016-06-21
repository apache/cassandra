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

Triggers
--------

CREATE TRIGGER
^^^^^^^^^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= CREATE TRIGGER ( IF NOT EXISTS )? ( )?
|  ON 
|  USING 

*Sample:*

| bc(sample).
| CREATE TRIGGER myTrigger ON myTable USING
  ‘org.apache.cassandra.triggers.InvertedIndex’;

The actual logic that makes up the trigger can be written in any Java
(JVM) language and exists outside the database. You place the trigger
code in a ``lib/triggers`` subdirectory of the Cassandra installation
directory, it loads during cluster startup, and exists on every node
that participates in a cluster. The trigger defined on a table fires
before a requested DML statement occurs, which ensures the atomicity of
the transaction.

DROP TRIGGER
^^^^^^^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= DROP TRIGGER ( IF EXISTS )? ( )?
|  ON 
| p.
| *Sample:*

| bc(sample).
| DROP TRIGGER myTrigger ON myTable;

``DROP TRIGGER`` statement removes the registration of a trigger created
using ``CREATE TRIGGER``.
