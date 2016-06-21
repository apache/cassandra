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

Materialized Views
------------------

CREATE MATERIALIZED VIEW
^^^^^^^^^^^^^^^^^^^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= CREATE MATERIALIZED VIEW ( IF NOT EXISTS )? AS
|  SELECT ( ‘(’ ( ‘,’ ) \* ‘)’ \| ‘\*’ )
|  FROM 
|  ( WHERE )?
|  PRIMARY KEY ‘(’ ( ‘,’ )\* ‘)’
|  ( WITH ( AND )\* )?
| p.
| *Sample:*

| bc(sample)..
| CREATE MATERIALIZED VIEW monkeySpecies\_by\_population AS
|  SELECT \*
|  FROM monkeySpecies
|  WHERE population IS NOT NULL AND species IS NOT NULL
|  PRIMARY KEY (population, species)
|  WITH comment=‘Allow query by population instead of species’;
| p.
| The ``CREATE MATERIALIZED VIEW`` statement creates a new materialized
  view. Each such view is a set of *rows* which corresponds to rows
  which are present in the underlying, or base, table specified in the
  ``SELECT`` statement. A materialized view cannot be directly updated,
  but updates to the base table will cause corresponding updates in the
  view.

Attempting to create an already existing materialized view will return
an error unless the ``IF NOT EXISTS`` option is used. If it is used, the
statement will be a no-op if the materialized view already exists.

``WHERE`` Clause
~~~~~~~~~~~~~~~~

The ``<where-clause>`` is similar to the `where clause of a ``SELECT``
statement <#selectWhere>`__, with a few differences. First, the where
clause must contain an expression that disallows ``NULL`` values in
columns in the view’s primary key. If no other restriction is desired,
this can be accomplished with an ``IS NOT NULL`` expression. Second,
only columns which are in the base table’s primary key may be restricted
with expressions other than ``IS NOT NULL``. (Note that this second
restriction may be lifted in the future.)

ALTER MATERIALIZED VIEW
^^^^^^^^^^^^^^^^^^^^^^^

*Syntax:*

| bc(syntax). ::= ALTER MATERIALIZED VIEW 
|  WITH ( AND )\*

The ``ALTER MATERIALIZED VIEW`` statement allows options to be update;
these options are the same as \ ``CREATE TABLE``\ ’s options.

DROP MATERIALIZED VIEW
^^^^^^^^^^^^^^^^^^^^^^

*Syntax:*

bc(syntax). ::= DROP MATERIALIZED VIEW ( IF EXISTS )?

*Sample:*

bc(sample). DROP MATERIALIZED VIEW monkeySpecies\_by\_population;

The ``DROP MATERIALIZED VIEW`` statement is used to drop an existing
materialized view.

If the materialized view does not exists, the statement will return an
error, unless ``IF EXISTS`` is used in which case the operation is a
no-op.
