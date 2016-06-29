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

.. _materialized-views:

Materialized Views
------------------

Materialized views names are defined by:

.. productionlist::
   view_name: re('[a-zA-Z_0-9]+')


.. _create-materialized-view-statement:

CREATE MATERIALIZED VIEW
^^^^^^^^^^^^^^^^^^^^^^^^

You can create a materialized view on a table using a ``CREATE MATERIALIZED VIEW`` statement:

.. productionlist::
   create_materialized_view_statement: CREATE MATERIALIZED VIEW [ IF NOT EXISTS ] `view_name` AS
                                     :     `select_statement`
                                     :     PRIMARY KEY '(' `primary_key` ')'
                                     :     WITH `table_options`

For instance::

    CREATE MATERIALIZED VIEW monkeySpecies_by_population AS
        SELECT * FROM monkeySpecies
        WHERE population IS NOT NULL AND species IS NOT NULL
        PRIMARY KEY (population, species)
        WITH comment='Allow query by population instead of species';

The ``CREATE MATERIALIZED VIEW`` statement creates a new materialized view. Each such view is a set of *rows* which
corresponds to rows which are present in the underlying, or base, table specified in the ``SELECT`` statement. A
materialized view cannot be directly updated, but updates to the base table will cause corresponding updates in the
view.

Creating a materialized view has 3 main parts:

- The :ref:`select statement <mv-select>` that restrict the data included in the view.
- The :ref:`primary key <mv-primary-key>` definition for the view.
- The :ref:`options <mv-options>` for the view.

Attempting to create an already existing materialized view will return an error unless the ``IF NOT EXISTS`` option is
used. If it is used, the statement will be a no-op if the materialized view already exists.

.. _mv-select:

MV select statement
```````````````````

The select statement of a materialized view creation defines which of the base table is included in the view. That
statement is limited in a number of ways:

- the :ref:`selection <selection-clause>` is limited to those that only select columns of the base table. In other
  words, you can't use any function (aggregate or not), casting, term, etc. Aliases are also not supported. You can
  however use `*` as a shortcut of selecting all columns. Further, :ref:`static columns <static-columns>` cannot be
  included in a materialized view (which means ``SELECT *`` isn't allowed if the base table has static columns).
- the ``WHERE`` clause have the following restrictions:

  - it cannot include any :token:`bind_marker`.
  - the columns that are not part of the *base table* primary key can only be restricted by an ``IS NOT NULL``
    restriction. No other restriction is allowed.
  - as the columns that are part of the *view* primary key cannot be null, they must always be at least restricted by a
    ``IS NOT NULL`` restriction (or any other restriction, but they must have one).

- it cannot have neither an :ref:`ordering clause <ordering-clause>`, nor a :ref:`limit <limit-clause>`, nor :ref:`ALLOW
  FILTERING <allow-filtering>`.

.. _mv-primary-key:

MV primary key
``````````````

A view must have a primary key and that primary key must conform to the following restrictions:

- it must contain all the primary key columns of the base table. This ensures that every row of the view correspond to
  exactly one row of the base table.
- it can only contain a single column that is not a primary key column in the base table.

So for instance, give the following base table definition::

    CREATE TABLE t (
        k int,
        c1 int,
        c2 int,
        v1 int,
        v2 int,
        PRIMARY KEY (k, c1, c2)
    )

then the following view definitions are allowed::

    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL
        PRIMARY KEY (c1, k, c2)

    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL
        PRIMARY KEY (v1, k, c1, c2)

but the following ones are **not** allowed::

    // Error: cannot include both v1 and v2 in the primary key as both are not in the base table primary key
    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE k IS NOT NULL AND c1 IS NOT NULL AND c2 IS NOT NULL AND v1 IS NOT NULL
        PRIMARY KEY (v1, v2, k, c1, c2)

    // Error: must include k in the primary as it's a base table primary key column
    CREATE MATERIALIZED VIEW mv1 AS
        SELECT * FROM t WHERE c1 IS NOT NULL AND c2 IS NOT NULL
        PRIMARY KEY (c1, c2)


.. _mv-options:

MV options
``````````

A materialized view is internally implemented by a table and as such, creating a MV allows the :ref:`same options than
creating a table <create-table-options>`.


.. _alter-materialized-view-statement:

ALTER MATERIALIZED VIEW
^^^^^^^^^^^^^^^^^^^^^^^

After creation, you can alter the options of a materialized view using the ``ALTER MATERIALIZED VIEW`` statement:

.. productionlist::
   alter_materialized_view_statement: ALTER MATERIALIZED VIEW `view_name` WITH `table_options`

The options that can be updated are the same than at creation time and thus the :ref:`same than for tables
<create-table-options>`.

.. _drop-materialized-view-statement:

DROP MATERIALIZED VIEW
^^^^^^^^^^^^^^^^^^^^^^

Dropping a materialized view users the ``DROP MATERIALIZED VIEW`` statement:

.. productionlist::
   drop_materialized_view_statement: DROP MATERIALIZED VIEW [ IF EXISTS ] `view_name`;

If the materialized view does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case
the operation is a no-op.
