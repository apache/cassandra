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

.. _cql-functions:

.. Need some intro for UDF and native functions in general and point those to it.
.. _udfs:
.. _native-functions:

Functions
---------

CQL supports 2 main categories of functions:

- the :ref:`scalar functions <scalar-functions>`, which simply take a number of values and produce an output with it.
- the :ref:`aggregate functions <aggregate-functions>`, which are used to aggregate multiple rows results from a
  ``SELECT`` statement.

In both cases, CQL provides a number of native "hard-coded" functions as well as the ability to create new user-defined
functions.

.. note:: By default, the use of user-defined functions is disabled by default for security concerns (even when
   enabled, the execution of user-defined functions is sandboxed and a "rogue" function should not be allowed to do
   evil, but no sandbox is perfect so using user-defined functions is opt-in). See the ``enable_user_defined_functions``
   in ``cassandra.yaml`` to enable them.

A function is identifier by its name:

.. productionlist::
   function_name: [ `keyspace_name` '.' ] `name`

.. _scalar-functions:

Scalar functions
^^^^^^^^^^^^^^^^

.. _scalar-native-functions:

Native functions
~~~~~~~~~~~~~~~~

Cast
````

The ``cast`` function can be used to converts one native datatype to another.

The following table describes the conversions supported by the ``cast`` function. Cassandra will silently ignore any
cast converting a datatype into its own datatype.

=============== =======================================================================================================
 From            To
=============== =======================================================================================================
 ``ascii``       ``text``, ``varchar``
 ``bigint``      ``tinyint``, ``smallint``, ``int``, ``float``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``boolean``     ``text``, ``varchar``
 ``counter``     ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``,
                 ``text``, ``varchar``
 ``date``        ``timestamp``
 ``decimal``     ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``varint``, ``text``,
                 ``varchar``
 ``double``      ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``float``       ``tinyint``, ``smallint``, ``int``, ``bigint``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``inet``        ``text``, ``varchar``
 ``int``         ``tinyint``, ``smallint``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``smallint``    ``tinyint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``,
                 ``varchar``
 ``time``        ``text``, ``varchar``
 ``timestamp``   ``date``, ``text``, ``varchar``
 ``timeuuid``    ``timestamp``, ``date``, ``text``, ``varchar``
 ``tinyint``     ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``,
                 ``text``, ``varchar``
 ``uuid``        ``text``, ``varchar``
 ``varint``      ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``text``,
                 ``varchar``
=============== =======================================================================================================

The conversions rely strictly on Java's semantics. For example, the double value 1 will be converted to the text value
'1.0'. For instance::

    SELECT avg(cast(count as double)) FROM myTable

Token
`````

The ``token`` function allows to compute the token for a given partition key. The exact signature of the token function
depends on the table concerned and of the partitioner used by the cluster.

The type of the arguments of the ``token`` depend on the type of the partition key columns. The return type depend on
the partitioner in use:

- For Murmur3Partitioner, the return type is ``bigint``.
- For RandomPartitioner, the return type is ``varint``.
- For ByteOrderedPartitioner, the return type is ``blob``.

For instance, in a cluster using the default Murmur3Partitioner, if a table is defined by::

    CREATE TABLE users (
        userid text PRIMARY KEY,
        username text,
    )

then the ``token`` function will take a single argument of type ``text`` (in that case, the partition key is ``userid``
(there is no clustering columns so the partition key is the same than the primary key)), and the return type will be
``bigint``.

Uuid
````
The ``uuid`` function takes no parameters and generates a random type 4 uuid suitable for use in ``INSERT`` or
``UPDATE`` statements.

.. _timeuuid-functions:

Timeuuid functions
``````````````````

``now``
#######

The ``now`` function takes no arguments and generates, on the coordinator node, a new unique timeuuid (at the time where
the statement using it is executed). Note that this method is useful for insertion but is largely non-sensical in
``WHERE`` clauses. For instance, a query of the form::

    SELECT * FROM myTable WHERE t = now()

will never return any result by design, since the value returned by ``now()`` is guaranteed to be unique.

``minTimeuuid`` and ``maxTimeuuid``
###################################

The ``minTimeuuid`` (resp. ``maxTimeuuid``) function takes a ``timestamp`` value ``t`` (which can be `either a timestamp
or a date string <timestamps>`) and return a *fake* ``timeuuid`` corresponding to the *smallest* (resp. *biggest*)
possible ``timeuuid`` having for timestamp ``t``. So for instance::

    SELECT * FROM myTable
     WHERE t > maxTimeuuid('2013-01-01 00:05+0000')
       AND t < minTimeuuid('2013-02-02 10:00+0000')

will select all rows where the ``timeuuid`` column ``t`` is strictly older than ``'2013-01-01 00:05+0000'`` but strictly
younger than ``'2013-02-02 10:00+0000'``. Please note that ``t >= maxTimeuuid('2013-01-01 00:05+0000')`` would still
*not* select a ``timeuuid`` generated exactly at '2013-01-01 00:05+0000' and is essentially equivalent to ``t >
maxTimeuuid('2013-01-01 00:05+0000')``.

.. note:: We called the values generated by ``minTimeuuid`` and ``maxTimeuuid`` *fake* UUID because they do no respect
   the Time-Based UUID generation process specified by the `RFC 4122 <http://www.ietf.org/rfc/rfc4122.txt>`__. In
   particular, the value returned by these 2 methods will not be unique. This means you should only use those methods
   for querying (as in the example above). Inserting the result of those methods is almost certainly *a bad idea*.

Time conversion functions
`````````````````````````

A number of functions are provided to “convert” a ``timeuuid``, a ``timestamp`` or a ``date`` into another ``native``
type.

===================== =============== ===================================================================
 Function name         Input type      Description
===================== =============== ===================================================================
 ``toDate``            ``timeuuid``    Converts the ``timeuuid`` argument into a ``date`` type
 ``toDate``            ``timestamp``   Converts the ``timestamp`` argument into a ``date`` type
 ``toTimestamp``       ``timeuuid``    Converts the ``timeuuid`` argument into a ``timestamp`` type
 ``toTimestamp``       ``date``        Converts the ``date`` argument into a ``timestamp`` type
 ``toUnixTimestamp``   ``timeuuid``    Converts the ``timeuuid`` argument into a ``bigInt`` raw value
 ``toUnixTimestamp``   ``timestamp``   Converts the ``timestamp`` argument into a ``bigInt`` raw value
 ``toUnixTimestamp``   ``date``        Converts the ``date`` argument into a ``bigInt`` raw value
 ``dateOf``            ``timeuuid``    Similar to ``toTimestamp(timeuuid)`` (DEPRECATED)
 ``unixTimestampOf``   ``timeuuid``    Similar to ``toUnixTimestamp(timeuuid)`` (DEPRECATED)
===================== =============== ===================================================================

Blob conversion functions
`````````````````````````
A number of functions are provided to “convert” the native types into binary data (``blob``). For every
``<native-type>`` ``type`` supported by CQL (a notable exceptions is ``blob``, for obvious reasons), the function
``typeAsBlob`` takes a argument of type ``type`` and return it as a ``blob``. Conversely, the function ``blobAsType``
takes a 64-bit ``blob`` argument and convert it to a ``bigint`` value. And so for instance, ``bigintAsBlob(3)`` is
``0x0000000000000003`` and ``blobAsBigint(0x0000000000000003)`` is ``3``.

.. _user-defined-scalar-functions:

User-defined functions
~~~~~~~~~~~~~~~~~~~~~~

User-defined functions allow execution of user-provided code in Cassandra. By default, Cassandra supports defining
functions in *Java* and *JavaScript*. Support for other JSR 223 compliant scripting languages (such as Python, Ruby, and
Scala) can be added by adding a JAR to the classpath.

UDFs are part of the Cassandra schema. As such, they are automatically propagated to all nodes in the cluster.

UDFs can be *overloaded* - i.e. multiple UDFs with different argument types but the same function name. Example::

    CREATE FUNCTION sample ( arg int ) ...;
    CREATE FUNCTION sample ( arg text ) ...;

User-defined functions are susceptible to all of the normal problems with the chosen programming language. Accordingly,
implementations should be safe against null pointer exceptions, illegal arguments, or any other potential source of
exceptions. An exception during function execution will result in the entire statement failing.

It is valid to use *complex* types like collections, tuple types and user-defined types as argument and return types.
Tuple types and user-defined types are handled by the conversion functions of the DataStax Java Driver. Please see the
documentation of the Java Driver for details on handling tuple types and user-defined types.

Arguments for functions can be literals or terms. Prepared statement placeholders can be used, too.

Note that you can use the double-quoted string syntax to enclose the UDF source code. For example::

    CREATE FUNCTION some_function ( arg int )
        RETURNS NULL ON NULL INPUT
        RETURNS int
        LANGUAGE java
        AS $$ return arg; $$;

    SELECT some_function(column) FROM atable ...;
    UPDATE atable SET col = some_function(?) ...;

    CREATE TYPE custom_type (txt text, i int);
    CREATE FUNCTION fct_using_udt ( udtarg frozen )
        RETURNS NULL ON NULL INPUT
        RETURNS text
        LANGUAGE java
        AS $$ return udtarg.getString("txt"); $$;

User-defined functions can be used in ``SELECT``, ``INSERT`` and ``UPDATE`` statements.

The implicitly available ``udfContext`` field (or binding for script UDFs) provides the necessary functionality to
create new UDT and tuple values::

    CREATE TYPE custom_type (txt text, i int);
    CREATE FUNCTION fct\_using\_udt ( somearg int )
        RETURNS NULL ON NULL INPUT
        RETURNS custom_type
        LANGUAGE java
        AS $$
            UDTValue udt = udfContext.newReturnUDTValue();
            udt.setString("txt", "some string");
            udt.setInt("i", 42);
            return udt;
        $$;

The definition of the ``UDFContext`` interface can be found in the Apache Cassandra source code for
``org.apache.cassandra.cql3.functions.UDFContext``.

.. code-block:: java

    public interface UDFContext
    {
        UDTValue newArgUDTValue(String argName);
        UDTValue newArgUDTValue(int argNum);
        UDTValue newReturnUDTValue();
        UDTValue newUDTValue(String udtName);
        TupleValue newArgTupleValue(String argName);
        TupleValue newArgTupleValue(int argNum);
        TupleValue newReturnTupleValue();
        TupleValue newTupleValue(String cqlDefinition);
    }

Java UDFs already have some imports for common interfaces and classes defined. These imports are:

.. code-block:: java

    import java.nio.ByteBuffer;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;
    import org.apache.cassandra.cql3.functions.UDFContext;
    import com.datastax.driver.core.TypeCodec;
    import com.datastax.driver.core.TupleValue;
    import com.datastax.driver.core.UDTValue;

Please note, that these convenience imports are not available for script UDFs.

.. _create-function-statement:

CREATE FUNCTION
```````````````

Creating a new user-defined function uses the ``CREATE FUNCTION`` statement:

.. productionlist::
   create_function_statement: CREATE [ OR REPLACE ] FUNCTION [ IF NOT EXISTS]
                            :     `function_name` '(' `arguments_declaration` ')'
                            :     [ CALLED | RETURNS NULL ] ON NULL INPUT
                            :     RETURNS `cql_type`
                            :     LANGUAGE `identifier`
                            :     AS `string`
   arguments_declaration: `identifier` `cql_type` ( ',' `identifier` `cql_type` )*

For instance::

    CREATE OR REPLACE FUNCTION somefunction(somearg int, anotherarg text, complexarg frozen<someUDT>, listarg list)
        RETURNS NULL ON NULL INPUT
        RETURNS text
        LANGUAGE java
        AS $$
            // some Java code
        $$;

    CREATE FUNCTION IF NOT EXISTS akeyspace.fname(someArg int)
        CALLED ON NULL INPUT
        RETURNS text
        LANGUAGE java
        AS $$
            // some Java code
        $$;

``CREATE FUNCTION`` with the optional ``OR REPLACE`` keywords either creates a function or replaces an existing one with
the same signature. A ``CREATE FUNCTION`` without ``OR REPLACE`` fails if a function with the same signature already
exists.

If the optional ``IF NOT EXISTS`` keywords are used, the function will
only be created if another function with the same signature does not
exist.

``OR REPLACE`` and ``IF NOT EXISTS`` cannot be used together.

Behavior on invocation with ``null`` values must be defined for each
function. There are two options:

#. ``RETURNS NULL ON NULL INPUT`` declares that the function will always
   return ``null`` if any of the input arguments is ``null``.
#. ``CALLED ON NULL INPUT`` declares that the function will always be
   executed.

Function Signature
##################

Signatures are used to distinguish individual functions. The signature consists of:

#. The fully qualified function name - i.e *keyspace* plus *function-name*
#. The concatenated list of all argument types

Note that keyspace names, function names and argument types are subject to the default naming conventions and
case-sensitivity rules.

Functions belong to a keyspace. If no keyspace is specified in ``<function-name>``, the current keyspace is used (i.e.
the keyspace specified using the ``USE`` statement). It is not possible to create a user-defined function in one of the
system keyspaces.

.. _drop-function-statement:

DROP FUNCTION
`````````````

Dropping a function uses the ``DROP FUNCTION`` statement:

.. productionlist::
   drop_function_statement: DROP FUNCTION [ IF EXISTS ] `function_name` [ '(' `arguments_signature` ')' ]
   arguments_signature: `cql_type` ( ',' `cql_type` )*

For instance::

    DROP FUNCTION myfunction;
    DROP FUNCTION mykeyspace.afunction;
    DROP FUNCTION afunction ( int );
    DROP FUNCTION afunction ( text );

You must specify the argument types (:token:`arguments_signature`) of the function to drop if there are multiple
functions with the same name but a different signature (overloaded functions).

``DROP FUNCTION`` with the optional ``IF EXISTS`` keywords drops a function if it exists, but does not throw an error if
it doesn't

.. _aggregate-functions:

Aggregate functions
^^^^^^^^^^^^^^^^^^^

Aggregate functions work on a set of rows. They receive values for each row and returns one value for the whole set.

If ``normal`` columns, ``scalar functions``, ``UDT`` fields, ``writetime`` or ``ttl`` are selected together with
aggregate functions, the values returned for them will be the ones of the first row matching the query.

Native aggregates
~~~~~~~~~~~~~~~~~

.. _count-function:

Count
`````

The ``count`` function can be used to count the rows returned by a query. Example::

    SELECT COUNT (*) FROM plays;
    SELECT COUNT (1) FROM plays;

It also can be used to count the non null value of a given column::

    SELECT COUNT (scores) FROM plays;

Max and Min
```````````

The ``max`` and ``min`` functions can be used to compute the maximum and the minimum value returned by a query for a
given column. For instance::

    SELECT MIN (players), MAX (players) FROM plays WHERE game = 'quake';

Sum
```

The ``sum`` function can be used to sum up all the values returned by a query for a given column. For instance::

    SELECT SUM (players) FROM plays;

Avg
```

The ``avg`` function can be used to compute the average of all the values returned by a query for a given column. For
instance::

    SELECT AVG (players) FROM plays;

.. _user-defined-aggregates-functions:

User-Defined Aggregates
~~~~~~~~~~~~~~~~~~~~~~~

User-defined aggregates allow the creation of custom aggregate functions. Common examples of aggregate functions are
*count*, *min*, and *max*.

Each aggregate requires an *initial state* (``INITCOND``, which defaults to ``null``) of type ``STYPE``. The first
argument of the state function must have type ``STYPE``. The remaining arguments of the state function must match the
types of the user-defined aggregate arguments. The state function is called once for each row, and the value returned by
the state function becomes the new state. After all rows are processed, the optional ``FINALFUNC`` is executed with last
state value as its argument.

``STYPE`` is mandatory in order to be able to distinguish possibly overloaded versions of the state and/or final
function (since the overload can appear after creation of the aggregate).

User-defined aggregates can be used in ``SELECT`` statement.

A complete working example for user-defined aggregates (assuming that a keyspace has been selected using the ``USE``
statement)::

    CREATE OR REPLACE FUNCTION averageState(state tuple<int,bigint>, val int)
        CALLED ON NULL INPUT
        RETURNS tuple
        LANGUAGE java
        AS $$
            if (val != null) {
                state.setInt(0, state.getInt(0)+1);
                state.setLong(1, state.getLong(1)+val.intValue());
            }
            return state;
        $$;

    CREATE OR REPLACE FUNCTION averageFinal (state tuple<int,bigint>)
        CALLED ON NULL INPUT
        RETURNS double
        LANGUAGE java
        AS $$
            double r = 0;
            if (state.getInt(0) == 0) return null;
            r = state.getLong(1);
            r /= state.getInt(0);
            return Double.valueOf(r);
        $$;

    CREATE OR REPLACE AGGREGATE average(int)
        SFUNC averageState
        STYPE tuple
        FINALFUNC averageFinal
        INITCOND (0, 0);

    CREATE TABLE atable (
        pk int PRIMARY KEY,
        val int
    );

    INSERT INTO atable (pk, val) VALUES (1,1);
    INSERT INTO atable (pk, val) VALUES (2,2);
    INSERT INTO atable (pk, val) VALUES (3,3);
    INSERT INTO atable (pk, val) VALUES (4,4);

    SELECT average(val) FROM atable;

.. _create-aggregate-statement:

CREATE AGGREGATE
````````````````

Creating (or replacing) a user-defined aggregate function uses the ``CREATE AGGREGATE`` statement:

.. productionlist::
   create_aggregate_statement: CREATE [ OR REPLACE ] AGGREGATE [ IF NOT EXISTS ]
                             :     `function_name` '(' `arguments_signature` ')'
                             :     SFUNC `function_name`
                             :     STYPE `cql_type`
                             :     [ FINALFUNC `function_name` ]
                             :     [ INITCOND `term` ]

See above for a complete example.

``CREATE AGGREGATE`` with the optional ``OR REPLACE`` keywords either creates an aggregate or replaces an existing one
with the same signature. A ``CREATE AGGREGATE`` without ``OR REPLACE`` fails if an aggregate with the same signature
already exists.

``CREATE AGGREGATE`` with the optional ``IF NOT EXISTS`` keywords either creates an aggregate if it does not already
exist.

``OR REPLACE`` and ``IF NOT EXISTS`` cannot be used together.

``STYPE`` defines the type of the state value and must be specified.

The optional ``INITCOND`` defines the initial state value for the aggregate. It defaults to ``null``. A non-\ ``null``
``INITCOND`` must be specified for state functions that are declared with ``RETURNS NULL ON NULL INPUT``.

``SFUNC`` references an existing function to be used as the state modifying function. The type of first argument of the
state function must match ``STYPE``. The remaining argument types of the state function must match the argument types of
the aggregate function. State is not updated for state functions declared with ``RETURNS NULL ON NULL INPUT`` and called
with ``null``.

The optional ``FINALFUNC`` is called just before the aggregate result is returned. It must take only one argument with
type ``STYPE``. The return type of the ``FINALFUNC`` may be a different type. A final function declared with ``RETURNS
NULL ON NULL INPUT`` means that the aggregate's return value will be ``null``, if the last state is ``null``.

If no ``FINALFUNC`` is defined, the overall return type of the aggregate function is ``STYPE``. If a ``FINALFUNC`` is
defined, it is the return type of that function.

.. _drop-aggregate-statement:

DROP AGGREGATE
``````````````

Dropping an user-defined aggregate function uses the ``DROP AGGREGATE`` statement:

.. productionlist::
   drop_aggregate_statement: DROP AGGREGATE [ IF EXISTS ] `function_name` [ '(' `arguments_signature` ')' ]

For instance::

    DROP AGGREGATE myAggregate;
    DROP AGGREGATE myKeyspace.anAggregate;
    DROP AGGREGATE someAggregate ( int );
    DROP AGGREGATE someAggregate ( text );

The ``DROP AGGREGATE`` statement removes an aggregate created using ``CREATE AGGREGATE``. You must specify the argument
types of the aggregate to drop if there are multiple aggregates with the same name but a different signature (overloaded
aggregates).

``DROP AGGREGATE`` with the optional ``IF EXISTS`` keywords drops an aggregate if it exists, and does nothing if a
function with the signature does not exist.
