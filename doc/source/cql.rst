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

.. _UUID: https://en.wikipedia.org/wiki/Universally_unique_identifier

The Cassandra Query Language (CQL)
==================================

This document describes the Cassandra Query Language (CQL) [#]_. Note that this document describes the last version of
the languages. However, the `changes <#changes>`_ section provides the diff between the different versions of CQL.

CQL offers a model close to SQL in the sense that data is put in *tables* containing *rows* of *columns*. For
that reason, when used in this document, these terms (tables, rows and columns) have the same definition than they have
in SQL. But please note that as such, they do **not** refer to the concept of rows and columns found in the deprecated
thrift API (and earlier version 1 and 2 of CQL).

.. _definitions:

Definitions
-----------

.. _conventions:

Conventions
^^^^^^^^^^^

To aid in specifying the CQL syntax, we will use the following conventions in this document:

- Language rules will be given in an informal `BNF variant
  <http://en.wikipedia.org/wiki/Backus%E2%80%93Naur_Form#Variants>`_ notation. In particular, we'll use square brakets
  (``[ item ]``) for optional items, ``*`` and ``+`` for repeated items (where ``+`` imply at least one).
- The grammar will also use the following convention for convenience: non-terminal term will be lowercase (and link to
  their definition) while terminal keywords will be provided "all caps". Note however that keywords are
  :ref:`identifiers` and are thus case insensitive in practice. We will also define some early construction using
  regexp, which we'll indicate with ``re(<some regular expression>)``.
- The grammar is provided for documentation purposes and leave some minor details out.  For instance, the comma on the
  last column definition in a ``CREATE TABLE`` statement is optional but supported if present even though the grammar in
  this document suggests otherwise. Also, not everything accepted by the grammar is necessarily valid CQL.
- References to keywords or pieces of CQL code in running text will be shown in a ``fixed-width font``.


.. _identifiers:

Identifiers and keywords
^^^^^^^^^^^^^^^^^^^^^^^^

The CQL language uses *identifiers* (or *names*) to identify tables, columns and other objects. An identifier is a token
matching the regular expression ``[a-zA-Z][a-zA-Z0-9_]*``.

A number of such identifiers, like ``SELECT`` or ``WITH``, are *keywords*. They have a fixed meaning for the language
and most are reserved. The list of those keywords can be found in :ref:`appendix-A`.

Identifiers and (unquoted) keywords are case insensitive. Thus ``SELECT`` is the same than ``select`` or ``sElEcT``, and
``myId`` is the same than ``myid`` or ``MYID``. A convention often used (in particular by the samples of this
documentation) is to use upper case for keywords and lower case for other identifiers.

There is a second kind of identifiers called *quoted identifiers* defined by enclosing an arbitrary sequence of
characters (non empty) in double-quotes(``"``). Quoted identifiers are never keywords. Thus ``"select"`` is not a
reserved keyword and can be used to refer to a column (note that using this is particularly advised), while ``select``
would raise a parsing error. Also, contrarily to unquoted identifiers and keywords, quoted identifiers are case
sensitive (``"My Quoted Id"`` is *different* from ``"my quoted id"``). A fully lowercase quoted identifier that matches
``[a-zA-Z][a-zA-Z0-9_]*`` is however *equivalent* to the unquoted identifier obtained by removing the double-quote (so
``"myid"`` is equivalent to ``myid`` and to ``myId`` but different from ``"myId"``).  Inside a quoted identifier, the
double-quote character can be repeated to escape it, so ``"foo "" bar"`` is a valid identifier.

.. note:: *quoted identifiers* allows to declare columns with arbitrary names, and those can sometime clash with
   specific names used by the server. For instance, when using conditional update, the server will respond with a
   result-set containing a special result named ``"[applied]"``. If you’ve declared a column with such a name, this
   could potentially confuse some tools and should be avoided. In general, unquoted identifiers should be preferred but
   if you use quoted identifiers, it is strongly advised to avoid any name enclosed by squared brackets (like
   ``"[applied]"``) and any name that looks like a function call (like ``"f(x)"``).

More formally, we have:

.. productionlist::
   identifier: `unquoted_identifier` | `quoted_identifier`
   unquoted_identifier: re('[a-zA-Z][a-zA-Z0-9_]*')
   quoted_identifier: '"' (any character where " can appear if doubled)+ '"'

.. _constants:

Constants
^^^^^^^^^

CQL defines the following kind of *constants*:

.. productionlist::
   constant: `string` | `integer` | `float` | `boolean` | `uuid` | `blob` | NULL
   string: '\'' (any character where ' can appear if doubled)+ '\''
         : '$$' (any character other than '$$') '$$'
   integer: re('-?[0-9]+')
   float: re('-?[0-9]+(\.[0-9]*)?([eE][+-]?[0-9+])?') | NAN | INFINITY
   boolean: TRUE | FALSE
   uuid: `hex`{8}-`hex`{4}-`hex`{4}-`hex`{4}-`hex`{12}
   hex: re("[0-9a-fA-F]")
   blob: '0' ('x' | 'X') `hex`+

In other words:

- A string constant is an arbitrary sequence of characters enclosed by single-quote(``'``). A single-quote
  can be included by repeating it, e.g. ``'It''s raining today'``. Those are not to be confused with quoted
  :ref:`identifiers` that use double-quotes. Alternatively, a string can be defined by enclosing the arbitrary sequence
  of characters by two dollar characters, in which case single-quote can be use without escaping (``$$It's raining
  today$$``). That latter form is often used when defining :ref:`user-defined functions <udfs>` to avoid having to
  escape single-quote characters in function body (as they are more likely to occur than ``$$``).
- Integer, float and boolean constant are defined as expected. Note however than float allows the special ``NaN`` and
  ``Infinity`` constants.
- CQL supports UUID_ constants.
- Blobs content are provided in hexadecimal and prefixed by ``0x``.
- The special ``NULL`` constant denotes the absence of value.

For how these constants are typed, see the :ref:`data-types` section.

Terms
^^^^^

CQL has the notion of a *term*, which denotes the kind of values that CQL support. Terms are defined by:

.. productionlist::
   term: `constant` | `literal` | `function_call` | `type_hint` | `bind_marker`
   literal: `collection_literal` | `udt_literal` | `tuple_literal`
   function_call: `identifier` '(' [ `term` (',' `term`)* ] ')'
   type_hint: '(' `cql_type` `)` term
   bind_marker: '?' | ':' `identifier`

A term is thus one of:

- A :ref:`constant <constants>`.
- A literal for either :ref:`a collection <collections>`, :ref:`a user-defined type <udts>` or :ref:`a tuple <tuples>`
  (see the linked sections for details).
- A function call: see :ref:`the section on functions <functions>` for details on which :ref:`native function
  <native-functions>` exists and how to define your own :ref:`user-defined ones <user-defined-functions>`.
- A *type hint*: see the :ref:`related section <type-hints>` for details.
- A bind marker, which denotes a variable to be bound at execution time. See the section on :ref:`prepared-statements`
  for details. A bind marker can be either anonymous (``?``) or named (``:some_name``). The latter form provides a more
  convenient way to refer to the variable for binding it and should generally be preferred.


Comments
^^^^^^^^

A comment in CQL is a line beginning by either double dashes (``--``) or double slash (``//``).

Multi-line comments are also supported through enclosure within ``/*`` and ``*/`` (but nesting is not supported).

::

    — This is a comment
    // This is a comment too
    /* This is
       a multi-line comment */

Statements
^^^^^^^^^^

CQL consists of statements that can be divided in the following categories:

- :ref:`data-definition` statements, to define and change how the data is stored (keyspaces and tables).
- :ref:`data-manipulation` statements, for selecting, inserting and deleting data.
- :ref:`index-and-views` statements.
- :ref:`roles-and-permissions` statements.
- :ref:`udfs` statements.
- :ref:`udts` statements.
- :ref:`triggers` statements.

All the statements are listed below and are described in the rest of this documentation (see links above):

.. productionlist::
   cql_statement: `statement` [ ';' ]
   statement: `ddl_statement`
            : | `dml_statement`
            : | `index_or_view_statement`
            : | `role_or_permission_statement`
            : | `udf_statement`
            : | `udt_statement`
            : | `trigger_statement`
   ddl_statement: `use_statement`
                : | `create_keyspace_statement`
                : | `alter_keyspace_statement`
                : | `drop_keyspace_statement`
                : | `create_table_statement`
                : | `alter_table_statement`
                : | `drop_table_statement`
                : | `truncate_statement`
    dml_statement: `select_statement`
                 : | `insert_statement`
                 : | `update_statement`
                 : | `delete_statement`
                 : | `batch_statement`
    index_or_view_statement: `create_index_statement`
                           : | `drop_index_statement`
                           : | `create_materialized_view_statement`
                           : | `drop_materialized_view_statement`
    role_or_permission_statement: `create_role_statement`
                                : | `alter_role_statement`
                                : | `drop_role_statement`
                                : | `grant_role_statement`
                                : | `revoke_role_statement`
                                : | `list_role_statement`
                                : | `grant_permission_statement`
                                : | `revoke_permission_statement`
                                : | `list_permission_statement`
                                : | `create_user_statement`
                                : | `alter_user_statement`
                                : | `drop_user_statement`
                                : | `list_user_statement`
    udf_statement: `create_function_statement`
                 : | `drop_function_statement`
                 : | `create_aggregate_statement`
                 : | `drop_aggregate_statement`
    udt_statement: `create_type_statement`
                 : | `alter_type_statement`
                 : | `drop_type_statement`
    trigger_statement: `create_trigger_statement`
                     : | `drop_trigger_statement`

.. _prepared-statements:

Prepared Statements
^^^^^^^^^^^^^^^^^^^

CQL supports *prepared statements*. Prepared statements are an optimization that allows to parse a query only once but
execute it multiple times with different concrete values.

Any statement that uses at least one bind marker (see :token:`bind_marker`) will need to be *prepared*. After which the statement
can be *executed* by provided concrete values for each of its marker. The exact details of how a statement is prepared
and then executed depends on the CQL driver used and you should refer to your driver documentation.


.. _data-types:

Data Types
----------

CQL is a typed language and supports a rich set of data types, including :ref:`native types <native-types>`,
:ref:`collection types <collections>`, :ref:`user-defined types <udts>`, :ref:`tuple types <tuples>` and :ref:`custom
types <custom-types>`:

.. productionlist::
   cql_type: `native_type` | `collection_type` | `user_defined_type` | `tuple_type` | `custom_type`


.. _native-types:

Native Types
^^^^^^^^^^^^

The native types supported by CQL are:

.. productionlist::
   native_type: ASCII
              : | BIGINT
              : | BLOB
              : | BOOLEAN
              : | COUNTER
              : | DATE
              : | DECIMAL
              : | DOUBLE
              : | FLOAT
              : | INET
              : | INT
              : | SMALLINT
              : | TEXT
              : | TIME
              : | TIMESTAMP
              : | TIMEUUID
              : | TINYINT
              : | UUID
              : | VARCHAR
              : | VARINT

The following table gives additional informations on the native data types, and on which kind of :ref:`constants
<constants>` each type supports:

=============== ===================== ==================================================================================
 type            constants supported   description
=============== ===================== ==================================================================================
 ``ascii``       :token:`string`       ASCII character string
 ``bigint``      :token:`integer`      64-bit signed long
 ``blob``        :token:`blob`         Arbitrary bytes (no validation)
 ``boolean``     :token:`boolean`      Either ``true`` or ``false``
 ``counter``     :token:`integer`      Counter column (64-bit signed value). See :ref:`counters` for details
 ``date``        :token:`integer`,     A date (with no corresponding time value). See :ref:`dates` below for details
                 :token:`string`
 ``decimal``     :token:`integer`,     Variable-precision decimal
                 :token:`float`
 ``double``      :token:`integer`      64-bit IEEE-754 floating point
                 :token:`float`
 ``float``       :token:`integer`,     32-bit IEEE-754 floating point
                 :token:`float`
 ``inet``        :token:`string`       An IP address, either IPv4 (4 bytes long) or IPv6 (16 bytes long). Note that
                                       there is no ``inet`` constant, IP address should be input as strings
 ``int``         :token:`integer`      32-bit signed int
 ``smallint``    :token:`integer`      16-bit signed int
 ``text``        :token:`string`       UTF8 encoded string
 ``time``        :token:`integer`,     A time (with no corresponding date value) with nanosecond precision. See
                 :token:`string`       :ref:`times` below for details
 ``timestamp``   :token:`integer`,     A timestamp (date and time) with millisecond precision. See :ref:`timestamps`
                 :token:`string`       below for details
 ``timeuuid``    :token:`uuid`         Version 1 UUID_, generally used as a “conflict-free” timestamp. Also see
                                       :ref:`timeuuid-functions`
 ``tinyint``     :token:`integer`      8-bit signed int
 ``uuid``        :token:`uuid`         A UUID_ (of any version)
 ``varchar``     :token:`string`       UTF8 encoded string
 ``varint``      :token:`integer`      Arbitrary-precision integer
=============== ===================== ==================================================================================

.. _counters:

Counters
~~~~~~~~

The ``counter`` type is used to define *counter columns*. A counter column is a column whose value is a 64-bit signed
integer and on which 2 operations are supported: incrementing and decrementing (see the :ref:`UPDATE statement
<update-statement>` for syntax). Note that the value of a counter cannot be set: a counter does not exist until first
incremented/decremented, and that first increment/decrement is made as if the prior value was 0.

.. _counter-limitations:

Counters have a number of important limitations:

- They cannot be used for columns part of the ``PRIMARY KEY`` of a table.
- A table that contains a counter can only contain counters. In other words, either all the columns of a table outside
  the ``PRIMARY KEY`` have the ``counter`` type, or none of them have it.
- Counters do not support :ref:`expiration <ttls>`.
- The deletion of counters is supported, but is only guaranteed to work the first time you delete a counter. In other
  words, you should not re-update a counter that you have deleted (if you do, proper behavior is not guaranteed).
- Counter updates are, by nature, not `idemptotent <https://en.wikipedia.org/wiki/Idempotence>`__. An important
  consequence is that if a counter update fails unexpectedly (timeout or loss of connection to the coordinator node),
  the client has no way to know if the update has been applied or not. In particular, replaying the update may or may
  not lead to an over count.

.. _timestamps:

Working with timestamps
^^^^^^^^^^^^^^^^^^^^^^^

Values of the ``timestamp`` type are encoded as 64-bit signed integers representing a number of milliseconds since the
standard base time known as `the epoch <https://en.wikipedia.org/wiki/Unix_time>`__: January 1 1970 at 00:00:00 GMT.

Timestamps can be input in CQL either using their value as an :token:`integer`, or using a :token:`string` that
represents an `ISO 8601 <https://en.wikipedia.org/wiki/ISO_8601>`__ date. For instance, all of the values below are
valid ``timestamp`` values for  Mar 2, 2011, at 04:05:00 AM, GMT:

- ``1299038700000``
- ``'2011-02-03 04:05+0000'``
- ``'2011-02-03 04:05:00+0000'``
- ``'2011-02-03 04:05:00.000+0000'``
- ``'2011-02-03T04:05+0000'``
- ``'2011-02-03T04:05:00+0000'``
- ``'2011-02-03T04:05:00.000+0000'``

The ``+0000`` above is an RFC 822 4-digit time zone specification; ``+0000`` refers to GMT. US Pacific Standard Time is
``-0800``. The time zone may be omitted if desired (``'2011-02-03 04:05:00'``), and if so, the date will be interpreted
as being in the time zone under which the coordinating Cassandra node is configured. There are however difficulties
inherent in relying on the time zone configuration being as expected, so it is recommended that the time zone always be
specified for timestamps when feasible.

The time of day may also be omitted (``'2011-02-03'`` or ``'2011-02-03+0000'``), in which case the time of day will
default to 00:00:00 in the specified or default time zone. However, if only the date part is relevant, consider using
the :ref:`date <dates>` type.

.. _dates:

Working with dates
^^^^^^^^^^^^^^^^^^

Values of the ``date`` type are encoded as 32-bit unsigned integers representing a number of days with “the epoch” at
the center of the range (2^31). Epoch is January 1st, 1970

As for :ref:`timestamp <timestamps>`, a date can be input either as an :token:`integer` or using a date
:token:`string`. In the later case, the format should be ``yyyy-mm-dd`` (so ``'2011-02-03'`` for instance).

.. _times:

Working with times
^^^^^^^^^^^^^^^^^^

Values of the ``time`` type are encoded as 64-bit signed integers representing the number of nanoseconds since midnight.

As for :ref:`timestamp <timestamps>`, a time can be input either as an :token:`integer` or using a :token:`string`
representing the time. In the later case, the format should be ``hh:mm:ss[.fffffffff]`` (where the sub-second precision
is optional and if provided, can be less than the nanosecond). So for instance, the following are valid inputs for a
time:

-  ``'08:12:54'``
-  ``'08:12:54.123'``
-  ``'08:12:54.123456'``
-  ``'08:12:54.123456789'``


.. _collections:

Collections
^^^^^^^^^^^

CQL supports 3 kind of collections: :ref:`maps`, :ref:`sets` and :ref:`lists`. The types of those collections is defined
by:

.. productionlist::
   collection_type: MAP '<' `cql_type` ',' `cql_type` '>'
                  : | SET '<' `cql_type` '>'
                  : | LIST '<' `cql_type` '>'

and their values can be inputd using collection literals:

.. productionlist::
   collection_literal: `map_literal` | `set_literal` | `list_literal`
   map_literal: '{' [ `term` ':' `term` (',' `term` : `term`)* ] '}'
   set_literal: '{' [ `term` (',' `term`)* ] '}'
   list_literal: '[' [ `term` (',' `term`)* ] ']'

Note however that neither :token:`bind_marker` nor ``NULL`` are supported inside collection literals.

Noteworthy characteristics
~~~~~~~~~~~~~~~~~~~~~~~~~~

Collections are meant for storing/denormalizing relatively small amount of data. They work well for things like “the
phone numbers of a given user”, “labels applied to an email”, etc. But when items are expected to grow unbounded (“all
messages sent by a user”, “events registered by a sensor”...), then collections are not appropriate and a specific table
(with clustering columns) should be used. Concretely, (non-frozen) collections have the following noteworthy
characteristics and limitations:

- Individual collections are not indexed internally. Which means that even to access a single element of a collection,
  the while collection has to be read (and reading one is not paged internally).
- While insertion operations on sets and maps never incur a read-before-write internally, some operations on lists do.
  Further, some lists operations are not idempotent by nature (see the section on :ref:`lists <lists>` below for
  details), making their retry in case of timeout problematic. It is thus advised to prefer sets over lists when
  possible.

Please note that while some of those limitations may or may not be removed/improved upon in the future, it is a
anti-pattern to use a (single) collection to store large amounts of data.

.. _maps:

Maps
~~~~

A ``map`` is a (sorted) set of key-value pairs, where keys are unique and the map is sorted by its keys. You can define
and insert a map with::

    CREATE TABLE users (
        id text PRIMARY KEY,
        name text,
        favs map<text, text> // A map of text keys, and text values
    );

    INSERT INTO users (id, name, favs)
               VALUES ('jsmith', 'John Smith', { 'fruit' : 'Apple', 'band' : 'Beatles' });

    // Replace the existing map entirely.
    UPDATE users SET favs = { 'fruit' : 'Banana' } WHERE id = 'jsmith';

Further, maps support:

- Updating or inserting one or more elements::

    UPDATE users SET favs['author'] = 'Ed Poe' WHERE id = 'jsmith';
    UPDATE users SET favs = favs + { 'movie' : 'Cassablanca', 'band' : 'ZZ Top' } WHERE id = 'jsmith';

- Removing one or more element (if an element doesn't exist, removing it is a no-op but no error is thrown)::

    DELETE favs['author'] FROM users WHERE id = 'jsmith';
    UPDATE users SET favs = favs - { 'movie', 'band'} WHERE id = 'jsmith';

  Note that for removing multiple elements in a ``map``, you remove from it a ``set`` of keys.

Lastly, TTLs are allowed for both ``INSERT`` and ``UPDATE``, but in both case the TTL set only apply to the newly
inserted/updated elements. In other words::

    UPDATE users USING TTL 10 SET favs['color'] = 'green' WHERE id = 'jsmith';

will only apply the TTL to the ``{ 'color' : 'green' }`` record, the rest of the map remaining unaffected.


.. _sets:

Sets
~~~~

A ``set`` is a (sorted) collection of unique values. You can define and insert a map with::

    CREATE TABLE images (
        name text PRIMARY KEY,
        owner text,
        tags set<text> // A set of text values
    );

    INSERT INTO images (name, owner, tags)
                VALUES ('cat.jpg', 'jsmith', { 'pet', 'cute' });

    // Replace the existing set entirely
    UPDATE images SET tags = { 'kitten', 'cat’, 'lol' } WHERE id = 'jsmith';

Further, sets support:

- Adding one or multiple elements (as this is a set, inserting an already existing element is a no-op)::

    UPDATE images SET tags = tags + { 'gray', 'cuddly' } WHERE name = 'cat.jpg';

- Removing one or multiple elements (if an element doesn't exist, removing it is a no-op but no error is thrown)::

    UPDATE images SET tags = tags - { 'cat' } WHERE name = 'cat.jpg';

Lastly, as for :ref:`maps <maps>`, TTLs if used only apply to the newly inserted values.

.. _lists:

Lists
~~~~~

.. note:: As mentioned above and further discussed at the end of this section, lists have limitations and specific
   performance considerations that you should take into account before using them. In general, if you can use a
   :ref:`set <sets>` instead of list, always prefer a set.

A ``list`` is a (sorted) collection of non-unique values where elements are ordered by there position in the list. You
can define and insert a list with::

    CREATE TABLE plays (
        id text PRIMARY KEY,
        game text,
        players int,
        scores list<int> // A list of integers
    )

    INSERT INTO plays (id, game, players, scores)
               VALUES ('123-afde', 'quake', 3, [17, 4, 2]);

    // Replace the existing list entirely
    UPDATE plays SET scores = [ 3, 9, 4] WHERE id = '123-afde';

Further, lists support:

- Appending and prepending values to a list::

    UPDATE plays SET players = 5, scores = scores + [ 14, 21 ] WHERE id = '123-afde';
    UPDATE plays SET players = 6, scores = [ 3 ] + scores WHERE id = '123-afde';

- Setting the value at a particular position in the list. This imply that the list has a pre-existing element for that
  position or an error will be thrown that the list is too small::

    UPDATE plays SET scores[1] = 7 WHERE id = '123-afde';

- Removing an element by its position in the list. This imply that the list has a pre-existing element for that position
  or an error will be thrown that the list is too small. Further, as the operation removes an element from the list, the
  list size will be diminished by 1, shifting the position of all the elements following the one deleted::

    DELETE scores[1] FROM plays WHERE id = '123-afde';

- Deleting *all* the occurrences of particular values in the list (if a particular element doesn't occur at all in the
  list, it is simply ignored and no error is thrown)::

    UPDATE plays SET scores = scores - [ 12, 21 ] WHERE id = '123-afde';

.. warning:: The append and prepend operations are not idempotent by nature. So in particular, if one of these operation
   timeout, then retrying the operation is not safe and it may (or may not) lead to appending/prepending the value
   twice.

.. warning:: Setting and removing an element by position and removing occurences of particular values incur an internal
   *read-before-write*. They will thus run more slowly and take more ressources than usual updates (with the exclusion
   of conditional write that have their own cost).

Lastly, as for :ref:`maps <maps>`, TTLs when used only apply to the newly inserted values.

.. _udts:

User-Defined Types
^^^^^^^^^^^^^^^^^^

CQL support the definition of user-defined types (UDT for short). Such a type can be created, modified and removed using
the :token:`create_type_statement`, :token:`alter_type_statement` and :token:`drop_type_statement` described below. But
once created, a UDT is simply referred to by its name:

.. productionlist::
   user_defined_type: `udt_name`
   udt_name: [ `keyspace_name` '.' ] `identifier`


Creating a UDT
~~~~~~~~~~~~~~

Creating a new user-defined type is done using a ``CREATE TYPE`` statement defined by:

.. productionlist::
   create_type_statement: CREATE TYPE [ IF NOT EXISTS ] `udt_name`
                        :     '(' `field_definition` ( ',' `field_definition` )* ')'
   field_definition: `identifier` `cql_type`

A UDT has a name (used to declared columns of that type) and is a set of named and typed fields. Fields name can be any
type, including collections or other UDT. For instance::

    CREATE TYPE phone (
        country_code int,
        number text,
    )

    CREATE TYPE address (
        street text,
        city text,
        zip int,
        phones map<text, phone>
    )

    CREATE TABLE user (
        name text PRIMARY KEY,
        addresses map<text, frozen<address>>
    )

Note that:

- Attempting to create an already existing type will result in an error unless the ``IF NOT EXISTS`` option is used. If
  it is used, the statement will be a no-op if the type already exists.
- A type is intrinsically bound to the keyspace in which it is created, and can only be used in that keyspace. At
  creation, if the type name is prefixed by a keyspace name, it is created in that keyspace. Otherwise, it is created in
  the current keyspace.
- As of Cassandra |version|, UDT have to be frozen in most cases, hence the ``frozen<address>`` in the table definition
  above. Please see the section on :ref:`frozen <frozen>` for more details.

UDT literals
~~~~~~~~~~~~

Once a used-defined type has been created, value can be input using a UDT literal:

.. productionlist::
   udt_literal: '{' `identifier` ':' `term` ( ',' `identifier` ':' `term` )* '}'

In other words, a UDT literal is like a :ref:`map <maps>` literal but its keys are the names of the fields of the type.
For instance, one could insert into the table define in the previous section using::

    INSERT INTO user (name, addresses)
              VALUES ('z3 Pr3z1den7', {
                  'home' : {
                      street: '1600 Pennsylvania Ave NW',
                      city: 'Washington',
                      zip: '20500',
                      phones: { 'cell' : { country_code: 1, number: '202 456-1111' },
                                'landline' : { country_code: 1, number: '...' } }
                  }
                  'work' : {
                      street: '1600 Pennsylvania Ave NW',
                      city: 'Washington',
                      zip: '20500',
                      phones: { 'fax' : { country_code: 1, number: '...' } }
                  }
              })

To be valid, a UDT literal should only include fields defined by the type it is a literal of, but it can omit some field
(in which case those will be ``null``).

Altering a UDT
~~~~~~~~~~~~~~

An existing user-defined type can be modified using an ``ALTER TYPE`` statement:

.. productionlist::
   alter_type_statement: ALTER TYPE `udt_name` `alter_type_modification`
   alter_type_modification: ALTER `identifier` TYPE `cql_type`
                          : | ADD `field_definition`
                          : | RENAME `identifier` TO `identifier` ( `identifier` TO `identifier` )*

You can:

- modify the type of particular field (``ALTER TYPE address ALTER zip TYPE bigint``). The restrictions for such change
  are the same than when :ref:`altering the type of column <alter-table>`.
- add a new field to the type (``ALTER TYPE address ADD country text``). That new field will be ``null`` for any values
  of the type created before the addition.
- rename the fields of the type (``ALTER TYPE address RENAME zip TO zipcode``).

Dropping a UDT
~~~~~~~~~~~~~~

You can drop an existing user-defined type using a ``DROP TYPE`` statement:

.. productionlist::
   drop_type_statement: DROP TYPE [ IF EXISTS ] `udt_name`

Dropping a type results in the immediate, irreversible removal of that type. However, attempting to drop a type that is
still in use by another type, table or function will result in an error.

If the type dropped does not exist, an error will be returned unless ``IF EXISTS`` is used, in which case the operation
is a no-op.

.. _tuples:

Tuples
^^^^^^

CQL also support tuples and tuple types (where the elements can be of different types). Functionally, tuples can be
though as anonymous UDT with anonymous fields. Tuple types and tuple literals are defined by:

.. productionlist::
   tuple_type: TUPLE '<' `cql_type` ( ',' `cql_type` )* '>'
   tuple_literal: '(' `term` ( ',' `term` )* ')'

and can be used thusly::

    CREATE TABLE durations (
        event text,
        duration tuple<int, text>,
    )

    INSERT INTO durations (event, duration) VALUES ('ev1', (3, 'hours'));

Unlike other "composed" types (collections and UDT), a tuple is always :ref:`frozen <frozen>` (without the need of the
`frozen` keyword) and it is not possible to update only some elements of a tuple (without updating the whole tuple).
Also, a tuple literal should always have the same number of value than declared in the type it is a tuple of (some of
those values can be null but they need to be explicitly declared as so).

.. _custom-types:

Custom Types
^^^^^^^^^^^^

.. note:: Custom types exists mostly for backward compatiliby purposes and their usage is discouraged. Their usage is
   complex, not user friendly and the other provided types, particularly :ref:`user-defined types <udts>`, should almost
   always be enough.

A custom type is defined by:

.. productionlist::
   custom_type: `string`

A custom type is a :token:`string` that contains the name of Java class that extends the server side ``AbstractType``
class and that can be loaded by Cassandra (it should thus be in the ``CLASSPATH`` of every node running Cassandra). That
class will define what values are valid for the type and how the time sorts when used for a clustering column. For any
other purpose, a value of a custom type is the same than that of a ``blob``, and can in particular be input using the
:token:`blob` literal syntax.


.. _data-definition:

Data Definition
---------------

CQL stores data in *tables*, whose schema defines the layout of said data in the table, and those tables are grouped in
*keyspaces*. A keyspace defines a number of options that applies to all the tables it contains, most prominently of
which is the :ref:`replication strategy <replication-strategy>` used by the keyspace. It is generally encouraged to use
one keyspace by *application*, and thus many cluster may define only one keyspace.

This section describes the statements used to create, modify, and remove those keyspace and tables.

Common definitions
^^^^^^^^^^^^^^^^^^

The names of the keyspaces and tables are defined by the following grammar:

.. productionlist::
   keyspace_name: `name`
   table_name: [ `keyspace_name` '.' ] `name`
   name: `unquoted_name` | `quoted_name`
   unquoted_name: re('[a-zA-Z_0-9]{1, 48}')
   quoted_name: '"' `unquoted_name` '"'

Both keyspace and table name should be comprised of only alphanumeric characters, cannot be empty and are limited in
size to 48 characters (that limit exists mostly to avoid filenames (which may include the keyspace and table name) to go
over the limits of certain file systems). By default, keyspace and table names are case insensitive (``myTable`` is
equivalent to ``mytable``) but case sensitivity can be forced by using double-quotes (``"myTable"`` is different from
``mytable``).

Further, a table is always part of a keyspace and a table name can be provided fully-qualified by the keyspace it is
part of. If is is not fully-qualified, the table is assumed to be in the *current* keyspace (see :ref:`USE statement
<use-statement>`).

We also define the notion of statement options for use in the following section:

.. productionlist::
   options: `option` ( AND `option` )*
   option: `identifier` '=' ( `identifier` | `constant` | `map_literal` )

.. _create-keyspace-statement:

CREATE KEYSPACE
^^^^^^^^^^^^^^^

A keyspace is created using a ``CREATE KEYSPACE`` statement:

.. productionlist::
   create_keyspace_statement: CREATE KEYSPACE [ IF NOT EXISTS ] `keyspace_name` WITH `options`

For instance::

    CREATE KEYSPACE Excelsior
               WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

    CREATE KEYSPACE Excalibur
               WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1' : 1, 'DC2' : 3}
                AND durable_writes = false;


.. _create-keyspace-options:
The supported ``options`` are:

=================== ========== =========== ========= ===================================================================
name                 kind       mandatory   default   description
=================== ========== =========== ========= ===================================================================
``replication``      *map*      yes                   The replication strategy and options to use for the keyspace (see
                                                      details below).
``durable_writes``   *simple*   no          true      Whether to use the commit log for updates on this keyspace
                                                      (disable this option at your own risk!).
=================== ========== =========== ========= ===================================================================

The ``replication`` property is mandatory and must at least contains the ``'class'`` sub-option which defines the
:ref:`replication strategy <replication-strategy>` class to use. The rest of the sub-options depends on what replication
strategy is used. By default, Cassandra support the following ``'class'``:

- ``'SimpleStrategy'``: A simple strategy that defines a replication factor for the whole cluster. The only sub-options
  supported is ``'replication_factor'`` to define that replication factor and is mandatory.
- ``'NetworkTopologyStrategy'``: A replication strategy that allows to set the replication factor independently for
  each data-center. The rest of the sub-options are key-value pairs where a key is a data-center name and its value is
  the associated replication factor.

Attempting to create a keyspace that already exists will return an error unless the ``IF NOT EXISTS`` option is used. If
it is used, the statement will be a no-op if the keyspace already exists.

.. _use-statement:

USE
^^^

The ``USE`` statement allows to change the *current* keyspace (for the *connection* on which it is executed). A number
of objects in CQL are bound to a keyspace (tables, user-defined types, functions, ...) and the current keyspace is the
default keyspace used when those objects are referred without a fully-qualified name (that is, without being prefixed a
keyspace name). A ``USE`` statement simply takes the keyspace to use as current as argument:

.. productionlist::
   use_statement: USE `keyspace_name`

.. _alter-keyspace-statement:

ALTER KEYSPACE
^^^^^^^^^^^^^^

An ``ALTER KEYSPACE`` statement allows to modify the options of a keyspace:

.. productionlist::
   alter_keyspace_statement: ALTER KEYSPACE `keyspace_name` WITH `options`

For instance::

    ALTER KEYSPACE Excelsior
              WITH replication = {’class’: ‘SimpleStrategy’, ‘replication_factor’ : 4};

The supported options are the same than for :ref:`creating a keyspace <create-keyspace-options>`.

.. _drop-keyspace-statement:

DROP KEYSPACE
^^^^^^^^^^^^^

Dropping a keyspace can be done using the ``DROP KEYSPACE`` statement:

.. productionlist::
   drop_keyspace_statement: DROP KEYSPACE [ IF EXISTS ] `keyspace_name`

For instance::

    DROP KEYSPACE Excelsior;

Dropping a keyspace results in the immediate, irreversible removal of that keyspace, including all the tables, UTD and
functions in it, and all the data contained in those tables.

If the keyspace does not exists, the statement will return an error, unless ``IF EXISTS`` is used in which case the
operation is a no-op.

.. _create-table-statement:

CREATE TABLE
^^^^^^^^^^^^

Creating a new table uses the ``CREATE TABLE`` statement:

.. productionlist::
   create_table_statement: CREATE TABLE [ IF NOT EXISTS ] `table_name`
                         : '('
                         :     `column_definition`
                         :     ( ',' `column_definition` )*
                         :     [ ',' PRIMARY KEY '(' `primary_key` ')' ]
                         : ')' [ WITH `table_options` ]
   column_definition: `identifier` `cql_type` [ STATIC ] [ PRIMARY KEY]
   primary_key: `partition_key` [ ',' `clustering_columns` ]
   partition_key: `identifier`
                : | '(' `identifier` ( ',' `identifier` )* ')'
   clustering_columns: `identifier` ( ',' `identifier` )*
   table_options: COMPACT STORAGE [ AND `table_options` ]
                   : | CLUSTERING ORDER BY '(' `clustering_order` ')' [ AND `table_options` ]
                   : | `options`
   clustering_order: `identifier` (ASC | DESC) ( ',' `identifier` (ASC | DESC) )*

For instance::

    CREATE TABLE monkeySpecies (
        species text PRIMARY KEY,
        common_name text,
        population varint,
        average_size int
    ) WITH comment=‘Important biological records’
       AND read_repair_chance = 1.0;

    CREATE TABLE timeline (
        userid uuid,
        posted_month int,
        posted_time uuid,
        body text,
        posted_by text,
        PRIMARY KEY (userid, posted_month, posted_time)
    ) WITH compaction = { ‘class’ : ‘LeveledCompactionStrategy’ };

    CREATE TABLE loads (
        machine inet,
        cpu int,
        mtime timeuuid,
        load float,
        PRIMARY KEY ((machine, cpu), mtime)
    ) WITH CLUSTERING ORDER BY (mtime DESC);

A CQL table has a name and is composed of a set of *rows*. Creating a table amounts to defining which :ref:`columns
<column-definition>` the rows will be composed, which of those columns compose the :ref:`primary key <primary-key>`, as
well as optional :ref:`options <create-table-options>` for the table.

Attempting to create an already existing table will return an error unless the ``IF NOT EXISTS`` directive is used. If
it is used, the statement will be a no-op if the table already exists.


.. _column-definition:

Column definitions
~~~~~~~~~~~~~~~~~~

Every rows in a CQL table has a set of predefined columns defined at the time of the table creation (or added later
using an :ref:`alter statement<alter-table-statement>`).

A :token:`column_definition` is primarily comprised of the name of the column defined and it's :ref:`type <data-types>`,
which restrict which values are accepted for that column. Additionally, a column definition can have the following
modifiers:

``STATIC``
    it declares the column as being a :ref:`static column <static-columns>`.

``PRIMARY KEY``
    it declares the column as being the sole component of the :ref:`primary key <primary-key>` of the table.

.. _static-columns:

Static columns
``````````````
Some columns can be declared as ``STATIC`` in a table definition. A column that is static will be “shared” by all the
rows belonging to the same partition (having the same :ref:`partition key <partition-key>`). For instance::

    CREATE TABLE t (
        pk int,
        t int,
        v text,
        s text static,
        PRIMARY KEY (pk, t)
    );

    INSERT INTO t (pk, t, v, s) VALUES (0, 0, 'val0', 'static0');
    INSERT INTO t (pk, t, v, s) VALUES (0, 1, 'val1', 'static1');

    SELECT * FROM t;
       pk | t | v      | s
      ----+---+--------+-----------
       0  | 0 | 'val0' | 'static1'
       0  | 1 | 'val1' | 'static1'

As can be seen, the ``s`` value is the same (``static1``) for both of the row in the partition (the partition key in
that example being ``pk``, both rows are in that same partition): the 2nd insertion has overridden the value for ``s``.

The use of static columns as the following restrictions:

- tables with the ``COMPACT STORAGE`` option (see below) cannot use them.
- a table without clustering columns cannot have static columns (in a table without clustering columns, every partition
  has only one row, and so every column is inherently static).
- only non ``PRIMARY KEY`` columns can be static.

.. _primary-key:

The Primary key
~~~~~~~~~~~~~~~

Within a table, a row is uniquely identified by its ``PRIMARY KEY``, and hence all table **must** define a PRIMARY KEY
(and only one). A ``PRIMARY KEY`` definition is composed of one or more of the columns defined in the table.
Syntactically, the primary key is defined the keywords ``PRIMARY KEY`` followed by comma-separated list of the column
names composing it within parenthesis, but if the primary key has only one column, one can alternatively follow that
column definition by the ``PRIMARY KEY`` keywords. The order of the columns in the primary key definition matter.

A CQL primary key is composed of 2 parts:

- the :ref:`partition key <partition-key>` part. It is the first component of the primary key definition. It can be a
  single column or, using additional parenthesis, can be multiple columns. A table always have at least a partition key,
  the smallest possible table definition is::

      CREATE TABLE t (k text PRIMARY KEY);

- the :ref:`clustering columns <clustering-columns>`. Those are the columns after the first component of the primary key
  definition, and the order of those columns define the *clustering order*.

Some example of primary key definition are:

- ``PRIMARY KEY (a)``: ``a`` is the partition key and there is no clustering columns.
- ``PRIMARY KEY (a, b, c)`` : ``a`` is the partition key and ``b`` and ``c`` are the clustering columns.
- ``PRIMARY KEY ((a, b), c)`` : ``a`` and ``b`` compose the partition key (this is often called a *composite* partition
  key) and ``c`` is the clustering column.


.. _partition-key:

The partition key
`````````````````

Within a table, CQL defines the notion of a *partition*. A partition is simply the set of rows that share the same value
for their partition key. Note that if the partition key is composed of multiple columns, then rows belong to the same
partition only they have the same values for all those partition key column. So for instance, given the following table
definition and content::

    CREATE TABLE t (
        a int,
        b int,
        c int,
        d int,
        PRIMARY KEY ((a, b), c, d)
    );

    SELECT * FROM t;
       a | b | c | d
      ---+---+---+---
       0 | 0 | 0 | 0    // row 1
       0 | 0 | 1 | 1    // row 2
       0 | 1 | 2 | 2    // row 3
       0 | 1 | 3 | 3    // row 4
       1 | 1 | 4 | 4    // row 5

``row 1`` and ``row 2`` are in the same partition, ``row 3`` and ``row 4`` are also in the same partition (but a
different one) and ``row 5`` is in yet another partition.

Note that a table always has a partition key, and that if the table has no :ref:`clustering columns
<clustering-columns>`, then every partition of that table is only comprised of a single row (since the primary key
uniquely identifies rows and the primary key is equal to the partition key if there is no clustering columns).

The most important property of partition is that all the rows belonging to the same partition are guarantee to be stored
on the same set of replica nodes. In other words, the partition key of a table defines which of the rows will be
localized together in the Cluster, and it is thus important to choose your partition key wisely so that rows that needs
to be fetch together are in the same partition (so that querying those rows together require contacting a minimum of
nodes).

Please note however that there is a flip-side to this guarantee: as all rows sharing a partition key are guaranteed to
be stored on the same set of replica node, a partition key that groups too much data can create a hotspot.

Another useful property of a partition is that when writing data, all the updates belonging to a single partition are
done *atomically* and in *isolation*, which is not the case across partitions.

The proper choice of the partition key and clustering columns for a table is probably one of the most important aspect
of data modeling in Cassandra, and it largely impact which queries can be performed, and how efficiently they are.


.. _clustering-columns:

The clustering columns
``````````````````````

The clustering columns of a table defines the clustering order for the partition of that table. For a given
:ref:`partition <partition-key>`, all the rows are physically ordered inside Cassandra by that clustering order. For
instance, given::

    CREATE TABLE t (
        a int,
        b int,
        c int,
        PRIMARY KEY (a, c, d)
    );

    SELECT * FROM t;
       a | b | c
      ---+---+---
       0 | 0 | 4     // row 1
       0 | 1 | 9     // row 2
       0 | 2 | 2     // row 3
       0 | 3 | 3     // row 4

then the rows (which all belong to the same partition) are all stored internally in the order of the values of their
``b`` column (the order they are displayed above). So where the partition key of the table allows to group rows on the
same replica set, the clustering columns controls how those rows are stored on the replica. That sorting allows the
retrieval of a range of rows within a partition (for instance, in the example above, ``SELECT * FROM t WHERE a = 0 AND b
> 1 and b <= 3``) very efficient.


.. _create-table-options

Table options
~~~~~~~~~~~~~

A CQL table has a number of options that can be set at creation (and, for most of them, :ref:`altered
<alter-table-statement>` later). These options are specified after the ``WITH`` keyword.

Amongst those options, two important ones cannot be changed after creation and influence which queries can be done
against the table: the ``COMPACT STORAGE`` option and the ``CLUSTERING ORDER`` option. Those, as well as the other
options of a table are described in the following sections.

.. _compact-storage:

Compact tables
``````````````

.. warning:: Since Cassandra 3.0, compact tables have the exact same layout internally than non compact ones (for the
   same schema obviously), and declaring a table compact **only** creates artificial limitations on the table definition
   and usage that are necessary to ensure backward compatibility with the deprecated Thrift API. And as ``COMPACT
   STORAGE`` cannot, as of Cassandra |3.8|, be removed, it is strongly discouraged to create new table with the
   ``COMPACT STORAGE`` option.

A *compact* table is one defined with the ``COMPACT STORAGE`` option. This option is mainly targeted towards backward
compatibility for definitions created before CQL version 3 (see `www.datastax.com/dev/blog/thrift-to-cql3
<http://www.datastax.com/dev/blog/thrift-to-cql3>`__ for more details) and shouldn't be used for new tables. Declaring a
table with this option creates limitations for the table which are largely arbitrary but necessary for backward
compatibility with the (deprecated) Thrift API. Amongst those limitation:

- a compact table cannot use collections nor static columns.
- if a compact table has at least one clustering column, then it must have *exactly* one column outside of the primary
  key ones. This imply you cannot add or remove columns after creation in particular.
- a compact table is limited in the indexes it can create, and no materialized view can be created on it.

.. _clustering-order:

Reversing the clustering order
``````````````````````````````

The clustering order of a table is defined by the :ref:`clustering columns <clustering-columns>` of that table. By
default, that ordering is based on natural order of those clustering order, but the ``CLUSTERING ORDER`` allows to
change that clustering order to use the *reverse* natural order for some (potentially all) of the columns.

The ``CLUSTERING ORDER`` option takes the comma-separated list of the clustering column, each with a ``ASC`` (for
*ascendant*, e.g. the natural order) or ``DESC`` (for *descendant*, e.g. the reverse natural order). Note in particular
that the default (if the ``CLUSTERING ORDER`` option is not used) is strictly equivalent to using the option with all
clustering columns using the ``ASC`` modifier.

Note that this option is basically a hint for the storage engine to change the order in which it stores the row but it
has 3 visible consequences:

# it limits which ``ORDER BY`` clause are allowed for :ref:`selects <select-statement>` on that table. You can only
  order results by the clustering order or the reverse clustering order. Meaning that if a table has 2 clustering column
  ``a`` and ``b`` and you defined ``WITH CLUSTERING ORDER (a DESC, b ASC)``, then in queries you will be allowed to use
  ``ORDER BY (a DESC, b ASC)`` and (reverse clustering order) ``ORDER BY (a ASC, b DESC)`` but **not** ``ORDER BY (a
  ASC, b ASC)`` (nor ``ORDER BY (a DESC, b DESC)``).
# it also change the default order of results when queried (if no ``ORDER BY`` is provided). Results are always returned
  in clustering order (within a partition).
# it has a small performance impact on some queries as queries in reverse clustering order are slower than the one in
  forward clustering order. In practice, this means that if you plan on querying mostly in the reverse natural order of
  your columns (which is common with time series for instance where you often want data from the newest to the oldest),
  it is an optimization to declare a descending clustering order.

.. _create-table-general-options:

Other table options
```````````````````

.. todo:: review (misses cdc if nothing else) and link to proper categories when appropriate (compaction for instance)

A table supports the following options:

+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                           | kind       | default       | description                                                                                                                                                                                                                     |
+==================================+============+===============+=================================================================================================================================================================================================================================+
| ``comment``                      | *simple*   | none          | A free-form, human-readable comment.                                                                                                                                                                                            |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``read_repair_chance``           | *simple*   | 0.1           | The probability with which to query extra nodes (e.g. more nodes than required by the consistency level) for the purpose of read repairs.                                                                                       |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``dclocal_read_repair_chance``   | *simple*   | 0             | The probability with which to query extra nodes (e.g. more nodes than required by the consistency level) belonging to the same data center than the read coordinator for the purpose of read repairs.                           |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``gc_grace_seconds``             | *simple*   | 864000        | Time to wait before garbage collecting tombstones (deletion markers).                                                                                                                                                           |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bloom_filter_fp_chance``       | *simple*   | 0.00075       | The target probability of false positive of the sstable bloom filters. Said bloom filters will be sized to provide the provided probability (thus lowering this value impact the size of bloom filters in-memory and on-disk)   |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``default_time_to_live``         | *simple*   | 0             | The default expiration time (“TTL”) in seconds for a table.                                                                                                                                                                     |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compaction``                   | *map*      | *see below*   | Compaction options, see “below”:#compactionOptions.                                                                                                                                                                             |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compression``                  | *map*      | *see below*   | Compression options, see “below”:#compressionOptions.                                                                                                                                                                           |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``caching``                      | *map*      | *see below*   | Caching options, see “below”:#cachingOptions.                                                                                                                                                                                   |
+----------------------------------+------------+---------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Compaction options
##################

The ``compaction`` property must at least define the ``'class'``
sub-option, that defines the compaction strategy class to use. The
default supported class are ``'SizeTieredCompactionStrategy'``,
``'LeveledCompactionStrategy'``, ``'DateTieredCompactionStrategy'`` and
``'TimeWindowCompactionStrategy'``. Custom strategy can be provided by
specifying the full class name as a `string constant <#constants>`__.
The rest of the sub-options depends on the chosen class. The sub-options
supported by the default classes are:

+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                               | supported compaction strategy   | default        | description                                                                                                                                                                                                                                                                                                                            |
+======================================+=================================+================+========================================================================================================================================================================================================================================================================================================================================+
| ``enabled``                          | *all*                           | true           | A boolean denoting whether compaction should be enabled or not.                                                                                                                                                                                                                                                                        |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tombstone_threshold``              | *all*                           | 0.2            | A ratio such that if a sstable has more than this ratio of gcable tombstones over all contained columns, the sstable will be compacted (with no other sstables) for the purpose of purging those tombstones.                                                                                                                           |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tombstone_compaction_interval``    | *all*                           | 1 day          | The minimum time to wait after an sstable creation time before considering it for “tombstone compaction”, where “tombstone compaction” is the compaction triggered if the sstable has more gcable tombstones than ``tombstone_threshold``.                                                                                             |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``unchecked_tombstone_compaction``   | *all*                           | false          | Setting this to true enables more aggressive tombstone compactions - single sstable tombstone compactions will run without checking how likely it is that they will be successful.                                                                                                                                                     |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``min_sstable_size``                 | SizeTieredCompactionStrategy    | 50MB           | The size tiered strategy groups SSTables to compact in buckets. A bucket groups SSTables that differs from less than 50% in size. However, for small sizes, this would result in a bucketing that is too fine grained. ``min_sstable_size`` defines a size threshold (in bytes) below which all SSTables belong to one unique bucket   |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``min_threshold``                    | SizeTieredCompactionStrategy    | 4              | Minimum number of SSTables needed to start a minor compaction.                                                                                                                                                                                                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``max_threshold``                    | SizeTieredCompactionStrategy    | 32             | Maximum number of SSTables processed by one minor compaction.                                                                                                                                                                                                                                                                          |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bucket_low``                       | SizeTieredCompactionStrategy    | 0.5            | Size tiered consider sstables to be within the same bucket if their size is within [average\_size \* ``bucket_low``, average\_size \* ``bucket_high`` ] (i.e the default groups sstable whose sizes diverges by at most 50%)                                                                                                           |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bucket_high``                      | SizeTieredCompactionStrategy    | 1.5            | Size tiered consider sstables to be within the same bucket if their size is within [average\_size \* ``bucket_low``, average\_size \* ``bucket_high`` ] (i.e the default groups sstable whose sizes diverges by at most 50%).                                                                                                          |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``sstable_size_in_mb``               | LeveledCompactionStrategy       | 5MB            | The target size (in MB) for sstables in the leveled strategy. Note that while sstable sizes should stay less or equal to ``sstable_size_in_mb``, it is possible to exceptionally have a larger sstable as during compaction, data for a given partition key are never split into 2 sstables                                            |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timestamp_resolution``             | DateTieredCompactionStrategy    | MICROSECONDS   | The timestamp resolution used when inserting data, could be MILLISECONDS, MICROSECONDS etc (should be understandable by Java TimeUnit) - don’t change this unless you do mutations with USING TIMESTAMP (or equivalent directly in the client)                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``base_time_seconds``                | DateTieredCompactionStrategy    | 60             | The base size of the time windows.                                                                                                                                                                                                                                                                                                     |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``max_sstable_age_days``             | DateTieredCompactionStrategy    | 365            | SSTables only containing data that is older than this will never be compacted.                                                                                                                                                                                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timestamp_resolution``             | TimeWindowCompactionStrategy    | MICROSECONDS   | The timestamp resolution used when inserting data, could be MILLISECONDS, MICROSECONDS etc (should be understandable by Java TimeUnit) - don’t change this unless you do mutations with USING TIMESTAMP (or equivalent directly in the client)                                                                                         |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compaction_window_unit``           | TimeWindowCompactionStrategy    | DAYS           | The Java TimeUnit used for the window size, set in conjunction with ``compaction_window_size``. Must be one of DAYS, HOURS, MINUTES                                                                                                                                                                                                    |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``compaction_window_size``           | TimeWindowCompactionStrategy    | 1              | The number of ``compaction_window_unit`` units that make up a time window.                                                                                                                                                                                                                                                             |
+--------------------------------------+---------------------------------+----------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Compression options
###################

For the ``compression`` property, the following sub-options are
available:

+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                 | default         | description                                                                                                                                                                                                                                                                                                                                                                                                           |
+========================+=================+=======================================================================================================================================================================================================================================================================================================================================================================================================================+
| ``class``              | LZ4Compressor   | The compression algorithm to use. Default compressor are: LZ4Compressor, SnappyCompressor and DeflateCompressor. Use ``'enabled' : false`` to disable compression. Custom compressor can be provided by specifying the full class name as a “string constant”:#constants.                                                                                                                                             |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``enabled``            | true            | By default compression is enabled. To disable it, set ``enabled`` to ``false``                                                                                                                                                                                                                                                                                                                                        |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|`` chunk_length_in_kb``  | 64KB            | On disk SSTables are compressed by block (to allow random reads). This defines the size (in KB) of said block. Bigger values may improve the compression rate, but increases the minimum size of data to be read from disk for a read                                                                                                                                                                                 |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``crc_check_chance``   | 1.0             | When compression is enabled, each compressed block includes a checksum of that block for the purpose of detecting disk bitrot and avoiding the propagation of corruption to other replica. This option defines the probability with which those checksums are checked during read. By default they are always checked. Set to 0 to disable checksum checking and to 0.5 for instance to check them every other read   |
+------------------------+-----------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Caching options
###############

For the ``caching`` property, the following sub-options are available:

+--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| option                   | default   | description                                                                                                                                                                                                                                                                |
+==========================+===========+============================================================================================================================================================================================================================================================================+
| ``keys``                 | ALL       | Whether to cache keys (“key cache”) for this table. Valid values are: ``ALL`` and ``NONE``.                                                                                                                                                                                |
+--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``rows_per_partition``   | NONE      | The amount of rows to cache per partition (“row cache”). If an integer ``n`` is specified, the first ``n`` queried rows of a partition will be cached. Other possible options are ``ALL``, to cache all rows of a queried partition, or ``NONE`` to disable row caching.   |
+--------------------------+-----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Other considerations:
#####################

-  When `inserting <#insertStmt>`__ / `updating <#updateStmt>`__ a given
   row, not all columns needs to be defined (except for those part of
   the key), and missing columns occupy no space on disk. Furthermore,
   adding new columns (see \ ``ALTER TABLE``\ ) is a constant time
   operation. There is thus no need to try to anticipate future usage
   (or to cry when you haven’t) when creating a table.

ALTER TABLE
^^^^^^^^^^^

Altering an existing table uses the ``ALTER TABLE`` statement:

.. productionlist::
   alter_table_statement: ALTER TABLE `table_name` `alter_table_instruction`
   alter_table_instruction: ALTER `identifier` TYPE `cql_type`
                          : | ADD `identifier` `cql_type` ( ',' `identifier` `cql_type` )*
                          : | DROP `identifier` ( `identifier` )*
                          : | WITH `options`

For instance::

    ALTER TABLE addamsFamily ALTER lastKnownLocation TYPE uuid;

    ALTER TABLE addamsFamily ADD gravesite varchar;

    ALTER TABLE addamsFamily
           WITH comment = ‘A most excellent and useful table’
           AND read_repair_chance = 0.2;

The ``ALTER TABLE`` statement can:

- Change the type of one of the column in the table (through the ``ALTER`` instruction). Note that the type of a column
  cannot be changed arbitrarily. The change of type should be such that any value of the previous type should be a valid
  value of the new type. Further, for :ref:`clustering columns <clustering-columns>` and columns on which a secondary
  index is defined, the new type must sort values in the same way the previous type does. See the :ref:`type
  compatibility table <alter-table-type-compatibility>` below for detail on which type changes are accepted.
- Add new column(s) to the table (through the ``ADD`` instruction). Note that the primary key of a table cannot be
  changed and thus newly added column will, by extension, never be part of the primary key. Also note that :ref:`compact
  tables <compact-tables>` have restrictions regarding column addition.
- Remove column(s) from the table. This drops both the column and all its content, but note that while the column
  becomes immediately unavailable, its content is only removed lazily during compaction. Please also see the warnings
  below.
- Change some of the table options (through the ``WITH`` instruction). The :ref:`supported options
  <create-table-options>` are the same that when creating a table (outside of ``COMPACT STORAGE`` and ``CLUSTERING
  ORDER`` that cannot be changed after creation). Note that setting any ``compaction`` sub-options has the effect of
  erasing all previous ``compaction`` options, so you need to re-specify all the sub-options if you want to keep them.
  The same note applies to the set of ``compression`` sub-options.

.. warning:: Dropping a column assumes that the timestamps used for the value of this column are "real" timestamp in
   microseconds. Using "real" timestamps in microseconds is the default is and is **strongly** recommended but as
   Cassandra allows the client to provide any timestamp on any table it is theoretically possible to use another
   convention. Please be aware that if you do so, dropping a column will not work correctly.

.. warning:: Once a column is dropped, it is allowed to re-add a column with the same name than the dropped one
   **unless* the type of the dropped column was a (non-frozen) column (due to an internal technical limitation).

.. _alter-table-type-compatibility:

CQL type compatibility:
~~~~~~~~~~~~~~~~~~~~~~~

CQL data types may be converted only as the following table.

+-------------------------------------------------------+--------------------+
| Existing type                                         | Can be altered to: |
+=======================================================+====================+
| timestamp                                             | bigint             |
+-------------------------------------------------------+--------------------+
| ascii, bigint, boolean, date, decimal, double, float, | blob               |
| inet, int, smallint, text, time, timestamp, timeuuid, |                    |
| tinyint, uuid, varchar, varint                        |                    |
+-------------------------------------------------------+--------------------+
| int                                                   | date               |
+-------------------------------------------------------+--------------------+
| ascii, varchar                                        | text               |
+-------------------------------------------------------+--------------------+
| bigint                                                | time               |
+-------------------------------------------------------+--------------------+
| bigint                                                | timestamp          |
+-------------------------------------------------------+--------------------+
| timeuuid                                              | uuid               |
+-------------------------------------------------------+--------------------+
| ascii, text                                           | varchar            |
+-------------------------------------------------------+--------------------+
| bigint, int, timestamp                                | varint             |
+-------------------------------------------------------+--------------------+

Clustering columns have stricter requirements, only the following conversions are allowed:

+------------------------+-------------------+
| Existing type          | Can be altered to |
+========================+===================+
| ascii, text, varchar   | blob              |
+------------------------+-------------------+
| ascii, varchar         | text              |
+------------------------+-------------------+
| ascii, text            | varchar           |
+------------------------+-------------------+

DROP TABLE
^^^^^^^^^^

*Syntax:*

bc(syntax). ::= DROP TABLE ( IF EXISTS )?

*Sample:*

bc(sample). DROP TABLE worldSeriesAttendees;

The ``DROP TABLE`` statement results in the immediate, irreversible
removal of a table, including all data contained in it. As for table
creation, ``DROP COLUMNFAMILY`` is allowed as an alias for
``DROP TABLE``.

If the table does not exist, the statement will return an error, unless
``IF EXISTS`` is used in which case the operation is a no-op.

TRUNCATE
^^^^^^^^

*Syntax:*

bc(syntax). ::= TRUNCATE ( TABLE \| COLUMNFAMILY )?

*Sample:*

bc(sample). TRUNCATE superImportantData;

The ``TRUNCATE`` statement permanently removes all data from a table.


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

Indexes
-------

CREATE INDEX
^^^^^^^^^^^^

*Syntax:*

| bc(syntax)..
|  ::= CREATE ( CUSTOM )? INDEX ( IF NOT EXISTS )? ( )?
|  ON ‘(’ ‘)’
|  ( USING ( WITH OPTIONS = )? )?

|  ::= 
|  \| keys( )
| p.
| *Sample:*

| bc(sample).
| CREATE INDEX userIndex ON NerdMovies (user);
| CREATE INDEX ON Mutants (abilityId);
| CREATE INDEX ON users (keys(favs));
| CREATE CUSTOM INDEX ON users (email) USING ‘path.to.the.IndexClass’;
| CREATE CUSTOM INDEX ON users (email) USING ‘path.to.the.IndexClass’
  WITH OPTIONS = {’storage’: ‘/mnt/ssd/indexes/’};

The ``CREATE INDEX`` statement is used to create a new (automatic)
secondary index for a given (existing) column in a given table. A name
for the index itself can be specified before the ``ON`` keyword, if
desired. If data already exists for the column, it will be indexed
asynchronously. After the index is created, new data for the column is
indexed automatically at insertion time.

Attempting to create an already existing index will return an error
unless the ``IF NOT EXISTS`` option is used. If it is used, the
statement will be a no-op if the index already exists.

Indexes on Map Keys
~~~~~~~~~~~~~~~~~~~

When creating an index on a `map column <#map>`__, you may index either
the keys or the values. If the column identifier is placed within the
``keys()`` function, the index will be on the map keys, allowing you to
use ``CONTAINS KEY`` in ``WHERE`` clauses. Otherwise, the index will be
on the map values.

DROP INDEX
^^^^^^^^^^

*Syntax:*

bc(syntax). ::= DROP INDEX ( IF EXISTS )? ( ‘.’ )?

*Sample:*

| bc(sample)..
| DROP INDEX userIndex;

| DROP INDEX userkeyspace.address\_index;
| p.
| The ``DROP INDEX`` statement is used to drop an existing secondary
  index. The argument of the statement is the index name, which may
  optionally specify the keyspace of the index.

If the index does not exists, the statement will return an error, unless
``IF EXISTS`` is used in which case the operation is a no-op.

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

.. _cql-security:

Security
--------

.. _roles:

Database Roles
^^^^^^^^^^^^^^

CREATE ROLE
~~~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= CREATE ROLE ( IF NOT EXISTS )? ( WITH ( AND )\* )?

|  ::= PASSWORD = 
|  \| LOGIN = 
|  \| SUPERUSER = 
|  \| OPTIONS = 
| p.

*Sample:*

| bc(sample).
| CREATE ROLE new\_role;
| CREATE ROLE alice WITH PASSWORD = ‘password\_a’ AND LOGIN = true;
| CREATE ROLE bob WITH PASSWORD = ‘password\_b’ AND LOGIN = true AND
  SUPERUSER = true;
| CREATE ROLE carlos WITH OPTIONS = { ‘custom\_option1’ :
  ‘option1\_value’, ‘custom\_option2’ : 99 };

By default roles do not possess ``LOGIN`` privileges or ``SUPERUSER``
status.

`Permissions <#permissions>`__ on database resources are granted to
roles; types of resources include keyspaces, tables, functions and roles
themselves. Roles may be granted to other roles to create hierarchical
permissions structures; in these hierarchies, permissions and
``SUPERUSER`` status are inherited, but the ``LOGIN`` privilege is not.

If a role has the ``LOGIN`` privilege, clients may identify as that role
when connecting. For the duration of that connection, the client will
acquire any roles and privileges granted to that role.

Only a client with with the ``CREATE`` permission on the database roles
resource may issue ``CREATE ROLE`` requests (see the `relevant
section <#permissions>`__ below), unless the client is a ``SUPERUSER``.
Role management in Cassandra is pluggable and custom implementations may
support only a subset of the listed options.

Role names should be quoted if they contain non-alphanumeric characters.

.. _setting-credentials-for-internal-authentication:

Setting credentials for internal authentication
```````````````````````````````````````````````

| Use the ``WITH PASSWORD`` clause to set a password for internal
  authentication, enclosing the password in single quotation marks.
| If internal authentication has not been set up or the role does not
  have ``LOGIN`` privileges, the ``WITH PASSWORD`` clause is not
  necessary.

Creating a role conditionally
`````````````````````````````

Attempting to create an existing role results in an invalid query
condition unless the ``IF NOT EXISTS`` option is used. If the option is
used and the role exists, the statement is a no-op.

| bc(sample).
| CREATE ROLE other\_role;
| CREATE ROLE IF NOT EXISTS other\_role;

ALTER ROLE
~~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= ALTER ROLE ( WITH ( AND )\* )?

|  ::= PASSWORD = 
|  \| LOGIN = 
|  \| SUPERUSER = 
|  \| OPTIONS = 
| p.

*Sample:*

| bc(sample).
| ALTER ROLE bob WITH PASSWORD = ‘PASSWORD\_B’ AND SUPERUSER = false;

Conditions on executing ``ALTER ROLE`` statements:

-  A client must have ``SUPERUSER`` status to alter the ``SUPERUSER``
   status of another role
-  A client cannot alter the ``SUPERUSER`` status of any role it
   currently holds
-  A client can only modify certain properties of the role with which it
   identified at login (e.g. ``PASSWORD``)
-  To modify properties of a role, the client must be granted ``ALTER``
   `permission <#permissions>`__ on that role

DROP ROLE
~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= DROP ROLE ( IF EXISTS )? 
| p.

*Sample:*

| bc(sample).
| DROP ROLE alice;
| DROP ROLE IF EXISTS bob;

| ``DROP ROLE`` requires the client to have ``DROP``
  `permission <#permissions>`__ on the role in question. In addition,
  client may not ``DROP`` the role with which it identified at login.
  Finaly, only a client with ``SUPERUSER`` status may ``DROP`` another
  ``SUPERUSER`` role.
| Attempting to drop a role which does not exist results in an invalid
  query condition unless the ``IF EXISTS`` option is used. If the option
  is used and the role does not exist the statement is a no-op.

GRANT ROLE
~~~~~~~~~~

*Syntax:*

| bc(syntax).
|  ::= GRANT TO 

*Sample:*

| bc(sample).
| GRANT report\_writer TO alice;

| This statement grants the ``report_writer`` role to ``alice``. Any
  permissions granted to ``report_writer`` are also acquired by
  ``alice``.
| Roles are modelled as a directed acyclic graph, so circular grants are
  not permitted. The following examples result in error conditions:

| bc(sample).
| GRANT role\_a TO role\_b;
| GRANT role\_b TO role\_a;

| bc(sample).
| GRANT role\_a TO role\_b;
| GRANT role\_b TO role\_c;
| GRANT role\_c TO role\_a;

REVOKE ROLE
~~~~~~~~~~~

*Syntax:*

| bc(syntax).
|  ::= REVOKE FROM 

*Sample:*

| bc(sample).
| REVOKE report\_writer FROM alice;

This statement revokes the ``report_writer`` role from ``alice``. Any
permissions that ``alice`` has acquired via the ``report_writer`` role
are also revoked.

LIST ROLES
~~~~~~~~~~

*Syntax:*

| bc(syntax).
|  ::= LIST ROLES ( OF )? ( NORECURSIVE )?

*Sample:*

| bc(sample).
| LIST ROLES;

Return all known roles in the system, this requires ``DESCRIBE``
permission on the database roles resource.

| bc(sample).
| LIST ROLES OF ``alice``;

Enumerate all roles granted to ``alice``, including those transitively
aquired.

| bc(sample).
| LIST ROLES OF ``bob`` NORECURSIVE

List all roles directly granted to ``bob``.

CREATE USER
~~~~~~~~~~~

Prior to the introduction of roles in Cassandra 2.2, authentication and
authorization were based around the concept of a ``USER``. For backward
compatibility, the legacy syntax has been preserved with ``USER``
centric statments becoming synonyms for the ``ROLE`` based equivalents.

*Syntax:*

| bc(syntax)..
|  ::= CREATE USER ( IF NOT EXISTS )? ( WITH PASSWORD )? ()?

|  ::= SUPERUSER
|  \| NOSUPERUSER
| p.

*Sample:*

| bc(sample).
| CREATE USER alice WITH PASSWORD ‘password\_a’ SUPERUSER;
| CREATE USER bob WITH PASSWORD ‘password\_b’ NOSUPERUSER;

``CREATE USER`` is equivalent to ``CREATE ROLE`` where the ``LOGIN``
option is ``true``. So, the following pairs of statements are
equivalent:

| bc(sample)..
| CREATE USER alice WITH PASSWORD ‘password\_a’ SUPERUSER;
| CREATE ROLE alice WITH PASSWORD = ‘password\_a’ AND LOGIN = true AND
  SUPERUSER = true;

| CREATE USER IF EXISTS alice WITH PASSWORD ‘password\_a’ SUPERUSER;
| CREATE ROLE IF EXISTS alice WITH PASSWORD = ‘password\_a’ AND LOGIN =
  true AND SUPERUSER = true;

| CREATE USER alice WITH PASSWORD ‘password\_a’ NOSUPERUSER;
| CREATE ROLE alice WITH PASSWORD = ‘password\_a’ AND LOGIN = true AND
  SUPERUSER = false;

| CREATE USER alice WITH PASSWORD ‘password\_a’ NOSUPERUSER;
| CREATE ROLE alice WITH PASSWORD = ‘password\_a’ WITH LOGIN = true;

| CREATE USER alice WITH PASSWORD ‘password\_a’;
| CREATE ROLE alice WITH PASSWORD = ‘password\_a’ WITH LOGIN = true;
| p.

ALTER USER
~~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= ALTER USER ( WITH PASSWORD )? ( )?

|  ::= SUPERUSER
|  \| NOSUPERUSER
| p.

| bc(sample).
| ALTER USER alice WITH PASSWORD ‘PASSWORD\_A’;
| ALTER USER bob SUPERUSER;

DROP USER
~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= DROP USER ( IF EXISTS )? 
| p.

*Sample:*

| bc(sample).
| DROP USER alice;
| DROP USER IF EXISTS bob;

LIST USERS
~~~~~~~~~~

*Syntax:*

| bc(syntax).
|  ::= LIST USERS;

*Sample:*

| bc(sample).
| LIST USERS;

This statement is equivalent to

| bc(sample).
| LIST ROLES;

but only roles with the ``LOGIN`` privilege are included in the output.

Data Control
^^^^^^^^^^^^

.. _permissions:

Permissions
~~~~~~~~~~~

Permissions on resources are granted to roles; there are several
different types of resources in Cassandra and each type is modelled
hierarchically:

-  The hierarchy of Data resources, Keyspaces and Tables has the
   structure ``ALL KEYSPACES`` [STRIKEOUT:> ``KEYSPACE``]> ``TABLE``
-  Function resources have the structure ``ALL FUNCTIONS`` [STRIKEOUT:>
   ``KEYSPACE``]> ``FUNCTION``
-  Resources representing roles have the structure ``ALL ROLES`` ->
   ``ROLE``
-  Resources representing JMX ObjectNames, which map to sets of
   MBeans/MXBeans, have the structure ``ALL MBEANS`` -> ``MBEAN``

Permissions can be granted at any level of these hierarchies and they
flow downwards. So granting a permission on a resource higher up the
chain automatically grants that same permission on all resources lower
down. For example, granting ``SELECT`` on a ``KEYSPACE`` automatically
grants it on all ``TABLES`` in that ``KEYSPACE``. Likewise, granting a
permission on ``ALL FUNCTIONS`` grants it on every defined function,
regardless of which keyspace it is scoped in. It is also possible to
grant permissions on all functions scoped to a particular keyspace.

Modifications to permissions are visible to existing client sessions;
that is, connections need not be re-established following permissions
changes.

The full set of available permissions is:

-  ``CREATE``
-  ``ALTER``
-  ``DROP``
-  ``SELECT``
-  ``MODIFY``
-  ``AUTHORIZE``
-  ``DESCRIBE``
-  ``EXECUTE``

Not all permissions are applicable to every type of resource. For
instance, ``EXECUTE`` is only relevant in the context of functions or
mbeans; granting ``EXECUTE`` on a resource representing a table is
nonsensical. Attempting to ``GRANT`` a permission on resource to which
it cannot be applied results in an error response. The following
illustrates which permissions can be granted on which types of resource,
and which statements are enabled by that permission.

+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| permission      | resource                        | operations                                                                                                                                                           |
+=================+=================================+======================================================================================================================================================================+
| ``CREATE``      | ``ALL KEYSPACES``               | ``CREATE KEYSPACE`` <br> ``CREATE TABLE`` in any keyspace                                                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``CREATE``      | ``KEYSPACE``                    | ``CREATE TABLE`` in specified keyspace                                                                                                                               |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``CREATE``      | ``ALL FUNCTIONS``               | ``CREATE FUNCTION`` in any keyspace <br> ``CREATE AGGREGATE`` in any keyspace                                                                                        |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``CREATE``      | ``ALL FUNCTIONS IN KEYSPACE``   | ``CREATE FUNCTION`` in keyspace <br> ``CREATE AGGREGATE`` in keyspace                                                                                                |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``CREATE``      | ``ALL ROLES``                   | ``CREATE ROLE``                                                                                                                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``ALL KEYSPACES``               | ``ALTER KEYSPACE`` <br> ``ALTER TABLE`` in any keyspace                                                                                                              |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``KEYSPACE``                    | ``ALTER KEYSPACE`` <br> ``ALTER TABLE`` in keyspace                                                                                                                  |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``TABLE``                       | ``ALTER TABLE``                                                                                                                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``ALL FUNCTIONS``               | ``CREATE FUNCTION`` replacing any existing <br> ``CREATE AGGREGATE`` replacing any existing                                                                          |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``ALL FUNCTIONS IN KEYSPACE``   | ``CREATE FUNCTION`` replacing existing in keyspace <br> ``CREATE AGGREGATE`` replacing any existing in keyspace                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``FUNCTION``                    | ``CREATE FUNCTION`` replacing existing <br> ``CREATE AGGREGATE`` replacing existing                                                                                  |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``ALL ROLES``                   | ``ALTER ROLE`` on any role                                                                                                                                           |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``ALTER``       | ``ROLE``                        | ``ALTER ROLE``                                                                                                                                                       |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``ALL KEYSPACES``               | ``DROP KEYSPACE`` <br> ``DROP TABLE`` in any keyspace                                                                                                                |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``KEYSPACE``                    | ``DROP TABLE`` in specified keyspace                                                                                                                                 |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``TABLE``                       | ``DROP TABLE``                                                                                                                                                       |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``ALL FUNCTIONS``               | ``DROP FUNCTION`` in any keyspace <br> ``DROP AGGREGATE`` in any existing                                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``ALL FUNCTIONS IN KEYSPACE``   | ``DROP FUNCTION`` in keyspace <br> ``DROP AGGREGATE`` in existing                                                                                                    |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``FUNCTION``                    | ``DROP FUNCTION``                                                                                                                                                    |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``ALL ROLES``                   | ``DROP ROLE`` on any role                                                                                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DROP``        | ``ROLE``                        | ``DROP ROLE``                                                                                                                                                        |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``SELECT``      | ``ALL KEYSPACES``               | ``SELECT`` on any table                                                                                                                                              |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``SELECT``      | ``KEYSPACE``                    | ``SELECT`` on any table in keyspace                                                                                                                                  |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``SELECT``      | ``TABLE``                       | ``SELECT`` on specified table                                                                                                                                        |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``SELECT``      | ``ALL MBEANS``                  | Call getter methods on any mbean                                                                                                                                     |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``SELECT``      | ``MBEANS``                      | Call getter methods on any mbean matching a wildcard pattern                                                                                                         |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``SELECT``      | ``MBEAN``                       | Call getter methods on named mbean                                                                                                                                   |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``MODIFY``      | ``ALL KEYSPACES``               | ``INSERT`` on any table <br> ``UPDATE`` on any table <br> ``DELETE`` on any table <br> ``TRUNCATE`` on any table                                                     |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``MODIFY``      | ``KEYSPACE``                    | ``INSERT`` on any table in keyspace <br> ``UPDATE`` on any table in keyspace <br>   ``DELETE`` on any table in keyspace <br> ``TRUNCATE`` on any table in keyspace   |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``MODIFY``      | ``TABLE``                       | ``INSERT`` <br> ``UPDATE`` <br> ``DELETE`` <br> ``TRUNCATE``                                                                                                         |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``MODIFY``      | ``ALL MBEANS``                  | Call setter methods on any mbean                                                                                                                                     |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``MODIFY``      | ``MBEANS``                      | Call setter methods on any mbean matching a wildcard pattern                                                                                                         |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``MODIFY``      | ``MBEAN``                       | Call setter methods on named mbean                                                                                                                                   |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ALL KEYSPACES``               | ``GRANT PERMISSION`` on any table <br> ``REVOKE PERMISSION`` on any table                                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``KEYSPACE``                    | ``GRANT PERMISSION`` on table in keyspace <br> ``REVOKE PERMISSION`` on table in keyspace                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``TABLE``                       | ``GRANT PERMISSION`` <br> ``REVOKE PERMISSION``                                                                                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ALL FUNCTIONS``               | ``GRANT PERMISSION`` on any function <br> ``REVOKE PERMISSION`` on any function                                                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ALL FUNCTIONS IN KEYSPACE``   | ``GRANT PERMISSION`` in keyspace <br> ``REVOKE PERMISSION`` in keyspace                                                                                              |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ALL FUNCTIONS IN KEYSPACE``   | ``GRANT PERMISSION`` in keyspace <br> ``REVOKE PERMISSION`` in keyspace                                                                                              |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``FUNCTION``                    | ``GRANT PERMISSION`` <br> ``REVOKE PERMISSION``                                                                                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ALL MBEANS``                  | ``GRANT PERMISSION`` on any mbean <br> ``REVOKE PERMISSION`` on any mbean                                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``MBEANS``                      | ``GRANT PERMISSION`` on any mbean matching a wildcard pattern <br> ``REVOKE PERMISSION`` on any mbean matching a wildcard pattern                                    |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``MBEAN``                       | ``GRANT PERMISSION`` on named mbean <br> ``REVOKE PERMISSION`` on named mbean                                                                                        |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ALL ROLES``                   | ``GRANT ROLE`` grant any role <br> ``REVOKE ROLE`` revoke any role                                                                                                   |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``AUTHORIZE``   | ``ROLES``                       | ``GRANT ROLE`` grant role <br> ``REVOKE ROLE`` revoke role                                                                                                           |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DESCRIBE``    | ``ALL ROLES``                   | ``LIST ROLES`` all roles or only roles granted to another, specified role                                                                                            |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DESCRIBE``    | @ALL MBEANS                     | Retrieve metadata about any mbean from the platform’s MBeanServer                                                                                                    |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DESCRIBE``    | @MBEANS                         | Retrieve metadata about any mbean matching a wildcard patter from the platform’s MBeanServer                                                                         |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``DESCRIBE``    | @MBEAN                          | Retrieve metadata about a named mbean from the platform’s MBeanServer                                                                                                |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``EXECUTE``     | ``ALL FUNCTIONS``               | ``SELECT``, ``INSERT``, ``UPDATE`` using any function <br> use of any function in ``CREATE AGGREGATE``                                                               |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``EXECUTE``     | ``ALL FUNCTIONS IN KEYSPACE``   | ``SELECT``, ``INSERT``, ``UPDATE`` using any function in keyspace <br> use of any function in keyspace in ``CREATE AGGREGATE``                                       |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``EXECUTE``     | ``FUNCTION``                    | ``SELECT``, ``INSERT``, ``UPDATE`` using function <br> use of function in ``CREATE AGGREGATE``                                                                       |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``EXECUTE``     | ``ALL MBEANS``                  | Execute operations on any mbean                                                                                                                                      |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``EXECUTE``     | ``MBEANS``                      | Execute operations on any mbean matching a wildcard pattern                                                                                                          |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``EXECUTE``     | ``MBEAN``                       | Execute operations on named mbean                                                                                                                                    |
+-----------------+---------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------+

GRANT PERMISSION
~~~~~~~~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= GRANT ( ALL ( PERMISSIONS )? \| ( PERMISSION )? ) ON TO 

 ::= CREATE \| ALTER \| DROP \| SELECT \| MODIFY \| AUTHORIZE \| DESCRIBE \| EXECUTE

|  ::= ALL KEYSPACES
|  \| KEYSPACE 
|  \| ( TABLE )? 
|  \| ALL ROLES
|  \| ROLE 
|  \| ALL FUNCTIONS ( IN KEYSPACE )?
|  \| FUNCTION 
|  \| ALL MBEANS
|  \| ( MBEAN \| MBEANS ) 
| p.

*Sample:*

| bc(sample).
| GRANT SELECT ON ALL KEYSPACES TO data\_reader;

This gives any user with the role ``data_reader`` permission to execute
``SELECT`` statements on any table across all keyspaces

| bc(sample).
| GRANT MODIFY ON KEYSPACE keyspace1 TO data\_writer;

This give any user with the role ``data_writer`` permission to perform
``UPDATE``, ``INSERT``, ``UPDATE``, ``DELETE`` and ``TRUNCATE`` queries
on all tables in the ``keyspace1`` keyspace

| bc(sample).
| GRANT DROP ON keyspace1.table1 TO schema\_owner;

This gives any user with the ``schema_owner`` role permissions to
``DROP`` ``keyspace1.table1``.

| bc(sample).
| GRANT EXECUTE ON FUNCTION keyspace1.user\_function( int ) TO
  report\_writer;

This grants any user with the ``report_writer`` role permission to
execute ``SELECT``, ``INSERT`` and ``UPDATE`` queries which use the
function ``keyspace1.user_function( int )``

| bc(sample).
| GRANT DESCRIBE ON ALL ROLES TO role\_admin;

This grants any user with the ``role_admin`` role permission to view any
and all roles in the system with a ``LIST ROLES`` statement

.. _grant-all:

GRANT ALL
`````````

When the ``GRANT ALL`` form is used, the appropriate set of permissions
is determined automatically based on the target resource.

Automatic Granting
``````````````````

When a resource is created, via a ``CREATE KEYSPACE``, ``CREATE TABLE``,
``CREATE FUNCTION``, ``CREATE AGGREGATE`` or ``CREATE ROLE`` statement,
the creator (the role the database user who issues the statement is
identified as), is automatically granted all applicable permissions on
the new resource.

REVOKE PERMISSION
~~~~~~~~~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= REVOKE ( ALL ( PERMISSIONS )? \| ( PERMISSION )? ) ON FROM 

 ::= CREATE \| ALTER \| DROP \| SELECT \| MODIFY \| AUTHORIZE \| DESCRIBE \| EXECUTE

|  ::= ALL KEYSPACES
|  \| KEYSPACE 
|  \| ( TABLE )? 
|  \| ALL ROLES
|  \| ROLE 
|  \| ALL FUNCTIONS ( IN KEYSPACE )?
|  \| FUNCTION 
|  \| ALL MBEANS
|  \| ( MBEAN \| MBEANS ) 
| p.

*Sample:*

| bc(sample)..
| REVOKE SELECT ON ALL KEYSPACES FROM data\_reader;
| REVOKE MODIFY ON KEYSPACE keyspace1 FROM data\_writer;
| REVOKE DROP ON keyspace1.table1 FROM schema\_owner;
| REVOKE EXECUTE ON FUNCTION keyspace1.user\_function( int ) FROM
  report\_writer;
| REVOKE DESCRIBE ON ALL ROLES FROM role\_admin;
| p.

LIST PERMISSIONS
~~~~~~~~~~~~~~~~

*Syntax:*

| bc(syntax)..
|  ::= LIST ( ALL ( PERMISSIONS )? \| )
|  ( ON )?
|  ( OF ( NORECURSIVE )? )?

|  ::= ALL KEYSPACES
|  \| KEYSPACE 
|  \| ( TABLE )? 
|  \| ALL ROLES
|  \| ROLE 
|  \| ALL FUNCTIONS ( IN KEYSPACE )?
|  \| FUNCTION 
|  \| ALL MBEANS
|  \| ( MBEAN \| MBEANS ) 
| p.

*Sample:*

| bc(sample).
| LIST ALL PERMISSIONS OF alice;

Show all permissions granted to ``alice``, including those acquired
transitively from any other roles.

| bc(sample).
| LIST ALL PERMISSIONS ON keyspace1.table1 OF bob;

Show all permissions on ``keyspace1.table1`` granted to ``bob``,
including those acquired transitively from any other roles. This also
includes any permissions higher up the resource hierarchy which can be
applied to ``keyspace1.table1``. For example, should ``bob`` have
``ALTER`` permission on ``keyspace1``, that would be included in the
results of this query. Adding the ``NORECURSIVE`` switch restricts the
results to only those permissions which were directly granted to ``bob``
or one of ``bob``\ ’s roles.

| bc(sample).
| LIST SELECT PERMISSIONS OF carlos;

Show any permissions granted to ``carlos`` or any of ``carlos``\ ’s
roles, limited to ``SELECT`` permissions on any resource.

Functions
---------

CQL3 distinguishes between built-in functions (so called ‘native
functions’) and `user-defined functions <#udfs>`__. CQL3 includes
several native functions, described below:

Scalar functions
^^^^^^^^^^^^^^^^

Native functions
~~~~~~~~~~~~~~~~

Cast
````

The ``cast`` function can be used to converts one native datatype to
another.

The following table describes the conversions supported by the ``cast``
function. Cassandra will silently ignore any cast converting a datatype
into its own datatype.

+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| from            | to                                                                                                                      |
+=================+=========================================================================================================================+
| ``ascii``       | ``text``, ``varchar``                                                                                                   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``bigint``      | ``tinyint``, ``smallint``, ``int``, ``float``, ``double``, ``decimal``, ``varint``, ``text``, ``varchar``               |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``boolean``     | ``text``, ``varchar``                                                                                                   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``counter``     | ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``, ``varchar``   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``date``        | ``timestamp``                                                                                                           |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``decimal``     | ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``varint``, ``text``, ``varchar``                |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``double``      | ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``decimal``, ``varint``, ``text``, ``varchar``               |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``float``       | ``tinyint``, ``smallint``, ``int``, ``bigint``, ``double``, ``decimal``, ``varint``, ``text``, ``varchar``              |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``inet``        | ``text``, ``varchar``                                                                                                   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``int``         | ``tinyint``, ``smallint``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``, ``varchar``            |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``smallint``    | ``tinyint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``, ``varchar``                 |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``time``        | ``text``, ``varchar``                                                                                                   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``timestamp``   | ``date``, ``text``, ``varchar``                                                                                         |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``timeuuid``    | ``timestamp``, ``date``, ``text``, ``varchar``                                                                          |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``tinyint``     | ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``varint``, ``text``, ``varchar``   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``uuid``        | ``text``, ``varchar``                                                                                                   |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+
| ``varint``      | ``tinyint``, ``smallint``, ``int``, ``bigint``, ``float``, ``double``, ``decimal``, ``text``, ``varchar``               |
+-----------------+-------------------------------------------------------------------------------------------------------------------------+

The conversions rely strictly on Java’s semantics. For example, the
double value 1 will be converted to the text value ‘1.0’.

| bc(sample).
| SELECT avg(cast(count as double)) FROM myTable

Token
`````

The ``token`` function allows to compute the token for a given partition
key. The exact signature of the token function depends on the table
concerned and of the partitioner used by the cluster.

The type of the arguments of the ``token`` depend on the type of the
partition key columns. The return type depend on the partitioner in use:

-  For Murmur3Partitioner, the return type is ``bigint``.
-  For RandomPartitioner, the return type is ``varint``.
-  For ByteOrderedPartitioner, the return type is ``blob``.

For instance, in a cluster using the default Murmur3Partitioner, if a
table is defined by

| bc(sample).
| CREATE TABLE users (
|  userid text PRIMARY KEY,
|  username text,
|  …
| )

then the ``token`` function will take a single argument of type ``text``
(in that case, the partition key is ``userid`` (there is no clustering
columns so the partition key is the same than the primary key)), and the
return type will be ``bigint``.

Uuid
````

The ``uuid`` function takes no parameters and generates a random type 4
uuid suitable for use in INSERT or SET statements.

Timeuuid functions
``````````````````

``now``
#######

The ``now`` function takes no arguments and generates, on the
coordinator node, a new unique timeuuid (at the time where the statement
using it is executed). Note that this method is useful for insertion but
is largely non-sensical in ``WHERE`` clauses. For instance, a query of
the form

| bc(sample).
| SELECT \* FROM myTable WHERE t = now()

will never return any result by design, since the value returned by
``now()`` is guaranteed to be unique.

``minTimeuuid`` and ``maxTimeuuid``
###################################

The ``minTimeuuid`` (resp. ``maxTimeuuid``) function takes a
``timestamp`` value ``t`` (which can be `either a timestamp or a date
string <#usingtimestamps>`__ ) and return a *fake* ``timeuuid``
corresponding to the *smallest* (resp. *biggest*) possible ``timeuuid``
having for timestamp ``t``. So for instance:

| bc(sample).
| SELECT \* FROM myTable WHERE t > maxTimeuuid(‘2013-01-01 00:05+0000’)
  AND t < minTimeuuid(‘2013-02-02 10:00+0000’)

will select all rows where the ``timeuuid`` column ``t`` is strictly
older than ‘2013-01-01 00:05+0000’ but strictly younger than ‘2013-02-02
10:00+0000’. Please note that
``t >= maxTimeuuid('2013-01-01 00:05+0000')`` would still *not* select a
``timeuuid`` generated exactly at ‘2013-01-01 00:05+0000’ and is
essentially equivalent to ``t > maxTimeuuid('2013-01-01 00:05+0000')``.

*Warning*: We called the values generated by ``minTimeuuid`` and
``maxTimeuuid`` *fake* UUID because they do no respect the Time-Based
UUID generation process specified by the `RFC
4122 <http://www.ietf.org/rfc/rfc4122.txt>`__. In particular, the value
returned by these 2 methods will not be unique. This means you should
only use those methods for querying (as in the example above). Inserting
the result of those methods is almost certainly *a bad idea*.

Time conversion functions
`````````````````````````

A number of functions are provided to “convert” a ``timeuuid``, a
``timestamp`` or a ``date`` into another ``native`` type.

+-----------------------+-----------------+-------------------------------------------------------------------+
| function name         | input type      | description                                                       |
+=======================+=================+===================================================================+
| ``toDate``            | ``timeuuid``    | Converts the ``timeuuid`` argument into a ``date`` type           |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``toDate``            | ``timestamp``   | Converts the ``timestamp`` argument into a ``date`` type          |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``toTimestamp``       | ``timeuuid``    | Converts the ``timeuuid`` argument into a ``timestamp`` type      |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``toTimestamp``       | ``date``        | Converts the ``date`` argument into a ``timestamp`` type          |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``toUnixTimestamp``   | ``timeuuid``    | Converts the ``timeuuid`` argument into a ``bigInt`` raw value    |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``toUnixTimestamp``   | ``timestamp``   | Converts the ``timestamp`` argument into a ``bigInt`` raw value   |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``toUnixTimestamp``   | ``date``        | Converts the ``date`` argument into a ``bigInt`` raw value        |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``dateOf``            | ``timeuuid``    | Similar to ``toTimestamp(timeuuid)`` (DEPRECATED)                 |
+-----------------------+-----------------+-------------------------------------------------------------------+
| ``unixTimestampOf``   | ``timeuuid``    | Similar to ``toUnixTimestamp(timeuuid)`` (DEPRECATED)             |
+-----------------------+-----------------+-------------------------------------------------------------------+

Blob conversion functions
`````````````````````````

A number of functions are provided to “convert” the native types into
binary data (``blob``). For every ``<native-type>`` ``type`` supported
by CQL3 (a notable exceptions is ``blob``, for obvious reasons), the
function ``typeAsBlob`` takes a argument of type ``type`` and return it
as a ``blob``. Conversely, the function ``blobAsType`` takes a 64-bit
``blob`` argument and convert it to a ``bigint`` value. And so for
instance, ``bigintAsBlob(3)`` is ``0x0000000000000003`` and
``blobAsBigint(0x0000000000000003)`` is ``3``.

User-defined functions
~~~~~~~~~~~~~~~~~~~~~~

User-defined functions allow execution of user-provided code in
Cassandra. By default, Cassandra supports defining functions in *Java*
and *JavaScript*. Support for other JSR 223 compliant scripting
languages (such as Python, Ruby, and Scala) can be added by adding a JAR
to the classpath.

UDFs are part of the Cassandra schema. As such, they are automatically
propagated to all nodes in the cluster.

UDFs can be *overloaded* - i.e. multiple UDFs with different argument
types but the same function name. Example:

| bc(sample).
| CREATE FUNCTION sample ( arg int ) …;
| CREATE FUNCTION sample ( arg text ) …;

User-defined functions are susceptible to all of the normal problems
with the chosen programming language. Accordingly, implementations
should be safe against null pointer exceptions, illegal arguments, or
any other potential source of exceptions. An exception during function
execution will result in the entire statement failing.

It is valid to use *complex* types like collections, tuple types and
user-defined types as argument and return types. Tuple types and
user-defined types are handled by the conversion functions of the
DataStax Java Driver. Please see the documentation of the Java Driver
for details on handling tuple types and user-defined types.

Arguments for functions can be literals or terms. Prepared statement
placeholders can be used, too.

Note that you can use the double-quoted string syntax to enclose the UDF
source code. For example:

| bc(sample)..
| CREATE FUNCTION some\_function ( arg int )
|  RETURNS NULL ON NULL INPUT
|  RETURNS int
|  LANGUAGE java
|  AS $$ return arg; $$;

| SELECT some\_function(column) FROM atable …;
| UPDATE atable SET col = some\_function(?) …;
| p.

| bc(sample).
| CREATE TYPE custom\_type (txt text, i int);
| CREATE FUNCTION fct\_using\_udt ( udtarg frozen )
|  RETURNS NULL ON NULL INPUT
|  RETURNS text
|  LANGUAGE java
|  AS $$ return udtarg.getString(“txt”); $$;

User-defined functions can be used in ```SELECT`` <#selectStmt>`__,
```INSERT`` <#insertStmt>`__ and ```UPDATE`` <#updateStmt>`__
statements.

The implicitly available ``udfContext`` field (or binding for script
UDFs) provides the neccessary functionality to create new UDT and tuple
values.

| bc(sample).
| CREATE TYPE custom\_type (txt text, i int);
| CREATE FUNCTION fct\_using\_udt ( somearg int )
|  RETURNS NULL ON NULL INPUT
|  RETURNS custom\_type
|  LANGUAGE java
|  AS $$
|  UDTValue udt = udfContext.newReturnUDTValue();
|  udt.setString(“txt”, “some string”);
|  udt.setInt(“i”, 42);
|  return udt;
|  $$;

The definition of the ``UDFContext`` interface can be found in the
Apache Cassandra source code for
``org.apache.cassandra.cql3.functions.UDFContext``.

| bc(sample).
| public interface UDFContext
| {
|  UDTValue newArgUDTValue(String argName);
|  UDTValue newArgUDTValue(int argNum);
|  UDTValue newReturnUDTValue();
|  UDTValue newUDTValue(String udtName);
|  TupleValue newArgTupleValue(String argName);
|  TupleValue newArgTupleValue(int argNum);
|  TupleValue newReturnTupleValue();
|  TupleValue newTupleValue(String cqlDefinition);
| }

| Java UDFs already have some imports for common interfaces and classes
  defined. These imports are:
| Please note, that these convenience imports are not available for
  script UDFs.

| bc(sample).
| import java.nio.ByteBuffer;
| import java.util.List;
| import java.util.Map;
| import java.util.Set;
| import org.apache.cassandra.cql3.functions.UDFContext;
| import com.datastax.driver.core.TypeCodec;
| import com.datastax.driver.core.TupleValue;
| import com.datastax.driver.core.UDTValue;

See ```CREATE FUNCTION`` <#createFunctionStmt>`__ and
```DROP FUNCTION`` <#dropFunctionStmt>`__.

CREATE FUNCTION
```````````````

*Syntax:*

| bc(syntax)..
|  ::= CREATE ( OR REPLACE )?
|  FUNCTION ( IF NOT EXISTS )?
|  ( ‘.’ )? 
|  ‘(’ ( ‘,’ )\* ‘)’
|  ( CALLED \| RETURNS NULL ) ON NULL INPUT
|  RETURNS 
|  LANGUAGE 
|  AS 
| p.
| *Sample:*

| bc(sample).
| CREATE OR REPLACE FUNCTION somefunction
|  ( somearg int, anotherarg text, complexarg frozen, listarg list )
|  RETURNS NULL ON NULL INPUT
|  RETURNS text
|  LANGUAGE java
|  AS $$
|  // some Java code
|  $$;
| CREATE FUNCTION akeyspace.fname IF NOT EXISTS
|  ( someArg int )
|  CALLED ON NULL INPUT
|  RETURNS text
|  LANGUAGE java
|  AS $$
|  // some Java code
|  $$;

``CREATE FUNCTION`` creates or replaces a user-defined function.

Function Signature
##################

Signatures are used to distinguish individual functions. The signature
consists of:

#. The fully qualified function name - i.e *keyspace* plus
   *function-name*
#. The concatenated list of all argument types

Note that keyspace names, function names and argument types are subject
to the default naming conventions and case-sensitivity rules.

``CREATE FUNCTION`` with the optional ``OR REPLACE`` keywords either
creates a function or replaces an existing one with the same signature.
A ``CREATE FUNCTION`` without ``OR REPLACE`` fails if a function with
the same signature already exists.

Behavior on invocation with ``null`` values must be defined for each
function. There are two options:

#. ``RETURNS NULL ON NULL INPUT`` declares that the function will always
   return ``null`` if any of the input arguments is ``null``.
#. ``CALLED ON NULL INPUT`` declares that the function will always be
   executed.

If the optional ``IF NOT EXISTS`` keywords are used, the function will
only be created if another function with the same signature does not
exist.

``OR REPLACE`` and ``IF NOT EXISTS`` cannot be used together.

Functions belong to a keyspace. If no keyspace is specified in
``<function-name>``, the current keyspace is used (i.e. the keyspace
specified using the ```USE`` <#useStmt>`__ statement). It is not
possible to create a user-defined function in one of the system
keyspaces.

See the section on `user-defined functions <#udfs>`__ for more
information.

DROP FUNCTION
`````````````

*Syntax:*

| bc(syntax)..
|  ::= DROP FUNCTION ( IF EXISTS )?
|  ( ‘.’ )? 
|  ( ‘(’ ( ‘,’ )\* ‘)’ )?

*Sample:*

| bc(sample).
| DROP FUNCTION myfunction;
| DROP FUNCTION mykeyspace.afunction;
| DROP FUNCTION afunction ( int );
| DROP FUNCTION afunction ( text );

| ``DROP FUNCTION`` statement removes a function created using
  ``CREATE FUNCTION``.
| You must specify the argument types
  (`signature <#functionSignature>`__ ) of the function to drop if there
  are multiple functions with the same name but a different signature
  (overloaded functions).

``DROP FUNCTION`` with the optional ``IF EXISTS`` keywords drops a
function if it exists.

Aggregate functions
^^^^^^^^^^^^^^^^^^^

| Aggregate functions work on a set of rows. They receive values for
  each row and returns one value for the whole set.
| If ``normal`` columns, ``scalar functions``, ``UDT`` fields,
  ``writetime`` or ``ttl`` are selected together with aggregate
  functions, the values returned for them will be the ones of the first
  row matching the query.

CQL3 distinguishes between built-in aggregates (so called ‘native
aggregates’) and `user-defined aggregates <#udas>`__. CQL3 includes
several native aggregates, described below:

Native aggregates
~~~~~~~~~~~~~~~~~

Count
`````

The ``count`` function can be used to count the rows returned by a
query. Example:

| bc(sample).
| SELECT COUNT (\*) FROM plays;
| SELECT COUNT (1) FROM plays;

It also can be used to count the non null value of a given column.
Example:

| bc(sample).
| SELECT COUNT (scores) FROM plays;

Max and Min
```````````

The ``max`` and ``min`` functions can be used to compute the maximum and
the minimum value returned by a query for a given column.

| bc(sample).
| SELECT MIN (players), MAX (players) FROM plays WHERE game = ‘quake’;

Sum
```

The ``sum`` function can be used to sum up all the values returned by a
query for a given column.

| bc(sample).
| SELECT SUM (players) FROM plays;

Avg
```

The ``avg`` function can be used to compute the average of all the
values returned by a query for a given column.

| bc(sample).
| SELECT AVG (players) FROM plays;

User-Defined Aggregates
~~~~~~~~~~~~~~~~~~~~~~~

User-defined aggregates allow creation of custom aggregate functions
using `UDFs <#udfs>`__. Common examples of aggregate functions are
*count*, *min*, and *max*.

Each aggregate requires an *initial state* (``INITCOND``, which defaults
to ``null``) of type ``STYPE``. The first argument of the state function
must have type ``STYPE``. The remaining arguments of the state function
must match the types of the user-defined aggregate arguments. The state
function is called once for each row, and the value returned by the
state function becomes the new state. After all rows are processed, the
optional ``FINALFUNC`` is executed with last state value as its
argument.

``STYPE`` is mandatory in order to be able to distinguish possibly
overloaded versions of the state and/or final function (since the
overload can appear after creation of the aggregate).

User-defined aggregates can be used in ```SELECT`` <#selectStmt>`__
statement.

A complete working example for user-defined aggregates (assuming that a
keyspace has been selected using the ```USE`` <#useStmt>`__ statement):

| bc(sample)..
| CREATE OR REPLACE FUNCTION averageState ( state tuple, val int )
|  CALLED ON NULL INPUT
|  RETURNS tuple
|  LANGUAGE java
|  AS ‘
   if (val != null) {
   state.setInt(0, state.getInt(0)+1);
   state.setLong(1, state.getLong(1)+val.intValue());
   }
   return state;
   ’;

| CREATE OR REPLACE FUNCTION averageFinal ( state tuple )
|  CALLED ON NULL INPUT
|  RETURNS double
|  LANGUAGE java
|  AS ‘
   double r = 0;
   if (state.getInt(0) == 0) return null;
   r = state.getLong(1);
   r /= state.getInt(0);
   return Double.valueOf®;
   ’;

| CREATE OR REPLACE AGGREGATE average ( int )
|  SFUNC averageState
|  STYPE tuple
|  FINALFUNC averageFinal
|  INITCOND (0, 0);

| CREATE TABLE atable (
|  pk int PRIMARY KEY,
|  val int);
| INSERT INTO atable (pk, val) VALUES (1,1);
| INSERT INTO atable (pk, val) VALUES (2,2);
| INSERT INTO atable (pk, val) VALUES (3,3);
| INSERT INTO atable (pk, val) VALUES (4,4);
| SELECT average(val) FROM atable;
| p.

See ```CREATE AGGREGATE`` <#createAggregateStmt>`__ and
```DROP AGGREGATE`` <#dropAggregateStmt>`__.

CREATE AGGREGATE
````````````````

*Syntax:*

| bc(syntax)..
|  ::= CREATE ( OR REPLACE )?
|  AGGREGATE ( IF NOT EXISTS )?
|  ( ‘.’ )? 
|  ‘(’ ( ‘,’ )\* ‘)’
|  SFUNC 
|  STYPE 
|  ( FINALFUNC )?
|  ( INITCOND )?
| p.
| *Sample:*

| bc(sample).
| CREATE AGGREGATE myaggregate ( val text )
|  SFUNC myaggregate\_state
|  STYPE text
|  FINALFUNC myaggregate\_final
|  INITCOND ‘foo’;

See the section on `user-defined aggregates <#udas>`__ for a complete
example.

``CREATE AGGREGATE`` creates or replaces a user-defined aggregate.

``CREATE AGGREGATE`` with the optional ``OR REPLACE`` keywords either
creates an aggregate or replaces an existing one with the same
signature. A ``CREATE AGGREGATE`` without ``OR REPLACE`` fails if an
aggregate with the same signature already exists.

``CREATE AGGREGATE`` with the optional ``IF NOT EXISTS`` keywords either
creates an aggregate if it does not already exist.

``OR REPLACE`` and ``IF NOT EXISTS`` cannot be used together.

Aggregates belong to a keyspace. If no keyspace is specified in
``<aggregate-name>``, the current keyspace is used (i.e. the keyspace
specified using the ```USE`` <#useStmt>`__ statement). It is not
possible to create a user-defined aggregate in one of the system
keyspaces.

Signatures for user-defined aggregates follow the `same
rules <#functionSignature>`__ as for user-defined functions.

``STYPE`` defines the type of the state value and must be specified.

The optional ``INITCOND`` defines the initial state value for the
aggregate. It defaults to ``null``. A non-\ ``null`` ``INITCOND`` must
be specified for state functions that are declared with
``RETURNS NULL ON NULL INPUT``.

``SFUNC`` references an existing function to be used as the state
modifying function. The type of first argument of the state function
must match ``STYPE``. The remaining argument types of the state function
must match the argument types of the aggregate function. State is not
updated for state functions declared with ``RETURNS NULL ON NULL INPUT``
and called with ``null``.

The optional ``FINALFUNC`` is called just before the aggregate result is
returned. It must take only one argument with type ``STYPE``. The return
type of the ``FINALFUNC`` may be a different type. A final function
declared with ``RETURNS NULL ON NULL INPUT`` means that the aggregate’s
return value will be ``null``, if the last state is ``null``.

If no ``FINALFUNC`` is defined, the overall return type of the aggregate
function is ``STYPE``. If a ``FINALFUNC`` is defined, it is the return
type of that function.

See the section on `user-defined aggregates <#udas>`__ for more
information.

DROP AGGREGATE
``````````````

*Syntax:*

| bc(syntax)..
|  ::= DROP AGGREGATE ( IF EXISTS )?
|  ( ‘.’ )? 
|  ( ‘(’ ( ‘,’ )\* ‘)’ )?
| p.

*Sample:*

| bc(sample).
| DROP AGGREGATE myAggregate;
| DROP AGGREGATE myKeyspace.anAggregate;
| DROP AGGREGATE someAggregate ( int );
| DROP AGGREGATE someAggregate ( text );

The ``DROP AGGREGATE`` statement removes an aggregate created using
``CREATE AGGREGATE``. You must specify the argument types of the
aggregate to drop if there are multiple aggregates with the same name
but a different signature (overloaded aggregates).

``DROP AGGREGATE`` with the optional ``IF EXISTS`` keywords drops an
aggregate if it exists, and does nothing if a function with the
signature does not exist.

Signatures for user-defined aggregates follow the `same
rules <#functionSignature>`__ as for user-defined functions.

JSON Support
------------

Cassandra 2.2 introduces JSON support to ```SELECT`` <#selectStmt>`__
and ```INSERT`` <#insertStmt>`__ statements. This support does not
fundamentally alter the CQL API (for example, the schema is still
enforced), it simply provides a convenient way to work with JSON
documents.

SELECT JSON
^^^^^^^^^^^

With ``SELECT`` statements, the new ``JSON`` keyword can be used to
return each row as a single ``JSON`` encoded map. The remainder of the
``SELECT`` statment behavior is the same.

The result map keys are the same as the column names in a normal result
set. For example, a statement like “``SELECT JSON a, ttl(b) FROM ...``”
would result in a map with keys ``"a"`` and ``"ttl(b)"``. However, this
is one notable exception: for symmetry with ``INSERT JSON`` behavior,
case-sensitive column names with upper-case letters will be surrounded
with double quotes. For example, “``SELECT JSON myColumn FROM ...``”
would result in a map key ``"\"myColumn\""`` (note the escaped quotes).

The map values will ``JSON``-encoded representations (as described
below) of the result set values.

INSERT JSON
^^^^^^^^^^^

With ``INSERT`` statements, the new ``JSON`` keyword can be used to
enable inserting a ``JSON`` encoded map as a single row. The format of
the ``JSON`` map should generally match that returned by a
``SELECT JSON`` statement on the same table. In particular,
case-sensitive column names should be surrounded with double quotes. For
example, to insert into a table with two columns named “myKey” and
“value”, you would do the following:

| bc(sample).
| INSERT INTO mytable JSON ‘{“\\”myKey\\“”: 0, `value <>`__ 0}’

Any columns which are ommitted from the ``JSON`` map will be defaulted
to a ``NULL`` value (which will result in a tombstone being created).

JSON Encoding of Cassandra Data Types
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Where possible, Cassandra will represent and accept data types in their
native ``JSON`` representation. Cassandra will also accept string
representations matching the CQL literal format for all single-field
types. For example, floats, ints, UUIDs, and dates can be represented by
CQL literal strings. However, compound types, such as collections,
tuples, and user-defined types must be represented by native ``JSON``
collections (maps and lists) or a JSON-encoded string representation of
the collection.

The following table describes the encodings that Cassandra will accept
in ``INSERT JSON`` values (and ``fromJson()`` arguments) as well as the
format Cassandra will use when returning data for ``SELECT JSON``
statements (and ``fromJson()``):

+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| type            | formats accepted         | return format   | notes                                                                                                                                                                                                         |
+=================+==========================+=================+===============================================================================================================================================================================================================+
| ``ascii``       | string                   | string          | Uses JSON’s ``\u`` character escape                                                                                                                                                                           |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``bigint``      | integer, string          | integer         | String must be valid 64 bit integer                                                                                                                                                                           |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``blob``        | string                   | string          | String should be 0x followed by an even number of hex digits                                                                                                                                                  |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``boolean``     | boolean, string          | boolean         | String must be “true” or “false”                                                                                                                                                                              |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``date``        | string                   | string          | Date in format ``YYYY-MM-DD``, timezone UTC                                                                                                                                                                   |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``decimal``     | integer, float, string   | float           | May exceed 32 or 64-bit IEEE-754 floating point precision in client-side decoder                                                                                                                              |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``double``      | integer, float, string   | float           | String must be valid integer or float                                                                                                                                                                         |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``float``       | integer, float, string   | float           | String must be valid integer or float                                                                                                                                                                         |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``inet``        | string                   | string          | IPv4 or IPv6 address                                                                                                                                                                                          |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``int``         | integer, string          | integer         | String must be valid 32 bit integer                                                                                                                                                                           |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``list``        | list, string             | list            | Uses JSON’s native list representation                                                                                                                                                                        |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``map``         | map, string              | map             | Uses JSON’s native map representation                                                                                                                                                                         |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``smallint``    | integer, string          | integer         | String must be valid 16 bit integer                                                                                                                                                                           |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``set``         | list, string             | list            | Uses JSON’s native list representation                                                                                                                                                                        |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``text``        | string                   | string          | Uses JSON’s ``\u`` character escape                                                                                                                                                                           |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``time``        | string                   | string          | Time of day in format ``HH-MM-SS[.fffffffff]``                                                                                                                                                                |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timestamp``   | integer, string          | string          | A timestamp. Strings constant are allow to input timestamps as dates, see `Working with dates <#usingdates>`__ below for more information. Datestamps with format ``YYYY-MM-DD HH:MM:SS.SSS`` are returned.   |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``timeuuid``    | string                   | string          | Type 1 UUID. See `Constants <#constants>`__ for the UUID format                                                                                                                                               |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tinyint``     | integer, string          | integer         | String must be valid 8 bit integer                                                                                                                                                                            |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``tuple``       | list, string             | list            | Uses JSON’s native list representation                                                                                                                                                                        |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``UDT``         | map, string              | map             | Uses JSON’s native map representation with field names as keys                                                                                                                                                |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``uuid``        | string                   | string          | See `Constants <#constants>`__ for the UUID format                                                                                                                                                            |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``varchar``     | string                   | string          | Uses JSON’s ``\u`` character escape                                                                                                                                                                           |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| ``varint``      | integer, string          | integer         | Variable length; may overflow 32 or 64 bit integers in client-side decoder                                                                                                                                    |
+-----------------+--------------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

The fromJson() Function
^^^^^^^^^^^^^^^^^^^^^^^

The ``fromJson()`` function may be used similarly to ``INSERT JSON``,
but for a single column value. It may only be used in the ``VALUES``
clause of an ``INSERT`` statement or as one of the column values in an
``UPDATE``, ``DELETE``, or ``SELECT`` statement. For example, it cannot
be used in the selection clause of a ``SELECT`` statement.

The toJson() Function
^^^^^^^^^^^^^^^^^^^^^

The ``toJson()`` function may be used similarly to ``SELECT JSON``, but
for a single column value. It may only be used in the selection clause
of a ``SELECT`` statement.

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


Appendices
----------

.. _appendix-A:

Appendix A: CQL Keywords
~~~~~~~~~~~~~~~~~~~~~~~~

CQL distinguishes between *reserved* and *non-reserved* keywords.
Reserved keywords cannot be used as identifier, they are truly reserved
for the language (but one can enclose a reserved keyword by
double-quotes to use it as an identifier). Non-reserved keywords however
only have a specific meaning in certain context but can used as
identifier otherwise. The only *raison d’être* of these non-reserved
keywords is convenience: some keyword are non-reserved when it was
always easy for the parser to decide whether they were used as keywords
or not.

+--------------------+-------------+
| Keyword            | Reserved?   |
+====================+=============+
| ``ADD``            | yes         |
+--------------------+-------------+
| ``AGGREGATE``      | no          |
+--------------------+-------------+
| ``ALL``            | no          |
+--------------------+-------------+
| ``ALLOW``          | yes         |
+--------------------+-------------+
| ``ALTER``          | yes         |
+--------------------+-------------+
| ``AND``            | yes         |
+--------------------+-------------+
| ``APPLY``          | yes         |
+--------------------+-------------+
| ``AS``             | no          |
+--------------------+-------------+
| ``ASC``            | yes         |
+--------------------+-------------+
| ``ASCII``          | no          |
+--------------------+-------------+
| ``AUTHORIZE``      | yes         |
+--------------------+-------------+
| ``BATCH``          | yes         |
+--------------------+-------------+
| ``BEGIN``          | yes         |
+--------------------+-------------+
| ``BIGINT``         | no          |
+--------------------+-------------+
| ``BLOB``           | no          |
+--------------------+-------------+
| ``BOOLEAN``        | no          |
+--------------------+-------------+
| ``BY``             | yes         |
+--------------------+-------------+
| ``CALLED``         | no          |
+--------------------+-------------+
| ``CLUSTERING``     | no          |
+--------------------+-------------+
| ``COLUMNFAMILY``   | yes         |
+--------------------+-------------+
| ``COMPACT``        | no          |
+--------------------+-------------+
| ``CONTAINS``       | no          |
+--------------------+-------------+
| ``COUNT``          | no          |
+--------------------+-------------+
| ``COUNTER``        | no          |
+--------------------+-------------+
| ``CREATE``         | yes         |
+--------------------+-------------+
| ``CUSTOM``         | no          |
+--------------------+-------------+
| ``DATE``           | no          |
+--------------------+-------------+
| ``DECIMAL``        | no          |
+--------------------+-------------+
| ``DELETE``         | yes         |
+--------------------+-------------+
| ``DESC``           | yes         |
+--------------------+-------------+
| ``DESCRIBE``       | yes         |
+--------------------+-------------+
| ``DISTINCT``       | no          |
+--------------------+-------------+
| ``DOUBLE``         | no          |
+--------------------+-------------+
| ``DROP``           | yes         |
+--------------------+-------------+
| ``ENTRIES``        | yes         |
+--------------------+-------------+
| ``EXECUTE``        | yes         |
+--------------------+-------------+
| ``EXISTS``         | no          |
+--------------------+-------------+
| ``FILTERING``      | no          |
+--------------------+-------------+
| ``FINALFUNC``      | no          |
+--------------------+-------------+
| ``FLOAT``          | no          |
+--------------------+-------------+
| ``FROM``           | yes         |
+--------------------+-------------+
| ``FROZEN``         | no          |
+--------------------+-------------+
| ``FULL``           | yes         |
+--------------------+-------------+
| ``FUNCTION``       | no          |
+--------------------+-------------+
| ``FUNCTIONS``      | no          |
+--------------------+-------------+
| ``GRANT``          | yes         |
+--------------------+-------------+
| ``IF``             | yes         |
+--------------------+-------------+
| ``IN``             | yes         |
+--------------------+-------------+
| ``INDEX``          | yes         |
+--------------------+-------------+
| ``INET``           | no          |
+--------------------+-------------+
| ``INFINITY``       | yes         |
+--------------------+-------------+
| ``INITCOND``       | no          |
+--------------------+-------------+
| ``INPUT``          | no          |
+--------------------+-------------+
| ``INSERT``         | yes         |
+--------------------+-------------+
| ``INT``            | no          |
+--------------------+-------------+
| ``INTO``           | yes         |
+--------------------+-------------+
| ``JSON``           | no          |
+--------------------+-------------+
| ``KEY``            | no          |
+--------------------+-------------+
| ``KEYS``           | no          |
+--------------------+-------------+
| ``KEYSPACE``       | yes         |
+--------------------+-------------+
| ``KEYSPACES``      | no          |
+--------------------+-------------+
| ``LANGUAGE``       | no          |
+--------------------+-------------+
| ``LIMIT``          | yes         |
+--------------------+-------------+
| ``LIST``           | no          |
+--------------------+-------------+
| ``LOGIN``          | no          |
+--------------------+-------------+
| ``MAP``            | no          |
+--------------------+-------------+
| ``MODIFY``         | yes         |
+--------------------+-------------+
| ``NAN``            | yes         |
+--------------------+-------------+
| ``NOLOGIN``        | no          |
+--------------------+-------------+
| ``NORECURSIVE``    | yes         |
+--------------------+-------------+
| ``NOSUPERUSER``    | no          |
+--------------------+-------------+
| ``NOT``            | yes         |
+--------------------+-------------+
| ``NULL``           | yes         |
+--------------------+-------------+
| ``OF``             | yes         |
+--------------------+-------------+
| ``ON``             | yes         |
+--------------------+-------------+
| ``OPTIONS``        | no          |
+--------------------+-------------+
| ``OR``             | yes         |
+--------------------+-------------+
| ``ORDER``          | yes         |
+--------------------+-------------+
| ``PASSWORD``       | no          |
+--------------------+-------------+
| ``PERMISSION``     | no          |
+--------------------+-------------+
| ``PERMISSIONS``    | no          |
+--------------------+-------------+
| ``PRIMARY``        | yes         |
+--------------------+-------------+
| ``RENAME``         | yes         |
+--------------------+-------------+
| ``REPLACE``        | yes         |
+--------------------+-------------+
| ``RETURNS``        | no          |
+--------------------+-------------+
| ``REVOKE``         | yes         |
+--------------------+-------------+
| ``ROLE``           | no          |
+--------------------+-------------+
| ``ROLES``          | no          |
+--------------------+-------------+
| ``SCHEMA``         | yes         |
+--------------------+-------------+
| ``SELECT``         | yes         |
+--------------------+-------------+
| ``SET``            | yes         |
+--------------------+-------------+
| ``SFUNC``          | no          |
+--------------------+-------------+
| ``SMALLINT``       | no          |
+--------------------+-------------+
| ``STATIC``         | no          |
+--------------------+-------------+
| ``STORAGE``        | no          |
+--------------------+-------------+
| ``STYPE``          | no          |
+--------------------+-------------+
| ``SUPERUSER``      | no          |
+--------------------+-------------+
| ``TABLE``          | yes         |
+--------------------+-------------+
| ``TEXT``           | no          |
+--------------------+-------------+
| ``TIME``           | no          |
+--------------------+-------------+
| ``TIMESTAMP``      | no          |
+--------------------+-------------+
| ``TIMEUUID``       | no          |
+--------------------+-------------+
| ``TINYINT``        | no          |
+--------------------+-------------+
| ``TO``             | yes         |
+--------------------+-------------+
| ``TOKEN``          | yes         |
+--------------------+-------------+
| ``TRIGGER``        | no          |
+--------------------+-------------+
| ``TRUNCATE``       | yes         |
+--------------------+-------------+
| ``TTL``            | no          |
+--------------------+-------------+
| ``TUPLE``          | no          |
+--------------------+-------------+
| ``TYPE``           | no          |
+--------------------+-------------+
| ``UNLOGGED``       | yes         |
+--------------------+-------------+
| ``UPDATE``         | yes         |
+--------------------+-------------+
| ``USE``            | yes         |
+--------------------+-------------+
| ``USER``           | no          |
+--------------------+-------------+
| ``USERS``          | no          |
+--------------------+-------------+
| ``USING``          | yes         |
+--------------------+-------------+
| ``UUID``           | no          |
+--------------------+-------------+
| ``VALUES``         | no          |
+--------------------+-------------+
| ``VARCHAR``        | no          |
+--------------------+-------------+
| ``VARINT``         | no          |
+--------------------+-------------+
| ``WHERE``          | yes         |
+--------------------+-------------+
| ``WITH``           | yes         |
+--------------------+-------------+
| ``WRITETIME``      | no          |
+--------------------+-------------+

Appendix B: CQL Reserved Types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following type names are not currently used by CQL, but are reserved
for potential future use. User-defined types may not use reserved type
names as their name.

+-----------------+
| type            |
+=================+
| ``bitstring``   |
+-----------------+
| ``byte``        |
+-----------------+
| ``complex``     |
+-----------------+
| ``date``        |
+-----------------+
| ``enum``        |
+-----------------+
| ``interval``    |
+-----------------+
| ``macaddr``     |
+-----------------+

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
----------

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

.. [#] Technically, this document CQL version 3, which is not backward compatible with CQL version 1 and 2 (which have
   been deprecated and remove) and differs from it in numerous ways.
