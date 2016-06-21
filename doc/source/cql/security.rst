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
