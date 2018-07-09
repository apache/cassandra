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

.. _cql-security:

Security
--------

.. _cql-roles:

Database Roles
^^^^^^^^^^^^^^

CQL uses database roles to represent users and group of users. Syntactically, a role is defined by:

.. productionlist::
   role_name: `identifier` | `string`

.. _create-role-statement:

CREATE ROLE
~~~~~~~~~~~

Creating a role uses the ``CREATE ROLE`` statement:

.. productionlist::
   create_role_statement: CREATE ROLE [ IF NOT EXISTS ] `role_name`
                        :     [ WITH `role_options` ]
   role_options: `role_option` ( AND `role_option` )*
   role_option: PASSWORD '=' `string`
              :| LOGIN '=' `boolean`
              :| SUPERUSER '=' `boolean`
              :| OPTIONS '=' `map_literal`
              :| ACCESS TO DATACENTERS `set_literal`
              :| ACCESS TO ALL DATACENTERS

For instance::

    CREATE ROLE new_role;
    CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true;
    CREATE ROLE bob WITH PASSWORD = 'password_b' AND LOGIN = true AND SUPERUSER = true;
    CREATE ROLE carlos WITH OPTIONS = { 'custom_option1' : 'option1_value', 'custom_option2' : 99 };
    CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND ACCESS TO DATACENTERS {'DC1', 'DC3'};
    CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND ACCESS TO ALL DATACENTERS;

By default roles do not possess ``LOGIN`` privileges or ``SUPERUSER`` status.

:ref:`Permissions <cql-permissions>` on database resources are granted to roles; types of resources include keyspaces,
tables, functions and roles themselves. Roles may be granted to other roles to create hierarchical permissions
structures; in these hierarchies, permissions and ``SUPERUSER`` status are inherited, but the ``LOGIN`` privilege is
not.

If a role has the ``LOGIN`` privilege, clients may identify as that role when connecting. For the duration of that
connection, the client will acquire any roles and privileges granted to that role.

Only a client with with the ``CREATE`` permission on the database roles resource may issue ``CREATE ROLE`` requests (see
the :ref:`relevant section <cql-permissions>` below), unless the client is a ``SUPERUSER``. Role management in Cassandra
is pluggable and custom implementations may support only a subset of the listed options.

Role names should be quoted if they contain non-alphanumeric characters.

.. _setting-credentials-for-internal-authentication:

Setting credentials for internal authentication
```````````````````````````````````````````````

Use the ``WITH PASSWORD`` clause to set a password for internal authentication, enclosing the password in single
quotation marks.

If internal authentication has not been set up or the role does not have ``LOGIN`` privileges, the ``WITH PASSWORD``
clause is not necessary.

Restricting connections to specific datacenters
```````````````````````````````````````````````

If a ``network_authorizer`` has been configured, you can restrict login roles to specific datacenters with the
``ACCESS TO DATACENTERS`` clause followed by a set literal of datacenters the user can access. Not specifiying
datacenters implicitly grants access to all datacenters. The clause ``ACCESS TO ALL DATACENTERS`` can be used for
explicitness, but there's no functional difference.

Creating a role conditionally
`````````````````````````````

Attempting to create an existing role results in an invalid query condition unless the ``IF NOT EXISTS`` option is used.
If the option is used and the role exists, the statement is a no-op::

    CREATE ROLE other_role;
    CREATE ROLE IF NOT EXISTS other_role;


.. _alter-role-statement:

ALTER ROLE
~~~~~~~~~~

Altering a role options uses the ``ALTER ROLE`` statement:

.. productionlist::
   alter_role_statement: ALTER ROLE `role_name` WITH `role_options`

For instance::

    ALTER ROLE bob WITH PASSWORD = 'PASSWORD_B' AND SUPERUSER = false;

Restricting connections to specific datacenters
```````````````````````````````````````````````

If a ``network_authorizer`` has been configured, you can restrict login roles to specific datacenters with the
``ACCESS TO DATACENTERS`` clause followed by a set literal of datacenters the user can access. To remove any
data center restrictions, use the ``ACCESS TO ALL DATACENTERS`` clause.

Conditions on executing ``ALTER ROLE`` statements:

-  A client must have ``SUPERUSER`` status to alter the ``SUPERUSER`` status of another role
-  A client cannot alter the ``SUPERUSER`` status of any role it currently holds
-  A client can only modify certain properties of the role with which it identified at login (e.g. ``PASSWORD``)
-  To modify properties of a role, the client must be granted ``ALTER`` :ref:`permission <cql-permissions>` on that role

.. _drop-role-statement:

DROP ROLE
~~~~~~~~~

Dropping a role uses the ``DROP ROLE`` statement:

.. productionlist::
   drop_role_statement: DROP ROLE [ IF EXISTS ] `role_name`

``DROP ROLE`` requires the client to have ``DROP`` :ref:`permission <cql-permissions>` on the role in question. In
addition, client may not ``DROP`` the role with which it identified at login. Finally, only a client with ``SUPERUSER``
status may ``DROP`` another ``SUPERUSER`` role.

Attempting to drop a role which does not exist results in an invalid query condition unless the ``IF EXISTS`` option is
used. If the option is used and the role does not exist the statement is a no-op.

.. _grant-role-statement:

GRANT ROLE
~~~~~~~~~~

Granting a role to another uses the ``GRANT ROLE`` statement:

.. productionlist::
   grant_role_statement: GRANT `role_name` TO `role_name`

For instance::

    GRANT report_writer TO alice;

This statement grants the ``report_writer`` role to ``alice``. Any permissions granted to ``report_writer`` are also
acquired by ``alice``.

Roles are modelled as a directed acyclic graph, so circular grants are not permitted. The following examples result in
error conditions::

    GRANT role_a TO role_b;
    GRANT role_b TO role_a;

    GRANT role_a TO role_b;
    GRANT role_b TO role_c;
    GRANT role_c TO role_a;

.. _revoke-role-statement:

REVOKE ROLE
~~~~~~~~~~~

Revoking a role uses the ``REVOKE ROLE`` statement:

.. productionlist::
   revoke_role_statement: REVOKE `role_name` FROM `role_name`

For instance::

    REVOKE report_writer FROM alice;

This statement revokes the ``report_writer`` role from ``alice``. Any permissions that ``alice`` has acquired via the
``report_writer`` role are also revoked.

.. _list-roles-statement:

LIST ROLES
~~~~~~~~~~

All the known roles (in the system or granted to specific role) can be listed using the ``LIST ROLES`` statement:

.. productionlist::
   list_roles_statement: LIST ROLES [ OF `role_name` ] [ NORECURSIVE ]

For instance::

    LIST ROLES;

returns all known roles in the system, this requires ``DESCRIBE`` permission on the database roles resource. And::

    LIST ROLES OF alice;

enumerates all roles granted to ``alice``, including those transitively acquired. But::

    LIST ROLES OF bob NORECURSIVE

lists all roles directly granted to ``bob`` without including any of the transitively acquired ones.

Users
^^^^^

Prior to the introduction of roles in Cassandra 2.2, authentication and authorization were based around the concept of a
``USER``. For backward compatibility, the legacy syntax has been preserved with ``USER`` centric statements becoming
synonyms for the ``ROLE`` based equivalents. In other words, creating/updating a user is just a different syntax for
creating/updating a role.

.. _create-user-statement:

CREATE USER
~~~~~~~~~~~

Creating a user uses the ``CREATE USER`` statement:

.. productionlist::
   create_user_statement: CREATE USER [ IF NOT EXISTS ] `role_name` [ WITH PASSWORD `string` ] [ `user_option` ]
   user_option: SUPERUSER | NOSUPERUSER

For instance::

    CREATE USER alice WITH PASSWORD 'password_a' SUPERUSER;
    CREATE USER bob WITH PASSWORD 'password_b' NOSUPERUSER;

``CREATE USER`` is equivalent to ``CREATE ROLE`` where the ``LOGIN`` option is ``true``. So, the following pairs of
statements are equivalent::

    CREATE USER alice WITH PASSWORD 'password_a' SUPERUSER;
    CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND SUPERUSER = true;

    CREATE USER IF EXISTS alice WITH PASSWORD 'password_a' SUPERUSER;
    CREATE ROLE IF EXISTS alice WITH PASSWORD = 'password_a' AND LOGIN = true AND SUPERUSER = true;

    CREATE USER alice WITH PASSWORD 'password_a' NOSUPERUSER;
    CREATE ROLE alice WITH PASSWORD = 'password_a' AND LOGIN = true AND SUPERUSER = false;

    CREATE USER alice WITH PASSWORD 'password_a' NOSUPERUSER;
    CREATE ROLE alice WITH PASSWORD = 'password_a' WITH LOGIN = true;

    CREATE USER alice WITH PASSWORD 'password_a';
    CREATE ROLE alice WITH PASSWORD = 'password_a' WITH LOGIN = true;

.. _alter-user-statement:

ALTER USER
~~~~~~~~~~

Altering the options of a user uses the ``ALTER USER`` statement:

.. productionlist::
   alter_user_statement: ALTER USER `role_name` [ WITH PASSWORD `string` ] [ `user_option` ]

For instance::

    ALTER USER alice WITH PASSWORD 'PASSWORD_A';
    ALTER USER bob SUPERUSER;

.. _drop-user-statement:

DROP USER
~~~~~~~~~

Dropping a user uses the ``DROP USER`` statement:

.. productionlist::
   drop_user_statement: DROP USER [ IF EXISTS ] `role_name`

.. _list-users-statement:

LIST USERS
~~~~~~~~~~

Existing users can be listed using the ``LIST USERS`` statement:

.. productionlist::
   list_users_statement: LIST USERS

Note that this statement is equivalent to::

    LIST ROLES;

but only roles with the ``LOGIN`` privilege are included in the output.

Data Control
^^^^^^^^^^^^

.. _cql-permissions:

Permissions
~~~~~~~~~~~

Permissions on resources are granted to roles; there are several different types of resources in Cassandra and each type
is modelled hierarchically:

- The hierarchy of Data resources, Keyspaces and Tables has the structure ``ALL KEYSPACES`` -> ``KEYSPACE`` ->
  ``TABLE``.
- Function resources have the structure ``ALL FUNCTIONS`` -> ``KEYSPACE`` -> ``FUNCTION``
- Resources representing roles have the structure ``ALL ROLES`` -> ``ROLE``
- Resources representing JMX ObjectNames, which map to sets of MBeans/MXBeans, have the structure ``ALL MBEANS`` ->
  ``MBEAN``

Permissions can be granted at any level of these hierarchies and they flow downwards. So granting a permission on a
resource higher up the chain automatically grants that same permission on all resources lower down. For example,
granting ``SELECT`` on a ``KEYSPACE`` automatically grants it on all ``TABLES`` in that ``KEYSPACE``. Likewise, granting
a permission on ``ALL FUNCTIONS`` grants it on every defined function, regardless of which keyspace it is scoped in. It
is also possible to grant permissions on all functions scoped to a particular keyspace.

Modifications to permissions are visible to existing client sessions; that is, connections need not be re-established
following permissions changes.

The full set of available permissions is:

- ``CREATE``
- ``ALTER``
- ``DROP``
- ``SELECT``
- ``MODIFY``
- ``AUTHORIZE``
- ``DESCRIBE``
- ``EXECUTE``

Not all permissions are applicable to every type of resource. For instance, ``EXECUTE`` is only relevant in the context
of functions or mbeans; granting ``EXECUTE`` on a resource representing a table is nonsensical. Attempting to ``GRANT``
a permission on resource to which it cannot be applied results in an error response. The following illustrates which
permissions can be granted on which types of resource, and which statements are enabled by that permission.

=============== =============================== =======================================================================
 Permission      Resource                        Operations
=============== =============================== =======================================================================
 ``CREATE``      ``ALL KEYSPACES``               ``CREATE KEYSPACE`` and ``CREATE TABLE`` in any keyspace
 ``CREATE``      ``KEYSPACE``                    ``CREATE TABLE`` in specified keyspace
 ``CREATE``      ``ALL FUNCTIONS``               ``CREATE FUNCTION`` in any keyspace and ``CREATE AGGREGATE`` in any
                                                 keyspace
 ``CREATE``      ``ALL FUNCTIONS IN KEYSPACE``   ``CREATE FUNCTION`` and ``CREATE AGGREGATE`` in specified keyspace
 ``CREATE``      ``ALL ROLES``                   ``CREATE ROLE``
 ``ALTER``       ``ALL KEYSPACES``               ``ALTER KEYSPACE`` and ``ALTER TABLE`` in any keyspace
 ``ALTER``       ``KEYSPACE``                    ``ALTER KEYSPACE`` and ``ALTER TABLE`` in specified keyspace
 ``ALTER``       ``TABLE``                       ``ALTER TABLE``
 ``ALTER``       ``ALL FUNCTIONS``               ``CREATE FUNCTION`` and ``CREATE AGGREGATE``: replacing any existing
 ``ALTER``       ``ALL FUNCTIONS IN KEYSPACE``   ``CREATE FUNCTION`` and ``CREATE AGGREGATE``: replacing existing in
                                                 specified keyspace
 ``ALTER``       ``FUNCTION``                    ``CREATE FUNCTION`` and ``CREATE AGGREGATE``: replacing existing
 ``ALTER``       ``ALL ROLES``                   ``ALTER ROLE`` on any role
 ``ALTER``       ``ROLE``                        ``ALTER ROLE``
 ``DROP``        ``ALL KEYSPACES``               ``DROP KEYSPACE`` and ``DROP TABLE`` in any keyspace
 ``DROP``        ``KEYSPACE``                    ``DROP TABLE`` in specified keyspace
 ``DROP``        ``TABLE``                       ``DROP TABLE``
 ``DROP``        ``ALL FUNCTIONS``               ``DROP FUNCTION`` and ``DROP AGGREGATE`` in any keyspace
 ``DROP``        ``ALL FUNCTIONS IN KEYSPACE``   ``DROP FUNCTION`` and ``DROP AGGREGATE`` in specified keyspace
 ``DROP``        ``FUNCTION``                    ``DROP FUNCTION``
 ``DROP``        ``ALL ROLES``                   ``DROP ROLE`` on any role
 ``DROP``        ``ROLE``                        ``DROP ROLE``
 ``SELECT``      ``ALL KEYSPACES``               ``SELECT`` on any table
 ``SELECT``      ``KEYSPACE``                    ``SELECT`` on any table in specified keyspace
 ``SELECT``      ``TABLE``                       ``SELECT`` on specified table
 ``SELECT``      ``ALL MBEANS``                  Call getter methods on any mbean
 ``SELECT``      ``MBEANS``                      Call getter methods on any mbean matching a wildcard pattern
 ``SELECT``      ``MBEAN``                       Call getter methods on named mbean
 ``MODIFY``      ``ALL KEYSPACES``               ``INSERT``, ``UPDATE``, ``DELETE`` and ``TRUNCATE`` on any table
 ``MODIFY``      ``KEYSPACE``                    ``INSERT``, ``UPDATE``, ``DELETE`` and ``TRUNCATE`` on any table in
                                                 specified keyspace
 ``MODIFY``      ``TABLE``                       ``INSERT``, ``UPDATE``, ``DELETE`` and ``TRUNCATE`` on specified table
 ``MODIFY``      ``ALL MBEANS``                  Call setter methods on any mbean
 ``MODIFY``      ``MBEANS``                      Call setter methods on any mbean matching a wildcard pattern
 ``MODIFY``      ``MBEAN``                       Call setter methods on named mbean
 ``AUTHORIZE``   ``ALL KEYSPACES``               ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on any table
 ``AUTHORIZE``   ``KEYSPACE``                    ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on any table in
                                                 specified keyspace
 ``AUTHORIZE``   ``TABLE``                       ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on specified table
 ``AUTHORIZE``   ``ALL FUNCTIONS``               ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on any function
 ``AUTHORIZE``   ``ALL FUNCTIONS IN KEYSPACE``   ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` in specified keyspace
 ``AUTHORIZE``   ``FUNCTION``                    ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on specified function
 ``AUTHORIZE``   ``ALL MBEANS``                  ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on any mbean
 ``AUTHORIZE``   ``MBEANS``                      ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on any mbean matching
                                                 a wildcard pattern
 ``AUTHORIZE``   ``MBEAN``                       ``GRANT PERMISSION`` and ``REVOKE PERMISSION`` on named mbean
 ``AUTHORIZE``   ``ALL ROLES``                   ``GRANT ROLE`` and ``REVOKE ROLE`` on any role
 ``AUTHORIZE``   ``ROLES``                       ``GRANT ROLE`` and ``REVOKE ROLE`` on specified roles
 ``DESCRIBE``    ``ALL ROLES``                   ``LIST ROLES`` on all roles or only roles granted to another,
                                                 specified role
 ``DESCRIBE``    ``ALL MBEANS``                  Retrieve metadata about any mbean from the platform's MBeanServer
 ``DESCRIBE``    ``MBEANS``                      Retrieve metadata about any mbean matching a wildcard patter from the
                                                 platform's MBeanServer
 ``DESCRIBE``    ``MBEAN``                       Retrieve metadata about a named mbean from the platform's MBeanServer
 ``EXECUTE``     ``ALL FUNCTIONS``               ``SELECT``, ``INSERT`` and ``UPDATE`` using any function, and use of
                                                 any function in ``CREATE AGGREGATE``
 ``EXECUTE``     ``ALL FUNCTIONS IN KEYSPACE``   ``SELECT``, ``INSERT`` and ``UPDATE`` using any function in specified
                                                 keyspace and use of any function in keyspace in ``CREATE AGGREGATE``
 ``EXECUTE``     ``FUNCTION``                    ``SELECT``, ``INSERT`` and ``UPDATE`` using specified function and use
                                                 of the function in ``CREATE AGGREGATE``
 ``EXECUTE``     ``ALL MBEANS``                  Execute operations on any mbean
 ``EXECUTE``     ``MBEANS``                      Execute operations on any mbean matching a wildcard pattern
 ``EXECUTE``     ``MBEAN``                       Execute operations on named mbean
=============== =============================== =======================================================================

.. _grant-permission-statement:

GRANT PERMISSION
~~~~~~~~~~~~~~~~

Granting a permission uses the ``GRANT PERMISSION`` statement:

.. productionlist::
   grant_permission_statement: GRANT `permissions` ON `resource` TO `role_name`
   permissions: ALL [ PERMISSIONS ] | `permission` [ PERMISSION ]
   permission: CREATE | ALTER | DROP | SELECT | MODIFY | AUTHORIZE | DESCRIBE | EXECUTE
   resource: ALL KEYSPACES
           :| KEYSPACE `keyspace_name`
           :| [ TABLE ] `table_name`
           :| ALL ROLES
           :| ROLE `role_name`
           :| ALL FUNCTIONS [ IN KEYSPACE `keyspace_name` ]
           :| FUNCTION `function_name` '(' [ `cql_type` ( ',' `cql_type` )* ] ')'
           :| ALL MBEANS
           :| ( MBEAN | MBEANS ) `string`

For instance::

    GRANT SELECT ON ALL KEYSPACES TO data_reader;

This gives any user with the role ``data_reader`` permission to execute ``SELECT`` statements on any table across all
keyspaces::

    GRANT MODIFY ON KEYSPACE keyspace1 TO data_writer;

This give any user with the role ``data_writer`` permission to perform ``UPDATE``, ``INSERT``, ``UPDATE``, ``DELETE``
and ``TRUNCATE`` queries on all tables in the ``keyspace1`` keyspace::

    GRANT DROP ON keyspace1.table1 TO schema_owner;

This gives any user with the ``schema_owner`` role permissions to ``DROP`` ``keyspace1.table1``::

    GRANT EXECUTE ON FUNCTION keyspace1.user_function( int ) TO report_writer;

This grants any user with the ``report_writer`` role permission to execute ``SELECT``, ``INSERT`` and ``UPDATE`` queries
which use the function ``keyspace1.user_function( int )``::

    GRANT DESCRIBE ON ALL ROLES TO role_admin;

This grants any user with the ``role_admin`` role permission to view any and all roles in the system with a ``LIST
ROLES`` statement

.. _grant-all:

GRANT ALL
`````````

When the ``GRANT ALL`` form is used, the appropriate set of permissions is determined automatically based on the target
resource.

Automatic Granting
``````````````````

When a resource is created, via a ``CREATE KEYSPACE``, ``CREATE TABLE``, ``CREATE FUNCTION``, ``CREATE AGGREGATE`` or
``CREATE ROLE`` statement, the creator (the role the database user who issues the statement is identified as), is
automatically granted all applicable permissions on the new resource.

.. _revoke-permission-statement:

REVOKE PERMISSION
~~~~~~~~~~~~~~~~~

Revoking a permission from a role uses the ``REVOKE PERMISSION`` statement:

.. productionlist::
   revoke_permission_statement: REVOKE `permissions` ON `resource` FROM `role_name`

For instance::

    REVOKE SELECT ON ALL KEYSPACES FROM data_reader;
    REVOKE MODIFY ON KEYSPACE keyspace1 FROM data_writer;
    REVOKE DROP ON keyspace1.table1 FROM schema_owner;
    REVOKE EXECUTE ON FUNCTION keyspace1.user_function( int ) FROM report_writer;
    REVOKE DESCRIBE ON ALL ROLES FROM role_admin;

Because of their function in normal driver operations, certain tables cannot have their `SELECT` permissions
revoked. The following tables will be available to all authorized users regardless of their assigned role::

* `system_schema.keyspaces`
* `system_schema.columns`
* `system_schema.tables`
* `system.local`
* `system.peers`

.. _list-permissions-statement:

LIST PERMISSIONS
~~~~~~~~~~~~~~~~

Listing granted permissions uses the ``LIST PERMISSIONS`` statement:

.. productionlist::
   list_permissions_statement: LIST `permissions` [ ON `resource` ] [ OF `role_name` [ NORECURSIVE ] ]

For instance::

    LIST ALL PERMISSIONS OF alice;

Show all permissions granted to ``alice``, including those acquired transitively from any other roles::

    LIST ALL PERMISSIONS ON keyspace1.table1 OF bob;

Show all permissions on ``keyspace1.table1`` granted to ``bob``, including those acquired transitively from any other
roles. This also includes any permissions higher up the resource hierarchy which can be applied to ``keyspace1.table1``.
For example, should ``bob`` have ``ALTER`` permission on ``keyspace1``, that would be included in the results of this
query. Adding the ``NORECURSIVE`` switch restricts the results to only those permissions which were directly granted to
``bob`` or one of ``bob``'s roles::

    LIST SELECT PERMISSIONS OF carlos;

Show any permissions granted to ``carlos`` or any of ``carlos``'s roles, limited to ``SELECT`` permissions on any
resource.
