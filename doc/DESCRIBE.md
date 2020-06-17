Server side DESCRIBE statement
==============================

High level overview of the server-side `DESCRIBE` statement, added by [CASSANDRA-14825](https://issues.apache.org/jira/browse/CASSANDRA-14825). 

The respective, current syntax is available in both `cqlsh` via `HELP DESCRIBE` and the Java class level javadoc of
`org.apache.cassandra.cql3.statements.DescribeStatement`.


Authorization / permission checks
---------------------------------

Clients can only access / see / list / describe on keyspaces to which they have access to (have been granted
the `DESCRIBE` permission on the respective keyspace). Permission checks are performed on keyspace or table
level, depending on the actual `DESCRIBE` statement sub-type.


Statement and result
--------------------

A `DESCRIBE` CQL statement returns a result set with a single column `schema_part` of type `text`.
The number of returned rows is arbitrary. Row-level paging is supported and encouraged to use.

The actual size of each `schema_part` value can be multiple kilobytes.

Clients must assume that a server splits the whole result string of a `DESCRIBE` statement at arbitrary,
non-deterministic positions, which means that clients must not insert any additional characters, for example 
a new-line character, between rows in the result set.

The correct way to re-construct the DDL script for a `DESCRIBE` statement looks like this (Java code):

```
    Session driverSession = ...;
    SimpleStatement stmt = new SimpleStatement(cql);
    stmt.setFetchSize(1000); // example value, bigger fetch-sizes should work
    ResultSet rs = driverSession.execute(stmt);
    StringBuilder sb = new StringBuilder();
    for (Row r : rs)
    {
        sb.append(r.getString("schema_part"));
    }
    String ddlScript = sb.toString();
```


Paging
------

Paging should be, as mentioned in the above, enabled and set to a reasonable value.

In case the schema changes between two result pages, the request will fail with an "invalid request" (see
section about `Error Codes` in the native-protocol spec, error code for `Invalid`). It is safe to retry the
whole statement in that case.


`cqlsh`
-------

`cqlsh` in Cassandra versions with [CASSANDRA-14825](https://issues.apache.org/jira/browse/CASSANDRA-14825) have support for server-side
`DESCRIBE` and contain the respective help text.


Concurrent schema changes & paging
----------------------------------

The implementation of `org.apache.cassandra.cql3.statements.DescribeStatement` is stateless and clients must assume
that a `DESCRIBE` statement fails with an `InvalidRequestException`, when a schema change is detected between any
two pages. Already retrieved pages must be considered as incomplete (therefore invalid) and should be discarded.
It is safe to retry the whole `DESCRIBE` statement after such an error. The whole result of a *successful* 
`DESCRIBE` statement is consistent (also single page results are consistent).

The paging-state is opaque and *must not* be interpreted in any way.


Limits
------

The Java Driver's default fetch-size (`5000`) seems reasonable for a `DESCRIBE` statement. Lower fetch-sizes cause
more server-roundtrips and bigger fetch-sizes bigger result-set-pages, with the risk of exceeding the native-protocol
result-size limit and unnecessary heap pressure on the server side.

A hard limit with the implementation of `org.apache.cassandra.cql3.statements.DescribeStatement` has not been hit
during development of this feature.

There is a unit test for a `DESCRIBE SCHEMA` against 200 keyspaces * 200 tables (40,000 tables in total), which
succeeds within about 5 seconds using the DataStax Java Driver's default fetch-size. (The unit test uses a mocked schema
and not a "real" schema generated using `CREATE KEYSPACE`/`CREATE TABLE`.)


Paging performance notes
------------------------

Although the implementation of `org.apache.cassandra.cql3.statements.DescribeStatement` has to walk though the
(selected parts of the) schema for every page, the time-complexity is *not* exponential, since at least the very
expensive DDL string snippets are not constructed for skipped rows.


Extension points
----------------

`org.apache.cassandra.cql3.statements.DescribeStatement` itself is an abstract class. It is possible to implement
more `DESCRIBE`-like CQL commands by implementing a new concrete class and adding support in the antlr files.
`protected abstract Stream<String> DescribeStatement.describe(QueryState state)` and/or 
`protected abstract Stream<String> DescribeStatement.Listing.perKeyspace(KeyspaceMetadata ksm)` are the extension
points. See `DescribeStatement` for details.


(Potentially outdated) syntax description
-----------------------------------------

For an up-to-date description, see `HELP DESCRIBE` in `cqlsh` or the Java class level javadoc of
`org.apache.cassandra.cql3.statements.DescribeStatement`.


``` 
        DESCRIBE

        (DESC may be used as a shorthand.)

          Outputs information about the connected Cassandra cluster, or about
          the data objects stored in the cluster. Use in one of the following ways:

        DESCRIBE KEYSPACES

          Output the names of all keyspaces.

        DESCRIBE [ONLY] KEYSPACE [<keyspacename>] [WITH INTERNALS]

          Output CQL commands that could be used to recreate the given keyspace,
          and the objects in it (such as tables, types, functions, etc.).
          In some cases, as the CQL interface matures, there will be some metadata
          about a keyspace that is not representable with CQL. That metadata will not be shown.

          The '<keyspacename>' argument may be omitted, in which case the current
          keyspace will be described.

          If WITH INTERNALS is specified, the output contains the table IDs and is
          adopted to represent the DDL necessary to "re-create" dropped columns.

          If ONLY is specified, only the DDL to recreate the keyspace will be created.
          All keyspace elements, like tables, types, functions, etc will be omitted.

        DESCRIBE TABLES [WITH INTERNALS]

          Output the names of all tables in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE TABLE [<keyspace>.]<tablename> [WITH INTERNALS]

          Output CQL commands that could be used to recreate the given table.
          In some cases, as above, there may be table metadata which is not
          representable and which will not be shown.

          If WITH INTERNALS is specified, the output contains the table ID and is
          adopted to represent the DDL necessary to "re-create" dropped columns.

        DESCRIBE INDEX <indexname>

          Output the CQL command that could be used to recreate the given index.
          In some cases, there may be index metadata which is not representable
          and which will not be shown.

        DESCRIBE MATERIALIZED VIEW <viewname> [WITH INTERNALS]

          Output the CQL command that could be used to recreate the given materialized view.
          In some cases, there may be materialized view metadata which is not representable
          and which will not be shown.

          If WITH INTERNALS is specified, the output contains the table ID and is
          adopted to represent the DDL necessary to "re-create" dropped columns.

        DESCRIBE CLUSTER

          Output information about the connected Cassandra cluster, such as the
          cluster name, and the partitioner and snitch in use. When you are
          connected to a non-system keyspace, also shows endpoint-range
          ownership information for the Cassandra ring.

        DESCRIBE [FULL] SCHEMA [WITH INTERNALS]

          Output CQL commands that could be used to recreate the entire (non-system) schema.
          Works as though "DESCRIBE KEYSPACE k" was invoked for each non-system keyspace
          k. Use DESCRIBE FULL SCHEMA to include the system keyspaces.

          If WITH INTERNALS is specified, the output contains the table IDs and is
          adopted to represent the DDL necessary to "re-create" dropped columns.

        DESCRIBE TYPES

          Output the names of all user-defined-types in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE TYPE [<keyspace>.]<type>

          Output the CQL command that could be used to recreate the given user-defined-type.

        DESCRIBE FUNCTIONS

          Output the names of all user-defined-functions in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE FUNCTION [<keyspace>.]<function>

          Output the CQL command that could be used to recreate the given user-defined-function.

        DESCRIBE AGGREGATES

          Output the names of all user-defined-aggregates in the current keyspace, or in all
          keyspaces if there is no current keyspace.

        DESCRIBE AGGREGATE [<keyspace>.]<aggregate>

          Output the CQL command that could be used to recreate the given user-defined-aggregate.

        DESCRIBE <objname> [WITH INTERNALS]

          Output CQL commands that could be used to recreate the entire object schema,
          where object can be either a keyspace or a table or an index or a materialized
          view (in this order).

          If WITH INTERNALS is specified and &lt;objname&gt; represents a keyspace, table
          materialized view, the output contains the table IDs and is
          adopted to represent the DDL necessary to "re-create" dropped columns.

          <objname> (obviously) cannot be any of the "describe what" qualifiers like
          "cluster", "table", etc.  
```
