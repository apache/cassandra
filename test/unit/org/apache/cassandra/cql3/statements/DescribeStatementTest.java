/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.util.Iterator;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.ArrayUtils;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SchemaConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DescribeStatementTest extends CQLTester
{
    @Test
    public void testSchemaChangeDuringPaging()
    {
            SimpleStatement stmt = new SimpleStatement("DESCRIBE KEYSPACES");
            stmt.setFetchSize(1);
            ResultSet rs = executeNet(ProtocolVersion.CURRENT, stmt);
            Iterator<Row> iter = rs.iterator();
            assertTrue(iter.hasNext());
            iter.next();

            createKeyspace("CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");

            try
            {
                iter.next();
                fail("Expected InvalidQueryException");
            }
            catch (InvalidQueryException e)
            {
                assertEquals(DescribeStatement.SCHEMA_CHANGED_WHILE_PAGING_MESSAGE, e.getMessage());
            }
    }

    @Test
    public void testDescribeFunctionAndAggregate() throws Throwable
    {
        String fNonOverloaded = createFunction(KEYSPACE,
                                               "",
                                               "CREATE OR REPLACE FUNCTION %s() " +
                                               "CALLED ON NULL INPUT " +
                                               "RETURNS int " +
                                               "LANGUAGE java " +
                                               "AS 'throw new RuntimeException();';");

        String fOverloaded = createFunction(KEYSPACE,
                                            "int, ascii",
                                            "CREATE FUNCTION %s (input int, other_in ascii) " +
                                            "RETURNS NULL ON NULL INPUT " +
                                            "RETURNS text " +
                                            "LANGUAGE java " +
                                            "AS 'return \"Hello World\";'");
        createFunctionOverload(fOverloaded,
                               "text, ascii",
                               "CREATE FUNCTION %s (input text, other_in ascii) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"Hello World\";'");

        for (String describeKeyword : new String[]{"DESCRIBE", "DESC"})
        {
            assertRowsNet(executeDescribeNet(describeKeyword + " FUNCTION " + fNonOverloaded),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(fNonOverloaded) + "()",
                              "CREATE FUNCTION " + fNonOverloaded + "()\n" +
                                      "    CALLED ON NULL INPUT\n" +
                                      "    RETURNS int\n" +
                                      "    LANGUAGE java\n" +
                                  "    AS $$throw new RuntimeException();$$;"));

            assertRowsNet(executeDescribeNet(describeKeyword + " FUNCTION " + fOverloaded),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(fOverloaded) + "(int, ascii)",
                              "CREATE FUNCTION " + fOverloaded + "(input int, other_in ascii)\n" +
                                      "    RETURNS NULL ON NULL INPUT\n" +
                                      "    RETURNS text\n" +
                                      "    LANGUAGE java\n" +
                                  "    AS $$return \"Hello World\";$$;"),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(fOverloaded) + "(text, ascii)",
                              "CREATE FUNCTION " + fOverloaded + "(input text, other_in ascii)\n" +
                                      "    RETURNS NULL ON NULL INPUT\n" +
                                      "    RETURNS text\n" +
                                      "    LANGUAGE java\n" +
                                  "    AS $$return \"Hello World\";$$;"));

            assertRowsNet(executeDescribeNet(describeKeyword + " FUNCTIONS"),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(fNonOverloaded) + "()"),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(fOverloaded) + "(int, ascii)"),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(fOverloaded) + "(text, ascii)"));
        }

        String fIntState = createFunction(KEYSPACE,
                                          "int, int",
                                          "CREATE FUNCTION %s (state int, add_to int) " +
                                          "CALLED ON NULL INPUT " +
                                          "RETURNS int " +
                                          "LANGUAGE java " +
                                          "AS 'return state + add_to;'");
        String fFinal = createFunction(KEYSPACE,
                                       "int",
                                       "CREATE FUNCTION %s (state int) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS int " +
                                       "LANGUAGE java " +
                                       "AS 'return state;'");

        String aNonDeterministic = createAggregate(KEYSPACE,
                                                   "int",
                                                   format("CREATE AGGREGATE %%s(int) " +
                                                                 "SFUNC %s " +
                                                                 "STYPE int " +
                                                                 "INITCOND 42",
                                                                 shortFunctionName(fIntState)));
        String aDeterministic = createAggregate(KEYSPACE,
                                                "int",
                                                format("CREATE AGGREGATE %%s(int) " +
                                                              "SFUNC %s " +
                                                              "STYPE int " +
                                                              "FINALFUNC %s ",
                                                              shortFunctionName(fIntState),
                                                              shortFunctionName(fFinal)));

        for (String describeKeyword : new String[]{"DESCRIBE", "DESC"})
        {
            assertRowsNet(executeDescribeNet(describeKeyword + " AGGREGATE " + aNonDeterministic),
                          row(KEYSPACE,
                              "aggregate",
                              shortFunctionName(aNonDeterministic) + "(int)",
                              "CREATE AGGREGATE " + aNonDeterministic + "(int)\n" +
                                      "    SFUNC " + shortFunctionName(fIntState) + "\n" +
                                      "    STYPE int\n" +
                                  "    INITCOND 42;"));
            assertRowsNet(executeDescribeNet(describeKeyword + " AGGREGATE " + aDeterministic),
                          row(KEYSPACE,
                              "aggregate",
                              shortFunctionName(aDeterministic) + "(int)",
                              "CREATE AGGREGATE " + aDeterministic + "(int)\n" +
                                      "    SFUNC " + shortFunctionName(fIntState) + "\n" +
                                      "    STYPE int\n" +
                                      "    FINALFUNC " + shortFunctionName(fFinal) + ";"));
            assertRowsNet(executeDescribeNet(describeKeyword + " AGGREGATES"),
                          row(KEYSPACE,
                              "aggregate",
                              shortFunctionName(aNonDeterministic) + "(int)"),
                          row(KEYSPACE,
                              "aggregate",
                              shortFunctionName(aDeterministic) + "(int)"));
        }
    }

    @Test
    public void testDescribeFunctionWithTuples() throws Throwable
    {
        String function = createFunction(KEYSPACE,
                                         "tuple<int>, list<frozen<tuple<int, text>>>, tuple<frozen<tuple<int, text>>, text>",
                                         "CREATE OR REPLACE FUNCTION %s(t tuple<int>, l list<frozen<tuple<int, text>>>, nt tuple<frozen<tuple<int, text>>, text>) " +
                                         "CALLED ON NULL INPUT " +
                                         "RETURNS tuple<int, text> " +
                                         "LANGUAGE java " +
                                         "AS 'throw new RuntimeException();';");

            assertRowsNet(executeDescribeNet("DESCRIBE FUNCTION " + function),
                          row(KEYSPACE,
                              "function",
                              shortFunctionName(function) + "(tuple<int>, list<frozen<tuple<int, text>>>, tuple<frozen<tuple<int, text>>, text>)",
                              "CREATE FUNCTION " + function + "(t tuple<int>, l list<frozen<tuple<int, text>>>, nt tuple<frozen<tuple<int, text>>, text>)\n" +
                              "    CALLED ON NULL INPUT\n" +
                              "    RETURNS tuple<int, text>\n" +
                              "    LANGUAGE java\n" +
                              "    AS $$throw new RuntimeException();$$;"));
    }

    @Test
    public void testDescribeMaterializedView() throws Throwable
    {
        assertRowsNet(executeDescribeNet("DESCRIBE ONLY KEYSPACE system_virtual_schema;"), 
                      row("system_virtual_schema",
                          "keyspace",
                          "system_virtual_schema",
                          "/*\n" + 
                          "Warning: Keyspace system_virtual_schema is a virtual keyspace and cannot be recreated with CQL.\n" +
                          "Structure, for reference:\n" +
                          "VIRTUAL KEYSPACE system_virtual_schema;\n" +
                          "*/"));

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE system_virtual_schema.columns;"), 
                      row("system_virtual_schema",
                          "table",
                          "columns",
                          "/*\n" + 
                          "Warning: Table system_virtual_schema.columns is a virtual table and cannot be recreated with CQL.\n" +
                          "Structure, for reference:\n" +
                          "VIRTUAL TABLE system_virtual_schema.columns (\n" +
                          "    keyspace_name text,\n" +
                          "    table_name text,\n" +
                          "    column_name text,\n" +
                          "    clustering_order text,\n" +
                          "    column_name_bytes blob,\n" +
                          "    kind text,\n" +
                          "    position int,\n" +
                          "    type text,\n" +
                          "    PRIMARY KEY (keyspace_name, table_name, column_name)\n" +
                          ") WITH CLUSTERING ORDER BY (table_name ASC, column_name ASC)\n" +
                          "    AND comment = 'virtual column definitions';\n" +
                          "*/"));
    }

    @Test
    public void testDescribe() throws Throwable
    {
        try
        {
            execute("CREATE KEYSPACE test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
            execute("CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int);");
            execute("CREATE INDEX myindex ON test.users (age);");
            execute("CREATE TABLE test.\"Test\" (id int, col int, val text, PRIMARY KEY(id, col));");
            execute("CREATE INDEX ON test.\"Test\" (col);");
            execute("CREATE INDEX ON test.\"Test\" (val)");
            execute("CREATE TABLE test.users_mv (username varchar, password varchar, gender varchar, session_token varchar, " +
                    "state varchar, birth_year bigint, PRIMARY KEY (username));");
            execute("CREATE MATERIALIZED VIEW test.users_by_state AS SELECT * FROM test.users_mv " +
                    "WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)");
            execute(allTypesTable());

            // Test describe schema

            Object[][] testSchemaOutput = rows(
                          row(KEYSPACE, "keyspace", KEYSPACE,
                              "CREATE KEYSPACE " + KEYSPACE +
                                  " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" +
                                  "  AND durable_writes = true;"),
                          row(KEYSPACE_PER_TEST, "keyspace", KEYSPACE_PER_TEST,
                              "CREATE KEYSPACE " + KEYSPACE_PER_TEST +
                                  " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" +
                                  "  AND durable_writes = true;"),
                          row("test", "keyspace", "test", keyspaceOutput()),
                          row("test", "table", "has_all_types", allTypesTable()),
                          row("test", "table", "\"Test\"", testTableOutput()),
                          row("test", "index", "\"Test_col_idx\"", indexOutput("\"Test_col_idx\"", "\"Test\"", "col")),
                          row("test", "index", "\"Test_val_idx\"", indexOutput("\"Test_val_idx\"", "\"Test\"", "val")),
                          row("test", "table", "users", userTableOutput()),
                          row("test", "index", "myindex", indexOutput("myindex", "users", "age")),
                          row("test", "table", "users_mv", usersMvTableOutput()),
                          row("test", "materialized_view", "users_by_state", usersByStateMvOutput()));

            assertRowsNet(executeDescribeNet("DESCRIBE SCHEMA"), testSchemaOutput);
            assertRowsNet(executeDescribeNet("DESC SCHEMA"), testSchemaOutput);

            // Test describe keyspaces/keyspace

            Object[][] testKeyspacesOutput = rows(row(KEYSPACE, "keyspace", KEYSPACE),
                                                  row(KEYSPACE_PER_TEST, "keyspace", KEYSPACE_PER_TEST),
                                                  row(SYSTEM_KEYSPACE_NAME, "keyspace", SYSTEM_KEYSPACE_NAME),
                                                  row(AUTH_KEYSPACE_NAME, "keyspace", AUTH_KEYSPACE_NAME),
                                                  row(DISTRIBUTED_KEYSPACE_NAME, "keyspace", DISTRIBUTED_KEYSPACE_NAME),
                                                  row(SCHEMA_KEYSPACE_NAME, "keyspace", SCHEMA_KEYSPACE_NAME),
                                                  row(TRACE_KEYSPACE_NAME, "keyspace", TRACE_KEYSPACE_NAME),
                                                  row(VIRTUAL_SCHEMA, "keyspace", VIRTUAL_SCHEMA),
                                                  row("test", "keyspace", "test"));

            for (String describeKeyword : new String[]{"DESCRIBE", "DESC"})
            {
                assertRowsNet(executeDescribeNet(describeKeyword + " KEYSPACES"), testKeyspacesOutput);
                assertRowsNet(executeDescribeNet("test", describeKeyword + " KEYSPACES"), testKeyspacesOutput);

                assertRowsNet(executeDescribeNet(describeKeyword + " ONLY KEYSPACE test"),
                              row("test", "keyspace", "test", keyspaceOutput()));
            }

            Object[][] testKeyspaceOutput = rows(row("test", "keyspace", "test", keyspaceOutput()),
                                                 row("test", "table", "has_all_types", allTypesTable()),
                                                 row("test", "table", "\"Test\"", testTableOutput()),
                                                 row("test", "index", "\"Test_col_idx\"", indexOutput("\"Test_col_idx\"", "\"Test\"", "col")),
                                                 row("test", "index", "\"Test_val_idx\"", indexOutput("\"Test_val_idx\"", "\"Test\"", "val")),
                                                 row("test", "table", "users", userTableOutput()),
                                                 row("test", "index", "myindex", indexOutput("myindex", "users", "age")),
                                                 row("test", "table", "users_mv", usersMvTableOutput()),
                                                 row("test", "materialized_view", "users_by_state", usersByStateMvOutput()));

            for (String describeKeyword : new String[]{"DESCRIBE", "DESC"})
            {
                assertRowsNet(executeDescribeNet(describeKeyword + " KEYSPACE test"), testKeyspaceOutput);
                assertRowsNet(executeDescribeNet(describeKeyword + " test"), testKeyspaceOutput);

                describeError(describeKeyword + " test2", "'test2' not found in keyspaces");
            }

            // Test describe tables/table
            for (String cmd : new String[]{"describe TABLES", "DESC tables"})
                assertRowsNet(executeDescribeNet("test", cmd),
                              row("test", "table", "has_all_types"),
                              row("test", "table", "\"Test\""),
                              row("test", "table", "users"),
                              row("test", "table", "users_mv"));

            testDescribeTable("test", "has_all_types", row("test", "table", "has_all_types", allTypesTable()));

            testDescribeTable("test", "\"Test\"",
                              row("test", "table", "\"Test\"", testTableOutput()),
                              row("test", "index", "\"Test_col_idx\"", indexOutput("\"Test_col_idx\"", "\"Test\"", "col")),
                              row("test", "index", "\"Test_val_idx\"", indexOutput("\"Test_val_idx\"", "\"Test\"", "val")));

            testDescribeTable("test", "users", row("test", "table", "users", userTableOutput()),
                                               row("test", "index", "myindex", indexOutput("myindex", "users", "age")));

            describeError("test", "DESCRIBE users2", "'users2' not found in keyspace 'test'");
            describeError("DESCRIBE test.users2", "'users2' not found in keyspace 'test'");

            // Test describe index

            testDescribeIndex("test", "myindex", row("test", "index", "myindex", indexOutput("myindex", "users", "age")));
            testDescribeIndex("test", "\"Test_col_idx\"", row("test", "index", "\"Test_col_idx\"", indexOutput("\"Test_col_idx\"", "\"Test\"", "col")));
            testDescribeIndex("test", "\"Test_val_idx\"", row("test", "index", "\"Test_val_idx\"", indexOutput("\"Test_val_idx\"", "\"Test\"", "val")));

            describeError("DESCRIBE test.myindex2", "'myindex2' not found in keyspace 'test'");
            describeError("test", "DESCRIBE myindex2", "'myindex2' not found in keyspace 'test'");

            // Test describe materialized view

            testDescribeMaterializedView("test", "users_by_state", row("test", "materialized_view", "users_by_state", usersByStateMvOutput()));
        }
        finally
        {
            execute("DROP KEYSPACE IF EXISTS test");
        }
    }

    private void testDescribeTable(String keyspace, String table, Object[]... rows) throws Throwable
    {
        for (String describeKeyword : new String[]{"describe", "desc"})
        {
            for (String cmd : new String[]{describeKeyword + " table " + keyspace + "." + table,
                                           describeKeyword + " columnfamily " + keyspace + "." + table,
                                           describeKeyword + " " + keyspace + "." + table})
            {
                assertRowsNet(executeDescribeNet(cmd), rows);
            }

            for (String cmd : new String[]{describeKeyword + " table " + table,
                                           describeKeyword + " columnfamily " + table,
                                           describeKeyword + " " + table})
            {
                assertRowsNet(executeDescribeNet(keyspace, cmd), rows);
            }
        }
    }

    private void testDescribeIndex(String keyspace, String index, Object[]... rows) throws Throwable
    {
        for (String describeKeyword : new String[]{"describe", "desc"})
        {
            for (String cmd : new String[]{describeKeyword + " index " + keyspace + "." + index,
                                           describeKeyword + " " + keyspace + "." + index})
            {
                assertRowsNet(executeDescribeNet(cmd), rows);
            }

            for (String cmd : new String[]{describeKeyword + " index " + index,
                                           describeKeyword + " " + index})
            {
                assertRowsNet(executeDescribeNet(keyspace, cmd), rows);
            }
        }
    }

    private void testDescribeMaterializedView(String keyspace, String view, Object[]... rows) throws Throwable
    {
        for (String describeKeyword : new String[]{"describe", "desc"})
        {
            for (String cmd : new String[]{describeKeyword + " materialized view " + keyspace + "." + view,
                                           describeKeyword + " " + keyspace + "." + view})
            {
                assertRowsNet(executeDescribeNet(cmd), rows);
            }

            for (String cmd : new String[]{describeKeyword + " materialized view " + view,
                                           describeKeyword + " " + view})
            {
                assertRowsNet(executeDescribeNet(keyspace, cmd), rows);
            }
        }
    }

    @Test
    public void testDescribeCluster() throws Throwable
    {
        for (String describeKeyword : new String[]{"DESCRIBE", "DESC"})
        {
            assertRowsNet(executeDescribeNet(describeKeyword + " CLUSTER"),
                         row("Test Cluster",
                             "ByteOrderedPartitioner",
                             DatabaseDescriptor.getEndpointSnitch().getClass().getName()));

            assertRowsNet(executeDescribeNet("system_virtual_schema", describeKeyword + " CLUSTER"),
                          row("Test Cluster",
                              "ByteOrderedPartitioner",
                              DatabaseDescriptor.getEndpointSnitch().getClass().getName()));
        }

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        Token token = tokenMetadata.sortedTokens().get(0);
        InetAddressAndPort addressAndPort = tokenMetadata.getAllEndpoints().iterator().next();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE CLUSTER"),
                      row("Test Cluster",
                          "ByteOrderedPartitioner",
                          DatabaseDescriptor.getEndpointSnitch().getClass().getName(),
                          ImmutableMap.of(token.toString(), ImmutableList.of(addressAndPort.toString()))));
    }

    @Test
    public void testDescribeTableWithInternals() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk1 text, pk2 int, c int, s decimal static, v1 text, v2 int, v3 int, PRIMARY KEY ((pk1, pk2), c ))");

        TableId id = Schema.instance.getTableMetadata(KEYSPACE, table).id;

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk1 text,\n" +
                                      "    pk2 int,\n" +
                                      "    c int,\n" +
                                      "    s decimal static,\n" +
                                      "    v1 text,\n" +
                                      "    v2 int,\n" +
                                      "    v3 int,\n" +
                                      "    PRIMARY KEY ((pk1, pk2), c)\n" +
                                      ") WITH ID = " + id + "\n" +
                                      "    AND CLUSTERING ORDER BY (c ASC)\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE + "." + table + " WITH INTERNALS"),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));

        String dropStatement = "ALTER TABLE " + KEYSPACE + "." + table + " DROP v3 USING TIMESTAMP 1589286942065000;";

        execute(dropStatement);

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE + "." + table + " WITH INTERNALS"),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement + "\n" +
                          dropStatement));

        String addStatement = "ALTER TABLE " + KEYSPACE + "." + table + " ADD v3 int;";

        execute(addStatement);

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE + "." + table + " WITH INTERNALS"),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement + "\n" +
                          dropStatement + "\n" +
                          addStatement));
    }

    @Test
    public void testPrimaryKeyPositionWithAndWithoutInternals() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (pk text, v1 text, v2 int, v3 int, PRIMARY KEY (pk))");

        TableId id = Schema.instance.getTableMetadata(KEYSPACE, table).id;

        String tableCreateStatement = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                      "    pk text PRIMARY KEY,\n" +
                                      "    v1 text,\n" +
                                      "    v2 int,\n" +
                                      "    v3 int\n" +
                                      ") WITH ID = " + id + "\n" +
                                      "    AND " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE + "." + table + " WITH INTERNALS"),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement));

        String dropStatement = "ALTER TABLE " + KEYSPACE + "." + table + " DROP v3 USING TIMESTAMP 1589286942065000;";

        execute(dropStatement);

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE + "." + table + " WITH INTERNALS"),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatement + "\n" +
                          dropStatement));

        String tableCreateStatementWithoutDroppedColumn = "CREATE TABLE " + KEYSPACE + "." + table + " (\n" +
                                                          "    pk text PRIMARY KEY,\n" +
                                                          "    v1 text,\n" +
                                                          "    v2 int\n" +
                                                          ") WITH " + tableParametersCql();

        assertRowsNet(executeDescribeNet("DESCRIBE TABLE " + KEYSPACE + "." + table),
                      row(KEYSPACE,
                          "table",
                          table,
                          tableCreateStatementWithoutDroppedColumn));
    }

    
    @Test
    public void testDescribeMissingKeyspace() throws Throwable
    {
        describeError("DESCRIBE TABLE foop",
                      "No keyspace specified and no current keyspace");
        describeError("DESCRIBE MATERIALIZED VIEW foop",
                      "No keyspace specified and no current keyspace");
        describeError("DESCRIBE INDEX foop",
                      "No keyspace specified and no current keyspace");
        describeError("DESCRIBE TYPE foop",
                      "No keyspace specified and no current keyspace");
        describeError("DESCRIBE FUNCTION foop",
                      "No keyspace specified and no current keyspace");
        describeError("DESCRIBE AGGREGATE foop",
                      "No keyspace specified and no current keyspace");
    }

    @Test
    public void testDescribeNotFound() throws Throwable
    {
        describeError(format("DESCRIBE AGGREGATE %s.%s", KEYSPACE, "aggr_foo"),
                      format("User defined aggregate '%s' not found in '%s'", "aggr_foo", KEYSPACE));

        describeError(format("DESCRIBE FUNCTION %s.%s", KEYSPACE, "func_foo"),
                      format("User defined function '%s' not found in '%s'", "func_foo", KEYSPACE));

        describeError(format("DESCRIBE %s.%s", KEYSPACE, "func_foo"),
                      format("'%s' not found in keyspace '%s'", "func_foo", KEYSPACE));

        describeError(format("DESCRIBE %s", "foo"),
                      format("'%s' not found in keyspaces", "foo"));
    }

    @Test
    public void testDescribeTypes() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s (a int)");
        String type2 = createType("CREATE TYPE %s (x text, y text)");
        String type3 = createType("CREATE TYPE %s (a text, b frozen<" + type2 + ">)");
        execute("ALTER TYPE " + KEYSPACE + "." + type1 + " ADD b frozen<" + type3 + ">");

        try
        {
            assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TYPES"),
                          row(KEYSPACE, "type", type1),
                          row(KEYSPACE, "type", type2),
                          row(KEYSPACE, "type", type3));

            assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TYPE " + type2),
                          row(KEYSPACE, "type", type2, "CREATE TYPE " + KEYSPACE + "." + type2 + " (\n" +
                                                       "    x text,\n" + 
                                                       "    y text\n" +
                                                       ");"));
            assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE TYPE " + type1),
                          row(KEYSPACE, "type", type1, "CREATE TYPE " + KEYSPACE + "." + type1 + " (\n" +
                                                       "    a int,\n" + 
                                                       "    b frozen<" + type3 + ">\n" +
                                                       ");"));

            assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE KEYSPACE " + KEYSPACE),
                          row(KEYSPACE, "keyspace", KEYSPACE, "CREATE KEYSPACE " + KEYSPACE +
                                                          " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}" +
                                                          "  AND durable_writes = true;"),
                          row(KEYSPACE, "type", type2, "CREATE TYPE " + KEYSPACE + "." + type2 + " (\n" +
                                                       "    x text,\n" + 
                                                       "    y text\n" +
                                                       ");"),
                          row(KEYSPACE, "type", type3, "CREATE TYPE " + KEYSPACE + "." + type3 + " (\n" +
                                                       "    a text,\n" + 
                                                       "    b frozen<" + type2 + ">\n" +
                                                       ");"),
                          row(KEYSPACE, "type", type1, "CREATE TYPE " + KEYSPACE + "." + type1 + " (\n" +
                                                       "    a int,\n" + 
                                                       "    b frozen<" + type3 + ">\n" +
                                                       ");"));
        }
        finally
        {
            execute("DROP TYPE " + KEYSPACE + "." + type1);
            execute("DROP TYPE " + KEYSPACE + "." + type3);
            execute("DROP TYPE " + KEYSPACE + "." + type2);
        }
    }

    /**
     * Tests for the error reported in CASSANDRA-9064 by:
     *
     * - creating the table described in the bug report, using LCS,
     * - DESCRIBE-ing that table via cqlsh, then DROPping it,
     * - running the output of the DESCRIBE statement as a CREATE TABLE statement, and
     * - inserting a value into the table.
     *
     * The final two steps of the test should not fall down. If one does, that
     * indicates the output of DESCRIBE is not a correct CREATE TABLE statement.
     */
    @Test
    public void testDescribeRoundtrip() throws Throwable
    {
        for (String withInternals : new String[]{"", " WITH INTERNALS"})
        {
            String table = createTable("CREATE TABLE %s (key int PRIMARY KEY) WITH compaction = {'class': 'LeveledCompactionStrategy'}");

            String output = executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + table + withInternals).all().get(0).getString("create_statement");

            execute("DROP TABLE %s");

            executeNet(output);

            String output2 = executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + table + withInternals).all().get(0).getString("create_statement");
            assertEquals(output, output2);

            execute("INSERT INTO %s (key) VALUES (1)");
        }
    }

    private static String allTypesTable()
    {
        return "CREATE TABLE test.has_all_types (\n" +
               "    num int PRIMARY KEY,\n" +
               "    asciicol ascii,\n" +
               "    bigintcol bigint,\n" +
               "    blobcol blob,\n" +
               "    booleancol boolean,\n" +
               "    decimalcol decimal,\n" +
               "    doublecol double,\n" +
               "    durationcol duration,\n" +
               "    floatcol float,\n" +
               "    frozenlistcol frozen<list<text>>,\n" +
               "    frozenmapcol frozen<map<timestamp, timeuuid>>,\n" +
               "    frozensetcol frozen<set<bigint>>,\n" +
               "    intcol int,\n" +
               "    smallintcol smallint,\n" +
               "    textcol text,\n" +
               "    timestampcol timestamp,\n" +
               "    tinyintcol tinyint,\n" +
               "    tuplecol frozen<tuple<text, int, frozen<tuple<timestamp>>>>,\n" +
               "    uuidcol uuid,\n" +
               "    varcharcol text,\n" +
               "    varintcol varint,\n" +
               "    listcol list<decimal>,\n" +
               "    mapcol map<timestamp, timeuuid>,\n" +
               "    setcol set<tinyint>\n" +
               ") WITH " + tableParametersCql();
    }

    private static String usersByStateMvOutput()
    {
        return "CREATE MATERIALIZED VIEW test.users_by_state AS\n" +
               "    SELECT *\n" +
               "    FROM test.users_mv\n" +
               "    WHERE state IS NOT NULL AND username IS NOT NULL\n" +
               "    PRIMARY KEY (state, username)\n" +
               " WITH CLUSTERING ORDER BY (username ASC)\n" +
               "    AND " + tableParametersCql();
    }

    private static String indexOutput(String index, String table, String col)
    {
        return format("CREATE INDEX %s ON %s.%s (%s);", index, "test", table, col);
    }

    private static String usersMvTableOutput()
    {
        return "CREATE TABLE test.users_mv (\n" +
               "    username text PRIMARY KEY,\n" +
               "    birth_year bigint,\n" +
               "    gender text,\n" +
               "    password text,\n" +
               "    session_token text,\n" +
               "    state text\n" +
               ") WITH " + tableParametersCql();
    }

    private static String userTableOutput()
    {
        return "CREATE TABLE test.users (\n" +
               "    userid text PRIMARY KEY,\n" +
               "    age int,\n" +
               "    firstname text,\n" +
               "    lastname text\n" +
               ") WITH " + tableParametersCql();
    }

    private static String testTableOutput()
    {
        return "CREATE TABLE test.\"Test\" (\n" +
                   "    id int,\n" +
                   "    col int,\n" +
                   "    val text,\n"  +
                   "    PRIMARY KEY (id, col)\n" +
                   ") WITH CLUSTERING ORDER BY (col ASC)\n" +
                   "    AND " + tableParametersCql();
    }

    private static String tableParametersCql()
    {
        return "additional_write_policy = '99p'\n" +
               "    AND bloom_filter_fp_chance = 0.01\n" +
               "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
               "    AND cdc = false\n" +
               "    AND comment = ''\n" +
               "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
               "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
               "    AND crc_check_chance = 1.0\n" +
               "    AND default_time_to_live = 0\n" +
               "    AND extensions = {}\n" +
               "    AND gc_grace_seconds = 864000\n" +
               "    AND max_index_interval = 2048\n" +
               "    AND memtable_flush_period_in_ms = 0\n" +
               "    AND min_index_interval = 128\n" +
               "    AND read_repair = 'BLOCKING'\n" +
               "    AND speculative_retry = '99p';";
    }

    private static String keyspaceOutput()
    {
        return "CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;";
    }

    private void describeError(String cql, String msg) throws Throwable
    {
        describeError(null, cql, msg);
    }

    private void describeError(String useKs, String cql, String msg) throws Throwable
    {
        assertInvalidThrowMessage(Optional.of(getProtocolVersion(useKs)), msg, InvalidQueryException.class, cql, ArrayUtils.EMPTY_OBJECT_ARRAY);
    }

    private ResultSet executeDescribeNet(String cql) throws Throwable
    {
        return executeDescribeNet(null, cql);
    }

    private ResultSet executeDescribeNet(String useKs, String cql) throws Throwable
    {
        return executeNetWithPaging(getProtocolVersion(useKs), cql, 3);
    }

    private ProtocolVersion getProtocolVersion(String useKs) throws Throwable
    {
        // We're using a trick here to distinguish driver sessions with a "USE keyspace" and without:
        // As different ProtocolVersions use different driver instances, we use different ProtocolVersions
        // for the with and without "USE keyspace" cases.

        ProtocolVersion v = useKs != null ? ProtocolVersion.CURRENT : ProtocolVersion.V5;

        if (useKs != null)
            executeNet(v, "USE " + useKs);
        return v;
    }

    private static Object[][] rows(Object[]... rows)
    {
        return rows;
    }
}
