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

import org.apache.commons.lang3.ArrayUtils;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.ProtocolVersion;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
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

            assertRowsNet(executeDescribeNet("DESCRIBE SCHEMA"),
                          row("CREATE KEYSPACE " + KEYSPACE + "\n" +
                                  "    WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}\n" +
                                  "     AND durable_writes = true;"),
                          row("CREATE KEYSPACE " + KEYSPACE_PER_TEST + "\n" +
                                  "    WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}\n" +
                                  "     AND durable_writes = true;"),
                          row(keyspaceOutput()),
                          row(allTypesTable()),
                          row(testTableOutput(true, true)),
                          row(indexOutput("\"Test_col_idx\"", "\"Test\"", "col")),
                          row(indexOutput("\"Test_val_idx\"", "\"Test\"", "val")),
                          row(userTableOutput()),
                          row(indexOutput("myindex", "users", "age")),
                          row(usersMvTableOutput()),
                          row(usersByStateMvOutput()));

            // Test describe keyspaces/keyspace

            assertRowsNet(executeDescribeNet("DESCRIBE KEYSPACES"),
                          row(KEYSPACE),
                          row(KEYSPACE_PER_TEST),
                          row(SchemaConstants.SYSTEM_KEYSPACE_NAME),
                          row(SchemaConstants.AUTH_KEYSPACE_NAME),
                          row(SchemaConstants.DISTRIBUTED_KEYSPACE_NAME),
                          row(SchemaConstants.SCHEMA_KEYSPACE_NAME),
                          row(SchemaConstants.TRACE_KEYSPACE_NAME),
                          row(SchemaConstants.VIRTUAL_SCHEMA),
                          row("test"));

            assertRowsNet(executeDescribeNet("DESCRIBE ONLY KEYSPACE test"),
                          row(keyspaceOutput()));

            Object[][] testKeyspaceOutput = rows(row(keyspaceOutput()),
                                                 row(allTypesTable()),
                                                 row(testTableOutput(true, true)),
                                                 row(indexOutput("\"Test_col_idx\"", "\"Test\"", "col")),
                                                 row(indexOutput("\"Test_val_idx\"", "\"Test\"", "val")),
                                                 row(userTableOutput()),
                                                 row(indexOutput("myindex", "users", "age")),
                                                 row(usersMvTableOutput()),
                                                 row(usersByStateMvOutput()));

            assertRowsNet(executeDescribeNet("DESCRIBE KEYSPACE test"), testKeyspaceOutput);
            assertRowsNet(executeDescribeNet("DESCRIBE test"), testKeyspaceOutput);

            describeError("DESCRIBE test2", "Keyspace 'test2' not found");

            // Test describe tables/table
            for (String cmd : new String[]{"describe TABLES", "DESC tables"})
            assertRowsNet(executeDescribeNet("test", cmd),
                          row("has_all_types"),
                          row("\"Test\""),
                          row("users"),
                          row("users_mv"));

            testDescribeTable("test", "has_all_types", row(allTypesTable()));

            testDescribeTable("test", "\"Test\"", row(testTableOutput(true, true)),
                                              row(indexOutput("\"Test_col_idx\"", "\"Test\"", "col")),
                                              row(indexOutput("\"Test_val_idx\"", "\"Test\"", "val")));

            testDescribeTable("test", "users", row(userTableOutput()),
                                               row(indexOutput("myindex", "users", "age")));

            describeError("test", "DESCRIBE users2", "'users2' not found in keyspace 'test'");
            describeError("DESCRIBE test.users2", "'users2' not found in keyspace 'test'");

            // Test describe index

            testDescribeIndex("test", "myindex", row(indexOutput("myindex", "users", "age")));
            testDescribeIndex("test", "\"Test_col_idx\"", row(indexOutput("\"Test_col_idx\"", "\"Test\"", "col")));
            testDescribeIndex("test", "\"Test_val_idx\"", row(indexOutput("\"Test_val_idx\"", "\"Test\"", "val")));

            describeError("DESCRIBE test.myindex2", "'myindex2' not found in keyspace 'test'");
            describeError("test", "DESCRIBE myindex2", "'myindex2' not found in keyspace 'test'");

            // Test describe materialized view

            testDescribeMaterializedView("test", "users_by_state", row(usersByStateMvOutput()));
        }
        finally
        {
            execute("DROP KEYSPACE IF EXISTS test");
        }
    }

    private void testDescribeTable(String keyspace, String table, Object[]... rows) throws Throwable
    {
        for (String cmd : new String[]{"describe table " + keyspace + "." + table,
                                       "describe columnfamily " + keyspace + "." + table,
                                       "describe " + keyspace + "." + table})
        {
            assertRowsNet(executeDescribeNet(cmd), rows);
        }

        for (String cmd : new String[]{"describe table " + table,
                                       "describe columnfamily " + table,
                                       "describe " + table})
        {
            assertRowsNet(executeDescribeNet(keyspace, cmd), rows);
        }
    }

    private void testDescribeIndex(String keyspace, String index, Object[]... rows) throws Throwable
    {
        for (String cmd : new String[]{"describe index " + keyspace + "." + index,
                                       "describe " + keyspace + "." + index})
        {
            assertRowsNet(executeDescribeNet(cmd), rows);
        }

        for (String cmd : new String[]{"describe index " + index,
                                       "describe " + index})
        {
            assertRowsNet(executeDescribeNet(keyspace, cmd), rows);
        }
    }

    private void testDescribeMaterializedView(String keyspace, String view, Object[]... rows) throws Throwable
    {
        for (String cmd : new String[]{"describe materialized view " + keyspace + "." + view,
                                       "describe " + keyspace + "." + view})
        {
            assertRowsNet(executeDescribeNet(cmd), rows);
        }

        for (String cmd : new String[]{"describe materialized view " + view,
                                       "describe " + view})
        {
            assertRowsNet(executeDescribeNet(keyspace, cmd), rows);
        }
    }

    @Test
    public void testDescribeCluster() throws Throwable
    {
        assertRowsNet(executeDescribeNet("DESCRIBE CLUSTER"),
                     row("Cluster: Test Cluster\n" +
                         "Partitioner: ByteOrderedPartitioner\n" +
                         "Snitch: " + DatabaseDescriptor.getEndpointSnitch().getClass().getName() + "\n"));

        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        Token token = tokenMetadata.sortedTokens().get(0);
        InetAddressAndPort addressAndPort = tokenMetadata.getAllEndpoints().iterator().next();

        assertRowsNet(executeDescribeNet(KEYSPACE, "DESCRIBE CLUSTER"),
                      row("Cluster: Test Cluster\n" +
                          "Partitioner: ByteOrderedPartitioner\n" +
                          "Snitch: " + DatabaseDescriptor.getEndpointSnitch().getClass().getName() + "\n\n" +
                          "Range ownership:\n" +
                          String.format(" %39s", token) + "  [" + addressAndPort + "]\n"));
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
                      format("Keyspace '%s' not found", "foo"));
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

        String fOverloaded = createFunction(KEYSPACE, "int, ascii",
                                       "CREATE FUNCTION %s (input int, other_in ascii) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return \"Hello World\";'");
        createFunctionOverload(fOverloaded, "text, ascii",
                               "CREATE FUNCTION %s (input text, other_in ascii) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"Hello World\";'");

        assertRowsNet(executeDescribeNet("DESCRIBE FUNCTION " + fNonOverloaded),
                      row("CREATE FUNCTION " + fNonOverloaded + "()\n" +
                          "    CALLED ON NULL INPUT\n" +
                          "    RETURNS int\n" +
                          "    LANGUAGE java\n" +
                          "    AS $$throw new RuntimeException();$$;"));

        assertRowsNet(executeDescribeNet("DESCRIBE FUNCTION " + fOverloaded),
                      row("CREATE FUNCTION " + fOverloaded + "(input int, other_in ascii)\n" +
                              "    RETURNS NULL ON NULL INPUT\n" +
                              "    RETURNS text\n" +
                              "    LANGUAGE java\n" +
                              "    AS $$return \"Hello World\";$$;"),
                      row("CREATE FUNCTION " + fOverloaded + "(input text, other_in ascii)\n" +
                              "    RETURNS NULL ON NULL INPUT\n" +
                              "    RETURNS text\n" +
                              "    LANGUAGE java\n" +
                              "    AS $$return \"Hello World\";$$;"));

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

        assertRowsNet(executeDescribeNet("DESCRIBE AGGREGATE " + aNonDeterministic),
                      row("CREATE AGGREGATE " + aNonDeterministic + "(int)\n" +
                          "    SFUNC " + shortFunctionName(fIntState) + "\n" +
                          "    STYPE int\n" +
                          "    INITCOND 42;"));
        assertRowsNet(executeDescribeNet("DESCRIBE AGGREGATE " + aDeterministic),
                      row("CREATE AGGREGATE " + aDeterministic + "(int)\n" +
                                         "    SFUNC " + shortFunctionName(fIntState) + "\n" +
                                         "    STYPE int\n" +
                                         "    FINALFUNC " + shortFunctionName(fFinal) + ";"));
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
        String table = createTable("CREATE TABLE %s (key int PRIMARY KEY) WITH compaction = {'class': 'LeveledCompactionStrategy'}");

        String output = executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + table).all().get(0).getString(0);

        execute("DROP TABLE %s");

        executeNet(output);

        String output2 = executeDescribeNet(KEYSPACE, "DESCRIBE TABLE " + table).all().get(0).getString(0);
        assertEquals(output, output2);

        execute("INSERT INTO %s (key) VALUES (1)");
    }

    private static String allTypesTable()
    {
        return "CREATE TABLE test.has_all_types (\n" +
               "    num int,\n" +
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
               "    setcol set<tinyint>,\n" +
               "    PRIMARY KEY (num)\n" +
               ") WITH bloom_filter_fp_chance = 0.01\n" +
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
               "    AND speculative_retry = '99p'\n" +
               "    AND additional_write_policy = '99p';";
    }

    private static String usersByStateMvOutput()
    {
        return "CREATE MATERIALIZED VIEW test.users_by_state AS\n" +
               "    SELECT *\n" +
               "    FROM test.users_mv\n" +
               "    WHERE state IS NOT NULL AND username IS NOT NULL\n" +
               "    PRIMARY KEY (state, username)\n" +
               " WITH CLUSTERING ORDER BY (username ASC)\n" +
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
               "    AND speculative_retry = '99p'\n" +
               "    AND additional_write_policy = '99p';";
    }

    private static String indexOutput(String index, String table, String col)
    {
        return format("CREATE INDEX %s ON %s.%s (%s);", index, "test", table, col);
    }

    private static String usersMvTableOutput()
    {
        return "CREATE TABLE test.users_mv (\n" +
               "    username text,\n" +
               "    birth_year bigint,\n" +
               "    gender text,\n" +
               "    password text,\n" +
               "    session_token text,\n" +
               "    state text,\n" +
               "    PRIMARY KEY (username)\n" +
               ") WITH bloom_filter_fp_chance = 0.01\n" +
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
               "    AND speculative_retry = '99p'\n" +
               "    AND additional_write_policy = '99p';";
    }

    private static String userTableOutput()
    {
        return "CREATE TABLE test.users (\n" +
               "    userid text,\n" +
               "    age int,\n" +
               "    firstname text,\n" +
               "    lastname text,\n" +
               "    PRIMARY KEY (userid)\n" +
               ") WITH bloom_filter_fp_chance = 0.01\n" +
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
               "    AND speculative_retry = '99p'\n" +
               "    AND additional_write_policy = '99p';";
    }

    private static String testTableOutput(boolean hasVal, boolean withParams)
    {
        String s = "CREATE TABLE test.\"Test\" (\n" +
                   "    id int,\n" +
                   "    col int,\n" +
                   (hasVal ? "    val text,\n" : "") +
                   "    PRIMARY KEY (id, col)\n" +
                   ") WITH ";

        if (withParams)
        {
            s +=
               "CLUSTERING ORDER BY (col ASC)\n" +
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
               "    AND speculative_retry = '99p'\n" +
               "    AND additional_write_policy = '99p'";
        }
        return s + ';';
    }

    private static String keyspaceOutput()
    {
        return "CREATE KEYSPACE test\n" +
               "    WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}\n" +
               "     AND durable_writes = true;";
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
