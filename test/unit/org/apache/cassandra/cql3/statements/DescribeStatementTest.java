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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DescribeStatementTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        VirtualKeyspaceRegistry.instance.register(SystemViewsKeyspace.instance);
    }

    @Test
    public void testDescribeKeyspacesMocked() throws Throwable
    {
        DescribeStatement.SchemaAccess oldSchema = DescribeStatement.schema;
        try
        {
            DescribeStatement.schema = () -> new DescribeStatement.SchemaSnapshot()
            {
                @Override
                public ByteBuffer schemaHash()
                {
                    return ByteBuffer.allocate(0);
                }

                @Override
                public Stream<String> rawKeyspaceNames()
                {
                    return Stream.of("abc", "def", "CqlshTests_Cl2_EF");
                }

                @Override
                public KeyspaceMetadata keyspaceMetadata(String ks)
                {
                    return null;
                }
            };

            assertThat(executeDescribe("DESC KEYSPACES"),
                       containsString("\"CqlshTests_Cl2_EF\"  abc  def\n"));
        }
        finally
        {
            DescribeStatement.schema = oldSchema;
        }
    }

    @Test
    public void testSchemaChangeDuringPaging()
    {
        DescribeStatement.SchemaAccess oldSchema = DescribeStatement.schema;
        try
        {
            AtomicReference<ByteBuffer> schemaHash = new AtomicReference<>(ByteBuffer.allocate(0));

            DescribeStatement.schema = () -> new DescribeStatement.SchemaSnapshot()
            {
                @Override
                public ByteBuffer schemaHash()
                {
                    return schemaHash.get().duplicate();
                }

                @Override
                public Stream<String> rawKeyspaceNames()
                {
                    return Stream.of("abc", "def", "def");
                }

                @Override
                public KeyspaceMetadata keyspaceMetadata(String ks)
                {
                    return null;
                }
            };

            SimpleStatement stmt = new SimpleStatement("DESCRIBE KEYSPACES");
            stmt.setFetchSize(1);
            ResultSet rs = executeNet(ProtocolVersion.CURRENT, stmt);
            Iterator<Row> iter = rs.iterator();
            assertTrue(iter.hasNext());
            iter.next();

            schemaHash.set(ByteBufferUtil.bytes(1));
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
        finally
        {
            DescribeStatement.schema = oldSchema;
        }
    }

    /**
     * Much of this test method has been ported from {@code ported from cqlsh_tests.cqlsh_tests.TestCqlsh#test_describe*}
     * methods.
     */
    @Test
    public void testDescribe() throws Throwable
    {
        try
        {
            // from test_describe()
            execute("CREATE KEYSPACE test WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
            execute("CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int);");
            execute("CREATE INDEX myindex ON test.users (age);");
            execute("CREATE INDEX \"QuotedNameIndex\" on test.users (firstName);");
            execute("CREATE TABLE test.test (id int, col int, val text, PRIMARY KEY(id, col));");
            execute("CREATE INDEX ON test.test (col);");
            execute("CREATE INDEX ON test.test (val)");
            // from test_describe_mv()
            execute("CREATE TABLE test.users_mv (username varchar, password varchar, gender varchar, session_token varchar, " +
                    "state varchar, birth_year bigint, PRIMARY KEY (username));");
            execute("CREATE MATERIALIZED VIEW test.users_by_state AS SELECT * FROM test.users_mv " +
                    "WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)");
            execute(allTypesTable());

            // actual tests follow

            testDescribeCluster();

            testDescribeFullSchema();

            testDescribeKeyspaces();

            testDescribeTables();

            testDescribeTable();

            assertThat(executeDescribe("DESCRIBE KEYSPACE test"),
                       allOf(containsString(keyspaceOutput()),
                             containsString("CREATE TABLE")));
            assertThat(executeDescribe("DESCRIBE ONLY KEYSPACE test"),
                       allOf(containsString(keyspaceOutput()),
                             not(containsString("CREATE TABLE"))));

            // tests as in cqlsh_tests.cqlsh_tests.TestCqlsh#test_describe

            Iterable<Matcher<? super String>> keyspaceOutput =
                    Arrays.asList(containsString(keyspaceOutput()),
                                  containsString(testTableOutput(true, true)),
                                  containsString(indexOutput("test_col_idx", "test", "col")),
                                  containsString(indexOutput("test_val_idx", "test", "val")),
                                  containsString(userTableOutput()),
                                  containsString(indexOutput("\"QuotedNameIndex\"", "users", "firstname")),
                                  containsString(indexOutput("myindex", "users", "age")));
            Iterable<Matcher<? super String>> testTableOutput =
                    Arrays.asList(containsString(testTableOutput(true, true)),
                                  containsString(indexOutput("test_col_idx", "test", "col")),
                                  containsString(indexOutput("test_val_idx", "test", "val")));
            Iterable<Matcher<? super String>> usersTableOutput =
                    Arrays.asList(containsString(userTableOutput()),
                                  containsString(indexOutput("\"QuotedNameIndex\"", "users", "firstname")),
                                  containsString(indexOutput("myindex", "users", "age")));

            assertThat(executeDescribe("DESCRIBE test"), allOf(keyspaceOutput));
            describeError("DESCRIBE test2", "Keyspace 'test2' not found");
            assertThat(executeDescribe("test", "DESCRIBE KEYSPACE"), allOf(keyspaceOutput));

            // Describe table
            assertThat(executeDescribe("DESCRIBE TABLE test.test"), allOf(testTableOutput));
            assertThat(executeDescribe("DESCRIBE TABLE test.users"), allOf(usersTableOutput));
            assertThat(executeDescribe("DESCRIBE test.test"), allOf(testTableOutput));
            assertThat(executeDescribe("DESCRIBE test.users"), allOf(usersTableOutput));
            describeError("DESCRIBE test.users2", "'users2' not found in keyspace 'test'");
            assertThat(executeDescribe("test", "DESCRIBE TABLE test"), allOf(testTableOutput));
            assertThat(executeDescribe("test", "DESCRIBE COLUMNFAMILY users"), allOf(usersTableOutput));
            assertThat(executeDescribe("test", "DESCRIBE test"), allOf(keyspaceOutput));
            assertThat(executeDescribe("test", "DESCRIBE users"), allOf(usersTableOutput));
            describeError("test", "DESCRIBE users2", "'users2' not found in keyspace 'test'");

            // Describe index
            assertThat(executeDescribe("DESCRIBE INDEX test.myindex"), containsString(indexOutput("myindex", "users", "age")));
            assertThat(executeDescribe("DESCRIBE INDEX test.test_col_idx"), containsString(indexOutput("test_col_idx", "test", "col")));
            assertThat(executeDescribe("DESCRIBE INDEX test.test_val_idx"), containsString(indexOutput("test_val_idx", "test", "val")));
            assertThat(executeDescribe("DESCRIBE test.myindex"), containsString(indexOutput("myindex", "users", "age")));
            assertThat(executeDescribe("DESCRIBE test.test_col_idx"), containsString(indexOutput("test_col_idx", "test", "col")));
            assertThat(executeDescribe("DESCRIBE test.test_val_idx"), containsString(indexOutput("test_val_idx", "test", "val")));
            describeError("DESCRIBE test.myindex2", "'myindex2' not found in keyspace 'test'");
            assertThat(executeDescribe("test", "DESCRIBE INDEX myindex"), containsString(indexOutput("myindex", "users", "age")));
            assertThat(executeDescribe("test", "DESCRIBE INDEX test_col_idx"), containsString(indexOutput("test_col_idx", "test", "col")));
            assertThat(executeDescribe("test", "DESCRIBE INDEX test_val_idx"), containsString(indexOutput("test_val_idx", "test", "val")));
            assertThat(executeDescribe("test", "DESCRIBE myindex"), containsString(indexOutput("myindex", "users", "age")));
            assertThat(executeDescribe("test", "DESCRIBE test_col_idx"), containsString(indexOutput("test_col_idx", "test", "col")));
            assertThat(executeDescribe("test", "DESCRIBE test_val_idx"), containsString(indexOutput("test_val_idx", "test", "val")));
            describeError("test", "DESCRIBE myindex2", "'myindex2' not found in keyspace 'test'");

            // Drop table and recreate
            execute("DROP TABLE test.users");
            describeError("DESCRIBE test.users", "'users' not found in keyspace 'test'");
            describeError("DESCRIBE test.myindex", "'myindex' not found in keyspace 'test'");
            execute("CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int)");
            execute("CREATE INDEX myindex ON test.users (age);");
            execute("CREATE INDEX \"QuotedNameIndex\" on test.users (firstname)");
            assertThat(executeDescribe("DESCRIBE test.users"), allOf(usersTableOutput));
            assertThat(executeDescribe("DESCRIBE test.myindex"), containsString(indexOutput("myindex", "users", "age")));

            // Drop index and recreate
            execute("DROP INDEX test.myindex");
            describeError("DESCRIBE test.myindex", "'myindex' not found in keyspace 'test'");
            execute("CREATE INDEX myindex ON test.users (age)");
            assertThat(executeDescribe("DESCRIBE INDEX test.myindex"), containsString(indexOutput("myindex", "users", "age")));
            execute("DROP INDEX test.\"QuotedNameIndex\"");
            describeError("DESCRIBE test.\"QuotedNameIndex\"", "'QuotedNameIndex' not found in keyspace 'test'");
            execute("CREATE INDEX \"QuotedNameIndex\" ON test.users (firstname)");
            assertThat(executeDescribe("DESCRIBE INDEX test.\"QuotedNameIndex\""), containsString(indexOutput("\"QuotedNameIndex\"", "users", "firstname")));

            // Alter table. Renaming indexed columns is not allowed, and since 3.0 neither is dropping them
            // Prior to 3.0 the index would have been automatically dropped, but now we need to explicitly do that.
            execute("DROP INDEX test.test_val_idx");
            execute("ALTER TABLE test.test DROP val");
            assertThat(executeDescribe("DESCRIBE test.test"), containsString(testTableOutput(false, true)));
            // A dropped column is mentioned 2 times:
            // - in the CREATE TABLE to be able to issue a ...
            // - proper ALTER TABLE DROP USING TIMESTAMP
            assertThat(executeDescribe("DESCRIBE test.test WITH INTERNALS"),
                       allOf(containsString(testTableOutput(true, false)),
                             containsString("ALTER TABLE test.test DROP val "),
                             not(containsString("ALTER TABLE test.test ADD val text;"))));
            describeError("DESCRIBE test.test_val_idx", "'test_val_idx' not found in keyspace 'test'");
            execute("ALTER TABLE test.test ADD val text");
            assertThat(executeDescribe("DESCRIBE test.test"), containsString(testTableOutput(true, true)));
            // A re-created column is mentioned 3 times:
            // - in the CREATE TABLE to be able to issue a ...
            // - proper ALTER TABLE DROP USING TIMESTAMP and finally ...
            // - an ALTER TABLE ADD
            assertThat(executeDescribe("DESCRIBE test.test WITH INTERNALS"),
                       allOf(containsString("ALTER TABLE test.test DROP val "),
                             containsString("ALTER TABLE test.test ADD val text;")));
            describeError("DESCRIBE test.test_val_idx", "'test_val_idx' not found in keyspace 'test'");

            // port of test_describe_describes_non_default_compaction_parameters()

            execute("CREATE TABLE test.tab (key int PRIMARY KEY ) " +
                    "WITH compaction = {'class': 'SizeTieredCompactionStrategy'," +
                    "'min_threshold': 10, 'max_threshold': 100 }");
            assertThat(executeDescribe("DESCRIBE test.tab"),
                       allOf(containsString("'min_threshold': '10'"),
                             containsString("'max_threshold': '100'")));

            // port of test_describe_on_non_reserved_keywords()

            execute("CREATE TABLE test.map (key int PRIMARY KEY, val text)");
            assertThat(executeDescribe("DESCRIBE test.map"),
                       containsString("CREATE TABLE test.map ("));
            assertThat(executeDescribe("test", "DESCRIBE map"),
                       containsString("CREATE TABLE test.map ("));

            // port of test_describe_mv()

            assertThat(executeDescribe("DESCRIBE KEYSPACE test"), containsString("users_by_state"));
            assertThat(executeDescribe("DESCRIBE MATERIALIZED VIEW test.users_by_state"), containsString(usersByStateMvOutput()));
            assertThat(executeDescribe("DESCRIBE test.users_by_state"), containsString(usersByStateMvOutput()));
            assertThat(executeDescribe("test", "DESCRIBE MATERIALIZED VIEW test.users_by_state"), containsString(usersByStateMvOutput()));
            assertThat(executeDescribe("test", "DESCRIBE MATERIALIZED VIEW users_by_state"), containsString(usersByStateMvOutput()));
            assertThat(executeDescribe("test", "DESCRIBE users_by_state"), containsString(usersByStateMvOutput()));

            // test quotes
            assertThat(executeDescribe("test", "DESCRIBE MATERIALIZED VIEW \"users_by_state\""), containsString(usersByStateMvOutput()));
            assertThat(executeDescribe("test", "DESCRIBE \"users_by_state\""), containsString(usersByStateMvOutput()));
        }
        finally
        {
            execute("DROP KEYSPACE IF EXISTS test");
        }
    }

    private void testDescribeTable() throws Throwable
    {
        for (String cmd : new String[]{"describe table test.has_all_types",
                                       "describe columnfamily test.has_all_types",
                                       "describe test.has_all_types"})
            assertThat(executeDescribe(cmd),
                       containsString(allTypesTable()));
        for (String cmd : new String[]{"describe table has_all_types",
                                       "describe columnfamily has_all_types",
                                       "describe has_all_types"})
            assertThat(executeDescribe("test", cmd),
                       containsString(allTypesTable()));
    }

    private void testDescribeTables() throws Throwable
    {
        for (String cmd : new String[]{"describe TABLES",
                                       "DESC tables"})
            assertThat(executeDescribe(cmd),
                       allOf(matchesRegex(Pattern.compile(".*" +
                                                          "\n" +
                                                          "Keyspace test\n" +
                                                          "-------------\n" +
                                                          ".*",
                                                          Pattern.DOTALL)),
                             matchesRegex(Pattern.compile(".*" +
                                                          "\n" +
                                                          "Keyspace system\n" +
                                                          "---------------\n" +
                                                          ".*",
                                                          Pattern.DOTALL))));
        assertThat(executeDescribe("test", "describe TABLES"),
                   allOf(containsString("\nhas_all_types  test  users  users_mv\n"),
                         not(containsString("Keyspace test\n")),
                         not(containsString("Keyspace system\n"))));
    }

    private void testDescribeKeyspaces() throws Throwable
    {
        assertThat(executeDescribe("DESCRIBE KEYSPACES"),
                   allOf(containsString("test"),
                         containsString("system")));
    }

    private void testDescribeFullSchema() throws Throwable
    {
        assertThat(executeDescribe("DESCRIBE FULL SCHEMA"),
                   allOf(containsString("\nCREATE KEYSPACE system\n" +
                                        "    WITH replication = {'class': 'org.apache.cassandra.locator.LocalStrategy'}\n" +
                                        "     AND durable_writes = true;\n"),
                         containsString("\n// VIRTUAL KEYSPACE system_views;\n")));
    }

    private void testDescribeCluster() throws Throwable
    {
        assertThat(executeDescribe("DESCRIBE CLUSTER"),
                   allOf(matchesRegex(Pattern.compile(".*" +
                                                      "Cluster: [a-zA-Z0-9 ]+\n" +
                                                      "Partitioner: [a-zA-Z0-9.]+\n" +
                                                      "Snitch: [a-zA-Z0-9.$]+\n" +
                                                      ".*",
                                                      Pattern.DOTALL)),
                         not(containsString("Range ownership:"))));
        assertThat(executeDescribe("test", "DESCRIBE CLUSTER"),
                   matchesRegex(Pattern.compile(".*" +
                                                "Cluster: [a-zA-Z0-9 ]+\n" +
                                                "Partitioner: [a-zA-Z0-9.]+\n" +
                                                "Snitch: [a-zA-Z0-9.$]+\n" +
                                                "\n" +
                                                "Range ownership:\n" +
                                                "([ ]+[^ ]+[ ][ ]\\[(\\d+[.]){3}\\d+:\\d+]\n)+" +
                                                "\n.*",
                                                Pattern.DOTALL)));
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

        String fOverloaded = createFunction(KEYSPACE, "int",
                                       "CREATE FUNCTION %s (input int, other_in ascii) " +
                                       "RETURNS NULL ON NULL INPUT " +
                                       "RETURNS text " +
                                       "LANGUAGE java " +
                                       "AS 'return \"Hello World\";'");
        createFunctionOverload(fOverloaded, "text",
                               "CREATE FUNCTION %s (input text, other_in ascii) " +
                               "RETURNS NULL ON NULL INPUT " +
                               "RETURNS text " +
                               "LANGUAGE java " +
                               "AS 'return \"Hello World\";'");

        assertThat(executeDescribe(format("DESCRIBE FUNCTION %s", fNonOverloaded)),
                   containsString(format("CREATE FUNCTION %s()\n" +
                                         "    CALLED ON NULL INPUT\n" +
                                         "    RETURNS int\n" +
                                         "    LANGUAGE java\n" +
                                         "    AS $$throw new RuntimeException();$$;",
                                         fNonOverloaded)));

        assertThat(executeDescribe(format("DESCRIBE FUNCTION %s", fOverloaded)),
                   allOf(containsString(format("CREATE FUNCTION %s(input int, other_in ascii)\n" +
                                               "    RETURNS NULL ON NULL INPUT\n" +
                                               "    RETURNS text\n" +
                                               "    LANGUAGE java\n" +
                                               "    AS $$return \"Hello World\";$$",
                                               fOverloaded)),
                         containsString(format("CREATE FUNCTION %s(input text, other_in ascii)\n" +
                                               "    RETURNS NULL ON NULL INPUT\n" +
                                               "    RETURNS text\n" +
                                               "    LANGUAGE java\n" +
                                               "    AS $$return \"Hello World\";$$",
                                               fOverloaded))));

        String fIntState = createFunction(KEYSPACE,
                                          "int",
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

        assertThat(executeDescribe(format("DESCRIBE AGGREGATE %s", aNonDeterministic)),
                   containsString(format("CREATE AGGREGATE %s(int)\n" +
                                         "    SFUNC %s\n" +
                                         "    STYPE int\n" +
                                         "    INITCOND 42;",
                                         aNonDeterministic,
                                         shortFunctionName(fIntState))));
        assertThat(executeDescribe(format("DESCRIBE AGGREGATE %s", aDeterministic)),
                   containsString(format("CREATE AGGREGATE %s(int)\n" +
                                         "    SFUNC %s\n" +
                                         "    STYPE int\n" +
                                         "    FINALFUNC %s;",
                                         aDeterministic,
                                         shortFunctionName(fIntState),
                                         shortFunctionName(fFinal))));
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
        try
        {
            execute("CREATE KEYSPACE test_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};");
            execute("CREATE TABLE test_ks.lcs_describe (key int PRIMARY KEY) WITH compaction = {'class': 'LeveledCompactionStrategy'}");

            String output = executeDescribe("DESCRIBE TABLE test_ks.lcs_describe");

            execute("DROP TABLE test_ks.lcs_describe");

            executeNet("USE test_ks");
            executeNet(output);

            String output2 = executeDescribe("DESCRIBE TABLE test_ks.lcs_describe");

            executeNet("INSERT INTO lcs_describe (key) VALUES (1)");

            assertEquals(output, output2);
        }
        finally
        {
            execute("DROP KEYSPACE IF EXISTS test_ks");
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

    private static String userTableOutput()
    {
        return "CREATE TABLE test.users (\n" +
               "    userid text PRIMARY KEY,\n" +
               "    age int,\n" +
               "    firstname text,\n" +
               "    lastname text\n" +
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
               "    AND additional_write_policy = '99p';\n";
    }

    private static String testTableOutput(boolean hasVal, boolean withParams)
    {
        String s = "CREATE TABLE test.test (\n" +
                   "    id int,\n" +
                   "    col int,\n" +
                   (hasVal ? "    val text,\n" : "") +
                   "    PRIMARY KEY (id, col)\n" +
                   ") WITH ";
        if (!withParams)
            return s;
        return s +
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

    private static String keyspaceOutput()
    {
        return "CREATE KEYSPACE test\n" +
               "    WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}\n" +
               "     AND durable_writes = true;\n";
    }

    private void describeError(String cql, String msg) throws Throwable
    {
        describeError(null, cql, msg);
    }

    private void describeError(String useKs, String cql, String msg) throws Throwable
    {
        try
        {
            executeDescribe(useKs, cql);
            fail();
        }
        catch (InvalidQueryException e)
        {
            assertThat(e.getMessage(), containsString(msg));
        }
    }

    private String executeDescribe(String cql) throws Throwable
    {
        return executeDescribe(null, cql);
    }

    private String executeDescribe(String useKs, String cql) throws Throwable
    {
        // We're using a trick here to distinguish driver sessions with a "USE keyspace" and without:
        // As different ProtocolVersions use different driver instances, we use different ProtocolVersions
        // for the with and without "USE keyspace" cases.

        ProtocolVersion v = useKs != null ? ProtocolVersion.V5 : ProtocolVersion.CURRENT;

        if (useKs != null)
            executeNet(v, "USE " + useKs);

        SimpleStatement stmt = new SimpleStatement(cql);
        stmt.setFetchSize(3);
        ResultSet rs = executeNet(v, stmt);
        StringBuilder sb = new StringBuilder();
        for (Row r : rs)
        {
            sb.append(r.getString("schema_part"));
        }
        return sb.toString();
    }

    // This can be replaced once a hamcrest version w/ support for regex is there (hamcrest 2.1 has)
    private static Matcher<String> matchesRegex(Pattern pattern) {
        return new MatchesRegex(pattern);
    }
    private static class MatchesRegex extends TypeSafeMatcher<String>
    {
        private final Pattern pattern;

        MatchesRegex(Pattern pattern) {
            this.pattern = pattern;
        }

        @Override
        protected boolean matchesSafely(String item) {
            return pattern.matcher(item).matches();
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a string matching the pattern '" + pattern + "'");
        }
    }

    /**
     * Tests a {@code DESCRIBE SCHEMA} on 200 keyspaces with 200 tables in each keyspace (i.e. 40,000 tables).
     */
    @Test
    public void testSuperLargeSchema() throws Throwable
    {
        DescribeStatement.SchemaAccess oldSchema = DescribeStatement.schema;
        try
        {
            DescribeStatement.schema = new DescribeStatement.SchemaAccess()
            {
                final DescribeStatement.SchemaSnapshot snapshot = new DescribeStatement.SchemaSnapshot()
                {
                    Map<String, KeyspaceMetadata> ksmMap = new HashMap<>();

                    @Override
                    public ByteBuffer schemaHash()
                    {
                        return ByteBuffer.allocate(0);
                    }

                    @Override
                    public Stream<String> rawKeyspaceNames()
                    {
                        return IntStream.range(1000, 1050)
                                        .mapToObj(i -> format("keyspace_%05d", i));
                    }

                    @Override
                    public KeyspaceMetadata keyspaceMetadata(String ks)
                    {
                        return ksmMap.computeIfAbsent(ks, k ->
                        {
                            Tables.Builder tables = Tables.builder();
                            for (int i = 1; i <= 200; i++)
                            {
                                TableMetadata.Builder tmb = TableMetadata.builder(k, format("table_%05d", i))
                                                                         .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                                         .addClusteringColumn("ck", IntegerType.instance);
                                for (int c = 1; c <= 50; c++)
                                    tmb.addRegularColumn(format("col_%03d", c), UTF8Type.instance);
                                tables.add(tmb.build());
                            }
                            return KeyspaceMetadata.create(k, KeyspaceParams.simple(1), tables.build());
                        });
                    }
                };

                @Override
                public DescribeStatement.SchemaSnapshot snapshot()
                {
                    return snapshot;
                }
            };

            ResultSet rs = executeNet("DESCRIBE SCHEMA");
            long rows = 0;
            long chars = 0;
            for (Row r : rs)
            {
                String snippet = r.getString("schema_part");
                chars += snippet.length();
                rows ++;
                if ((rows % 1000) == 0)
                    System.out.println(format("Got %d rows / %d chars ...%n", rows, chars));
            }
            System.out.println(format("Received %d rows / %d chars for absurdly large schema", rows, chars));
        }
        finally
        {
            DescribeStatement.schema = oldSchema;
        }
    }
}
