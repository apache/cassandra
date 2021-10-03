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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.SyntaxError;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.index.StubIndex;
import org.apache.cassandra.serializers.BooleanSerializer;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PreparedStatementsTest extends CQLTester
{
    private static final String KEYSPACE = "prepared_stmt_cleanup";
    private static final String createKsStatement = "CREATE KEYSPACE " + KEYSPACE +
                                                    " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    private static final String dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE;

    @Before
    public void setup()
    {
        requireNetwork();
    }

    @Test
    public void testInvalidatePreparedStatementsOnDrop()
    {
        Session session = sessionNet(ProtocolVersion.V5);
        session.execute(dropKsStatement);
        session.execute(createKsStatement);

        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (id int PRIMARY KEY, cid int, val text);";
        String dropTableStatement = "DROP TABLE IF EXISTS " + KEYSPACE + ".qp_cleanup;";

        session.execute(createTableStatement);

        PreparedStatement prepared = session.prepare("INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?)");
        PreparedStatement preparedBatch = session.prepare("BEGIN BATCH " +
                                                          "INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?);" +
                                                          "APPLY BATCH;");
        session.execute(dropTableStatement);
        session.execute(createTableStatement);
        session.execute(prepared.bind(1, 1, "value"));
        session.execute(preparedBatch.bind(2, 2, "value2"));

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        // The driver will get a response about the prepared statement being invalid, causing it to transparently
        // re-prepare the statement.  We'll rely on the fact that we get no errors while executing this to show that
        // the statements have been invalidated.
        session.execute(prepared.bind(1, 1, "value"));
        session.execute(preparedBatch.bind(2, 2, "value2"));
        session.execute(dropKsStatement);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterV5()
    {
        testInvalidatePreparedStatementOnAlter(ProtocolVersion.V5, true);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterV4()
    {
        testInvalidatePreparedStatementOnAlter(ProtocolVersion.V4, false);
    }

    private void testInvalidatePreparedStatementOnAlter(ProtocolVersion version, boolean supportsMetadataChange)
    {
        Session session = sessionNet(version);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (a int PRIMARY KEY, b int, c int);";
        String alterTableStatement = "ALTER TABLE " + KEYSPACE + ".qp_cleanup ADD d int;";

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        PreparedStatement preparedSelect = session.prepare("SELECT * FROM " + KEYSPACE + ".qp_cleanup");
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        1, 2, 3);
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        2, 3, 4);

        assertRowsNet(session.execute(preparedSelect.bind()),
                      row(1, 2, 3),
                      row(2, 3, 4));

        session.execute(alterTableStatement);
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c, d) VALUES (?, ?, ?, ?);",
                        3, 4, 5, 6);

        ResultSet rs;
        if (supportsMetadataChange)
        {
            rs = session.execute(preparedSelect.bind());
            assertRowsNet(version,
                          rs,
                          row(1, 2, 3, null),
                          row(2, 3, 4, null),
                          row(3, 4, 5, 6));
            assertEquals(rs.getColumnDefinitions().size(), 4);
        }
        else
        {
            rs = session.execute(preparedSelect.bind());
            assertRowsNet(rs,
                          row(1, 2, 3),
                          row(2, 3, 4),
                          row(3, 4, 5));
            assertEquals(rs.getColumnDefinitions().size(), 3);
        }

        session.execute(dropKsStatement);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterUnchangedMetadataV4()
    {
        testInvalidatePreparedStatementOnAlterUnchangedMetadata(ProtocolVersion.V4);
    }

    @Test
    public void testInvalidatePreparedStatementOnAlterUnchangedMetadataV5()
    {
        testInvalidatePreparedStatementOnAlterUnchangedMetadata(ProtocolVersion.V5);
    }

    private void testInvalidatePreparedStatementOnAlterUnchangedMetadata(ProtocolVersion version)
    {
        Session session = sessionNet(version);
        String createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (a int PRIMARY KEY, b int, c int);";
        String alterTableStatement = "ALTER TABLE " + KEYSPACE + ".qp_cleanup ADD d int;";

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        session.execute(createTableStatement);

        PreparedStatement preparedSelect = session.prepare("SELECT a, b, c FROM " + KEYSPACE + ".qp_cleanup");
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        1, 2, 3);
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (?, ?, ?);",
                        2, 3, 4);

        ResultSet rs = session.execute(preparedSelect.bind());

        assertRowsNet(rs,
                      row(1, 2, 3),
                      row(2, 3, 4));
        assertEquals(rs.getColumnDefinitions().size(), 3);

        session.execute(alterTableStatement);
        session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c, d) VALUES (?, ?, ?, ?);",
                        3, 4, 5, 6);

        rs = session.execute(preparedSelect.bind());
        assertRowsNet(rs,
                      row(1, 2, 3),
                      row(2, 3, 4),
                      row(3, 4, 5));
        assertEquals(rs.getColumnDefinitions().size(), 3);

        session.execute(dropKsStatement);
    }

    @Test
    public void testStatementRePreparationOnReconnect()
    {
        Session session = sessionNet(ProtocolVersion.V5);
        session.execute("USE " + keyspace());

        session.execute(dropKsStatement);
        session.execute(createKsStatement);

        createTable("CREATE TABLE %s (id int PRIMARY KEY, cid int, val text);");


        String insertCQL = "INSERT INTO " + currentTable() + " (id, cid, val) VALUES (?, ?, ?)";
        String selectCQL = "Select * from " + currentTable() + " where id = ?";

        PreparedStatement preparedInsert = session.prepare(insertCQL);
        PreparedStatement preparedSelect = session.prepare(selectCQL);

        session.execute(preparedInsert.bind(1, 1, "value"));
        assertEquals(1, session.execute(preparedSelect.bind(1)).all().size());

        try (Cluster newCluster = Cluster.builder()
                                 .addContactPoints(nativeAddr)
                                 .withClusterName("Test Cluster")
                                 .withPort(nativePort)
                                 .withoutJMXReporting()
                                 .allowBetaProtocolVersion()
                                 .build())
        {
            try (Session newSession = newCluster.connect())
            {
                newSession.execute("USE " + keyspace());
                preparedInsert = newSession.prepare(insertCQL);
                preparedSelect = newSession.prepare(selectCQL);
                newSession.execute(preparedInsert.bind(1, 1, "value"));

                assertEquals(1, newSession.execute(preparedSelect.bind(1)).all().size());
            }
        }
    }

    @Test
    public void prepareAndExecuteWithCustomExpressions() throws Throwable
    {
        Session session = sessionNet(ProtocolVersion.V5);

        session.execute(dropKsStatement);
        session.execute(createKsStatement);
        String table = "custom_expr_test";
        String index = "custom_index";

        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int PRIMARY KEY, cid int, val text);",
                                      KEYSPACE, table));
        session.execute(String.format("CREATE CUSTOM INDEX %s ON %s.%s(val) USING '%s'",
                                      index, KEYSPACE, table, StubIndex.class.getName()));
        session.execute(String.format("INSERT INTO %s.%s(id, cid, val) VALUES (0, 0, 'test')", KEYSPACE, table));

        PreparedStatement prepared1 = session.prepare(String.format("SELECT * FROM %s.%s WHERE expr(%s, 'foo')",
                                                                    KEYSPACE, table, index));
        assertEquals(1, session.execute(prepared1.bind()).all().size());

        PreparedStatement prepared2 = session.prepare(String.format("SELECT * FROM %s.%s WHERE expr(%s, ?)",
                                                                    KEYSPACE, table, index));
        assertEquals(1, session.execute(prepared2.bind("foo bar baz")).all().size());

        try
        {
            session.prepare(String.format("SELECT * FROM %s.%s WHERE expr(?, 'foo bar baz')", KEYSPACE, table));
            fail("Expected syntax exception, but none was thrown");
        }
        catch(SyntaxError e)
        {
            assertEquals("Bind variables cannot be used for index names", e.getMessage());
        }
    }

    @Test
    public void testMetadataFlagsWithLWTs() throws Throwable
    {
        // Verify the behavior of CASSANDRA-10786 (result metadata IDs) on the protocol level.
        // Tests are against an LWT statement and a "regular" SELECT statement.
        // The fundamental difference between a SELECT and an LWT statement is that the result metadata
        // of an LWT can change between invocations - therefore we always return the resultset metadata
        // for LWTs. For "normal" SELECTs, the resultset metadata can only change when DDLs happen
        // (aka the famous prepared 'SELECT * FROM ks.tab' stops working after the schema of that table
        // changes). In those cases, the Result.Rows message contains a METADATA_CHANGED flag to tell
        // clients that the cached metadata for this statement has changed and is included in the result,
        // whereas the resultset metadata is omitted, if the metadata ID sent with the EXECUTE message
        // matches the one for the (current) schema.
        // Note: this test does not cover all aspects of 10786 (yet) - it was intended to test the
        // changes for CASSANDRA-13992.

        createTable("CREATE TABLE %s (pk int, v1 int, v2 int, PRIMARY KEY (pk))");
        execute("INSERT INTO %s (pk, v1, v2) VALUES (1,1,1)");

        try (SimpleClient simpleClient = newSimpleClient(ProtocolVersion.BETA.orElse(ProtocolVersion.CURRENT)))
        {
            ResultMessage.Prepared prepUpdate = simpleClient.prepare(String.format("UPDATE %s.%s SET v1 = ?, v2 = ? WHERE pk = 1 IF v1 = ?",
                                                                                   keyspace(), currentTable()));
            ResultMessage.Prepared prepSelect = simpleClient.prepare(String.format("SELECT * FROM %s.%s WHERE pk = ?",
                                                                                   keyspace(), currentTable()));

            // This is a _successful_ LWT update
            verifyMetadataFlagsWithLWTsUpdate(simpleClient,
                                              prepUpdate,
                                              Arrays.asList(Int32Serializer.instance.serialize(10),
                                                            Int32Serializer.instance.serialize(20),
                                                            Int32Serializer.instance.serialize(1)),
                                              Arrays.asList("[applied]"),
                                              Arrays.asList(BooleanSerializer.instance.serialize(true)));

            prepSelect = verifyMetadataFlagsWithLWTsSelect(simpleClient,
                                                           prepSelect,
                                                           Arrays.asList("pk", "v1", "v2"),
                                                           Arrays.asList(Int32Serializer.instance.serialize(1),
                                                                         Int32Serializer.instance.serialize(10),
                                                                         Int32Serializer.instance.serialize(20)),
                                                           EnumSet.of(org.apache.cassandra.cql3.ResultSet.Flag.GLOBAL_TABLES_SPEC));

            // This is an _unsuccessful_ LWT update (as the condition fails)
            verifyMetadataFlagsWithLWTsUpdate(simpleClient,
                                              prepUpdate,
                                              Arrays.asList(Int32Serializer.instance.serialize(10),
                                                            Int32Serializer.instance.serialize(20),
                                                            Int32Serializer.instance.serialize(1)),
                                              Arrays.asList("[applied]", "v1"),
                                              Arrays.asList(BooleanSerializer.instance.serialize(false),
                                                            Int32Serializer.instance.serialize(10)));

            prepSelect = verifyMetadataFlagsWithLWTsSelect(simpleClient,
                                                           prepSelect,
                                                           Arrays.asList("pk", "v1", "v2"),
                                                           Arrays.asList(Int32Serializer.instance.serialize(1),
                                                                         Int32Serializer.instance.serialize(10),
                                                                         Int32Serializer.instance.serialize(20)),
                                                           EnumSet.of(org.apache.cassandra.cql3.ResultSet.Flag.GLOBAL_TABLES_SPEC));

            // force a schema change on that table
            simpleClient.execute(String.format("ALTER TABLE %s.%s ADD v3 int",
                                               keyspace(), currentTable()),
                                 ConsistencyLevel.LOCAL_ONE);

            try
            {
                simpleClient.executePrepared(prepUpdate,
                                             Arrays.asList(Int32Serializer.instance.serialize(1),
                                                           Int32Serializer.instance.serialize(30),
                                                           Int32Serializer.instance.serialize(10)),
                                             ConsistencyLevel.LOCAL_ONE);
                fail();
            }
            catch (RuntimeException re)
            {
                assertTrue(re.getCause() instanceof PreparedQueryNotFoundException);
                // the prepared statement has been removed from the pstmt cache, need to re-prepare it
                // only prepare the statement on the server side but don't set the variable
                simpleClient.prepare(String.format("UPDATE %s.%s SET v1 = ?, v2 = ? WHERE pk = 1 IF v1 = ?",
                                                   keyspace(), currentTable()));
            }
            try
            {
                simpleClient.executePrepared(prepSelect,
                                             Arrays.asList(Int32Serializer.instance.serialize(1)),
                                             ConsistencyLevel.LOCAL_ONE);
                fail();
            }
            catch (RuntimeException re)
            {
                assertTrue(re.getCause() instanceof PreparedQueryNotFoundException);
                // the prepared statement has been removed from the pstmt cache, need to re-prepare it
                // only prepare the statement on the server side but don't set the variable
                simpleClient.prepare(String.format("SELECT * FROM %s.%s WHERE pk = ?",
                                                   keyspace(), currentTable()));
            }

            // This is a _successful_ LWT update
            verifyMetadataFlagsWithLWTsUpdate(simpleClient,
                                              prepUpdate,
                                              Arrays.asList(Int32Serializer.instance.serialize(1),
                                                            Int32Serializer.instance.serialize(30),
                                                            Int32Serializer.instance.serialize(10)),
                                              Arrays.asList("[applied]"),
                                              Arrays.asList(BooleanSerializer.instance.serialize(true)));

            // Re-assign prepSelect here, as the resultset metadata changed to submit the updated
            // resultset-metadata-ID in the next SELECT. This behavior does not apply to LWT statements.
            prepSelect = verifyMetadataFlagsWithLWTsSelect(simpleClient,
                                                           prepSelect,
                                                           Arrays.asList("pk", "v1", "v2", "v3"),
                                                           Arrays.asList(Int32Serializer.instance.serialize(1),
                                                                         Int32Serializer.instance.serialize(1),
                                                                         Int32Serializer.instance.serialize(30),
                                                                         null),
                                                           EnumSet.of(org.apache.cassandra.cql3.ResultSet.Flag.GLOBAL_TABLES_SPEC,
                                                                      org.apache.cassandra.cql3.ResultSet.Flag.METADATA_CHANGED));

            // This is an _unsuccessful_ LWT update (as the condition fails)
            verifyMetadataFlagsWithLWTsUpdate(simpleClient,
                                              prepUpdate,
                                              Arrays.asList(Int32Serializer.instance.serialize(1),
                                                            Int32Serializer.instance.serialize(30),
                                                            Int32Serializer.instance.serialize(10)),
                                              Arrays.asList("[applied]", "v1"),
                                              Arrays.asList(BooleanSerializer.instance.serialize(false),
                                                            Int32Serializer.instance.serialize(1)));

            verifyMetadataFlagsWithLWTsSelect(simpleClient,
                                              prepSelect,
                                              Arrays.asList("pk", "v1", "v2", "v3"),
                                              Arrays.asList(Int32Serializer.instance.serialize(1),
                                                            Int32Serializer.instance.serialize(1),
                                                            Int32Serializer.instance.serialize(30),
                                                            null),
                                              EnumSet.of(org.apache.cassandra.cql3.ResultSet.Flag.GLOBAL_TABLES_SPEC));
        }
    }

    private ResultMessage.Prepared verifyMetadataFlagsWithLWTsSelect(SimpleClient simpleClient,
                                                                     ResultMessage.Prepared prepSelect,
                                                                     List<String> columnNames,
                                                                     List<ByteBuffer> expectedRow,
                                                                     EnumSet<org.apache.cassandra.cql3.ResultSet.Flag> expectedFlags)
    {
        ResultMessage result = simpleClient.executePrepared(prepSelect,
                                                            Collections.singletonList(Int32Serializer.instance.serialize(1)),
                                                            ConsistencyLevel.LOCAL_ONE);
        ResultMessage.Rows rows = (ResultMessage.Rows) result;
        EnumSet<org.apache.cassandra.cql3.ResultSet.Flag> resultFlags = rows.result.metadata.getFlags();
        assertEquals(expectedFlags,
                     resultFlags);
        assertEquals(columnNames.size(),
                     rows.result.metadata.getColumnCount());
        assertEquals(columnNames,
                     rows.result.metadata.names.stream().map(cs -> cs.name.toString()).collect(Collectors.toList()));
        assertEquals(1,
                     rows.result.size());
        assertEquals(expectedRow,
                     rows.result.rows.get(0));

        if (resultFlags.contains(org.apache.cassandra.cql3.ResultSet.Flag.METADATA_CHANGED))
            prepSelect = prepSelect.withResultMetadata(rows.result.metadata);
        return prepSelect;
    }

    private void verifyMetadataFlagsWithLWTsUpdate(SimpleClient simpleClient,
                                                   ResultMessage.Prepared prepUpdate,
                                                   List<ByteBuffer> params,
                                                   List<String> columnNames,
                                                   List<ByteBuffer> expectedRow)
    {
        ResultMessage result = simpleClient.executePrepared(prepUpdate,
                                                            params,
                                                            ConsistencyLevel.LOCAL_ONE);
        ResultMessage.Rows rows = (ResultMessage.Rows) result;
        EnumSet<org.apache.cassandra.cql3.ResultSet.Flag> resultFlags = rows.result.metadata.getFlags();
        assertEquals(EnumSet.of(org.apache.cassandra.cql3.ResultSet.Flag.GLOBAL_TABLES_SPEC),
                     resultFlags);
        assertEquals(columnNames.size(),
                     rows.result.metadata.getColumnCount());
        assertEquals(columnNames,
                     rows.result.metadata.names.stream().map(cs -> cs.name.toString()).collect(Collectors.toList()));
        assertEquals(1,
                     rows.result.size());
        assertEquals(expectedRow,
                     rows.result.rows.get(0));
    }

    @Test
    public void testPrepareWithLWT() throws Throwable
    {
        testPrepareWithLWT(ProtocolVersion.V4);
        testPrepareWithLWT(ProtocolVersion.V5);
    }

    private void testPrepareWithLWT(ProtocolVersion version) throws Throwable
    {
        Session session = sessionNet(version);
        session.execute("USE " + keyspace());
        createTable("CREATE TABLE %s (pk int, v1 int, v2 int, PRIMARY KEY (pk))");

        PreparedStatement prepared1 = session.prepare(String.format("UPDATE %s SET v1 = ?, v2 = ?  WHERE pk = 1 IF v1 = ?", currentTable()));
        PreparedStatement prepared2 = session.prepare(String.format("INSERT INTO %s (pk, v1, v2) VALUES (?, 200, 300) IF NOT EXISTS", currentTable()));
        execute("INSERT INTO %s (pk, v1, v2) VALUES (1,1,1)");
        execute("INSERT INTO %s (pk, v1, v2) VALUES (2,2,2)");

        ResultSet rs;

        rs = session.execute(prepared1.bind(10, 20, 1));
        assertRowsNet(rs,
                      row(true));
        assertEquals(rs.getColumnDefinitions().size(), 1);

        rs = session.execute(prepared1.bind(100, 200, 1));
        assertRowsNet(rs,
                      row(false, 10));
        assertEquals(rs.getColumnDefinitions().size(), 2);

        rs = session.execute(prepared1.bind(30, 40, 10));
        assertRowsNet(rs,
                      row(true));
        assertEquals(rs.getColumnDefinitions().size(), 1);

        // Try executing the same message once again
        rs = session.execute(prepared1.bind(100, 200, 1));
        assertRowsNet(rs,
                      row(false, 30));
        assertEquals(rs.getColumnDefinitions().size(), 2);

        rs = session.execute(prepared2.bind(1));
        assertRowsNet(rs,
                      row(false, 1, 30, 40));
        assertEquals(rs.getColumnDefinitions().size(), 4);

        alterTable("ALTER TABLE %s ADD v3 int;");

        rs = session.execute(prepared2.bind(1));
        assertRowsNet(rs,
                      row(false, 1, 30, 40, null));
        assertEquals(rs.getColumnDefinitions().size(), 5);

        rs = session.execute(prepared2.bind(20));
        assertRowsNet(rs,
                      row(true));
        assertEquals(rs.getColumnDefinitions().size(), 1);

        rs = session.execute(prepared2.bind(20));
        assertRowsNet(rs,
                      row(false, 20, 200, 300, null));
        assertEquals(rs.getColumnDefinitions().size(), 5);
    }

    @Test
    public void testPrepareWithBatchLWT() throws Throwable
    {
        testPrepareWithBatchLWT(ProtocolVersion.V4);
        testPrepareWithBatchLWT(ProtocolVersion.V5);
    }

    private void testPrepareWithBatchLWT(ProtocolVersion version) throws Throwable
    {
        Session session = sessionNet(version);
        session.execute("USE " + keyspace());
        createTable("CREATE TABLE %s (pk int, v1 int, v2 int, PRIMARY KEY (pk))");

        PreparedStatement prepared1 = session.prepare("BEGIN BATCH " +
                                                      "UPDATE " + currentTable() + " SET v1 = ? WHERE pk = 1 IF v1 = ?;" +
                                                      "UPDATE " + currentTable() + " SET v2 = ? WHERE pk = 1 IF v2 = ?;" +
                                                      "APPLY BATCH;");
        PreparedStatement prepared2 = session.prepare("BEGIN BATCH " +
                                                      "INSERT INTO " + currentTable() + " (pk, v1, v2) VALUES (1, 200, 300) IF NOT EXISTS;" +
                                                      "APPLY BATCH");
        execute("INSERT INTO %s (pk, v1, v2) VALUES (1,1,1)");
        execute("INSERT INTO %s (pk, v1, v2) VALUES (2,2,2)");

        com.datastax.driver.core.ResultSet rs;

        rs = session.execute(prepared1.bind(10, 1, 20, 1));
        assertRowsNet(rs,
                      row(true));
        assertEquals(rs.getColumnDefinitions().size(), 1);

        rs = session.execute(prepared1.bind(100, 1, 200, 1));
        assertRowsNet(rs,
                      row(false, 1, 10, 20));
        assertEquals(rs.getColumnDefinitions().size(), 4);

        // Try executing the same message once again
        rs = session.execute(prepared1.bind(100, 1, 200, 1));
        assertRowsNet(rs,
                      row(false, 1, 10, 20));
        assertEquals(rs.getColumnDefinitions().size(), 4);

        rs = session.execute(prepared2.bind());
        assertRowsNet(rs,
                      row(false, 1, 10, 20));
        assertEquals(rs.getColumnDefinitions().size(), 4);

        alterTable("ALTER TABLE %s ADD v3 int;");

        rs = session.execute(prepared2.bind());
        assertRowsNet(rs,
                      row(false, 1, 10, 20, null));
        assertEquals(rs.getColumnDefinitions().size(), 5);
    }
}
