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

package org.apache.cassandra.metrics;

import java.io.IOException;
import java.util.Collection;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.triggers.ITrigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CQLMetricsTest
{
    private static Cluster cluster;
    private static Session session;
    private static EmbeddedCassandraService cassandra;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = ServerTestUtils.startEmbeddedCassandraService();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS junit WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE IF NOT EXISTS junit.metricstest (id int PRIMARY KEY, val text);");
    }

    @AfterClass
    public static void tearDown()
    {
        if (cluster != null)
            cluster.close();
        if (cassandra != null)
            cassandra.stop();
    }

    @Test
    public void testConnectionWithUseDisabled()
    {
        long useCountBefore = QueryProcessor.metrics.useStatementsExecuted.getCount();
        DatabaseDescriptor.setUseStatementsEnabled(false);

        try (Session ignored = cluster.connect("junit"))
        {
            fail("expected USE statement to fail with use_statements_enabled = false");
        }
        catch (InvalidQueryException e)
        {
            Assert.assertEquals(useCountBefore, QueryProcessor.metrics.useStatementsExecuted.getCount());
            assertTrue(e.getMessage().contains("USE statements prohibited"));
        }
        finally
        {
            DatabaseDescriptor.setUseStatementsEnabled(true);
        }
    }

    @Test
    public void testPreparedStatementsCount()
    {
        int n = QueryProcessor.metrics.preparedStatementsCount.getValue();
        long useCountBefore = QueryProcessor.metrics.useStatementsExecuted.getCount();
        session.execute("use junit");
        Assert.assertEquals(useCountBefore + 1, QueryProcessor.metrics.useStatementsExecuted.getCount());
        session.prepare("SELECT * FROM junit.metricstest WHERE id = ?");
        assertEquals(n+2, (int) QueryProcessor.metrics.preparedStatementsCount.getValue());
    }

    @Test
    public void testRegularStatementsExecuted()
    {
        clearMetrics();
        PreparedStatement metricsStatement = session.prepare("INSERT INTO junit.metricstest (id, val) VALUES (?, ?)");

        assertEquals(0, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(0, QueryProcessor.metrics.regularStatementsExecuted.getCount());

        for (int i = 0; i < 10; i++)
            session.execute(String.format("INSERT INTO junit.metricstest (id, val) VALUES (%d, '%s')", i, "val" + i));

        assertEquals(0, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(10, QueryProcessor.metrics.regularStatementsExecuted.getCount());
    }

    @Test
    public void testPreparedStatementsExecuted()
    {
        clearMetrics();
        PreparedStatement metricsStatement = session.prepare("INSERT INTO junit.metricstest (id, val) VALUES (?, ?)");

        assertEquals(0, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(0, QueryProcessor.metrics.regularStatementsExecuted.getCount());

        for (int i = 0; i < 10; i++)
            session.execute(metricsStatement.bind(i, "val" + i));

        assertEquals(10, QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        assertEquals(0, QueryProcessor.metrics.regularStatementsExecuted.getCount());
    }

    @Test
    public void testPreparedStatementsRatio()
    {
        clearMetrics();
        PreparedStatement metricsStatement = session.prepare("INSERT INTO junit.metricstest (id, val) VALUES (?, ?)");

        assertEquals(Double.NaN, QueryProcessor.metrics.preparedStatementsRatio.getValue(), 0.0);

        for (int i = 0; i < 10; i++)
            session.execute(metricsStatement.bind(i, "val" + i));
        assertEquals(1.0, QueryProcessor.metrics.preparedStatementsRatio.getValue(), 0.0);

        for (int i = 0; i < 10; i++)
            session.execute(String.format("INSERT INTO junit.metricstest (id, val) VALUES (%d, '%s')", i, "val" + i));
        assertEquals(0.5, QueryProcessor.metrics.preparedStatementsRatio.getValue(), 0.0);
    }

    @Test
    public void testCreateStatementCount() {
        clearMetrics();
        session.execute("CREATE TABLE IF NOT EXISTS junit.createtest1 (id uuid PRIMARY KEY, content text);");
        assertEquals(1, QueryProcessor.metrics.createStatementCount.getCount());
        // create statement is counted regardless whether it has effects or not
        session.execute("CREATE TABLE IF NOT EXISTS junit.createtest1 (id uuid PRIMARY KEY, content text);");
        assertEquals(2, QueryProcessor.metrics.createStatementCount.getCount());
    }

    @Test
    public void testCreateStatementWithCompactionSpecifiedCount() {
        clearMetrics();
        session.execute("CREATE TABLE IF NOT EXISTS junit.createtest1 (id uuid PRIMARY KEY, content text) WITH " +
                        "compaction={'class': 'SizeTieredCompactionStrategy'} AND comment='test text';");
        assertEquals(1, QueryProcessor.metrics.createStatementWithCompactionSpecifiedCount.getCount());
        session.execute("CREATE TABLE IF NOT EXISTS junit.createtest2 (id uuid PRIMARY KEY, content text);");
        assertEquals(1, QueryProcessor.metrics.createStatementWithCompactionSpecifiedCount.getCount());
        // create statement with other options specified
        session.execute("CREATE TABLE IF NOT EXISTS junit.createtest3 (id uuid PRIMARY KEY, content text) WITH "
                        + "comment='test text';");
        assertEquals(1, QueryProcessor.metrics.createStatementWithCompactionSpecifiedCount.getCount());

        // create statement is counted regardless whether it's rejected or not
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        try {
            session.execute("CREATE TABLE IF NOT EXISTS junit.createtest4 (id uuid PRIMARY KEY, content text) WITH " +
                            "comment='test text' AND compaction={'class': 'SizeTieredCompactionStrategy'};");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.createStatementWithCompactionSpecifiedCount.getCount());
            assertEquals(4, QueryProcessor.metrics.createStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Received unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testAlterStatementCount() {
        clearMetrics();
        session.execute("CREATE TABLE IF NOT EXISTS junit.altertest (id uuid PRIMARY KEY, content text, todrop text);");

        session.execute("ALTER TABLE junit.altertest DROP todrop;");
        assertEquals(1, QueryProcessor.metrics.alterStatementCount.getCount());

        session.execute("ALTER TABLE junit.altertest ADD newcol text;");
        assertEquals(2, QueryProcessor.metrics.alterStatementCount.getCount());

        session.execute("ALTER TABLE junit.altertest RENAME id TO newid;");
        assertEquals(3, QueryProcessor.metrics.alterStatementCount.getCount());

        session.execute("ALTER TABLE junit.altertest WITH compaction={'class': 'LeveledCompactionStrategy'} AND comment='test text';");
        assertEquals(4, QueryProcessor.metrics.alterStatementCount.getCount());

        try {
            // should fail, because not created with compact storage
            session.execute("ALTER TABLE junit.altertest DROP COMPACT STORAGE");
        }
        catch (InvalidQueryException e) {
            assertEquals(5, QueryProcessor.metrics.alterStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }

        // invalid ALTER statement is also counted
        try {
            session.execute("ALTER TABLE junit.nonexisting DROP whatever;");
        }
        catch (InvalidQueryException e) {
            assertEquals(6, QueryProcessor.metrics.alterStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testAlterStatementWithCompactionSpecifiedCount() {
        clearMetrics();
        session.execute("CREATE TABLE IF NOT EXISTS junit.altertest2 (id uuid PRIMARY KEY, content text, todrop text);");
        session.execute("ALTER TABLE junit.altertest2 DROP todrop;");
        session.execute("ALTER TABLE junit.altertest2 ADD newcol text;");
        session.execute("ALTER TABLE junit.altertest2 RENAME id TO newid;");
        session.execute("ALTER TABLE junit.altertest2 WITH compaction={'class': 'LeveledCompactionStrategy'};");
        assertEquals(1, QueryProcessor.metrics.alterStatementWithCompactionSpecifiedCount.getCount());

        // LCS enforcement disallow mutation on compaction option, but is counted
        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.hard);
        try {
            session.execute("ALTER TABLE junit.altertest2 WITH compaction={'class': 'SizeTieredCompactionStrategy'};");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.alterStatementWithCompactionSpecifiedCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }

        DatabaseDescriptor.setLCSEnforcementLevel(Config.LCSEnforcementLevel.soft);
        try {
            session.execute("ALTER TABLE junit.altertest2 WITH compaction={'class': 'SizeTieredCompactionStrategy'};");
        }
        catch (InvalidQueryException e) {
            assertEquals(3, QueryProcessor.metrics.alterStatementWithCompactionSpecifiedCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }

        // alter on other option is not counted
        session.execute("ALTER TABLE junit.altertest2 WITH comment='test text';");
        assertEquals(3, QueryProcessor.metrics.alterStatementWithCompactionSpecifiedCount.getCount());
    }

    @Test
    public void testDropTableStatementCount() {
        clearMetrics();
        session.execute("CREATE TABLE IF NOT EXISTS junit.droptest (id uuid PRIMARY KEY, content text);");
        session.execute("DROP TABLE junit.droptest;");
        assertEquals(1, QueryProcessor.metrics.dropTableStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP TABLE junit.nonexisting;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropTableStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testDropKeyspaceStatementCount()
    {
        clearMetrics();
        session.execute("CREATE KEYSPACE IF NOT EXISTS junit2 WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("DROP KEYSPACE junit2;");
        assertEquals(1, QueryProcessor.metrics.dropKeyspaceStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP KEYSPACE junit2;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropKeyspaceStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testDropIndexStatementCount()
    {
        clearMetrics();
        session.execute("CREATE INDEX IF NOT EXISTS idx ON junit.metricstest (val);");
        session.execute("DROP INDEX junit.idx;");
        assertEquals(1, QueryProcessor.metrics.dropIndexStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP INDEX junit.idx;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropIndexStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testDropAggregateStatementCount()
    {
        clearMetrics();
        session.execute("CREATE OR REPLACE FUNCTION junit.tester(state int, val int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE javascript AS '\"string\";';");
        session.execute("CREATE OR REPLACE AGGREGATE junit.agg(int) SFUNC tester STYPE int INITCOND 0;");
        session.execute("DROP AGGREGATE junit.agg;");
        assertEquals(1, QueryProcessor.metrics.dropAggregateStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP AGGREGATE junit.agg;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropAggregateStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testDropViewStatementCount()
    {
        clearMetrics();
        session.execute("CREATE MATERIALIZED VIEW junit.viewtest AS SELECT * FROM junit.metricstest WHERE id IS NOT NULL PRIMARY KEY (id);");
        session.execute("DROP MATERIALIZED VIEW junit.viewtest;");
        assertEquals(1, QueryProcessor.metrics.dropViewStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP MATERIALIZED VIEW junit.viewtest;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropViewStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    public static class NoOpTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            return null;
        }
    }

    @Test
    public void testDropTriggerStatementCount()
    {
        clearMetrics();
        session.execute(String.format("CREATE TRIGGER IF NOT EXISTS new_trigger ON junit.metricstest USING '%s';", NoOpTrigger.class.getName()));
        session.execute("DROP TRIGGER new_trigger ON junit.metricstest;");
        assertEquals(1, QueryProcessor.metrics.dropTriggerStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP TRIGGER new_trigger ON junit.metricstest;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropTriggerStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testDropTypeStatementCount()
    {
        clearMetrics();
        session.execute("CREATE TYPE junit.type (id uuid, content text);");
        session.execute("DROP TYPE junit.type;");
        assertEquals(1, QueryProcessor.metrics.dropTypeStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP TYPE junit.type;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropTypeStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testDropFunctionStatementCount()
    {
        clearMetrics();
        session.execute("CREATE OR REPLACE FUNCTION junit.func (input int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;';");
        session.execute("DROP FUNCTION junit.func;");
        assertEquals(1, QueryProcessor.metrics.dropFunctionStatementCount.getCount());

        // invalid DROP statement is also counted
        try {
            session.execute("DROP FUNCTION junit.func;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.dropFunctionStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    @Test
    public void testTruncateStatementCount()
    {
        clearMetrics();
        session.execute("CREATE TABLE IF NOT EXISTS junit.truncatetest (id uuid PRIMARY KEY, content text);");
        session.execute("TRUNCATE junit.truncatetest;");
        assertEquals(1, QueryProcessor.metrics.truncateStatementCount.getCount());

        // invalid TRUNCATE statement is also counted
        try {
            session.execute("TRUNCATE junit.nonexisting;");
        }
        catch (InvalidQueryException e) {
            assertEquals(2, QueryProcessor.metrics.truncateStatementCount.getCount());
        }
        catch (Exception e) {
            fail(String.format("Unexpected exception: %s", e.getMessage()));
        }
    }

    private void clearMetrics()
    {
        QueryProcessor.metrics.preparedStatementsExecuted.dec(QueryProcessor.metrics.preparedStatementsExecuted.getCount());
        QueryProcessor.metrics.regularStatementsExecuted.dec(QueryProcessor.metrics.regularStatementsExecuted.getCount());
        QueryProcessor.metrics.preparedStatementsEvicted.dec(QueryProcessor.metrics.preparedStatementsEvicted.getCount());
        QueryProcessor.metrics.createStatementCount.dec(QueryProcessor.metrics.createStatementCount.getCount());
        QueryProcessor.metrics.createStatementWithCompactionSpecifiedCount.dec(QueryProcessor.metrics.createStatementWithCompactionSpecifiedCount.getCount());
        QueryProcessor.metrics.alterStatementCount.dec(QueryProcessor.metrics.alterStatementCount.getCount());
        QueryProcessor.metrics.alterStatementWithCompactionSpecifiedCount.dec(QueryProcessor.metrics.alterStatementWithCompactionSpecifiedCount.getCount());
    }
}

