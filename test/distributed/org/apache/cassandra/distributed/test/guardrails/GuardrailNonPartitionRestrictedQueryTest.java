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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.util.Auth;
import org.apache.cassandra.exceptions.QueryReferencesTooManyIndexesAbortException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;
import org.awaitility.Awaitility;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.apache.cassandra.service.reads.thresholds.WarningsSnapshot.tooManyIndexesReadWarnMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GuardrailNonPartitionRestrictedQueryTest extends GuardrailTester
{
    private static Cluster cluster;
    private static com.datastax.driver.core.Cluster driverCluster;
    private Session driverSession;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = Cluster.build(3)
                         .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                           .set("read_thresholds_enabled", "true")
                                           .set("authenticator", "PasswordAuthenticator"))
                         .withDataDirCount(1)
                         .start();

        Auth.waitForExistingRoles(cluster.get(1));

        // create a regular user, since the default superuser is excluded from guardrails
        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1");
        try (com.datastax.driver.core.Cluster c = builder.withCredentials("cassandra", "cassandra").build();
             Session session = c.connect())
        {
            session.execute("CREATE USER test WITH PASSWORD 'test'");
        }

        // connect using that superuser, we use the driver to get access to the client warnings
        driverCluster = builder.withCredentials("test", "test").build();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (driverCluster != null)
            driverCluster.close();

        if (cluster != null)
            cluster.close();
    }

    @Before
    public void beforeTest()
    {
        super.beforeTest();
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster);
        driverSession = driverCluster.connect();
    }

    @After
    public void afterTest()
    {
        if (driverSession != null)
            driverSession.close();
    }

    @Override
    protected Cluster getCluster()
    {
        return cluster;
    }

    @Test
    public void testGuardrailForLegacy2i()
    {
        prepareSchema(false);
        testGuardrailInternal();
    }

    @Test
    public void testGuardrailForSAI()
    {
        prepareSchema(true);
        testGuardrailInternal();
    }

    @Test
    public void testSAIWarnThreshold()
    {
        prepareSchema(true);
        setThresholds(2, 5);

        // flushing just 1 table will not trigger any threshold
        long valueToQuery = createSSTables(1);
        assertThat(executeSelect(valueToQuery, false)).isNull();

        assertWarnAborts(0, 0);

        // create 3 more SSTables on each node, this will trigger warn threshold (3 > 2 but < 5)
        valueToQuery = createSSTables(3);
        String expectedMessage = tooManyIndexesReadWarnMessage(cluster.size(),
                                                               3,
                                                               String.format("SELECT * FROM %s.%s WHERE v1 = %s ALLOW FILTERING",
                                                                             KEYSPACE, tableName, valueToQuery));
        assertThat(getOnlyElement(executeSelect(valueToQuery, false))).contains(expectedMessage);

        assertWarnAborts(1, 0);

        // we effectively stop reacting on warnings
        setThresholds(32, -1);
        assertThat(executeSelect(valueToQuery, false)).isNull();

        assertWarnAborts(1, 0);

        // we compacted SSTables of node 1 but not on 2 and 3,
        // we set tresholds to all nodes hence we violated thresholds on node 2 and 3
        compact(1);
        setThresholds(2, 5);

        // notice we expect warnings from 2 nodes
        expectedMessage = tooManyIndexesReadWarnMessage(cluster.size() - 1,
                                                        3,
                                                        String.format("SELECT * FROM %s.%s WHERE v1 = %s ALLOW FILTERING",
                                                                      KEYSPACE, tableName, valueToQuery));

        assertThat(getOnlyElement(executeSelect(valueToQuery, false))).contains(expectedMessage);

        assertWarnAborts(2, 0);

        // disable warn thresholds on nodes 2 and 3
        setThresholds(32, -1, 2, 3);
        // we will not warn because nodes 2 and 3 are disabled and 1 does not violate warn threshold
        assertThat(executeSelect(valueToQuery, false)).isNull();

        assertWarnAborts(2, 0);

        // set thresholds back on nodes 2 and 3
        setThresholds(2, 5, 2, 3);
        assertThat(getOnlyElement(executeSelect(valueToQuery, false))).contains(expectedMessage);

        assertWarnAborts(3, 0);

        // we compacted SSTables on the third node as well, so we will not violate any thresholds
        compact(2);
        compact(3);

        assertThat(executeSelect(valueToQuery, false)).isNull();

        // metrics stay same as they were
        assertWarnAborts(3, 0);
    }

    @Test
    public void testSAIFailThreshold()
    {
        prepareSchema(true);
        setThresholds(2, 5);

        // create 6 SSTables on each node, this will trigger fail threshold (6 > 5)
        long valueToQuery = createSSTables(6);

        String expectedMessage = String.format("referenced %s SSTable indexes for a query without restrictions on partition key " +
                                               "and aborted the query SELECT * FROM %s.%s WHERE v1 = %s ALLOW FILTERING",
                                               6, KEYSPACE, tableName, valueToQuery);

        assertThat(getOnlyElement(executeSelect(valueToQuery, true))).contains(expectedMessage);

        assertWarnAborts(0, 1);

        // we effectively stop reacting on failures
        setThresholds(32, -1);
        assertThat(executeSelect(valueToQuery, false)).isNull();

        // we compacted SSTables of node 1 but not on 2 and 3,
        // we set tresholds to all nodes hence we violated thresholds on node 2 and 3
        compact(1);
        setThresholds(2, 5);
        assertThat(getOnlyElement(executeSelect(valueToQuery, true))).contains(expectedMessage);

        assertWarnAborts(0, 2);

        // disable fail thresholds on nodes 2 and 3
        setThresholds(32, -1, 2, 3);
        // we will not fail because nodes 2 and 3 are disabled and 1 does not violate fail threshold
        assertThat(executeSelect(valueToQuery, false)).isNull();

        // set thresholds back on nodes 2 and 3
        setThresholds(2, 5, 2, 3);
        assertThat(getOnlyElement(executeSelect(valueToQuery, true))).contains(expectedMessage);

        assertWarnAborts(0, 3);

        // we compacted SSTables on the second and the third node as well, so we will not violate any thresholds
        compact(2);
        compact(3);

        assertThat(executeSelect(valueToQuery, false)).isNull();

        // metrics stay same as they were
        assertWarnAborts(0, 3);
    }

    private void testGuardrailInternal()
    {
        enableGuardrail();

        Awaitility.await()
                  .pollDelay(5, TimeUnit.SECONDS)
                  .atMost(1, TimeUnit.MINUTES)
                  .pollInterval(5, TimeUnit.SECONDS)
                  .until(() -> {
                      try
                      {
                          assertThat(executeViaDriver(String.format("SELECT * from %s.%s WHERE k = 0 AND v1 = 0", KEYSPACE, tableName))).isEmpty();
                          assertThat(executeViaDriver(String.format("SELECT * from %s.%s WHERE v1 = 0", KEYSPACE, tableName))).isEmpty();

                          return true;
                      }
                      catch (ReadFailureException ex)
                      {
                          return false;
                      }
                  });

        disableGuardrail();

        try
        {
            executeViaDriver(String.format("SELECT * from %s.%s WHERE v1 = 0", KEYSPACE, tableName));
            fail("selection on non-partition restricted queries should be forbidden");
        }
        catch (InvalidQueryException e)
        {
            assertThat(e.getMessage()).contains(Guardrails.nonPartitionRestrictedIndexQueryEnabled.reason);
        }

        // even we disabled guardrail, if we query by primary key and clustering column, it passes
        assertThat(executeViaDriver(String.format("SELECT * from %s.%s WHERE k = 0 AND v1 = 0", KEYSPACE, tableName))).isEmpty();
        assertThat(executeViaDriver(String.format("SELECT * from %s.%s WHERE k = 0 AND c = 0", KEYSPACE, tableName))).isEmpty();
        assertThat(executeViaDriver(String.format("SELECT * from %s.%s WHERE k = 0", KEYSPACE, tableName))).isEmpty();

        // enable it back and do non-partition key queries
        enableGuardrail();

        assertThat(executeViaDriver(String.format("SELECT * from %s.%s WHERE v1 = 0", KEYSPACE, tableName))).isEmpty();
    }

    private void assertWarnAborts(int warns, int aborts)
    {
        assertThat(totalWarnings()).as("warnings").isEqualTo(warns);
        assertThat(totalAborts()).as("aborts").isEqualTo(aborts);
    }

    /**
     * Execution of statements via driver will not bypass guardrails as internal queries would do as they are
     * done by superuser / they do not have any notion of roles
     *
     * @return list of warnings
     */
    private List<String> executeViaDriver(String query)
    {
        SimpleStatement stmt = new SimpleStatement(query);
        stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.QUORUM);
        ResultSet resultSet = driverSession.execute(stmt);
        return resultSet.getExecutionInfo().getWarnings();
    }

    private List<String> executeSelect(long valueToQuery, boolean expectToFail)
    {
        return cluster.get(1).applyOnInstance((IIsolatedExecutor.SerializableTriFunction<String, String, Long, List<String>>) (keyspace, table, v1) -> {
            Awaitility.await()
                      .pollInterval(5, TimeUnit.SECONDS)
                      .atMost(1, TimeUnit.MINUTES)
                      .until(() -> {
                          ColumnFamilyStore cs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                          if (cs == null)
                              return false;

                          SecondaryIndexManager indexManager = cs.indexManager;
                          if (indexManager == null)
                              return false;

                          Index v1Idx = indexManager.getIndexByName("v1_idx");
                          Index v2Idx = indexManager.getIndexByName("v2_idx");

                          if (v1Idx == null || v2Idx == null)
                              return false;

                          return indexManager.isIndexQueryable(v1Idx) && indexManager.isIndexQueryable(v2Idx);
                      });

            ClientWarn.instance.captureWarnings();
            CoordinatorWarnings.init();

            String query = String.format("SELECT * from %s.%s WHERE v1 = %s", keyspace, table, v1);

            try
            {
                QueryProcessor.execute(query, ALL, QueryState.forInternalCalls());
                if (expectToFail)
                    fail("expected to fail");
            }
            catch (QueryReferencesTooManyIndexesAbortException e)
            {
                if (!expectToFail)
                    fail("did not expect to fail");

                assertTrue(e.nodes >= 1 && e.nodes <= 3);
            }

            CoordinatorWarnings.done();
            CoordinatorWarnings.reset();
            return ClientWarn.instance.getWarnings();
        }, KEYSPACE, tableName, valueToQuery);
    }

    private boolean indexesQueryable()
    {
        for (int i = 1; i < cluster.size() + 1; i++)
        {
            boolean indexesQueryable = cluster.get(i).applyOnInstance((IIsolatedExecutor.SerializableBiFunction<String, String, Boolean>) (ks, tb) -> {
                for (ColumnFamilyStore cs : Keyspace.open(ks).getColumnFamilyStores())
                {
                    if (!cs.name.equals(tb))
                        continue;

                    SecondaryIndexManager indexManager = cs.indexManager;

                    if (indexManager == null)
                        return false;

                    Index v1Idx = indexManager.getIndexByName("v1_idx");
                    Index v2Idx = indexManager.getIndexByName("v2_idx");

                    if (v1Idx == null || v2Idx == null)
                        return false;

                    return indexManager.isIndexQueryable(v1Idx) && indexManager.isIndexQueryable(v2Idx);
                }

                return false;
            }, KEYSPACE, tableName);

            if (!indexesQueryable)
                return false;
        }

        return true;
    }

    private void prepareSchema(boolean sai)
    {
        schemaChange("CREATE TABLE IF NOT EXISTS %s (k bigint, c bigint, v1 bigint, v2 bigint, PRIMARY KEY (k, c));");

        if (sai)
        {
            schemaChange("CREATE CUSTOM INDEX IF NOT EXISTS v1_idx ON %s (v1) USING 'StorageAttachedIndex'");
            schemaChange("CREATE CUSTOM INDEX IF NOT EXISTS v2_idx ON %s (v2) USING 'StorageAttachedIndex'");
        }
        else
        {
            schemaChange("CREATE INDEX IF NOT EXISTS v1_idx ON %s (v1)");
            schemaChange("CREATE INDEX IF NOT EXISTS v2_idx ON %s (v2)");
        }

        Awaitility.await().atMost(1, TimeUnit.MINUTES).until(this::indexesQueryable);

        for (int i = 1; i < cluster.size() + 1; i++)
        {
            cluster.get(i).acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<String, String>) (ks, tb) -> {
                for (ColumnFamilyStore cs : Keyspace.open(ks).getColumnFamilyStores())
                {
                    if (cs.name.equals(tb))
                    {
                        cs.disableAutoCompaction();
                        break;
                    }
                }
            }).accept(KEYSPACE, tableName);
        }
    }

    private void compact(int node)
    {
        cluster.get(node).acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<String, String>) (ks, tb) -> {
            for (ColumnFamilyStore cs : Keyspace.open(ks).getColumnFamilyStores())
            {
                if (cs.name.equals(tb))
                {
                    cs.forceMajorCompaction();
                    break;
                }
            }
        }).accept(KEYSPACE, tableName);
    }

    private long createSSTables(int numberOfSSTables, int... nodesToFlush)
    {
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        long k = System.currentTimeMillis();
        long value = k + 10;
        for (int i = 0; i < numberOfSSTables; i++)
        {
            // this will be replicated to each node
            cluster.coordinator(1)
                   .execute(format("INSERT INTO %s (k, c, v1, v2) VALUES (?, ?, ?, ?)"),
                            ConsistencyLevel.ALL, k, k, value, value + 10);

            if (nodesToFlush.length == 0)
                cluster.forEach((instance) -> instance.flush(KEYSPACE));
            else
                cluster.get(nodesToFlush).forEach((instance) -> instance.flush(KEYSPACE));
        }

        return value;
    }

    private void setThresholds(int warn, int fail, int... nodes)
    {
        Stream<IInvokableInstance> instances;

        if (nodes.length == 0)
            instances = cluster.stream();
        else
            instances = cluster.get(nodes).stream();

        instances.forEach(instance -> instance.acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<Integer, Integer>)
                                                                 (w, f) -> Guardrails.instance.setSaiSSTableIndexesPerQueryThreshold(w, f))
                                              .accept(warn, fail));
        assertTresholds(warn, fail, nodes);
    }

    private void enableGuardrail()
    {
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setNonPartitionRestrictedQueryEnabled(true)));
        cluster.forEach(instance -> assertTrue(instance.callsOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> Guardrails.instance.getNonPartitionRestrictedQueryEnabled()).call()));
    }

    private void disableGuardrail()
    {
        cluster.forEach(instance -> instance.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> Guardrails.instance.setNonPartitionRestrictedQueryEnabled(false)));
        cluster.forEach(instance -> assertFalse(instance.callsOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> Guardrails.instance.getNonPartitionRestrictedQueryEnabled()).call()));
    }

    private void assertTresholds(int expectedWarn, int expectedFail, int... nodes)
    {
        Stream<IInvokableInstance> instances;

        if (nodes.length == 0)
            instances = cluster.stream();
        else
            instances = cluster.get(nodes).stream();

        instances.forEach(instance -> {
            assertEquals(expectedWarn,
                         instance.callsOnInstance((IIsolatedExecutor.SerializableCallable<Integer>) () -> Guardrails.instance.getSaiSSTableIndexesPerQueryWarnThreshold())
                                 .call().intValue());
            assertEquals(expectedFail,
                         instance.callsOnInstance((IIsolatedExecutor.SerializableCallable<Integer>) () -> Guardrails.instance.getSaiSSTableIndexesPerQueryFailThreshold())
                                 .call().intValue());
        });
    }

    private long totalWarnings()
    {
        return cluster.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.TooManySSTableIndexesReadWarnings.distributed_test_keyspace")).sum();
    }

    private long totalAborts()
    {
        return cluster.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.TooManySSTableIndexesReadAborts.distributed_test_keyspace")).sum();
    }
}
