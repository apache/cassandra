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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.Byteman;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class NativeIndexDDLTest extends TestBaseImpl
{
    private static final String FAILURE_SCRIPT = "RULE fail IndexGCTransaction\n" +
                                                 "CLASS org.apache.cassandra.index.SecondaryIndexManager$IndexGCTransaction\n" +
                                                 "METHOD <init>\n" +
                                                 "AT ENTRY\n" +
                                                 "IF TRUE\n" +
                                                 "DO\n" +
                                                 "   throw new java.lang.RuntimeException(\"Injected index failure\")\n" +
                                                 "ENDRULE\n" +
                                                 "RULE fail CleanupGCTransaction\n" +
                                                 "CLASS org.apache.cassandra.index.SecondaryIndexManager$CleanupGCTransaction\n" +
                                                 "METHOD <init>\n" +
                                                 "AT ENTRY\n" +
                                                 "IF TRUE\n" +
                                                 "DO\n" +
                                                 "   throw new java.lang.RuntimeException(\"Injected index failure\")\n" +
                                                 "ENDRULE\n";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE %s.%s (id TEXT PRIMARY KEY, v1 INT, v2 TEXT) " +
                                                        "WITH compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled' : false }";
    protected static final String CREATE_INDEX_TEMPLATE = "CREATE CUSTOM INDEX ON %s.%s(%s) USING 'StorageAttachedIndex'";

    private Cluster cluster;

    @Before
    public void setupClusterWithSingleNode() throws Throwable
    {
        cluster = builder().withNodes(1)
                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(2))
                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(2, "dc0", "rack0"))
                           .withConfig(config -> config.with(GOSSIP, NETWORK))
                           .withInstanceInitializer((cl, nodeNumber) -> {
                               Byteman.createFromText(FAILURE_SCRIPT).install(cl);
                           })
                           .start();

        cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"));
    }

    @After
    public void destroyCluster() throws Throwable
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void verifyIndexWithDecommission() throws Exception
    {
        // prepare schema ks rf=1 with 2 indexes
        String table = "verify_ndi_during_decommission_test";
        cluster.schemaChange(String.format(CREATE_TABLE_TEMPLATE, KEYSPACE, table));
        cluster.schemaChange(String.format(CREATE_INDEX_TEMPLATE, KEYSPACE, table, "v1"));
        cluster.schemaChange(String.format(CREATE_INDEX_TEMPLATE, KEYSPACE, table, "v2"));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE);

        // create 100 rows in 1 sstable
        int num = 100;
        for (int i = 0; i < num; i++)
        {
            cluster.coordinator(1).execute(String.format("INSERT INTO %s.%s (id, v1, v2) VALUES ('%s', 0, '0')", KEYSPACE, table, i), ConsistencyLevel.ONE);
        }

        cluster.get(1).flush(KEYSPACE);

        verifyIndexQuery(1, table, num, num);
        assertEquals(num, getIndexedCellCount(1, table, "v1"));
        assertEquals(num, getIndexedCellCount(1, table, "v2"));

        // Start node2
        bootstrapAndJoinNode(cluster);

        // node1 still has all indexed data before cleanup
        assertEquals(num, getIndexedCellCount(1, table, "v1"));
        assertEquals(num, getIndexedCellCount(1, table, "v2"));

        // compaction won't cleanup data
        upgradeSSTables(1, KEYSPACE, table);
        assertEquals(num, getIndexedCellCount(1, table, "v1"));
        assertEquals(num, getIndexedCellCount(1, table, "v2"));

        // repair streaming does not transfer entire storage-attached indexes
        //TODO Is this assumption correct?
        long indexRowsNode2 = getIndexedCellCount(2, table, "v1");
        assertNotEquals(0, indexRowsNode2);
        assertNotEquals(num, indexRowsNode2);
        assertEquals(indexRowsNode2, getIndexedCellCount(2, table, "v2"));
        verifyIndexQuery(2, table, num, num);

        // rewrite storage-attached indexes on node2, SAI indexes should not contain rows belonging to node1
        upgradeSSTables(2, KEYSPACE, table);
        indexRowsNode2 = getIndexedCellCount(2, table, "v1");
        assertNotEquals(0, indexRowsNode2);
        assertNotEquals(num, indexRowsNode2);
        assertEquals(indexRowsNode2, getIndexedCellCount(2, table, "v2"));

        // verify data with concurrent nodetool cleanup
        TestWithConcurrentVerification cleanupTest = new TestWithConcurrentVerification(() -> {
            try
            {
                verifyIndexQuery(1, table, num, num);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }, () -> cluster.get(1).runOnInstance(() -> {
            try
            {
                StorageService.instance.forceKeyspaceCleanup(KEYSPACE, table);
            }
            catch (IOException | ExecutionException | InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }));

        cleanupTest.start();

        // verify indexed rows on node1 and it should remove transferred data
        long indexRowsNode1 = getIndexedCellCount(1, table, "v1");
        assertNotEquals(0, indexRowsNode1);
        assertEquals(indexRowsNode1, getIndexedCellCount(1, table, "v2"));
        assertEquals(num, indexRowsNode1 + indexRowsNode2);

        verifyIndexQuery(1, table, num, num);

        // have to change system_distributed and system_traces to RF=1 for decommission to pass in 2-node setup
        for (String ks : Arrays.asList("system_traces", "system_distributed"))
        {
            cluster.schemaChange("ALTER KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        }

        // verify data with concurrent decommission
        TestWithConcurrentVerification decommissionTest = new TestWithConcurrentVerification(() -> {
            try
            {
                verifyIndexQuery(1, table, num, num);
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }, () -> cluster.get(2).runOnInstance(() -> {
            try
            {
                StorageService.instance.decommission(false);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }));

        decommissionTest.start();
        cluster.get(2).shutdown().get();

        verifyIndexQuery(1, table, num, num);

        // node1 has all indexed data after decommission
        assertEquals(num, getIndexedCellCount(1, table, "v1"));
        assertEquals(num, getIndexedCellCount(1, table, "v2"));
    }

    private void upgradeSSTables(int node, String keyspace, String table)
    {
        cluster.get(node).runOnInstance(() -> {
            try
            {
                StorageService.instance.upgradeSSTables(keyspace, false, table);
            }
            catch (IOException | ExecutionException | InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        });
    }

    private void verifyIndexQuery(int node, String table, int numericIndexRows, int stringIndexRows) throws Exception
    {
        verifyNumericIndexQuery(node, table, numericIndexRows);
        verifyStringIndexQuery(node, table, stringIndexRows);
    }

    private void verifyNumericIndexQuery(int node, String table, int numericIndexRows) throws Exception
    {
        Object[][] result = cluster.coordinator(node).execute(String.format("SELECT id FROM %s.%s WHERE v1=0", KEYSPACE, table), ConsistencyLevel.ONE);
        assertEquals(numericIndexRows, result.length);
    }

    private void verifyStringIndexQuery(int node, String table, int stringIndexRows) throws Exception
    {
        Object[][] result = cluster.coordinator(node).execute(String.format("SELECT id FROM %s.%s WHERE v2='0'", KEYSPACE, table), ConsistencyLevel.ONE);
        assertEquals(stringIndexRows, result.length);
    }

    protected long getIndexedCellCount(int node, String table, String column) throws Exception
    {
        return cluster.get(node).callOnInstance(() -> {
            try
            {
                ColumnIdentifier columnID = ColumnIdentifier.getInterned(column, true);
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(table);
                String indexName = IndexMetadata.generateDefaultIndexName(table, columnID);
                StorageAttachedIndex index = (StorageAttachedIndex) cfs.indexManager.getIndexByName(indexName);
                return index.getContext().getCellCount();
            }
            catch (Throwable e)
            {
                return -1L;
            }
        });
    }

    protected static class TestWithConcurrentVerification
    {
        private final Runnable verificationTask;
        private final CountDownLatch verificationStarted = new CountDownLatch(1);

        private final Runnable targetTask;
        private final CountDownLatch taskCompleted = new CountDownLatch(1);

        private final int verificationIntervalInMs;
        private final int verificationMaxInMs = 30000; // 30s

        public TestWithConcurrentVerification(Runnable verificationTask, Runnable targetTask)
        {
            this(verificationTask, targetTask, 10);
        }

        /**
         * @param verificationTask to be run concurrently with target task
         * @param targetTask task to be performed once
         * @param verificationIntervalInMs interval between each verification task, -1 to run verification task once
         */
        public TestWithConcurrentVerification(Runnable verificationTask, Runnable targetTask, int verificationIntervalInMs)
        {
            this.verificationTask = verificationTask;
            this.targetTask = targetTask;
            this.verificationIntervalInMs = verificationIntervalInMs;
        }

        public void start()
        {
            Thread verificationThread = new Thread(() -> {
                verificationStarted.countDown();

                while (true)
                {
                    try
                    {
                        verificationTask.run();

                        if (verificationIntervalInMs < 0 || taskCompleted.await(verificationIntervalInMs, TimeUnit.MILLISECONDS))
                            break;
                    }
                    catch (Throwable e)
                    {
                        throw Throwables.unchecked(e);
                    }
                }
            });

            try
            {
                verificationThread.start();
                verificationStarted.await();

                targetTask.run();
                taskCompleted.countDown();

                verificationThread.join(verificationMaxInMs);
            }
            catch (InterruptedException e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
