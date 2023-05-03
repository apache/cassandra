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

package org.apache.cassandra.distributed.test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.utils.concurrent.Condition;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.PendingRangeCalculatorService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.repair.BlockingReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static net.bytebuddy.matcher.ElementMatchers.named;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT;
import static org.apache.cassandra.db.Keyspace.open;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertEquals;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.READ_REPAIR_REQ;
import static org.apache.cassandra.net.Verb.READ_REPAIR_RSP;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.concurrent.Condition.newOneTimeCondition;
import static org.junit.Assert.fail;

public class ReadRepairTest extends TestBaseImpl
{
    /**
     * Tests basic behaviour of read repair with {@code BLOCKING} read repair strategy.
     */
    @Test
    public void testBlockingReadRepair() throws Throwable
    {
        testReadRepair(ReadRepairStrategy.BLOCKING);
    }
    /**
     *
     * Tests basic behaviour of read repair with {@code NONE} read repair strategy.
     */
    @Test
    public void testNoneReadRepair() throws Throwable
    {
        testReadRepair(ReadRepairStrategy.NONE);
    }

    private void testReadRepair(ReadRepairStrategy strategy) throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY (k, c)) " +
                                              String.format("WITH read_repair='%s'", strategy)));

            Object[] row = row(1, 1, 1);
            String insertQuery = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
            String selectQuery = withKeyspace("SELECT * FROM %s.t WHERE k=1");

            // insert data in two nodes, simulating a quorum write that has missed one node
            cluster.get(1).executeInternal(insertQuery, row);
            cluster.get(2).executeInternal(insertQuery, row);

            // verify that the third node doesn't have the row
            assertRows(cluster.get(3).executeInternal(selectQuery));

            // read with CL=QUORUM to trigger read repair
            assertRows(cluster.coordinator(3).execute(selectQuery, QUORUM), row);

            // verify whether the coordinator has the repaired row depending on the read repair strategy
            if (strategy == ReadRepairStrategy.NONE)
                assertRows(cluster.get(3).executeInternal(selectQuery));
            else
                assertRows(cluster.get(3).executeInternal(selectQuery), row);
        }
    }

    @Test
    public void readRepairTimeoutTest() throws Throwable
    {
        final long reducedReadTimeout = 3000L;
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.forEach(i -> i.runOnInstance(() -> DatabaseDescriptor.setReadRpcTimeout(reducedReadTimeout)));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");
            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));
            cluster.verbs(READ_REPAIR_RSP).to(1).drop();
            final long start = currentTimeMillis();
            try
            {
                cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", ConsistencyLevel.ALL);
                fail("Read timeout expected but it did not occur");
            }
            catch (Exception ex)
            {
                // the containing exception class was loaded by another class loader. Comparing the message as a workaround to assert the exception
                Assert.assertTrue(ex.getClass().toString().contains("ReadTimeoutException"));
                long actualTimeTaken = currentTimeMillis() - start;
                long magicDelayAmount = 100L; // it might not be the best way to check if the time taken is around the timeout value.
                // Due to the delays, the actual time taken from client perspective is slighly more than the timeout value
                Assert.assertTrue(actualTimeTaken > reducedReadTimeout);
                // But it should not exceed too much
                Assert.assertTrue(actualTimeTaken < reducedReadTimeout + magicDelayAmount);
                assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"),
                           row(1, 1, 1)); // the partition happened when the repaired node sending back ack. The mutation should be in fact applied.
            }
        }
    }

    @Test
    public void failingReadRepairTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            for (int i = 1 ; i <= 2 ; ++i)
                cluster.get(i).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1)");

            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));

            cluster.filters().verbs(READ_REPAIR_REQ.id).to(3).drop();
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1",
                                                      ConsistencyLevel.QUORUM),
                       row(1, 1, 1));

            // Data was not repaired
            assertRows(cluster.get(3).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1"));
        }
    }

    @Test
    public void movingTokenReadRepairTest() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = init(Cluster.build(4).withoutVNodes().start(), 3))
        {
            List<Token> tokens = cluster.tokens();

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH read_repair='blocking'");

            int i = 0;
            while (true)
            {
                Token t = Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose(i));
                // the list of tokens uses zero-based numbering, whereas the cluster nodes use one-based numbering
                if (t.compareTo(tokens.get(2 - 1)) < 0 && t.compareTo(tokens.get(1 - 1)) > 0)
                    break;
                ++i;
            }

            // write only to #4
            cluster.get(4).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, 1, 1)", i);
            // mark #2 as leaving in #4
            cluster.get(4).acceptsOnInstance((InetSocketAddress endpoint) -> {
                StorageService.instance.getTokenMetadata().addLeavingEndpoint(InetAddressAndPort.getByAddressOverrideDefaults(endpoint.getAddress(), endpoint.getPort()));
                PendingRangeCalculatorService.instance.update();
                PendingRangeCalculatorService.instance.blockUntilFinished();
            }).accept(cluster.get(2).broadcastAddress());

            // prevent #4 from reading or writing to #3, so our QUORUM must contain #2 and #4
            // since #1 is taking over the range, this means any read-repair must make it to #1 as well
            // (as a speculative repair in this case, as we prefer to send repair mutations to the initial
            // set of read replicas, which are 2 and 3 here).
            cluster.filters().verbs(READ_REQ.id).from(4).to(3).drop();
            cluster.filters().verbs(READ_REPAIR_REQ.id).from(4).to(3).drop();
            assertRows(cluster.coordinator(4).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?",
                                                      ConsistencyLevel.QUORUM, i),
                       row(i, 1, 1));

            // verify that #1 receives the write
            assertRows(cluster.get(1).executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = ?", i),
                       row(i, 1, 1));
        }
    }

    /**
     * Test that there is no read repair with RF=1, and that altering it to RF>1 doesn't trigger any repair by
     * itself but following queries will use read repair accordingly with the new RF.
     */
    @Test
    public void alterRFAndRunReadRepair() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2).start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 1}"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int PRIMARY KEY, a int, b int)" +
                                              " WITH read_repair='blocking'"));

            // insert a row that will only get to one node due to the RF=1
            Object[] row = row(1, 1, 1);
            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.t (k, a, b) VALUES (?, ?, ?)"), row);

            // flush to ensure reads come from sstables
            cluster.get(1).flush(KEYSPACE);

            // at RF=1 it shouldn't matter which node we query, as the data should always come from the only replica
            String query = withKeyspace("SELECT * FROM %s.t WHERE k = 1");
            for (int i = 1; i <= cluster.size(); i++)
                assertRows(cluster.coordinator(i).execute(query, ALL), row);

            // at RF=1 the prevoius queries shouldn't have triggered read repair
            assertRows(cluster.get(1).executeInternal(query), row);
            assertRows(cluster.get(2).executeInternal(query));

            // alter RF
            ALLOW_ALTER_RF_DURING_RANGE_MOVEMENT.setBoolean(true);
            cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 2}"));

            // altering the RF shouldn't have triggered any read repair
            assertRows(cluster.get(1).executeInternal(query), row);
            assertRows(cluster.get(2).executeInternal(query));

            // query again at CL=ALL, this time the data should be repaired
            assertRows(cluster.coordinator(2).execute(query, ALL), row);
            assertRows(cluster.get(1).executeInternal(query), row);
            assertRows(cluster.get(2).executeInternal(query), row);
        }
    }

    @Test
    public void testRangeSliceQueryWithTombstonesInMemory() throws Throwable
    {
        testRangeSliceQueryWithTombstones(false);
    }

    @Test
    public void testRangeSliceQueryWithTombstonesOnDisk() throws Throwable
    {
        testRangeSliceQueryWithTombstones(true);
    }

    /**
     * Verify that range queries with CL>ONE don't do unnecessary read-repairs when there are tombstones.
     * <p>
     * See CASSANDRA-8989 and CASSANDRA-9502.
     * <p>
     * Migrated from Python dtest read_repair_test.py:TestReadRepair.test_range_slice_query_with_tombstones()
     */
    private void testRangeSliceQueryWithTombstones(boolean flush) throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, v int, PRIMARY KEY(k, c))"));

            ICoordinator coordinator = cluster.coordinator(1);

            // insert some rows in all nodes
            String insertQuery = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
            for (int k = 0; k < 10; k++)
            {
                for (int c = 0; c < 10; c++)
                    coordinator.execute(insertQuery, ALL, k, c, k * c);
            }

            // delete a subset of the inserted partitions, plus some others that don't exist
            String deletePartitionQuery = withKeyspace("DELETE FROM %s.t WHERE k = ?");
            for (int k = 5; k < 15; k++)
            {
                coordinator.execute(deletePartitionQuery, ALL, k);
            }

            // delete some of the rows of some of the partitions, including deleted and not deleted partitions
            String deleteRowQuery = withKeyspace("DELETE FROM %s.t WHERE k = ? AND c = ?");
            for (int k = 2; k < 7; k++)
            {
                for (int c = 0; c < 5; c++)
                    coordinator.execute(deleteRowQuery, ALL, k, c);
            }

            // delete some of the rows of some not-existent partitions, including deleted and never-written partitions
            for (int k = 12; k < 17; k++)
            {
                for (int c = 0; c < 5; c++)
                    coordinator.execute(deleteRowQuery, ALL, k, c);
            }

            // flush all the nodes if specified
            if (flush)
            {
                for (int n = 1; n <= cluster.size(); n++)
                    cluster.get(n).flush(KEYSPACE);
            }

            // run a bunch of queries verifying that they don't trigger read repair
            coordinator.execute(withKeyspace("SELECT * FROM %s.t LIMIT 100"), QUORUM);
            for (int k = 0; k < 15; k++)
            {
                coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE k=?"), QUORUM, k);
                for (int c = 0; c < 10; c++)
                {
                    coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE k=? AND c=?"), QUORUM, k, c);
                    coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE k=? AND c>?"), QUORUM, k, c);
                    coordinator.execute(withKeyspace("SELECT * FROM %s.t WHERE k=? AND c<?"), QUORUM, k, c);
                }
            }
            long requests = ReadRepairTester.readRepairRequestsCount(cluster.get(1), "t");
            assertEquals("No read repair requests were expected, found " + requests, 0, requests);
        }
    }

    @Test
    public void readRepairRTRangeMovementTest() throws Throwable
    {
        ExecutorService es = Executors.newFixedThreadPool(1);
        String key = "test1";
        try (Cluster cluster = init(Cluster.build()
                                           .withConfig(config -> config.with(Feature.GOSSIP, Feature.NETWORK)
                                                                       .set("read_request_timeout", String.format("%dms", Integer.MAX_VALUE)))
                                           .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(4))
                                           .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(4, "dc0", "rack0"))
                                           .withNodes(3)
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE distributed_test_keyspace.tbl (\n" +
                                 "    key text,\n" +
                                 "    column1 int,\n" +
                                 "    PRIMARY KEY (key, column1)\n" +
                                 ") WITH CLUSTERING ORDER BY (column1 ASC)");

            cluster.forEach(i -> i.runOnInstance(() -> open(KEYSPACE).getColumnFamilyStore("tbl").disableAutoCompaction()));

            for (int i = 1; i <= 2; i++)
            {
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 50 WHERE key=?;", key);
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 80 WHERE key=? and column1 >= ? and column1 < ?;", key, 10, 100);
                cluster.get(i).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 70 WHERE key=? and column1 = ?;", key, 30);
                cluster.get(i).flush(KEYSPACE);
            }
            cluster.get(3).executeInternal("DELETE FROM distributed_test_keyspace.tbl USING TIMESTAMP 100 WHERE key=?;", key);
            cluster.get(3).flush(KEYSPACE);

            // pause the read until we have bootstrapped a new node below
            Condition continueRead = newOneTimeCondition();
            Condition readStarted = newOneTimeCondition();
            cluster.filters().outbound().from(3).to(1,2).verbs(READ_REQ.id).messagesMatching((i, i1, iMessage) -> {
                try
                {
                    readStarted.signalAll();
                    continueRead.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                return false;
            }).drop();
            Future<Object[][]> read = es.submit(() -> cluster.coordinator(3)
                                                          .execute("SELECT * FROM distributed_test_keyspace.tbl WHERE key=? and column1 >= ? and column1 <= ?",
                                                                   ALL, key, 20, 40));
            readStarted.await();
            IInstanceConfig config = cluster.newInstanceConfig();
            config.set("auto_bootstrap", true);
            cluster.bootstrap(config).startup();
            continueRead.signalAll();
            read.get();
        }
        finally
        {
            es.shutdown();
        }
    }

    /**
     * Range queries before CASSANDRA-11427 will trigger read repairs for puregable tombstones on hosts that already
     * compacted given tombstones. This will result in constant transfer and compaction actions sourced by few nodes
     * seeding purgeable tombstones and triggered e.g. by periodical jobs scanning data range wise.
     * <p>
     * See CASSANDRA-11427.
     * <p>
     * Migrated from Python dtest read_repair_test.py:TestReadRepair.test_gcable_tombstone_resurrection_on_range_slice_query()
     */
    @Test
    public void testGCableTombstoneResurrectionOnRangeSliceQuery() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k int, c int, PRIMARY KEY(k, c)) " +
                                              "WITH gc_grace_seconds=0 AND compaction = " +
                                              "{'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}"));

            ICoordinator coordinator = cluster.coordinator(1);

            // insert some data
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c) VALUES (0, 0)"), ALL);
            coordinator.execute(withKeyspace("INSERT INTO %s.t(k, c) VALUES (1, 1)"), ALL);

            // create partition tombstones in all nodes for both existent and not existent partitions
            coordinator.execute(withKeyspace("DELETE FROM %s.t WHERE k=0"), ALL); // exists
            coordinator.execute(withKeyspace("DELETE FROM %s.t WHERE k=2"), ALL); // doesn't exist

            // create row tombstones in all nodes for both existent and not existent rows
            coordinator.execute(withKeyspace("DELETE FROM %s.t WHERE k=1 AND c=1"), ALL); // exists
            coordinator.execute(withKeyspace("DELETE FROM %s.t WHERE k=3 AND c=1"), ALL); // doesn't exist

            // flush single sstable with tombstones
            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);

            // purge tombstones from node2 with compaction (gc_grace_seconds=0)
            cluster.get(2).forceCompact(KEYSPACE, "t");

            // run an unrestricted range query verifying that it doesn't trigger read repair
            coordinator.execute(withKeyspace("SELECT * FROM %s.t"), ALL);
            long requests = ReadRepairTester.readRepairRequestsCount(cluster.get(1), "t");
            assertEquals("No read repair requests were expected, found " + requests, 0, requests);
        }
    }

    @Test
    public void partitionDeletionRTTimestampTieTest() throws Throwable
    {
        try (Cluster cluster = init(builder()
                                    .withNodes(3)
                                    .withInstanceInitializer(RRHelper::install)
                                    .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE distributed_test_keyspace.tbl0 (pk bigint,ck bigint,value bigint, PRIMARY KEY (pk, ck)) WITH  CLUSTERING ORDER BY (ck ASC) AND read_repair='blocking';"));
            long pk = 0L;
            cluster.coordinator(1).execute("INSERT INTO distributed_test_keyspace.tbl0 (pk, ck, value) VALUES (?,?,?) USING TIMESTAMP 1", ConsistencyLevel.ALL, pk, 1L, 1L);
            cluster.coordinator(1).execute("DELETE FROM distributed_test_keyspace.tbl0 USING TIMESTAMP 2 WHERE pk=? AND ck>?;", ConsistencyLevel.ALL, pk, 2L);
            cluster.get(3).executeInternal("DELETE FROM distributed_test_keyspace.tbl0 USING TIMESTAMP 2 WHERE pk=?;", pk);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM distributed_test_keyspace.tbl0 WHERE pk=? AND ck>=? AND ck<?;",
                                                      ConsistencyLevel.ALL, pk, 1L, 3L));
        }
    }

    public static class RRHelper
    {
        static void install(ClassLoader cl, int nodeNumber)
        {
            // Only on coordinating node
            if (nodeNumber == 1)
            {
                new ByteBuddy().rebase(BlockingReadRepair.class)
                               .method(named("repairPartition"))
                               .intercept(MethodDelegation.to(RRHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        // This verifies new behaviour in 4.0 that was introduced in CASSANDRA-15369, but did not work
        // on timestamp tie of RT and partition deletion: we should not generate RT bounds in such case,
        // since monotonicity is already ensured by the partition deletion, and RT is unnecessary there.
        // For details, see CASSANDRA-16453.
        public static Object repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan, @SuperCall Callable<Void> r) throws Exception
        {
            Assert.assertEquals(2, mutations.size());
            for (Mutation value : mutations.values())
            {
                for (PartitionUpdate update : value.getPartitionUpdates())
                {
                    Assert.assertFalse(update.hasRows());
                    Assert.assertFalse(update.partitionLevelDeletion().isLive());
                }
            }
            return r.call();
        }
    }
}
