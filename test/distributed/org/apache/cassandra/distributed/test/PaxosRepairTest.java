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
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.*;
import org.apache.cassandra.service.paxos.cleanup.PaxosCleanup;
import org.apache.cassandra.service.paxos.uncommitted.PaxosRows;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_FINISH_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_COMMIT_AND_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PREPARE_RSP;
import static org.apache.cassandra.net.Verb.PAXOS2_PROPOSE_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_PROPOSE_RSP;
import static org.apache.cassandra.net.Verb.PAXOS2_REPAIR_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_RSP;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;

public class PaxosRepairTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosRepairTest.class);
    private static final String TABLE = "tbl";

    static
    {
        CassandraRelevantProperties.PAXOS_USE_SELF_EXECUTION.setBoolean(false);
        DatabaseDescriptor.daemonInitialization();
    }

    private static int getUncommitted(IInvokableInstance instance, String keyspace, String table)
    {
        if (instance.isShutdown())
            return 0;
        int uncommitted = instance.callsOnInstance(() -> {
            TableMetadata meta = Schema.instance.getTableMetadata(keyspace, table);
            return Iterators.size(PaxosState.uncommittedTracker().uncommittedKeyIterator(meta.id, null));
        }).call();
        logger.info("{} has {} uncommitted instances", instance, uncommitted);
        return uncommitted;
    }

    private static void assertAllAlive(Cluster cluster)
    {
        Set<InetAddressAndPort> allEndpoints = cluster.stream().map(i -> InetAddressAndPort.getByAddress(i.broadcastAddress())).collect(Collectors.toSet());
        cluster.stream().forEach(instance -> {
            instance.runOnInstance(() -> {
                ImmutableSet<InetAddressAndPort> endpoints = Gossiper.instance.getEndpoints();
                Assert.assertEquals(allEndpoints, endpoints);
                for (InetAddressAndPort endpoint : endpoints)
                    Assert.assertTrue(FailureDetector.instance.isAlive(endpoint));
            });
        });
    }

    private static void assertUncommitted(IInvokableInstance instance, String ks, String table, int expected)
    {
        Assert.assertEquals(expected, getUncommitted(instance, ks, table));
    }

    private static boolean hasUncommitted(Cluster cluster, String ks, String table)
    {
        return cluster.stream().map(instance -> getUncommitted(instance, ks, table)).reduce((a, b) -> a + b).get() > 0;
    }

    private static boolean hasUncommittedQuorum(Cluster cluster, String ks, String table)
    {
        int uncommitted = 0;
        for (int i=0; i<cluster.size(); i++)
        {
            if (getUncommitted(cluster.get(i+1), ks, table) > 0)
                uncommitted++;
        }
        return uncommitted >= ((cluster.size() / 2) + 1);
    }

    private static void repair(Cluster cluster, String keyspace, String table, boolean force)
    {
        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, RepairParallelism.SEQUENTIAL.getName());
        options.put(RepairOption.PRIMARY_RANGE_KEY, Boolean.toString(false));
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.toString(false));
        options.put(RepairOption.JOB_THREADS_KEY, Integer.toString(1));
        options.put(RepairOption.TRACE_KEY, Boolean.toString(false));
        options.put(RepairOption.COLUMNFAMILIES_KEY, "");
        options.put(RepairOption.PULL_REPAIR_KEY, Boolean.toString(false));
        options.put(RepairOption.FORCE_REPAIR_KEY, Boolean.toString(force));
        options.put(RepairOption.PREVIEW, PreviewKind.NONE.toString());
        options.put(RepairOption.IGNORE_UNREPLICATED_KS, Boolean.toString(false));
        options.put(RepairOption.REPAIR_PAXOS_KEY, Boolean.toString(true));
        options.put(RepairOption.PAXOS_ONLY_KEY, Boolean.toString(true));

        cluster.get(1).runOnInstance(() -> {
            int cmd = StorageService.instance.repairAsync(keyspace, options);

            while (true)
            {
                try
                {
                    Thread.sleep(500);
                }
                catch (InterruptedException e)
                {
                    throw new AssertionError(e);
                }
                Pair<ActiveRepairService.ParentRepairStatus, List<String>> status = ActiveRepairService.instance().getRepairStatus(cmd);
                if (status == null)
                    continue;

                switch (status.left)
                {
                    case IN_PROGRESS:
                        continue;
                    case COMPLETED:
                        return;
                    default:
                        throw new AssertionError("Repair failed with errors: " + status.right);
                }
            }
        });
    }

    private static void repair(Cluster cluster, String keyspace, String table)
    {
        repair(cluster, keyspace, table, false);
    }

    private static final Consumer<IInstanceConfig> WITH_NETWORK = cfg -> {
        cfg.with(Feature.NETWORK);
        cfg.with(Feature.GOSSIP);
        cfg.set("paxos_purge_grace_period", "0s");
        cfg.set("paxos_state_purging", Config.PaxosStatePurging.repaired.toString());
        cfg.set("paxos_variant", "v2_without_linearizable_reads");
        cfg.set("truncate_request_timeout", "1000ms");
        cfg.set("partitioner", "ByteOrderedPartitioner");
        cfg.set("initial_token", ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(cfg.num() * 100)));
    };

    private static final Consumer<IInstanceConfig> WITHOUT_NETWORK = cfg -> {
        cfg.set("paxos_purge_grace_period", "0s");
        cfg.set("paxos_state_purging", Config.PaxosStatePurging.repaired.toString());
        cfg.set("paxos_variant", "v2_without_linearizable_reads");
        cfg.set("truncate_request_timeout", "1000ms");
        cfg.set("partitioner", "ByteOrderedPartitioner");
        cfg.set("initial_token", ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes(cfg.num() * 100)));
    };

    @Test
    public void paxosRepairTest() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = init(Cluster.build(3).withConfig(WITH_NETWORK).withoutVNodes().start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            Assert.assertFalse(hasUncommittedQuorum(cluster, KEYSPACE, TABLE));

            assertAllAlive(cluster);
            cluster.verbs(PAXOS_COMMIT_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (400, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }

            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));

            cluster.filters().reset();

            assertAllAlive(cluster);
            repair(cluster, KEYSPACE, TABLE);

            Assert.assertFalse(hasUncommitted(cluster, KEYSPACE, TABLE));

            cluster.forEach(i -> i.runOnInstance(() -> {
                compactPaxos();
                Map<Integer, PaxosRow> rows = getPaxosRows();
                assertLowBoundPurged(rows.values());
                Assert.assertEquals(Sets.newHashSet(400), rows.keySet());
            }));

            // check that operations occuring after the last repair are not purged
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (500, 3, 3) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            cluster.forEach(i -> i.runOnInstance(() -> {
                compactPaxos();
                Map<Integer, PaxosRow> rows = getPaxosRows();
                assertLowBoundPurged(rows.values());
                Assert.assertEquals(Sets.newHashSet(400, 500), rows.keySet());
            }));
        }
    }

    @Ignore
    @Test
    public void topologyChangePaxosTest() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = Cluster.build(4).withConfig(WITH_NETWORK).withoutVNodes().createWithoutStarting())
        {
            for (int i=1; i<=3; i++)
                cluster.get(i).startup();

            init(cluster);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);

            cluster.verbs(PAXOS_COMMIT_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (350, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }
            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));

            cluster.filters().reset();

            // node 4 starting should repair paxos and inform the other nodes of its gossip state
            cluster.get(4).startup();
            Assert.assertFalse(hasUncommittedQuorum(cluster, KEYSPACE, TABLE));
        }
    }

    @Test
    public void paxosCleanupWithReproposal() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(cfg -> cfg
                                                              .set("paxos_variant", "v2")
                                                              .set("paxos_purge_grace_period", "0s")
                                                              .set("paxos_state_purging", Config.PaxosStatePurging.repaired.toString())
                                                              .set("truncate_request_timeout", "1000ms"))
                                           .withoutVNodes()
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            cluster.verbs(PAXOS_COMMIT_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }
            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));
            cluster.forEach(i -> i.runOnInstance(() -> Keyspace.open("system").getColumnFamilyStore("paxos").forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS)));

            CountDownLatch haveFetchedLowBound = new CountDownLatch(1);
            CountDownLatch haveReproposed = new CountDownLatch(1);
            cluster.verbs(PAXOS2_CLEANUP_FINISH_PREPARE_REQ).inbound().messagesMatching((from, to, verb) -> {
                haveFetchedLowBound.countDown();
                Uninterruptibles.awaitUninterruptibly(haveReproposed);
                return false;
            }).drop();

            ExecutorService executor = Executors.newCachedThreadPool();
            List<InetAddressAndPort> endpoints = cluster.stream().map(IInstance::broadcastAddress).map(InetAddressAndPort::getByAddress).collect(Collectors.toList());
            Future<?> cleanup = cluster.get(1).appliesOnInstance((List<? extends InetSocketAddress> es, ExecutorService exec)-> {
                TableMetadata metadata = Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable(TABLE);
                return PaxosCleanup.cleanup(es.stream().map(InetAddressAndPort::getByAddress).collect(Collectors.toSet()), metadata, StorageService.instance.getLocalRanges(KEYSPACE), false, exec);
            }).apply(endpoints, executor);

            Uninterruptibles.awaitUninterruptibly(haveFetchedLowBound);
            IMessageFilters.Filter filter2 = cluster.verbs(PAXOS_COMMIT_REQ, PAXOS2_COMMIT_AND_PREPARE_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }
            filter2.off();
            haveReproposed.countDown();
            cluster.filters().reset();

            cleanup.get();
            ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);
            Assert.assertFalse(hasUncommitted(cluster, KEYSPACE, TABLE));
            cluster.forEach(i -> i.runOnInstance(PaxosRepairTest::compactPaxos));
            for (int i = 1 ; i <= 3 ; ++i)
                assertRows(cluster.get(i).executeInternal("SELECT * FROM " + KEYSPACE + '.' + TABLE + " WHERE pk = 1"), row(1, 1, 1));

            Assert.assertFalse(hasUncommittedQuorum(cluster, KEYSPACE, TABLE));
            assertLowBoundPurged(cluster);
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void paxosCleanupWithReproposalClashingTimestamp() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = init(Cluster.build(5)
                                           .withConfig(cfg -> cfg
                                                              .set("paxos_variant", "v2")
                                                              .set("paxos_purge_grace_period", "0s")
                                                              .set("paxos_cache_size", "0MiB")
                                                              .set("truncate_request_timeout", "1000ms"))
                                           .withoutVNodes()
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            // we ensure:
            //  - node 1 only witnesses a promise that conflicts with something we committed
            //  - node 2 does not witness the commit, so it has an in progress proposal
            //  - node 3 does not witness the proposal, so that we have an incomplete commit
            //  - node 1's response arrives first, so that it might retain its promise as latestWitnessed (without bugfix)

            CountDownLatch haveStartedCleanup = new CountDownLatch(1);
            CountDownLatch haveInsertedClashingPromise = new CountDownLatch(1);
            IMessageFilters.Filter pauseCleanupUntilCommitted = cluster.verbs(PAXOS2_CLEANUP_REQ).from(1).to(1).outbound().messagesMatching((from, to, verb) -> {
                haveStartedCleanup.countDown();
                Uninterruptibles.awaitUninterruptibly(haveInsertedClashingPromise);
                return false;
            }).drop();

            ExecutorService executor = Executors.newCachedThreadPool();
            List<InetAddressAndPort> endpoints = cluster.stream().map(i -> InetAddressAndPort.getByAddress(i.broadcastAddress())).collect(Collectors.toList());
            Future<?> cleanup = cluster.get(1).appliesOnInstance((List<? extends InetSocketAddress> es, ExecutorService exec)-> {
                TableMetadata metadata = Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable(TABLE);
                return PaxosCleanup.cleanup(es.stream().map(InetAddressAndPort::getByAddress).collect(Collectors.toSet()), metadata, StorageService.instance.getLocalRanges(KEYSPACE), false, exec);
            }).apply(endpoints, executor);

            IMessageFilters.Filter dropAllTo1 = cluster.verbs(PAXOS2_PREPARE_REQ, PAXOS2_PROPOSE_REQ, PAXOS_COMMIT_REQ).from(2).to(1).outbound().drop();
            IMessageFilters.Filter dropCommitTo3 = cluster.verbs(PAXOS_COMMIT_REQ).from(2).to(3).outbound().drop();
            IMessageFilters.Filter dropAcceptTo4 = cluster.verbs(PAXOS2_PROPOSE_REQ).from(2).to(4).outbound().drop();

            CountDownLatch haveFetchedClashingRepair = new CountDownLatch(1);
            AtomicIntegerArray fetchResponseIds = new AtomicIntegerArray(new int[] { -1, -1, -1, -1, -1, -1 });
            cluster.verbs(PAXOS2_REPAIR_REQ).outbound().from(1).messagesMatching((from, to, msg) -> {
                fetchResponseIds.set(to, msg.id());
                return false;
            }).drop();
            cluster.verbs(PAXOS2_PREPARE_RSP, PAXOS2_PROPOSE_RSP, PAXOS_COMMIT_RSP).outbound().to(1).messagesMatching((from, to, msg) -> {
                if (fetchResponseIds.get(from) == msg.id())
                {
                    if (from == 1) haveFetchedClashingRepair.countDown();
                    else Uninterruptibles.awaitUninterruptibly(haveFetchedClashingRepair);
                }
                return false;
            }).drop();

            Uninterruptibles.awaitUninterruptibly(haveStartedCleanup);
            cluster.coordinator(2).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.ONE);

            UUID cfId = cluster.get(2).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).metadata.id.asUUID());
            TimeUUID uuid = (TimeUUID) cluster.get(2).executeInternal("select in_progress_ballot from system.paxos WHERE row_key = ? and cf_id = ?", Int32Type.instance.decompose(1), cfId)[0][0];
            TimeUUID clashingUuid = TimeUUID.fromBytes(uuid.msb(), 0);
            cluster.get(1).executeInternal("update system.paxos set in_progress_ballot = ? WHERE row_key = ? and cf_id = ?", clashingUuid, Int32Type.instance.decompose(1), cfId);
            Assert.assertEquals(clashingUuid, cluster.get(1).executeInternal("select in_progress_ballot from system.paxos WHERE row_key = ? and cf_id = ?", Int32Type.instance.decompose(1), cfId)[0][0]);

            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));
            haveInsertedClashingPromise.countDown();

            cleanup.get();
            ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);
        }
    }

    @Test
    public void paxosCleanupWithDelayedProposal() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(cfg -> cfg
                                                              .set("paxos_variant", "v2")
                                                              .set("paxos_purge_grace_period", "0s")
                                                              .set("paxos_state_purging", Config.PaxosStatePurging.repaired.toString())
                                                              .set("truncate_request_timeout", "1000ms"))
                                           .withoutVNodes()
                                           .start())
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            CountDownLatch haveFinishedRepair = new CountDownLatch(1);
            cluster.verbs(PAXOS2_PREPARE_REQ).messagesMatching((from, to, verb) -> {
                Uninterruptibles.awaitUninterruptibly(haveFinishedRepair);
                return false;
            }).drop();
            cluster.verbs(PAXOS_COMMIT_REQ).drop();
            Future<?> insert = cluster.get(1).async(() -> {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }).call();
            cluster.verbs(PAXOS2_CLEANUP_FINISH_PREPARE_REQ).messagesMatching((from, to, verb) -> {
                haveFinishedRepair.countDown();
                try { insert.get(); } catch (Throwable t) {}
                cluster.filters().reset();
                return false;
            }).drop();

            ExecutorService executor = Executors.newCachedThreadPool();

            Uninterruptibles.sleepUninterruptibly(10L, TimeUnit.MILLISECONDS);

            List<InetAddressAndPort> endpoints = cluster.stream().map(i -> InetAddressAndPort.getByAddress(i.broadcastAddress())).collect(Collectors.toList());
            Future<?> cleanup = cluster.get(1).appliesOnInstance((List<? extends InetSocketAddress> es, ExecutorService exec)-> {
                TableMetadata metadata = Keyspace.open(KEYSPACE).getMetadata().getTableOrViewNullable(TABLE);
                return PaxosCleanup.cleanup(es.stream().map(InetAddressAndPort::getByAddress).collect(Collectors.toSet()), metadata, StorageService.instance.getLocalRanges(KEYSPACE), false, exec);
            }).apply(endpoints, executor);

            cleanup.get();
            try
            {
                insert.get();
            }
            catch (Throwable t)
            {
            }
            ExecutorUtils.shutdownNowAndWait(1L, TimeUnit.MINUTES, executor);
            Assert.assertFalse(hasUncommittedQuorum(cluster, KEYSPACE, TABLE));

            assertLowBoundPurged(cluster);
        }
    }

    private static void setVersion(IInvokableInstance instance, InetSocketAddress peer, String version)
    {
        instance.runOnInstance(() -> {
            Gossiper.runInGossipStageBlocking(() -> {
                EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(InetAddressAndPort.getByAddress(peer.getAddress()));
                VersionedValue value = version != null ? StorageService.instance.valueFactory.rack(version) : null;
                epState.addApplicationState(ApplicationState.RELEASE_VERSION, value);
            });
        });
    }

    private static void assertRepairFailsWithVersion(Cluster cluster, String version)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            setVersion(cluster.get(i), cluster.get(2).broadcastAddress(), version);
        try
        {
            repair(cluster, KEYSPACE, TABLE);
        }
        catch (AssertionError e)
        {
            return;
        }
        Assert.fail("Repair should have failed on unsupported version");
    }

    private static void assertRepairSucceedsWithVersion(Cluster cluster, String version)
    {
        for (int i = 1 ; i <= cluster.size() ; ++i)
            setVersion(cluster.get(i), cluster.get(2).broadcastAddress(), version);
        repair(cluster, KEYSPACE, TABLE);
    }

    @Test
    public void paxosRepairVersionGate() throws Throwable
    {
        // TODO: fails with vnode enabled
        try (Cluster cluster = init(Cluster.build(3).withConfig(WITHOUT_NETWORK).withoutVNodes().start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            Assert.assertFalse(hasUncommittedQuorum(cluster, KEYSPACE, TABLE));

            assertAllAlive(cluster);
            cluster.verbs(PAXOS_COMMIT_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (400, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }

            Assert.assertTrue(hasUncommitted(cluster, KEYSPACE, TABLE));

            cluster.filters().reset();

            assertAllAlive(cluster);

            assertRepairFailsWithVersion(cluster, "3.0.24");
            assertRepairFailsWithVersion(cluster, "4.0.0");

            // test valid versions
            assertRepairSucceedsWithVersion(cluster, "4.1.0");
        }
    }

    private static class PaxosRow
    {
        final DecoratedKey key;
        final Row row;

        PaxosRow(DecoratedKey key, Row row)
        {
            this.key = key;
            this.row = row;
        }

        public String toString()
        {
            TableMetadata table = Schema.instance.getTableMetadata(SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS);
            return ByteBufferUtil.bytesToHex(key.getKey()) + " -> " + row.toString(table, true);
        }
    }

    private static void compactPaxos()
    {
        ColumnFamilyStore paxos = Keyspace.open(SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(SystemKeyspace.PAXOS);
        FBUtilities.waitOnFuture(paxos.forceFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS));
        FBUtilities.waitOnFutures(CompactionManager.instance.submitMaximal(paxos, 0, false));
    }

    private static Map<Integer, PaxosRow> getPaxosRows()
    {
        Map<Integer, PaxosRow> rows = new HashMap<>();
        String queryStr = "SELECT * FROM " + SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PAXOS;
        SelectStatement stmt = (SelectStatement) QueryProcessor.parseStatement(queryStr).prepare(ClientState.forInternalCalls());
        ReadQuery query = stmt.getQuery(QueryOptions.DEFAULT, FBUtilities.nowInSeconds());
        try (ReadExecutionController controller = query.executionController(); PartitionIterator partitions = query.executeInternal(controller))
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    while (partition.hasNext())
                    {
                        rows.put(Int32Type.instance.compose(partition.partitionKey().getKey()),
                                 new PaxosRow(partition.partitionKey(), partition.next()));
                    }
                }
            }
        }
        return rows;
    }

    private static void assertLowBoundPurged(Collection<PaxosRow> rows)
    {
        Assert.assertEquals(0, DatabaseDescriptor.getPaxosPurgeGrace(SECONDS));
        String ip = FBUtilities.getBroadcastAddressAndPort().toString();
        for (PaxosRow row : rows)
        {
            Ballot keyLowBound = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).getPaxosRepairLowBound(row.key);
            Assert.assertTrue(ip, Commit.isAfter(keyLowBound, Ballot.none()));
            Assert.assertFalse(ip, PaxosRows.hasBallotBeforeOrEqualTo(row.row, keyLowBound));
        }
    }

    private static void assertLowBoundPurged(IInvokableInstance instance)
    {
        instance.runOnInstance(() -> assertLowBoundPurged(getPaxosRows().values()));
    }

    private static void assertLowBoundPurged(Cluster cluster)
    {
        cluster.forEach(PaxosRepairTest::assertLowBoundPurged);
    }
}
