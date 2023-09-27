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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.awaitility.Awaitility;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.exceptions.CasWriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.service.paxos.uncommitted.PaxosKeyState;
import org.apache.cassandra.service.paxos.uncommitted.PaxosRows;
import org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTracker;
import org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedTracker.UpdateSupplier;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.AUTO_REPAIR_FREQUENCY_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_PAXOS_AUTO_REPAIRS;
import static org.apache.cassandra.schema.SchemaConstants.SYSTEM_KEYSPACE_NAME;
import static org.apache.cassandra.service.paxos.Ballot.Flag.GLOBAL;
import static org.apache.cassandra.service.paxos.BallotGenerator.Global.staleBallot;

import org.apache.cassandra.utils.CloseableIterator;

// quick workaround for metaspace ooms, will properly reuse clusters later
public class PaxosRepair2Test extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosRepair2Test.class);
    private static final String TABLE = "tbl";
    public static final String OFFSETTABLE_CLOCK_NAME = OffsettableClock.class.getName();

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
            TableMetadata cfm = Schema.instance.getTableMetadata(keyspace, table);
            return Iterators.size(PaxosState.uncommittedTracker().uncommittedKeyIterator(cfm.id, null));
        }).call();
        logger.info("{} has {} uncommitted instances", instance, uncommitted);
        return uncommitted;
    }

    private static void assertUncommitted(IInvokableInstance instance, String ks, String table, int expected)
    {
        Assert.assertEquals(expected, getUncommitted(instance, ks, table));
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

    @Test
    public void paxosRepairPreventsStaleReproposal() throws Throwable
    {
        Ballot staleBallot = Paxos.newBallot(Ballot.none(), org.apache.cassandra.db.ConsistencyLevel.SERIAL);
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg
                                                             .set("paxos_variant", "v2")
                                                             .set("paxos_purge_grace_period", "0s")
                                                             .set("truncate_request_timeout_in_ms", 1000L)))
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int primary key, v int)");
            repair(cluster, KEYSPACE, TABLE);

            // stop and start node 2 to test loading paxos repair history from disk
            cluster.get(2).shutdown().get();
            cluster.get(2).startup();

            for (int i=0; i<cluster.size(); i++)
            {
                cluster.get(i+1).runOnInstance(() -> {
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                    DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(1));
                    Assert.assertFalse(FBUtilities.getBroadcastAddressAndPort().toString(), Commit.isAfter(staleBallot, cfs.getPaxosRepairLowBound(key)));
                });
            }

            // add in the stale proposal
            cluster.get(1).runOnInstance(() -> {
                TableMetadata cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
                DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(1));
                ColumnMetadata cdef = cfm.getColumn(new ColumnIdentifier("v", false));
                Cell cell = BufferCell.live(cdef, staleBallot.unixMicros(), ByteBufferUtil.bytes(1));
                Row row = BTreeRow.singleCellRow(Clustering.EMPTY, cell);
                PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, key, row);
                Commit.Proposal proposal = new Commit.Proposal(staleBallot, update);
                SystemKeyspace.savePaxosProposal(proposal);
            });

            // shutdown node 3 so we're guaranteed to see the stale proposal
            cluster.get(3).shutdown().get();

            // the stale inflight proposal should be ignored and the query should succeed
            String query = "INSERT INTO " + KEYSPACE + '.' + TABLE + " (k, v) VALUES (1, 2) IF NOT EXISTS";
            Object[][] result = cluster.coordinator(1).execute(query, ConsistencyLevel.QUORUM);
            Assert.assertEquals(new Object[][]{new Object[]{ true }}, result);

            assertLowBoundPurged(cluster.get(1));
            assertLowBoundPurged(cluster.get(2));
        }
    }

    @Test
    public void paxosRepairHistoryIsntUpdatedInForcedRepair() throws Throwable
    {
        Ballot staleBallot = staleBallot(System.currentTimeMillis() - 1000000, System.currentTimeMillis() - 100000, GLOBAL);
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg.with(Feature.GOSSIP, Feature.NETWORK)
                                                                .set("paxos_variant", "v2")
                                                                .set("truncate_request_timeout_in_ms", 1000L)))
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k int primary key, v int)");
            ClusterUtils.stopUnchecked(cluster.get(3));
            InetAddressAndPort node3 = InetAddressAndPort.getByAddress(cluster.get(3).broadcastAddress());

            // make sure node1 knows node3 is down
            Awaitility.waitAtMost(1,TimeUnit.MINUTES).until(
            () -> !cluster.get(1).callOnInstance(() -> FailureDetector.instance.isAlive(node3)));

            repair(cluster, KEYSPACE, TABLE, true);
            for (int i = 0; i < cluster.size() - 1; i++)
            {
                cluster.get(i + 1).runOnInstance(() -> {
                    Assert.assertFalse(CassandraRelevantProperties.CLOCK_GLOBAL.isPresent());
                    ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
                    DecoratedKey key = cfs.decorateKey(ByteBufferUtil.bytes(1));
                    Assert.assertTrue(FBUtilities.getBroadcastAddressAndPort().toString(), Commit.isAfter(staleBallot, cfs.getPaxosRepairLowBound(key)));
                });
            }
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
            TableMetadata cfm = Schema.instance.getTableMetadata(SYSTEM_KEYSPACE_NAME, SystemKeyspace.PAXOS);
            return ByteBufferUtil.bytesToHex(key.getKey()) + " -> " + row.toString(cfm, true);
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
                RowIterator partition = partitions.next();
                while (partition.hasNext())
                {
                    rows.put(Int32Type.instance.compose(partition.partitionKey().getKey()),
                             new PaxosRow(partition.partitionKey(), partition.next()));
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
        cluster.forEach(PaxosRepair2Test::assertLowBoundPurged);
    }

    @Test
    public void paxosAutoRepair() throws Throwable
    {
        try (WithProperties properties = new WithProperties().set(AUTO_REPAIR_FREQUENCY_SECONDS, 1).set(DISABLE_PAXOS_AUTO_REPAIRS, true);
             Cluster cluster = init(Cluster.create(3, cfg -> cfg
                                                             .set("paxos_variant", "v2")
                                                             .set("paxos_repair_enabled", true)
                                                             .set("truncate_request_timeout_in_ms", 1000L)));
             )
        {
            cluster.forEach(i -> {
                Assert.assertFalse(CassandraRelevantProperties.CLOCK_GLOBAL.isPresent());
                Assert.assertEquals(1, CassandraRelevantProperties.AUTO_REPAIR_FREQUENCY_SECONDS.getInt());
                Assert.assertTrue(CassandraRelevantProperties.DISABLE_PAXOS_AUTO_REPAIRS.getBoolean());
            });
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            ClusterUtils.stopUnchecked(cluster.get(3));
            cluster.verbs(Verb.PAXOS_COMMIT_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (Throwable t)
            {
                // expected
            }
            assertUncommitted(cluster.get(1), KEYSPACE, TABLE, 1);
            assertUncommitted(cluster.get(2), KEYSPACE, TABLE, 1);

            cluster.filters().reset();
            // paxos table needs at least 1 flush to be picked up by auto-repairs
            cluster.get(1).flush("system");
            cluster.get(2).flush("system");
            // re-enable repairs
            cluster.get(1).runOnInstance(() -> StorageService.instance.setPaxosAutoRepairsEnabled(true));
            cluster.get(2).runOnInstance(() -> StorageService.instance.setPaxosAutoRepairsEnabled(true));
            Thread.sleep(2000);
            for (int i=0; i<20; i++)
            {
                if (!cluster.get(1).callsOnInstance(() -> PaxosState.uncommittedTracker().hasInflightAutoRepairs()).call()
                    && !cluster.get(2).callsOnInstance(() -> PaxosState.uncommittedTracker().hasInflightAutoRepairs()).call())
                    break;
                logger.info("Waiting for auto repairs to finish...");
                Thread.sleep(1000);
            }
            assertUncommitted(cluster.get(1), KEYSPACE, TABLE, 0);
            assertUncommitted(cluster.get(2), KEYSPACE, TABLE, 0);
        }
    }

    @Test
    public void paxosPurgeGraceSeconds() throws Exception
    {
        int graceSeconds = 5;
        try (Cluster cluster = init(Cluster.create(3, cfg -> cfg
                                                             .set("paxos_variant", "v2")
                                                             .set("paxos_purge_grace_period", graceSeconds + "s")
                                                             .set("paxos_state_purging", Config.PaxosStatePurging.repaired.toString())
                                                             .set("truncate_request_timeout_in_ms", 1000L)))
        )
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);

            repair(cluster, KEYSPACE, TABLE);
            cluster.forEach(i -> i.runOnInstance(() -> {
                Assert.assertFalse(CassandraRelevantProperties.CLOCK_GLOBAL.isPresent());
                compactPaxos();
                Map<Integer, PaxosRow> rows = getPaxosRows();
                Assert.assertEquals(Sets.newHashSet(1), rows.keySet());
            }));

            // wait for the grace period to pass, repair again, and the rows should be removed
            Thread.sleep((graceSeconds + 1) * 1000);
            repair(cluster, KEYSPACE, TABLE);
            cluster.forEach(i -> i.runOnInstance(() -> {
                compactPaxos();
                Map<Integer, PaxosRow> rows = getPaxosRows();
                Assert.assertEquals(Sets.newHashSet(), rows.keySet());
            }));
        }
    }

    static void assertTimeout(Runnable runnable)
    {
        try
        {
            runnable.run();
            Assert.fail("timeout expected");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(CasWriteTimeoutException.class.getName(), e.getClass().getName());
        }
    }

    private static int ballotDeletion(Commit commit)
    {
        return (int) TimeUnit.MICROSECONDS.toSeconds(commit.ballot.unixMicros()) + SystemKeyspace.legacyPaxosTtlSec(commit.update.metadata());
    }

    private static void backdateTimestamps(int seconds)
    {
        long offsetMillis = SECONDS.toMillis(seconds);
        ClientState.resetLastTimestamp(System.currentTimeMillis() - offsetMillis);
        OffsettableClock.offsetMillis = -offsetMillis;
    }

    public static class OffsettableClock implements Clock
    {
        private static volatile long offsetMillis = 0;
        public long nanoTime()
        {
            return System.nanoTime(); // checkstyle: permit system clock
        }

        public long currentTimeMillis()
        {
            return System.currentTimeMillis() + offsetMillis; // checkstyle: permit system clock
        }
    }

    @Test
    public void legacyPurgeRepairLoop() throws Exception
    {
        try
        {
            CassandraRelevantProperties.CLOCK_GLOBAL.setString(OFFSETTABLE_CLOCK_NAME);
            try (Cluster cluster = init(Cluster.create(3, cfg -> cfg
                                                                 .set("paxos_variant", "v2")
                                                                 .set("paxos_state_purging", "legacy")
                                                                 .set("paxos_purge_grace_period", "0s")
                                                                 .set("truncate_request_timeout_in_ms", 1000L)))
            )
            {
                cluster.forEach(i -> Assert.assertEquals(OFFSETTABLE_CLOCK_NAME, CassandraRelevantProperties.CLOCK_GLOBAL.getString()));
                int ttl = 3 * 3600;
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH gc_grace_seconds=" + ttl);

                // prepare an operation ttl + 1 hour into the past on a single node
                cluster.forEach(instance -> {
                    instance.runOnInstance(() -> {
                        Assert.assertEquals(OFFSETTABLE_CLOCK_NAME, CassandraRelevantProperties.CLOCK_GLOBAL.getString());
                        backdateTimestamps(ttl + 3600);
                    });
                });
                cluster.filters().inbound().to(1, 2).drop();
                assertTimeout(() -> cluster.coordinator(3).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (400, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM));
                Ballot oldBallot = Ballot.fromUuid(cluster.get(3).callOnInstance(() -> {
                    TableMetadata cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
                    DecoratedKey dk = cfm.partitioner.decorateKey(ByteBufferUtil.bytes(400));
                    try (PaxosState state = PaxosState.get(dk, cfm))
                    {
                        return state.currentSnapshot().promised.asUUID();
                    }
                }));

                assertUncommitted(cluster.get(1), KEYSPACE, TABLE, 0);
                assertUncommitted(cluster.get(2), KEYSPACE, TABLE, 0);
                assertUncommitted(cluster.get(3), KEYSPACE, TABLE, 1);

                // commit an operation just over ttl in the past on the other nodes
                cluster.filters().reset();
                cluster.filters().inbound().to(2).drop();
                cluster.forEach(instance -> {
                    instance.runOnInstance(() -> {
                        backdateTimestamps(ttl + 2);
                    });
                });
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, ck, v) VALUES (400, 2, 2) IF NOT EXISTS", ConsistencyLevel.QUORUM);

                // expire the cache entries
                long nowInSec = FBUtilities.nowInSeconds();
                cluster.get(1).runOnInstance(() -> {
                    TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
                    DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(400));
                    try (PaxosState state = PaxosState.get(dk, table))
                    {
                        state.updateStateUnsafe(s -> {
                            Assert.assertNull(s.accepted);
                            Assert.assertTrue(Commit.isAfter(s.committed.ballot, oldBallot));
                            Commit.CommittedWithTTL committed = new Commit.CommittedWithTTL(s.committed.ballot,
                                                                                            s.committed.update,
                                                                                            ballotDeletion(s.committed));
                            Assert.assertTrue(committed.localDeletionTime < nowInSec);
                            return new PaxosState.Snapshot(Ballot.none(), Ballot.none(), null, committed);
                        });
                    }
                });

                cluster.get(3).runOnInstance(() -> {
                    TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
                    DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(400));
                    try (PaxosState state = PaxosState.get(dk, table))
                    {
                        state.updateStateUnsafe(s -> {
                            Assert.assertNull(s.accepted);
                            Assert.assertTrue(Commit.isAfter(s.committed.ballot, oldBallot));
                            Commit.CommittedWithTTL committed = new Commit.CommittedWithTTL(s.committed.ballot,
                                                                                            s.committed.update,
                                                                                            ballotDeletion(s.committed));
                            Assert.assertTrue(committed.localDeletionTime < nowInSec);
                            return new PaxosState.Snapshot(oldBallot, oldBallot, null, committed);
                        });
                    }
                });

                cluster.forEach(instance -> {
                    instance.runOnInstance(() -> {
                        backdateTimestamps(0);
                    });
                });

                cluster.filters().reset();
                cluster.filters().inbound().to(2).drop();
                cluster.get(3).runOnInstance(() -> {

                    TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
                    DecoratedKey dk = table.partitioner.decorateKey(ByteBufferUtil.bytes(400));

                    UpdateSupplier supplier = PaxosState.uncommittedTracker().unsafGetUpdateSupplier();
                    try
                    {
                        PaxosUncommittedTracker.unsafSetUpdateSupplier(new SingleUpdateSupplier(table, dk, oldBallot));
                        StorageService.instance.autoRepairPaxos(table.id).get();
                    }
                    catch (Exception e)
                    {
                        throw new RuntimeException(e);
                    }
                    finally
                    {
                        PaxosUncommittedTracker.unsafSetUpdateSupplier(supplier);
                    }
                });

                assertUncommitted(cluster.get(1), KEYSPACE, TABLE, 0);
                assertUncommitted(cluster.get(2), KEYSPACE, TABLE, 0);
                assertUncommitted(cluster.get(3), KEYSPACE, TABLE, 0);
            }
        }
        finally
        {
            CassandraRelevantProperties.CLOCK_GLOBAL.reset();
        }
    }

    private static class SingleUpdateSupplier implements UpdateSupplier
    {
        private final TableMetadata cfm;
        private final DecoratedKey dk;
        private final Ballot ballot;

        public SingleUpdateSupplier(TableMetadata cfm, DecoratedKey dk, Ballot ballot)
        {
            this.cfm = cfm;
            this.dk = dk;
            this.ballot = ballot;
        }

        public CloseableIterator<PaxosKeyState> repairIterator(TableId cfId, Collection<Range<Token>> ranges)
        {
            if (!cfId.equals(cfm.id))
                return CloseableIterator.empty();
            return CloseableIterator.wrap(Collections.singleton(new PaxosKeyState(cfId, dk, ballot, false)).iterator());
        }

        public CloseableIterator<PaxosKeyState> flushIterator(Memtable paxos)
        {
            throw new UnsupportedOperationException();
        }
    }
}
