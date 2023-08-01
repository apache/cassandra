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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.concurrent.SEPExecutor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.StatsComponent;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.utils.DiagnosticSnapshotService;
import org.apache.cassandra.utils.Throwables;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RepairDigestTrackingTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";
    private static final String KS_TABLE = KEYSPACE + '.' + TABLE;

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void testInconsistenciesFound() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {

            cluster.get(1).runOnInstance(() -> StorageProxy.instance.enableRepairedDataTrackingForRangeReads());

            cluster.schemaChange("CREATE TABLE " + KS_TABLE+ " (k INT, c INT, v INT, PRIMARY KEY (k,c)) with read_repair='NONE'");
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (?, ?, ?)",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }
            cluster.forEach(i -> i.flush(KEYSPACE));

            for (int i = 10; i < 20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (?, ?, ?)",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }
            cluster.forEach(i -> i.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(assertNotRepaired()));

            // mark everything on node 2 repaired
            cluster.get(2).runOnInstance(markAllRepaired());
            cluster.get(2).runOnInstance(assertRepaired());

            // insert more data on node1 to generate an initial mismatch
            cluster.get(1).executeInternal("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (?, ?, ?)", 5, 5, 55);
            cluster.get(1).runOnInstance(assertNotRepaired());

            long ccBefore = getConfirmedInconsistencies(cluster.get(1));
            cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE, ConsistencyLevel.ALL);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1));
            Assert.assertEquals("confirmed count should differ by 1 after range read", ccBefore + 1, ccAfter);
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void testPurgeableTombstonesAreIgnored() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(2).start()))
        {
            cluster.get(1).runOnInstance(() -> StorageProxy.instance.enableRepairedDataTrackingForRangeReads());

            cluster.schemaChange("CREATE TABLE " + KS_TABLE + " (k INT, c INT, v1 INT, v2 INT, PRIMARY KEY (k,c)) WITH gc_grace_seconds=0");
            // on node1 only insert some tombstones, then flush
            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).executeInternal("DELETE v1 FROM " + KS_TABLE + " USING TIMESTAMP 0 WHERE k=? and c=? ", i, i);
            }
            cluster.get(1).flush(KEYSPACE);

            // insert data on both nodes and flush
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v2) VALUES (?, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }
            cluster.forEach(i -> i.flush(KEYSPACE));

            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(assertNotRepaired()));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(markAllRepaired()));
            cluster.forEach(i -> i.runOnInstance(assertRepaired()));

            // now overwrite on node2 only to generate digest mismatches, but don't flush so the repaired dataset is not affected
            for (int i = 0; i < 10; i++)
            {
                cluster.get(2).executeInternal("INSERT INTO " + KS_TABLE + " (k, c, v2) VALUES (?, ?, ?) USING TIMESTAMP 2", i, i, i * 2);
            }

            long ccBefore = getConfirmedInconsistencies(cluster.get(1));
            // Unfortunately we need to sleep here to ensure that nowInSec > the local deletion time of the tombstones
            TimeUnit.SECONDS.sleep(2);
            cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE, ConsistencyLevel.ALL);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1));

            Assert.assertEquals("No repaired data inconsistencies should be detected", ccBefore, ccAfter);
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void testSnapshottingOnInconsistency() throws Throwable
    {
        try (Cluster cluster = init(Cluster.create(2)))
        {
            cluster.get(1).runOnInstance(() -> StorageProxy.instance.enableRepairedDataTrackingForPartitionReads());

            cluster.schemaChange("CREATE TABLE " + KS_TABLE + " (k INT, c INT, v INT, PRIMARY KEY (k,c))");
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));

            for (int i = 10; i < 20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (0, ?, ?)",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(assertNotRepaired()));
            // Mark everything repaired on node2
            cluster.get(2).runOnInstance(markAllRepaired());
            cluster.get(2).runOnInstance(assertRepaired());

            // now overwrite on node1 only to generate digest mismatches
            cluster.get(1).executeInternal("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (0, ?, ?)", 5, 55);
            cluster.get(1).runOnInstance(assertNotRepaired());

            // Execute a partition read and assert inconsistency is detected (as nothing is repaired on node1)
            long ccBefore = getConfirmedInconsistencies(cluster.get(1));
            cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " WHERE k=0", ConsistencyLevel.ALL);
            long ccAfter = getConfirmedInconsistencies(cluster.get(1));
            Assert.assertEquals("confirmed count should increment by 1 after each partition read", ccBefore + 1, ccAfter);

            String snapshotName = DiagnosticSnapshotService.getSnapshotName(DiagnosticSnapshotService.REPAIRED_DATA_MISMATCH_SNAPSHOT_PREFIX);

            cluster.forEach(i -> i.runOnInstance(assertSnapshotNotPresent(snapshotName)));

            // re-introduce a mismatch, enable snapshotting and try again
            cluster.get(1).executeInternal("INSERT INTO " + KS_TABLE + " (k, c, v) VALUES (0, ?, ?)", 5, 555);
            cluster.get(1).runOnInstance(() -> StorageProxy.instance.enableSnapshotOnRepairedDataMismatch());

            cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " WHERE k=0", ConsistencyLevel.ALL);
            ccAfter = getConfirmedInconsistencies(cluster.get(1));
            Assert.assertEquals("confirmed count should increment by 1 after each partition read", ccBefore + 2, ccAfter);

            cluster.forEach(i -> i.runOnInstance(assertSnapshotPresent(snapshotName)));
        }
    }

    @Test
    public void testRepairedReadCountNormalizationWithInitialUnderread() throws Throwable
    {
        // Asserts that the amount of repaired data read for digest generation is consistent
        // across replicas where one has to read less repaired data to satisfy the original
        // limits of the read request.
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KS_TABLE + " (k INT, c INT, v1 INT, PRIMARY KEY (k,c)) " +
                                 "WITH CLUSTERING ORDER BY (c DESC)");

            // insert data on both nodes and flush
            for (int i=0; i<20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 0",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(assertNotRepaired()));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(markAllRepaired()));
            cluster.forEach(i -> i.runOnInstance(assertRepaired()));

            // Add some unrepaired data to both nodes
            for (int i=20; i<30; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            // And some more unrepaired data to node2 only. This causes node2 to read less repaired data than node1
            // when satisfying the limits of the read. So node2 needs to overread more repaired data than node1 when
            // calculating the repaired data digest.
            cluster.get(2).executeInternal("INSERT INTO "  + KS_TABLE + " (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1", 30, 30);

            // Verify single partition read
            long ccBefore = getConfirmedInconsistencies(cluster.get(1));
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " WHERE k=1 LIMIT 20", ConsistencyLevel.ALL),
                       rows(1, 30, 11));
            long ccAfterPartitionRead = getConfirmedInconsistencies(cluster.get(1));

            // Recreate a mismatch in unrepaired data and verify partition range read
            cluster.get(2).executeInternal("INSERT INTO "  + KS_TABLE + " (k, c, v1) VALUES (1, ?, ?)", 31, 31);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " LIMIT 30", ConsistencyLevel.ALL),
                       rows(1, 31, 2));
            long ccAfterRangeRead = getConfirmedInconsistencies(cluster.get(1));

            if (ccAfterPartitionRead != ccAfterRangeRead)
                if (ccAfterPartitionRead != ccBefore)
                    fail("Both range and partition reads reported data inconsistencies but none were expected");
                else
                    fail("Reported inconsistency during range read but none were expected");
            else if (ccAfterPartitionRead != ccBefore)
                fail("Reported inconsistency during partition read but none were expected");
        }
    }

    @Test
    public void testRepairedReadCountNormalizationWithInitialOverread() throws Throwable
    {
        // Asserts that the amount of repaired data read for digest generation is consistent
        // across replicas where one has to read more repaired data to satisfy the original
        // limits of the read request.
        try (Cluster cluster = init(Cluster.create(2)))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
                StorageProxy.instance.enableRepairedDataTrackingForPartitionReads();
            });

            cluster.schemaChange("CREATE TABLE " + KS_TABLE + " (k INT, c INT, v1 INT, PRIMARY KEY (k,c)) " +
                                 "WITH CLUSTERING ORDER BY (c DESC)");

            // insert data on both nodes and flush
            for (int i=0; i<10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 0",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // nothing is repaired yet
            cluster.forEach(i -> i.runOnInstance(assertNotRepaired()));
            // mark everything repaired
            cluster.forEach(i -> i.runOnInstance(markAllRepaired()));
            cluster.forEach(i -> i.runOnInstance(assertRepaired()));

            // Add some unrepaired data to both nodes
            for (int i=10; i<13; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (0, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
                cluster.coordinator(1).execute("INSERT INTO " + KS_TABLE + " (k, c, v1) VALUES (1, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL, i, i);
            }
            cluster.forEach(c -> c.flush(KEYSPACE));
            // And some row deletions on node2 only which cover data in the repaired set
            // This will cause node2 to read more repaired data in satisfying the limit of the read request
            // so it should overread less than node1 (in fact, it should not overread at all) in order to
            // calculate the repaired data digest.
            for (int i=7; i<10; i++)
            {
                cluster.get(2).executeInternal("DELETE FROM " + KS_TABLE + " USING TIMESTAMP 2 WHERE k = 0 AND c = ?", i);
                cluster.get(2).executeInternal("DELETE FROM " + KS_TABLE + " USING TIMESTAMP 2 WHERE k = 1 AND c = ?", i);
            }

            // Verify single partition read
            long ccBefore = getConfirmedInconsistencies(cluster.get(1));
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " WHERE k=0 LIMIT 5", ConsistencyLevel.ALL),
                       rows(rows(0, 12, 10), rows(0, 6, 5)));
            long ccAfterPartitionRead = getConfirmedInconsistencies(cluster.get(1));

            // Verify partition range read
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " LIMIT 11", ConsistencyLevel.ALL),
                       rows(rows(1, 12, 10), rows(1, 6, 0), rows(0, 12, 12)));
            long ccAfterRangeRead = getConfirmedInconsistencies(cluster.get(1));

            if (ccAfterPartitionRead != ccAfterRangeRead)
                if (ccAfterPartitionRead != ccBefore)
                    fail("Both range and partition reads reported data inconsistencies but none were expected");
                else
                    fail("Reported inconsistency during range read but none were expected");
            else if (ccAfterPartitionRead != ccBefore)
                fail("Reported inconsistency during partition read but none were expected");
        }
    }

    /**
     * In CASSANDRA-16721, we discovered that if responses from remote replicas came back while the local runnable was 
     * still executing, the fact that {@link ReadCommand} was mutable meant that the trackRepairedStatus flag on the
     * command instance could move from false to true in executeLocally(), between setting the 
     * RepairedDataInfo/gathering the sstables and calling extend(). When this happened, the RDI was still the 
     * stand-in object NO_OP_REPAIRED_DATA_INFO, which has a null repairedDataCounter, and we hit the NPE.
     * 
     * Similarly, the trackRepairedStatus flag could be set after the point at which the RDI is set on the local 
     * read, assigned to the repairedDataInfo in {@link ReadCommand}, and improperly shared between initial local read
     * and the local read triggered by read repair.
     * 
     * These problems are sidestepped completely by CASSANDRA-16721, as an RDI instance is now created and destroyed 
     * entirely within the scope of single {@link LocalReadRunnable}, but this test still attempts to validate some
     * assumptions about the cleanliness of the logs and the correctness of queries made when initial local reads and
     * local reads triggered by read repair (after speculative reads) execute at roughly the same time.
     *
     * This test depends on whether node1 gets a data or a digest request first, we force it to be a digest request
     * in the forTokenReadLiveSorted ByteBuddy rule below.
     */
    @Test
    public void testLocalDataAndRemoteRequestConcurrency() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withInstanceInitializer(BBHelper::install)
                                           .withConfig(config -> config.set("repaired_data_tracking_for_partition_reads_enabled", true)
                                                                       .with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            // A speculative read is the reason we have two remote replicas in play that can return results before
            // the local replica does.
            setupSchema(cluster, "create table " + KS_TABLE + " (id int primary key, t int) WITH speculative_retry = 'ALWAYS'");

            cluster.get(1).executeInternal("INSERT INTO " + KS_TABLE + " (id, t) values (0, 0)");
            cluster.get(2).executeInternal("INSERT INTO " + KS_TABLE + " (id, t) values (0, 0)");
            cluster.get(3).executeInternal("INSERT INTO " + KS_TABLE + " (id, t) values (0, 1)");
            cluster.forEach(c -> c.flush(KEYSPACE));
            cluster.forEach(i -> i.runOnInstance(markAllRepaired()));
            cluster.forEach(i -> i.runOnInstance(assertRepaired()));

            long logPositionBeforeQuery = cluster.get(1).logs().mark();
            Object[][] rows = cluster.coordinator(1).execute("SELECT * FROM " + KS_TABLE + " WHERE id=0", ConsistencyLevel.QUORUM);
            assertEquals(1, rows.length);
            
            // Given we didn't write at QUORUM, both 0 and 1 are acceptable values.
            assertTrue((int) rows[0][1] == 0 || (int) rows[0][1] == 1);

            List<String> result = cluster.get(1).logs().grepForErrors(logPositionBeforeQuery).getResult();
            assertEquals(Collections.emptyList(), result);
            Assert.assertTrue("Encountered an error", result.isEmpty());
        }
    }

    public static class BBHelper
    {
        private static final CyclicBarrier barrier = new CyclicBarrier(2);

        public static void install(ClassLoader classLoader, Integer num)
        {
            // Only install on the coordinating node, which is also a replica...
            if (num == 1)
            {
                new ByteBuddy().rebase(SEPExecutor.class)
                               .method(named("maybeExecuteImmediately"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(SinglePartitionReadCommand.class)
                               .method(named("executeLocally"))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(ReplicaLayout.class)
                               .method(named("forTokenReadLiveSorted").and(takesArguments(AbstractReplicationStrategy.class, Token.class)))
                               .intercept(MethodDelegation.to(BBHelper.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void maybeExecuteImmediately(Runnable command)
        {
            // Force local read runnables (from initial read and read-repair) to execute in separate threads.
            new Thread(command).start();
        }

        @SuppressWarnings({ "unused" })
        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController,
                                                                 @SuperCall Callable<UnfilteredPartitionIterator> zuperCall)
        {
            try
            {
                if (executionController.metadata().name.equals(TABLE))
                {
                    // Force both the initial local read and the local read triggered by read-repair to proceed at
                    // roughly the same time.
                    barrier.await();
                }
                return zuperCall.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }

        @SuppressWarnings({ "unused" })
        public static ReplicaLayout.ForTokenRead forTokenReadLiveSorted(AbstractReplicationStrategy replicationStrategy, Token token)
        {
            try
            {
                EndpointsForToken.Builder builder = EndpointsForToken.builder(token, 3);
                builder.add(ReplicaUtils.full(InetAddressAndPort.getByName("127.0.0.3")));
                builder.add(ReplicaUtils.full(InetAddressAndPort.getByName("127.0.0.2")));
                builder.add(ReplicaUtils.full(InetAddressAndPort.getByName("127.0.0.1")));
                return new ReplicaLayout.ForTokenRead(replicationStrategy, builder.build());
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }

    private Object[][] rows(Object[][] head, Object[][]...tail)
    {
        return Stream.concat(Stream.of(head),
                             Stream.of(tail).flatMap(Stream::of))
                     .toArray(Object[][]::new);
    }

    private Object[][] rows(int partitionKey, int start, int end)
    {
        if (start == end)
            return new Object[][] { new Object[] { partitionKey, start, end } };

        IntStream clusterings = start > end
                                ? IntStream.range(end -1, start).map(i -> start - i + end - 1)
                                : IntStream.range(start, end);

        return clusterings.mapToObj(i -> new Object[] {partitionKey, i, i}).toArray(Object[][]::new);
    }

    private IIsolatedExecutor.SerializableRunnable assertNotRepaired()
    {
        return () ->
        {
            try
            {
                Iterator<SSTableReader> sstables = Keyspace.open(KEYSPACE)
                                                           .getColumnFamilyStore(TABLE)
                                                           .getLiveSSTables()
                                                           .iterator();
                while (sstables.hasNext())
                {
                    SSTableReader sstable = sstables.next();
                    Descriptor descriptor = sstable.descriptor;
                    StatsMetadata stats = StatsComponent.load(descriptor).statsMetadata();
                    Assert.assertEquals("repaired at is set for sstable: " + descriptor,
                                        stats.repairedAt,
                                        ActiveRepairService.UNREPAIRED_SSTABLE);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private IIsolatedExecutor.SerializableRunnable markAllRepaired()
    {
        return () ->
        {
            try
            {
                Iterator<SSTableReader> sstables = Keyspace.open(KEYSPACE)
                                                           .getColumnFamilyStore(TABLE)
                                                           .getLiveSSTables()
                                                           .iterator();
                while (sstables.hasNext())
                {
                    SSTableReader sstable = sstables.next();
                    Descriptor descriptor = sstable.descriptor;
                    descriptor.getMetadataSerializer()
                              .mutateRepairMetadata(descriptor, currentTimeMillis(), null, false);
                    sstable.reloadSSTableMetadata();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private IIsolatedExecutor.SerializableRunnable assertRepaired()
    {
        return () ->
        {
            try
            {
                Iterator<SSTableReader> sstables = Keyspace.open(KEYSPACE)
                                                           .getColumnFamilyStore(TABLE)
                                                           .getLiveSSTables()
                                                           .iterator();
                while (sstables.hasNext())
                {
                    SSTableReader sstable = sstables.next();
                    Descriptor descriptor = sstable.descriptor;
                    StatsMetadata stats = StatsComponent.load(descriptor).statsMetadata();
                    Assert.assertTrue("repaired at is not set for sstable: " + descriptor, stats.repairedAt > 0);
                }
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        };
    }

    @SuppressWarnings("UnstableApiUsage")
    private IInvokableInstance.SerializableRunnable assertSnapshotPresent(String snapshotName)
    {
        return () ->
        {
            // snapshots are taken asynchronously, this is crude but it gives it a chance to happen
            int attempts = 100;
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);

            while (cfs.listSnapshots().isEmpty())
            {
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                if (attempts-- < 0)
                    throw new AssertionError(String.format("Snapshot %s not found for for %s", snapshotName, KS_TABLE));
            }
        };
    }

    private IInvokableInstance.SerializableRunnable assertSnapshotNotPresent(String snapshotName)
    {
        return () ->
        {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
            Assert.assertFalse(cfs.snapshotExists(snapshotName));
        };
    }

    private long getConfirmedInconsistencies(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                     .getColumnFamilyStore(TABLE)
                                             .metric
                                             .confirmedRepairedInconsistencies
                                             .table
                                             .getCount());
    }

    private void setupSchema(Cluster cluster, String cql)
    {
        cluster.schemaChange(cql);
        // disable auto compaction to prevent nodes from trying to compact
        // new sstables with ones we've modified to mark repaired
        cluster.forEach(i -> i.runOnInstance(() -> Keyspace.open(KEYSPACE)
                                                           .getColumnFamilyStore(TABLE)
                                                           .disableAutoCompaction()));
    }
}
