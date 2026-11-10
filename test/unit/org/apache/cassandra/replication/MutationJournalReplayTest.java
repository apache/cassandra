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

package org.apache.cassandra.replication;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.harry.checker.TestHelper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.DeserializedRecordConsumer;
import org.apache.cassandra.journal.RecordPointer;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.journal.StaticSegment;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.FieldUtil;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.apache.cassandra.replication.MutationJournalTest.assertMutationsEqual;

public class MutationJournalReplayTest
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE_PREFIX = "tbl";
    private static final TableMetadata[] TABLES = new TableMetadata[10];
    private static MutationJournal journal;

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();

        File directory = new File(Files.createTempDirectory("mutation-journal-replay-test"));
        directory.deleteRecursiveOnExit();

        journal = new MutationJournal(directory, new TestParams(MessagingService.current_version)
        {
            @Override
            public long flushPeriod(TimeUnit units)
            {
                return 1;
            }

            @Override
            public FlushMode flushMode()
            {
                return FlushMode.PERIODIC;
            }
        });
        FieldUtil.setInstanceUnsafe(MutationJournal.class, journal, "instance");
        journal.start();

        for (int i = 0; i < TABLES.length; i++)
        {
            TABLES[i] = TableMetadata.builder(KEYSPACE, String.format("%s_%d", TABLE_PREFIX, i))
                                     .keyspaceReplicationType(ReplicationType.tracked)
                                     .addPartitionKeyColumn("pk", UTF8Type.instance)
                                     .addClusteringColumn("ck", UTF8Type.instance)
                                     .addRegularColumn("value", UTF8Type.instance)
                                     .build();
        }
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1, ReplicationType.tracked), TABLES);
        if (!MutationTrackingService.instance().isStarted())
            MutationTrackingService.start();
    }

    @org.junit.Before
    public void beforeTest()
    {
        journal.truncateForTesting();
    }

    @AfterClass
    public static void tearDown()
    {
        journal.shutdownBlocking();
    }

    @Test
    public void testReplay() throws Throwable
    {
        long seed = 0l;
        TestHelper.withRandom(seed,
                              rng -> {
                                  List<Mutation> original = new ArrayList<>();
                                  for (int i = 1; i <= 10_000; i++)
                                  {
                                      MutationId id = new MutationId(100L, i, i);
                                      Mutation mutation = mutation(i % 10, i).withMutationId(id);
                                      journal.write(id, mutation);
                                      original.add(mutation);
                                      if (i % rng.nextInt(1, 100) > 90)
                                          journal.closeCurrentSegmentForTestingIfNonEmpty();
                                  }

                                  journal.closeCurrentSegmentForTestingIfNonEmpty();

                                  List<Mutation> replayed = new ArrayList<>();
                                  journal.replay(new DeserializedRecordConsumer<ShortMutationId, Mutation>(MutationJournal.MutationSerializer.INSTANCE)
                                  {
                                      @Override
                                      protected void accept(long segment, int position, ShortMutationId key, Mutation mutation)
                                      {
                                          replayed.add(mutation);
                                      }
                                  }, 1);

                                  assertMutationsEqual(original, replayed);
                              });
    }

    @Test
    public void testReplayFlushed() throws Throwable
    {
        long seed = 0l;
        class Bounds
        {
            final CommitLogPosition start;
            final CommitLogPosition end;
            final int count;

            Bounds(CommitLogPosition start, CommitLogPosition end, int count)
            {
                this.start = start;
                this.end = end;
                this.count = count;
            }
        }
        TestHelper.withRandom(seed,
                              rng -> {
                                  List<Mutation> original = new ArrayList<>();

                                  List<Bounds> testFlushBounds = new ArrayList<>();
                                  CommitLogPosition prevPos = journal.getCurrentPosition();
                                  int count = 0;
                                  for (int i = 1; i <= 1000; i++)
                                  {
                                      MutationId id = new MutationId(100L, i, i);
                                      Mutation mutation = mutation(i % TABLES.length, i).withMutationId(id);
                                      journal.write(id, mutation);
                                      count++;
                                      original.add(mutation);
                                      if (i % rng.nextInt(1, 100) > 90)
                                      {
                                          CommitLogPosition curPos = journal.getCurrentPosition();
                                          journal.closeCurrentSegmentForTestingIfNonEmpty();
                                          testFlushBounds.add(new Bounds(prevPos, curPos, count));
                                          count = 0;
                                          prevPos = curPos;
                                      }
                                  }

                                  journal.closeCurrentSegmentForTestingIfNonEmpty();

                                  int flushed = 0;
                                  for (Bounds bounds : testFlushBounds)
                                  {
                                      if (rng.nextBoolean())
                                      {
                                          for (TableMetadata table : TABLES)
                                              journal.notifyFlushed(table.id, bounds.start, bounds.end);
                                          flushed += bounds.count;
                                      }
                                  }

                                  // Persisting needsReplay=false is deferred until LogStatePersister drains the
                                  // queue. There's no persister in this test, so drain manually before replay.
                                  journal.drainCleanup(journal.snapshotPendingClearReplay());

                                  List<Mutation> replayed = new ArrayList<>();
                                  journal.replay(new DeserializedRecordConsumer<ShortMutationId, Mutation>(MutationJournal.MutationSerializer.INSTANCE)
                                  {
                                      @Override
                                      protected void accept(long segment, int position, ShortMutationId key, Mutation mutation)
                                      {
                                          replayed.add(mutation);
                                      }
                                  }, 1);

                                  Assert.assertEquals(original.size() - flushed,
                                                      replayed.size());
                              });
    }

    @Test
    public void cleanActiveSegmentClearedOnShutdown() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-active-shutdown-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        TableMetadata table = TABLES[0];
        ShortMutationId id = new ShortMutationId(100L, 1);
        RecordPointer ptr = testJournal.write(id, mutation(0, 1));
        testJournal.closeCurrentSegmentIfNonEmpty();
        testJournal.notifyFlushed(table.id, ptr, ptr);

        Assert.assertEquals(1, testJournal.snapshotPendingClearReplay().size());
        testJournal.drainCleanup(testJournal.snapshotPendingClearReplay(), false);
        testJournal.shutdownBlocking();

        MutationJournal reopenedJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        reopenedJournal.startInternal();
        try
        {
            boolean foundSegment = false;
            for (Segment<ShortMutationId, Mutation> seg : reopenedJournal.getAllSegments())
            {
                if (seg.isStatic())
                {
                    foundSegment = true;
                    Assert.assertFalse("Static segment on disk should not need replay", seg.metadata().needsReplay());
                }
            }
            Assert.assertTrue("Should have found at least one static segment", foundSegment);
        }
        finally
        {
            reopenedJournal.shutdownBlocking();
        }
    }

    @Test
    public void unreplayedStaticSegmentRetainsNeedsReplayOnShutdown() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-unreplayed-shutdown-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        TableMetadata table = TABLES[0];
        ShortMutationId id = new ShortMutationId(100L, 1);
        testJournal.write(id, mutation(0, 1));
        testJournal.closeCurrentSegmentIfNonEmpty();
        testJournal.shutdownBlocking();

        MutationJournal reopenedJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        reopenedJournal.startInternal();
        try
        {
            // Do not replay; shutdown directly without draining
            Assert.assertEquals(0, reopenedJournal.snapshotPendingClearReplay().size());
            reopenedJournal.shutdownBlocking();
        }
        catch (Throwable t)
        {
            reopenedJournal.shutdownBlocking();
            throw t;
        }

        MutationJournal thirdJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        thirdJournal.startInternal();
        try
        {
            boolean foundSegment = false;
            for (Segment<ShortMutationId, Mutation> seg : thirdJournal.getAllSegments())
            {
                if (seg.isStatic())
                {
                    foundSegment = true;
                    Assert.assertTrue("Unreplayed static segment on disk must still need replay",
                                      seg.metadata().needsReplay());
                }
            }
            Assert.assertTrue("Should have found at least one static segment", foundSegment);
        }
        finally
        {
            thirdJournal.shutdownBlocking();
        }
    }

    @Test
    public void dirtyActiveSegmentRetainsNeedsReplayAcrossShutdown() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-dirty-shutdown-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        TableMetadata table0 = TABLES[0];
        TableMetadata table1 = TABLES[1];
        ShortMutationId id0 = new ShortMutationId(100L, 1);
        ShortMutationId id1 = new ShortMutationId(100L, 2);
        RecordPointer ptr0 = testJournal.write(id0, mutation(0, 1));
        testJournal.write(id1, mutation(1, 2));

        testJournal.closeCurrentSegmentIfNonEmpty();
        testJournal.notifyFlushed(table0.id, ptr0, ptr0);

        Assert.assertEquals(0, testJournal.snapshotPendingClearReplay().size());
        testJournal.drainCleanup(testJournal.snapshotPendingClearReplay(), false);
        testJournal.shutdownBlocking();

        MutationJournal reopenedJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        reopenedJournal.startInternal();
        try
        {
            boolean foundSegment = false;
            for (Segment<ShortMutationId, Mutation> seg : reopenedJournal.getAllSegments())
            {
                if (seg.isStatic())
                {
                    foundSegment = true;
                    Assert.assertTrue("Dirty static segment on disk must retain needsReplay",
                                      seg.metadata().needsReplay());
                }
            }
            Assert.assertTrue("Should have found at least one static segment", foundSegment);
        }
        finally
        {
            reopenedJournal.shutdownBlocking();
        }
    }

    @Test
    public void drainCleanupRevertsNeedsReplayWhenPersistMetadataFails() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-persist-fail-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        TableMetadata table = TABLES[0];
        ShortMutationId id = new ShortMutationId(100L, 1);
        RecordPointer ptr = testJournal.write(id, mutation(0, 1));
        testJournal.closeCurrentSegmentIfNonEmpty();
        testJournal.notifyFlushed(table.id, ptr, ptr);

        Assert.assertEquals(1, testJournal.snapshotPendingClearReplay().size());
        long segId = testJournal.pendingCleanupForTesting().iterator().next();

        Segment<ShortMutationId, Mutation> targetSegment = null;
        for (Segment<ShortMutationId, Mutation> seg : testJournal.getAllSegments())
        {
            if (seg.id() == segId)
            {
                targetSegment = seg;
                break;
            }
        }
        Assert.assertNotNull("Target segment must exist", targetSegment);
        Assert.assertTrue("Segment must need replay before drain", targetSegment.metadata().needsReplay());

        java.io.File dirFile = directory.toPath().toFile();
        Assert.assertTrue(dirFile.setWritable(false));
        try
        {
            testJournal.drainCleanup(testJournal.snapshotPendingClearReplay(), false);

            Assert.assertTrue("needsReplay must be reverted to true on metadata persistence failure",
                              targetSegment.metadata().needsReplay());
            Assert.assertTrue("Segment must remain in pendingCleanup after failure",
                              testJournal.pendingCleanupForTesting().contains(segId));
        }
        finally
        {
            Assert.assertTrue(dirFile.setWritable(true));
        }

        testJournal.drainCleanup(testJournal.snapshotPendingClearReplay(), false);
        Assert.assertFalse("needsReplay must be false after successful metadata persistence",
                           targetSegment.metadata().needsReplay());
        Assert.assertTrue("pendingCleanup must be empty after successful drain",
                          testJournal.pendingCleanupForTesting().isEmpty());

        testJournal.shutdownBlocking();

        MutationJournal reopenedJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        reopenedJournal.startInternal();
        try
        {
            boolean foundSegment = false;
            for (Segment<ShortMutationId, Mutation> seg : reopenedJournal.getAllSegments())
            {
                if (seg.id() == segId)
                {
                    foundSegment = true;
                    Assert.assertFalse("Static segment on disk should not need replay after clean drain",
                                       seg.metadata().needsReplay());
                }
            }
            Assert.assertTrue("Should have found target static segment", foundSegment);
        }
        finally
        {
            reopenedJournal.shutdownBlocking();
        }
    }

    @Test
    public void trackerlessStaticSegmentsClearedAfterReplay() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-trackerless-replay-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, new TestParams(MessagingService.current_version)
        {
            @Override
            public long flushPeriod(TimeUnit units)
            {
                return 1;
            }

            @Override
            public FlushMode flushMode()
            {
                return FlushMode.PERIODIC;
            }
        });
        testJournal.startInternal();

        TableMetadata droppedTable = TableMetadata.builder("ks", "dropped_tbl")
                                                  .keyspaceReplicationType(ReplicationType.tracked)
                                                  .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                  .addRegularColumn("v", UTF8Type.instance)
                                                  .build();
        Mutation mutation = new RowUpdateBuilder(droppedTable, 0, "pk1")
                            .add("v", "val")
                            .build()
                            .withMutationId(new MutationId(200L, 1, 1));

        testJournal.write(mutation.id(), mutation);
        testJournal.closeCurrentSegmentForTestingIfNonEmpty();

        Segment<ShortMutationId, Mutation> staticSegment = null;
        for (Segment<ShortMutationId, Mutation> seg : testJournal.getAllSegments())
        {
            if (seg.isStatic())
            {
                staticSegment = seg;
                break;
            }
        }
        Assert.assertNotNull(staticSegment);
        Assert.assertTrue(staticSegment.metadata().needsReplay());
        long staticSegId = staticSegment.id();

        testJournal.shutdownBlocking();

        // Simulate startup of a new daemon on the directory
        MutationJournal startupJournal = new MutationJournal(directory, new TestParams(MessagingService.current_version)
        {
            @Override
            public long flushPeriod(TimeUnit units)
            {
                return 1;
            }

            @Override
            public FlushMode flushMode()
            {
                return FlushMode.PERIODIC;
            }
        });
        startupJournal.startInternal();
        try
        {
            // Run replayStaticSegments on startup
            startupJournal.replayStaticSegments();

            // Static segment should be queued into pendingClearReplay because it has no dirty tracker
            Assert.assertTrue("Trackerless static segment should be queued into pendingClearReplay",
                              startupJournal.pendingCleanupForTesting().contains(staticSegId));
        }
        finally
        {
            startupJournal.shutdownBlocking();
        }
    }

    @FunctionalInterface
    private interface ThresholdTestRunner
    {
        void run(MutationJournal testJournal, Log2OffsetsMap.Mutable offsets) throws Exception;
    }

    private void runThresholdTest(String prefix, ThresholdTestRunner test) throws Exception
    {
        File directory = new File(Files.createTempDirectory(prefix));
        directory.deleteRecursiveOnExit();

        Log2OffsetsMap.Mutable offsets = new Log2OffsetsMap.Mutable();
        MutationJournal testJournal = new MutationJournal(directory, new TestParams(MessagingService.current_version)
        {
            @Override
            public long flushPeriod(TimeUnit units)
            {
                return 1;
            }

            @Override
            public FlushMode flushMode()
            {
                return FlushMode.PERIODIC;
            }
        }, () -> offsets);
        testJournal.startInternal();

        DataStorageSpec.LongBytesBound originalThreshold = DatabaseDescriptor.getMutationTrackingConfig().journal_reclaim_threshold;
        try
        {
            test.run(testJournal, offsets);
        }
        finally
        {
            DatabaseDescriptor.getMutationTrackingConfig().journal_reclaim_threshold = originalThreshold;
            testJournal.shutdownBlocking();
            for (TableMetadata table : TABLES)
            {
                ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
                if (cfs != null && !cfs.getTracker().getView().getCurrentMemtable().isClean())
                    resetTable(cfs);
            }
        }
    }

    private static MutationId writeTracked(MutationJournal journal, TableMetadata table, String keyName, Log2OffsetsMap.Mutable offsets)
    {
        DecoratedKey key = key(table, keyName);
        MutationId id = MutationTrackingService.instance().nextMutationId(table.keyspace, key.getToken());
        Mutation mut = mutation(table, key, keyName + "_val", id);
        if (offsets != null)
            offsets.add(new ShortMutationId(id));
        Keyspace.open(table.keyspace).apply(mut, false);
        journal.write(id, mut);
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        return id;
    }

    private static long writeDroppedTableSegment(MutationJournal journal, String tableName, long offset, Log2OffsetsMap.Mutable offsets)
    {
        TableMetadata droppedTable = TableMetadata.builder("ks", tableName)
                                                  .keyspaceReplicationType(ReplicationType.tracked)
                                                  .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                  .addRegularColumn("v", UTF8Type.instance)
                                                  .build();
        MutationId droppedId = new MutationId(offset, 1, 1);
        Mutation mutDropped = new RowUpdateBuilder(droppedTable, 0, "pk1")
                              .add("v", "val")
                              .build()
                              .withMutationId(droppedId);
        if (offsets != null)
            offsets.add(new ShortMutationId(droppedId));
        journal.write(mutDropped.id(), mutDropped);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        List<StaticSegment<ShortMutationId, Mutation>> staticSegments = new ArrayList<>();
        for (Segment<ShortMutationId, Mutation> seg : journal.getAllSegments())
            if (seg.isStatic()) staticSegments.add((StaticSegment<ShortMutationId, Mutation>) seg);
        staticSegments.sort(StaticSegment::compareTo);
        return staticSegments.get(staticSegments.size() - 1).id();
    }

    private static void setJournalReclaimThreshold(String spec)
    {
        DatabaseDescriptor.getMutationTrackingConfig().journal_reclaim_threshold = new DataStorageSpec.LongBytesBound(spec);
    }

    @Test
    public void thresholdBasedFlushTriggersMemtableFlushForPinnedSegments() throws Exception
    {
        runThresholdTest("mutation-journal-flush-threshold-test", (testJournal, offsets) -> {
            TableMetadata table = TABLES[0];
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);

            writeTracked(testJournal, table, "threshold_live_key", offsets);
            long seg2Id = writeDroppedTableSegment(testJournal, "dropped_tbl_flush_combined", 301L, offsets);

            long initialSwitches = cfs.metric.memtableSwitchCount.getCount();
            setJournalReclaimThreshold("1B");

            testJournal.maybeFlushReclaimableSegments();

            Util.spinAssertEquals(initialSwitches + 1, () -> cfs.metric.memtableSwitchCount.getCount());
            Assert.assertTrue(testJournal.pendingCleanupForTesting().contains(seg2Id));
        });
    }

    @Test
    public void thresholdBasedFlushEarlyReturnsWhenThresholdZeroOrNotExceeded() throws Exception
    {
        runThresholdTest("mutation-journal-early-return-test", (testJournal, offsets) -> {
            TableMetadata table = TABLES[0];
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);

            writeTracked(testJournal, table, "early_return_key", offsets);

            Memtable memtableBefore = cfs.getTracker().getView().getCurrentMemtable();
            long initialSwitches = cfs.metric.memtableSwitchCount.getCount();

            testJournal.maybeFlushReclaimableSegments(0, offsets);
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());

            testJournal.maybeFlushReclaimableSegments(-1, offsets);
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());

            long diskSpaceUsed = testJournal.getDiskSpaceUsed();
            testJournal.maybeFlushReclaimableSegments(diskSpaceUsed, offsets);
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());

            testJournal.maybeFlushReclaimableSegments(diskSpaceUsed + 1024, offsets);
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());
        });
    }

    @Test
    public void thresholdBasedFlushStopsAtBytesToReclaimCutoff() throws Exception
    {
        runThresholdTest("mutation-journal-cutoff-test", (testJournal, offsets) -> {
            TableMetadata table0 = TABLES[0];
            TableMetadata table1 = TABLES[1];
            ColumnFamilyStore cfs0 = Schema.instance.getColumnFamilyStoreInstance(table0.id);
            ColumnFamilyStore cfs1 = Schema.instance.getColumnFamilyStoreInstance(table1.id);

            writeTracked(testJournal, table0, "cutoff_key_0", offsets);
            writeTracked(testJournal, table1, "cutoff_key_1", offsets);

            List<StaticSegment<ShortMutationId, Mutation>> staticSegments = new ArrayList<>();
            for (Segment<ShortMutationId, Mutation> seg : testJournal.getAllSegments())
                if (seg.isStatic()) staticSegments.add((StaticSegment<ShortMutationId, Mutation>) seg);
            Assert.assertEquals(2, staticSegments.size());
            staticSegments.sort(StaticSegment::compareTo);
            long seg0Size = staticSegments.get(0).segmentSizeOnDisk();

            long diskSpaceUsed = testJournal.getDiskSpaceUsed();
            long bytesToReclaim = Math.max(1, seg0Size / 2);
            long threshold = diskSpaceUsed - bytesToReclaim;

            Memtable memtableBefore0 = cfs0.getTracker().getView().getCurrentMemtable();
            Memtable memtableBefore1 = cfs1.getTracker().getView().getCurrentMemtable();
            long initialSwitches0 = cfs0.metric.memtableSwitchCount.getCount();
            long initialSwitches1 = cfs1.metric.memtableSwitchCount.getCount();

            testJournal.maybeFlushReclaimableSegments(threshold, offsets);

            Assert.assertNotSame(memtableBefore0, cfs0.getTracker().getView().getCurrentMemtable());
            Assert.assertSame(memtableBefore1, cfs1.getTracker().getView().getCurrentMemtable());
            Util.spinAssertEquals(initialSwitches0 + 1, () -> cfs0.metric.memtableSwitchCount.getCount());
            Assert.assertEquals(initialSwitches1, cfs1.metric.memtableSwitchCount.getCount());
        });
    }

    @Test
    public void thresholdBasedFlushSkipsReferencedSegments() throws Exception
    {
        runThresholdTest("mutation-journal-skip-referenced-test", (testJournal, offsets) -> {
            TableMetadata table = TABLES[0];
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);

            DecoratedKey sstableKey = key(table, "sstable_ref_key");
            MutationId sstableMutationId = MutationTrackingService.instance().nextMutationId(table.keyspace, sstableKey.getToken());
            Mutation sstableMut = mutation(table, sstableKey, "sstable_val", sstableMutationId);
            Keyspace.open(table.keyspace).apply(sstableMut, false);
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

            Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
            SSTableReader referrer = cfs.getLiveSSTables().iterator().next();

            writeTracked(testJournal, table, "skip_ref_key", offsets);

            List<StaticSegment<ShortMutationId, Mutation>> staticSegments = new ArrayList<>();
            for (Segment<ShortMutationId, Mutation> seg : testJournal.getAllSegments())
                if (seg.isStatic()) staticSegments.add((StaticSegment<ShortMutationId, Mutation>) seg);
            Assert.assertEquals(1, staticSegments.size());
            long segId = staticSegments.get(0).id();

            testJournal.segmentReferenceTracker().addReferenceForTesting(segId, referrer);

            Memtable memtableBefore = cfs.getTracker().getView().getCurrentMemtable();
            long initialSwitches = cfs.metric.memtableSwitchCount.getCount();

            testJournal.maybeFlushReclaimableSegments(1L, offsets);
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());

            testJournal.segmentReferenceTracker().removeReferenceForTesting(segId, referrer);
            testJournal.maybeFlushReclaimableSegments(1L, offsets);
            Assert.assertNotSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Util.spinAssertEquals(initialSwitches + 1, () -> cfs.metric.memtableSwitchCount.getCount());
        });
    }

    @Test
    public void compactorOnCompactedGatingTriggersFlushOnlyWhenThresholdExceeded() throws Exception
    {
        runThresholdTest("mutation-journal-compactor-gating-test", (testJournal, offsets) -> {
            TableMetadata table = TABLES[0];
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);

            writeTracked(testJournal, table, "compactor_gating_key", offsets);

            Memtable memtableBefore = cfs.getTracker().getView().getCurrentMemtable();
            long initialSwitches = cfs.metric.memtableSwitchCount.getCount();

            setJournalReclaimThreshold("0B");
            testJournal.segmentCompactor.onCompacted();
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());

            setJournalReclaimThreshold(String.valueOf(testJournal.getDiskSpaceUsed() + 1024) + "B");
            testJournal.segmentCompactor.onCompacted();
            Assert.assertSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Assert.assertEquals(initialSwitches, cfs.metric.memtableSwitchCount.getCount());

            setJournalReclaimThreshold("1B");
            testJournal.segmentCompactor.onCompacted();
            Assert.assertNotSame(memtableBefore, cfs.getTracker().getView().getCurrentMemtable());
            Util.spinAssertEquals(initialSwitches + 1, () -> cfs.metric.memtableSwitchCount.getCount());
        });
    }

    @Test
    public void replayBypassSkipsMemtableApplicationAndQueuesPendingClear() throws IOException
    {
        if (!MutationTrackingService.instance().isStarted())
            MutationTrackingService.start();

        File directory = new File(Files.createTempDirectory("mutation-journal-bypass-replay-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        TableMetadata table = TABLES[0];
        DecoratedKey key = key(table, "bypass_test_key");
        MutationId id = MutationTrackingService.instance().nextMutationId(table.keyspace, key.getToken());
        Mutation mut = mutation(table, key, "bypass_val", id);
        testJournal.write(id, mut);
        testJournal.closeCurrentSegmentForTestingIfNonEmpty();

        Segment<ShortMutationId, Mutation> staticSegment = null;
        for (Segment<ShortMutationId, Mutation> seg : testJournal.getAllSegments())
        {
            if (seg.isStatic())
            {
                staticSegment = seg;
                break;
            }
        }
        Assert.assertNotNull(staticSegment);
        Assert.assertTrue(staticSegment.metadata().needsReplay());
        long staticSegId = staticSegment.id();

        testJournal.shutdownBlocking();

        // Ensure table memtable is clean before replay
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        long memtableOpsBefore = cfs.getTracker().getView().getCurrentMemtable().operationCount();

        MutationSummary summaryBefore = MutationTrackingService.instance().createSummaryForKey(key, table.id, false);
        Assert.assertFalse("Mutation ID should not be witnessed before replay", summaryBefore.contains(id));

        String prevBypass = CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.getString();
        CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.setString("true");

        MutationJournal startupJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        startupJournal.startInternal();
        try
        {
            startupJournal.replayStaticSegments();

            // Memtable was not updated
            long memtableOpsAfter = cfs.getTracker().getView().getCurrentMemtable().operationCount();
            Assert.assertEquals("Memtable should not receive writes when replay is bypassed",
                                memtableOpsBefore, memtableOpsAfter);

            MutationSummary summaryAfter = MutationTrackingService.instance().createSummaryForKey(key, table.id, false);
            Assert.assertTrue("Mutation ID should be witnessed in MutationTrackingService after bypass replay",
                              summaryAfter.contains(id));

            // Static segment should be queued into pendingClearReplay, but needsReplay is not cleared synchronously
            Assert.assertTrue("Segment should be queued into pendingClearReplay",
                              startupJournal.pendingCleanupForTesting().contains(staticSegId));
            Assert.assertTrue("Segment needsReplay flag should not be cleared synchronously before drain",
                              staticSegment.metadata().needsReplay());
        }
        finally
        {
            if (prevBypass != null)
                CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.setString(prevBypass);
            else
                System.clearProperty(CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.getKey());
            startupJournal.shutdownBlocking();
        }
    }

    @Test
    public void replayBypassLogsWarningWhenPropertySet() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-bypass-warning-test"));
        directory.deleteRecursiveOnExit();

        String prevBypass = CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.getString();
        CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.setString("true");

        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        Logger mjLogger = (Logger) LoggerFactory.getLogger(MutationJournal.class);
        mjLogger.addAppender(listAppender);
        listAppender.start();

        MutationJournal startupJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        startupJournal.startInternal();
        try
        {
            startupJournal.replayStaticSegments();

            boolean foundWarning = listAppender.list.stream().anyMatch(event ->
                event.getLevel() == Level.WARN &&
                event.getFormattedMessage().contains("Replay bypass enabled via " + CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.getKey())
            );
            Assert.assertTrue("Expected warning log when replay bypass property is set", foundWarning);
        }
        finally
        {
            mjLogger.detachAppender(listAppender);
            listAppender.stop();
            if (prevBypass != null)
                CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.setString(prevBypass);
            else
                System.clearProperty(CassandraRelevantProperties.MUTATION_JOURNAL_SKIP_REPLAY_TO_MEMTABLE.getKey());
            startupJournal.shutdownBlocking();
        }
    }

    @Test
    public void replaySkipsPreviouslyFlushedOffsetsWhenSSTableMissing() throws Exception
    {
        if (!MutationTrackingService.instance().isStarted())
            MutationTrackingService.start();

        File directory = new File(Files.createTempDirectory("mutation-journal-flushed-replay-test"));
        directory.deleteRecursiveOnExit();

        TableMetadata table = TABLES[0];
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        cfs.truncateBlocking();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        DecoratedKey key = table.partitioner.decorateKey(UTF8Type.instance.decompose("flushed_test_key"));
        MutationId mutationId = MutationTrackingService.instance().nextMutationId(table.keyspace, key.getToken());

        Mutation mutation = new RowUpdateBuilder(table, 0, key)
                            .clustering("ck")
                            .add("value", "flushed_val")
                            .build()
                            .withMutationId(mutationId);

        testJournal.write(mutation.id(), mutation);
        testJournal.closeCurrentSegmentForTestingIfNonEmpty();
        testJournal.shutdownBlocking();

        // Prove the table has no live SSTables
        Assert.assertTrue(cfs.getLiveSSTables().isEmpty());
        long memtableOpsBefore = cfs.getTracker().getView().getCurrentMemtable().operationCount();

        // Record the offset as flushed in MutationTrackingService
        ImmutableCoordinatorLogOffsets offsets = new ImmutableCoordinatorLogOffsets.Builder()
                                                 .add(mutationId)
                                                 .build();
        MutationTrackingService.instance().ensureFlushedOffsetsDurable(offsets);
        Assert.assertTrue(MutationTrackingService.instance().isFlushed(mutationId));

        MutationJournal startupJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        startupJournal.startInternal();
        try
        {
            startupJournal.replayStaticSegments();

            // Memtable should not receive writes because the mutation ID was already flushed
            long memtableOpsAfter = cfs.getTracker().getView().getCurrentMemtable().operationCount();
            Assert.assertEquals("Memtable should not receive writes when mutation was already flushed",
                                memtableOpsBefore, memtableOpsAfter);
        }
        finally
        {
            startupJournal.shutdownBlocking();
        }
    }

    @Test
    public void replayMutationsAreVisibleInSummariesWithoutPending() throws Exception
    {
        if (!MutationTrackingService.instance().isStarted())
            MutationTrackingService.start();

        File directory = new File(Files.createTempDirectory("mutation-journal-visible-replay-test"));
        directory.deleteRecursiveOnExit();

        TableMetadata table = TABLES[0];
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        cfs.truncateBlocking();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        DecoratedKey key = table.partitioner.decorateKey(UTF8Type.instance.decompose("visible_replay_key"));
        MutationId mutationId = MutationTrackingService.instance().nextMutationId(table.keyspace, key.getToken());

        Mutation mutation = new RowUpdateBuilder(table, 0, key)
                            .clustering("ck")
                            .add("value", "visible_val")
                            .build()
                            .withMutationId(mutationId);

        testJournal.write(mutation.id(), mutation);
        testJournal.closeCurrentSegmentForTestingIfNonEmpty();
        testJournal.shutdownBlocking();

        MutationJournal startupJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        startupJournal.startInternal();
        try
        {
            startupJournal.replayStaticSegments();

            MutationSummary summary = MutationTrackingService.instance().createSummaryForKey(key, table.id, false);
            Assert.assertTrue("Replayed mutation should be visible in summary when includePending=false",
                              summary.contains(mutationId));
        }
        finally
        {
            startupJournal.shutdownBlocking();
        }
    }

    @FunctionalInterface
    private interface ReplayWriter
    {
        void write(MutationJournal testJournal) throws Exception;
    }

    private void runReplayTest(String testPrefix, ReplayWriter writer, Runnable verifyAfterReplay) throws Exception
    {
        if (!MutationTrackingService.instance().isStarted())
            MutationTrackingService.start();

        File directory = new File(Files.createTempDirectory(testPrefix));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();
        FieldUtil.setInstanceUnsafe(MutationJournal.class, testJournal, "instance");
        try
        {
            writer.write(testJournal);
            testJournal.closeCurrentSegmentForTestingIfNonEmpty();
            testJournal.shutdownBlocking();

            MutationJournal startupJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
            startupJournal.startInternal();
            try
            {
                startupJournal.replayStaticSegments();
                verifyAfterReplay.run();
            }
            finally
            {
                startupJournal.shutdownBlocking();
            }
        }
        finally
        {
            FieldUtil.setInstanceUnsafe(MutationJournal.class, journal, "instance");
        }
    }

    private void clearInFlightMtsFlushedOffset(MutationId mutationId)
    {
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.getKeyspaceShards(MutationTrackingService.instance(), KEYSPACE);
        if (shards != null)
            shards.forEachShard(shard -> shard.forEachLog(log -> log.flushedOffsets.remove(mutationId.offset())));
    }

    private static DecoratedKey key(TableMetadata table, String key)
    {
        return table.partitioner.decorateKey(UTF8Type.instance.decompose(key));
    }

    private static Mutation mutation(TableMetadata table, DecoratedKey key, String value, MutationId id)
    {
        return mutation(table, 0, key, value, id);
    }

    private static Mutation mutation(TableMetadata table, long timestamp, DecoratedKey key, String value, MutationId id)
    {
        return new RowUpdateBuilder(table, timestamp, key)
               .clustering("ck")
               .add("value", value)
               .build()
               .withMutationId(id);
    }

    private static PartitionUpdate partitionUpdate(TableMetadata table, DecoratedKey key, String value)
    {
        return new RowUpdateBuilder(table, 0, key)
               .clustering("ck")
               .add("value", value)
               .buildUpdate();
    }

    private static String readValue(ColumnFamilyStore cfs, TableMetadata table, DecoratedKey key)
    {
        Row row = Util.getOnlyRow(Util.cmd(cfs, key).build());
        Assert.assertNotNull(row);
        return UTF8Type.instance.compose(row.getCell(table.getColumn(UTF8Type.instance.decompose("value"))).buffer());
    }

    private static void resetTable(ColumnFamilyStore cfs)
    {
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
        cfs.truncateBlocking();
    }

    @Test
    public void replaySkipsMutationsCoveredByLiveSSTableOffsets() throws Exception
    {
        TableMetadata table = TABLES[0];
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        resetTable(cfs);

        DecoratedKey key = key(table, "sstable_filter_key");
        MutationId mutationId = new MutationId(999001L, 1, 1);
        Mutation mutation = mutation(table, key, "sstable_val", mutationId);

        runReplayTest("mutation-journal-sstable-filter-test",
                      testJournal -> {
                          Keyspace.open(table.keyspace).apply(mutation, false);
                          cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

                          Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
                          SSTableReader sstable = cfs.getLiveSSTables().iterator().next();
                          Assert.assertTrue(sstable.getCoordinatorLogOffsets().contains(mutationId));

                          clearInFlightMtsFlushedOffset(mutationId);
                          Assert.assertFalse(MutationTrackingService.instance().isFlushed(mutationId));
                      },
                      () -> Assert.assertEquals(0, cfs.getTracker().getView().getCurrentMemtable().operationCount()));
    }

    @Test
    public void replayAppliesMutationsWhenOffsetsAbsentFromLiveSSTables() throws Exception
    {
        TableMetadata table = TABLES[0];
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        resetTable(cfs);

        DecoratedKey key1 = key(table, "flushed_key");
        MutationId id1 = new MutationId(999002L, 1, 1);
        Mutation mutation1 = mutation(table, key1, "flushed_val", id1);

        DecoratedKey key2 = key(table, "unflushed_key");
        MutationId id2 = new MutationId(999002L, 2, 2);
        Mutation mutation2 = mutation(table, key2, "unflushed_val", id2);

        runReplayTest("mutation-journal-absent-offset-test",
                      testJournal -> {
                          Keyspace.open(table.keyspace).apply(mutation1, false);
                          cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

                          testJournal.write(id2, mutation2);
                      },
                      () -> {
                          Assert.assertTrue(cfs.getTracker().getView().getCurrentMemtable().operationCount() > 0);
                          Assert.assertEquals("unflushed_val", readValue(cfs, table, key2));
                      });
    }

    @Test
    public void replayFiltersOnlyCoveredTablesInMultiTableMutation() throws Exception
    {
        TableMetadata table0 = TABLES[0];
        TableMetadata table1 = TABLES[1];
        ColumnFamilyStore cfs0 = Schema.instance.getColumnFamilyStoreInstance(table0.id);
        ColumnFamilyStore cfs1 = Schema.instance.getColumnFamilyStoreInstance(table1.id);
        resetTable(cfs0);
        resetTable(cfs1);

        DecoratedKey key = key(table0, "multi_table_key");
        MutationId mutationId = new MutationId(999003L, 1, 1);

        Mutation multiMutation = new Mutation.PartitionUpdateCollector(mutationId, KEYSPACE, key)
                                 .add(partitionUpdate(table0, key, "multi_val_0"))
                                 .add(partitionUpdate(table1, key, "multi_val_1"))
                                 .build();

        runReplayTest("mutation-journal-multi-table-replay-test",
                      testJournal -> {
                          Keyspace.open(KEYSPACE).apply(multiMutation, false);
                          cfs0.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
                          cfs1.truncateBlocking();

                          Assert.assertFalse(cfs0.getLiveSSTables().isEmpty());
                          Assert.assertTrue(cfs0.getLiveSSTables().iterator().next().getCoordinatorLogOffsets().contains(mutationId));
                          Assert.assertTrue(cfs1.getLiveSSTables().isEmpty());

                          clearInFlightMtsFlushedOffset(mutationId);
                          Assert.assertFalse(MutationTrackingService.instance().isFlushed(mutationId));
                      },
                      () -> {
                          Assert.assertEquals(0, cfs0.getTracker().getView().getCurrentMemtable().operationCount());
                          Assert.assertTrue(cfs1.getTracker().getView().getCurrentMemtable().operationCount() > 0);
                          Assert.assertEquals("multi_val_1", readValue(cfs1, table1, key));
                      });
    }

    @Test
    public void replayPreventsDataResurrectionForFlushedAndDeletedRow() throws Exception
    {
        TableMetadata table = TABLES[2];
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(table.id);
        resetTable(cfs);

        DecoratedKey key = key(table, "resurrect_key");
        MutationId insertId = new MutationId(999004L, 1, 1);
        MutationId deleteId = new MutationId(999004L, 2, 2);

        Mutation insertMutation = mutation(table, 1000, key, "resurrect_val", insertId);
        Mutation deleteMutation = RowUpdateBuilder.deleteRow(table, 2000, key, "ck").withMutationId(deleteId);

        runReplayTest("mutation-journal-resurrection-test",
                      testJournal -> {
                          Keyspace.open(KEYSPACE).apply(insertMutation, false);
                          cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);

                          Assert.assertFalse(cfs.getLiveSSTables().isEmpty());
                          Assert.assertTrue(cfs.getLiveSSTables().iterator().next().getCoordinatorLogOffsets().contains(insertId));

                          deleteMutation.apply();
                          cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.USER_FORCED);
                          Util.assertEmpty(Util.cmd(cfs, key).build());
                      },
                      () -> {
                          Assert.assertEquals(0, cfs.getTracker().getView().getCurrentMemtable().operationCount());
                          Util.assertEmpty(Util.cmd(cfs, key).build());
                      });
    }

    private static String CACHED_STRING = null;
    private static Mutation mutation(int table, int value)
    {
        if (CACHED_STRING == null)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 512; i++)
            {
                sb.append('.');
            }
            CACHED_STRING = sb.toString();
        }
        return new RowUpdateBuilder(TABLES[table], 0, "key_" + value)
                .clustering("ck")
                .add("value", CACHED_STRING)
                .build();
    }
}
