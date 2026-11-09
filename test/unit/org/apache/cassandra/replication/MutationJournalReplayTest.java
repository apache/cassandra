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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.harry.checker.TestHelper;
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

    @Test
    public void thresholdBasedFlushTriggersMemtableFlushForPinnedSegments() throws Exception
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-flush-threshold-test"));
        directory.deleteRecursiveOnExit();

        Log2OffsetsMap.Mutable offsets = new Log2OffsetsMap.Mutable();
        offsets.add(new ShortMutationId(100L, 1));
        offsets.add(new ShortMutationId(300L, 1));
        offsets.add(new ShortMutationId(301L, 1));
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
            // Set reclaim threshold to 1 byte so any journal usage exceeds it
            DatabaseDescriptor.getMutationTrackingConfig().journal_reclaim_threshold =
                new DataStorageSpec.LongBytesBound("1B");

            TableMetadata table = TABLES[0];
            MutationId id = new MutationId(300L, 1, 1);
            Mutation mut = mutation(0, 1).withMutationId(id);
            testJournal.write(id, mut);
            testJournal.closeCurrentSegmentForTestingIfNonEmpty();

            TableMetadata droppedTable = TableMetadata.builder("ks", "dropped_tbl_flush")
                                                      .keyspaceReplicationType(ReplicationType.tracked)
                                                      .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                      .addRegularColumn("v", UTF8Type.instance)
                                                      .build();
            Mutation mutDropped = new RowUpdateBuilder(droppedTable, 0, "pk1")
                                  .add("v", "val")
                                  .build()
                                  .withMutationId(new MutationId(301L, 1, 1));
            testJournal.write(mutDropped.id(), mutDropped);
            testJournal.closeCurrentSegmentForTestingIfNonEmpty();

            List<StaticSegment<ShortMutationId, Mutation>> staticSegments = new ArrayList<>();
            for (Segment<ShortMutationId, Mutation> seg : testJournal.getAllSegments())
            {
                if (seg.isStatic())
                    staticSegments.add((StaticSegment<ShortMutationId, Mutation>) seg);
            }
            Assert.assertEquals(2, staticSegments.size());
            staticSegments.sort(StaticSegment::compareTo);
            long seg2Id = staticSegments.get(1).id();

            // Run maybeFlushPinnedSegments
            testJournal.maybeFlushReclaimableSegments();

            // Segment 2 has dropped table (no CFS), so it was marked clean and added to pendingClearReplay
            Assert.assertTrue("Dropped table segment should be queued into pendingClearReplay",
                              testJournal.pendingCleanupForTesting().contains(seg2Id));
        }
        finally
        {
            DatabaseDescriptor.getMutationTrackingConfig().journal_reclaim_threshold = originalThreshold;
            testJournal.shutdownBlocking();
        }
    }

    @Test
    public void replayBypassSkipsMemtableApplicationAndQueuesPendingClear() throws IOException
    {
        File directory = new File(Files.createTempDirectory("mutation-journal-bypass-replay-test"));
        directory.deleteRecursiveOnExit();

        MutationJournal testJournal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        testJournal.startInternal();

        TableMetadata table = TABLES[0];
        MutationId id = new MutationId(400L, 1, 1);
        Mutation mut = mutation(0, 1).withMutationId(id);
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
