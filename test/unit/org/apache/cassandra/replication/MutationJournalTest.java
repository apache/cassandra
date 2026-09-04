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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.replication.MutationJournal.ActiveOffsetRanges;
import org.apache.cassandra.replication.MutationJournal.OffsetRangesFactory;
import org.apache.cassandra.replication.MutationJournal.StaticOffsetRanges;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests to sanity-check the integration points with Journal
 * (mutation id and mutation ser/de, comparison, etc.)
 */
public class MutationJournalTest
{
    private static final String KEYSPACE = "mjtks";
    private static final String TABLE = "mjtt";

    private static TestDurablyReconciledOffsetsSupplier durablyReconciledOffsetsSupplier;
    private static MutationJournal journal;
    private static File directory;

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(3),
                                    TableMetadata.builder(KEYSPACE, TABLE)
                                                 .addPartitionKeyColumn("pk", UTF8Type.instance)
                                                 .addClusteringColumn("ck", UTF8Type.instance)
                                                 .addRegularColumn("value", UTF8Type.instance)
                                                 .build());

        directory = new File(Files.createTempDirectory("mutation-journal-test-simple"));
        directory.deleteRecursiveOnExit();

        durablyReconciledOffsetsSupplier = new TestDurablyReconciledOffsetsSupplier();

        journal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL, durablyReconciledOffsetsSupplier);
        journal.startInternal();
    }

    @AfterClass
    public static void tearDown()
    {
        journal.shutdownBlocking();
    }

    @Test
    public void testWriteOneReadOne()
    {
        ShortMutationId id = id(100L, 0);
        Mutation expected = mutation("key", "ck", "value");
        journal.write(id, expected);

        // regular read
        Mutation actual = journal.read(id);
        assertMutationEquals(expected, actual);

        // read via RecordConsumer
        journal.read(id, ((segment, position, key, buffer, userVersion) ->
                          {
                              assertEquals(id, key);
                              assertEquals(serialize(expected), buffer);
                          }));
    }

    @Test
    public void testWriteManyReadMany()
    {
        ShortMutationId id1 = id(100L, 1);
        ShortMutationId id2 = id(100L, 2);
        List<ShortMutationId> ids = List.of(id1, id2);

        Mutation expected1 = mutation("key1", "ck1", "value1");
        Mutation expected2 = mutation("key2", "ck2", "value2");
        List<Mutation> expected = List.of(expected1, expected2);

        journal.write(id1, expected1);
        journal.write(id2, expected2);

        List<Mutation> actual = new ArrayList<>();
        journal.readAll(ids, actual);
        assertMutationsEqual(expected, actual);
    }

    @Test
    public void testActiveOffsetRanges()
    {
        {
            ActiveOffsetRanges ranges = new ActiveOffsetRanges();
            assertFalse(ranges.mayContain(id(0, 0)));
        }

        {
            ActiveOffsetRanges ranges = new ActiveOffsetRanges();
            ranges.update(id(0, 1));
            ranges.update(id(0, 9));
            assertFalse(ranges.mayContain(id(0, 0)));
            assertFalse(ranges.mayContain(id(0, 10)));
            for (int i = 1; i < 10; i++)
                assertTrue(ranges.mayContain(id(0, i)));
        }
    }

    @Test
    public void testStaticOffsetRanges()
    {
        Descriptor descriptor = Descriptor.create(directory, 0, 1);

        ActiveOffsetRanges active = new ActiveOffsetRanges();
        for (int l = 1; l < 11; l++)
        {
            for (int o = 5; o > 0 ; o--) active.update(id(l, o));
            for (int o = 6; o < 11; o++) active.update(id(l, o));
        }

        active.persist(descriptor);
        StaticOffsetRanges loaded = OffsetRangesFactory.INSTANCE.load(descriptor);
        assertEquals(active.asMap(), loaded.asMap());

        // absent log ids
        for (int o = 0; o < 11; o++)
        {
            assertFalse(active.mayContain(id(0, o)));
            assertFalse(loaded.mayContain(id(0, o)));
            assertFalse(active.mayContain(id(11, o)));
            assertFalse(loaded.mayContain(id(11, o)));
        }

        // present log ids
        for (int l = 1; l < 11; l++)
        {
            assertFalse(active.mayContain(id(l, 0)));
            assertFalse(loaded.mayContain(id(l, 0)));
            assertFalse(active.mayContain(id(l, 11)));
            assertFalse(loaded.mayContain(id(l, 11)));
            for (int o = 1; o < 11; o++)
            {
                assertTrue(active.mayContain(id(l, o)));
                assertTrue(loaded.mayContain(id(l, o)));
            }
        }
    }

    @Test
    public void testDropSegments()
    {
        // Distinct logIds from the read tests (which use logId 100) so writes to the shared journal don't collide.
        ShortMutationId id1 = id(1000L, 0);
        ShortMutationId id2 = id(1000L, 1);
        ShortMutationId id3 = id(2000L, 2);
        ShortMutationId id4 = id(2000L, 3);

        Mutation mutation1 = mutation("key1", "ck1", "value1");
        Mutation mutation2 = mutation("key2", "ck2", "value2");
        Mutation mutation3 = mutation("key3", "ck3", "value3");
        Mutation mutation4 = mutation("key4", "ck4", "value4");

        SegmentReferenceTracker refs = journal.segmentReferenceTracker();

        int baseline = journal.countStaticSegmentsForTesting();

        // write two mutations to the first segment and flush it to make static
        long firstSegment = journal.getCurrentPosition().segmentId;
        journal.write(id1, mutation1);
        journal.write(id2, mutation2);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        // write two mutations to the second segment and flush it to make static
        journal.write(id3, mutation3);
        journal.write(id4, mutation4);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        // durably-reconciled offsets covering every mutation written above (logId 1000 -> {0,1}, 2000 -> {2,3})
        Log2OffsetsMap.Mutable allReconciled = new Log2OffsetsMap.Mutable();
        for (ShortMutationId id : List.of(id1, id2, id3, id4))
            allReconciled.add(id);

        {
            // Both segments still need replay; even fully reconciled and unreferenced they must be retained.
            durablyReconciledOffsetsSupplier.setDurablyReconciledOffsetsSupplierForTesting(() -> allReconciled);
            journal.runCompactionBlocking();
            assertEquals(baseline + 2, journal.countStaticSegmentsForTesting());
        }

        // mark both segments as not needing replay (simulate their memtables having been flushed)
        journal.clearNeedsReplayForTesting();

        {
            // Not reconciled -> retained even when !needsReplay and unreferenced (the witness gate).
            durablyReconciledOffsetsSupplier.setDurablyReconciledOffsetsSupplierForTesting(Log2OffsetsMap.Mutable::new);
            journal.runCompactionBlocking();
            assertEquals(baseline + 2, journal.countStaticSegmentsForTesting());
        }

        durablyReconciledOffsetsSupplier.setDurablyReconciledOffsetsSupplierForTesting(() -> allReconciled);

        {
            // An unrepaired sstable referencing the first segment retains it; the second (unreferenced,
            // reconciled, !needsReplay) is dropped.
            SSTableReader referrer = Mockito.mock(SSTableReader.class);
            refs.addReferenceForTesting(firstSegment, referrer);
            journal.runCompactionBlocking();
            assertEquals(baseline + 1, journal.countStaticSegmentsForTesting());

            // Releasing the last reference allows the first segment to drop too.
            refs.removeReferenceForTesting(firstSegment, referrer);
            journal.runCompactionBlocking();
            assertEquals(baseline, journal.countStaticSegmentsForTesting());
        }
    }

    @Test
    public void testWitnessOnlyWritesDoNotPinNeedsReplay()
    {
        // Isolate a fresh segment holding only the witness-only writes below.
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        long witnessSegment = journal.getCurrentPosition().segmentId;

        // Witnessed-only writes (fullReplica=false): journaled but never applied to a memtable, so never marked
        // dirty. A segment holding only such data has nothing to flush.
        journal.write(id(300L, 0), mutation("wk1", "ck", "v1"), false);
        journal.write(id(300L, 1), mutation("wk2", "ck", "v2"), false);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        assertTrue("witness-only segment should be eligible to clear needsReplay without a flush to prevent " +
                   "the journal to grow unbounded on witness-only nodes",
                   journal.pendingCleanupForTesting().contains(witnessSegment));
    }

    @Test
    public void testFullReplicaWritesPinNeedsReplayUntilFlushed()
    {
        // Isolate a fresh segment holding only the full-replica write below.
        journal.closeCurrentSegmentForTestingIfNonEmpty();
        long fullSegment = journal.getCurrentPosition().segmentId;

        // Full-replica write marks the segment dirty; without a flush it still needs replay.
        journal.write(id(400L, 0), mutation("fk1", "ck", "v1"), /* fullReplica = */ true);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        assertFalse("full-replica segment must not clear needsReplay before its memtable is flushed",
                    journal.pendingCleanupForTesting().contains(fullSegment));
    }

    private static class TestDurablyReconciledOffsetsSupplier implements Supplier<Log2OffsetsMap<?>>
    {
        private Supplier<Log2OffsetsMap<?>> nonDefaultSupplier;

        @Override
        public Log2OffsetsMap<?> get()
        {
            if (nonDefaultSupplier != null)
                return nonDefaultSupplier.get();
            Log2OffsetsMap.Mutable durablyReconciled = new Log2OffsetsMap.Mutable();
            if (MutationTrackingService.isEnabled())
                MutationTrackingService.instance().collectDurablyReconciledOffsets(durablyReconciled);
            return durablyReconciled;
        }

        public void setDurablyReconciledOffsetsSupplierForTesting(Supplier<Log2OffsetsMap<?>> nonDefaultSupplier)
        {
            this.nonDefaultSupplier = nonDefaultSupplier;
        }
    }

    private ShortMutationId id(long logId, int offset)
    {
        return new ShortMutationId(logId, offset);
    }

    private Mutation mutation(String pk, String ck, String column)
    {
        return new RowUpdateBuilder(Schema.instance.getTableMetadata(KEYSPACE, TABLE), 0, pk)
               .clustering(ck)
               .add("value", column)
               .build();
    }

    public static void assertMutationEquals(Mutation expected, Mutation actual)
    {
        if (!serialize(expected).equals(serialize(actual)))
            throw new AssertionError(String.format("Expected %s but got %s", expected, actual));
    }

    public static void assertMutationsEqual(List<Mutation> expected, List<Mutation> actual)
    {
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++)
            assertMutationEquals(expected.get(i), actual.get(i));
    }

    public static ByteBuffer serialize(Mutation mutation)
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            Mutation.serializer.serialize(mutation, out, MessagingService.maximum_version);
            return out.asNewBuffer();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }
}
