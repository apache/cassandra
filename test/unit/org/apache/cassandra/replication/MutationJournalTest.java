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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.journal.TestParams;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.replication.MutationJournal.ActiveOffsetRanges;
import org.apache.cassandra.replication.MutationJournal.StaticOffsetRanges;
import org.apache.cassandra.replication.MutationJournal.OffsetRangesFactory;

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

        journal = new MutationJournal(directory, TestParams.MUTATION_JOURNAL);
        journal.start();
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
    public void testDropReconcileSegments()
    {
        ShortMutationId id1 = id(100L, 0);
        ShortMutationId id2 = id(100L, 1);
        ShortMutationId id3 = id(200L, 2);
        ShortMutationId id4 = id(200L, 3);

        Mutation mutation1 = mutation("key1", "ck1", "value1");
        Mutation mutation2 = mutation("key2", "ck2", "value2");
        Mutation mutation3 = mutation("key3", "ck3", "value3");
        Mutation mutation4 = mutation("key4", "ck4", "value4");

        // write two mutations to the first segment and flush it to make static
        journal.write(id1, mutation1);
        journal.write(id2, mutation2);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        // write two mutations to the second segment and flush it to make static
        journal.write(id3, mutation3);
        journal.write(id4, mutation4);
        journal.closeCurrentSegmentForTestingIfNonEmpty();

        {
            // call dropReconciledSegments() with a log2offsets map that covers both segments fully
            // *BUT* with the segments still marked as needing replay nothing should be dropped
            Log2OffsetsMap.Immutable.Builder builder = new Log2OffsetsMap.Immutable.Builder();
            builder.add(id1);
            builder.add(id2);
            builder.add(id3);
            builder.add(id4);
            assertEquals(0, journal.dropReconciledSegments(builder.build()));
            // confirm that no static segments have been dropped
            assertEquals(2, journal.countStaticSegmentsForTesting());
        }

        // mark both segments as not needing replay
        journal.clearNeedsReplayForTesting();

        {
            // call dropReconciledSegments() with a log2offsets map that doesn't cover any segments fully
            Log2OffsetsMap.Immutable.Builder builder = new Log2OffsetsMap.Immutable.Builder();
            builder.add(id1);
            assertEquals(0, journal.dropReconciledSegments(builder.build()));
            // confirm that no static segments got dropped
            assertEquals(2, journal.countStaticSegmentsForTesting());
        }

        {
            // call dropReconciledSegments() with a log2offsets map that covers only the first segment fully
            Log2OffsetsMap.Immutable.Builder builder = new Log2OffsetsMap.Immutable.Builder();
            builder.add(id1);
            builder.add(id2);
            assertEquals(1, journal.dropReconciledSegments(builder.build()));
            // confirm that only one static segment got dropped
            assertEquals(1, journal.countStaticSegmentsForTesting());
        }

        {
            // call dropReconciledSegments() with a log2offsets map that covers both segments fully
            Log2OffsetsMap.Immutable.Builder builder = new Log2OffsetsMap.Immutable.Builder();
            builder.add(id1);
            builder.add(id2);
            builder.add(id3);
            builder.add(id4);
            assertEquals(1, journal.dropReconciledSegments(builder.build()));
            // confirm that all static segments have now been dropped
            assertEquals(0, journal.countStaticSegmentsForTesting());
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
