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
package org.apache.cassandra.hints;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

import static org.apache.cassandra.hints.HintsTestUtil.assertMutationsEqual;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

// TODO: test split into several files
@SuppressWarnings("deprecation")
public class LegacyHintsMigratorTest
{
    private static final String KEYSPACE = "legacy_hints_migrator_test";
    private static final String TABLE = "table";

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Test
    public void testNothingToMigrate() throws IOException
    {
        File directory = Files.createTempDirectory(null).toFile();
        try
        {
            testNothingToMigrate(directory);
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    private static void testNothingToMigrate(File directory)
    {
        // truncate system.hints to enseure nothing inside
        Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_HINTS).truncateBlocking();
        new LegacyHintsMigrator(directory, 128 * 1024 * 1024).migrate();
        HintsCatalog catalog = HintsCatalog.load(directory, HintsService.EMPTY_PARAMS);
        assertEquals(0, catalog.stores().count());
    }

    @Test
    public void testMigrationIsComplete() throws IOException
    {
        File directory = Files.createTempDirectory(null).toFile();
        try
        {
            testMigrationIsComplete(directory);
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    private static void testMigrationIsComplete(File directory)
    {
        long timestamp = System.currentTimeMillis();

        // write 100 mutations for each of the 10 generated endpoints
        Map<UUID, Queue<Mutation>> mutations = new HashMap<>();
        for (int i = 0; i < 10; i++)
        {
            UUID hostId = UUID.randomUUID();
            Queue<Mutation> queue = new LinkedList<>();
            mutations.put(hostId, queue);

            for (int j = 0; j < 100; j++)
            {
                Mutation mutation = createMutation(j, timestamp + j);
                queue.offer(mutation);
                Mutation legacyHint = createLegacyHint(mutation, timestamp, hostId);
                legacyHint.applyUnsafe();
            }
        }

        // run the migration
        new LegacyHintsMigrator(directory, 128 * 1024 * 1024).migrate();

        // validate that the hints table is truncated now
        assertTrue(Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_HINTS).isEmpty());

        HintsCatalog catalog = HintsCatalog.load(directory, HintsService.EMPTY_PARAMS);

        // assert that we've correctly loaded 10 hints stores
        assertEquals(10, catalog.stores().count());

        // for each of the 10 stores, make sure the mutations have been migrated correctly
        for (Map.Entry<UUID, Queue<Mutation>> entry : mutations.entrySet())
        {
            HintsStore store = catalog.get(entry.getKey());
            assertNotNull(store);

            HintsDescriptor descriptor = store.poll();
            assertNotNull(descriptor);

            // read all the hints
            Queue<Hint> actualHints = new LinkedList<>();
            try (HintsReader reader = HintsReader.open(new File(directory, descriptor.fileName())))
            {
                for (HintsReader.Page page : reader)
                    page.hintsIterator().forEachRemaining(actualHints::offer);
            }

            // assert the size matches
            assertEquals(100, actualHints.size());

            // compare expected hints to actual hints
            for (int i = 0; i < 100; i++)
            {
                Hint hint = actualHints.poll();
                Mutation mutation = entry.getValue().poll();
                int ttl = mutation.smallestGCGS();

                assertEquals(timestamp, hint.creationTime);
                assertEquals(ttl, hint.gcgs);
                assertMutationsEqual(mutation, hint.mutation);
            }
        }
    }

    // legacy hint mutation creation code, copied more or less verbatim from the previous implementation
    private static Mutation createLegacyHint(Mutation mutation, long now, UUID targetId)
    {
        int version = MessagingService.VERSION_21;
        int ttl = mutation.smallestGCGS();
        UUID hintId = UUIDGen.getTimeUUID();

        ByteBuffer key = UUIDType.instance.decompose(targetId);
        Clustering clustering = SystemKeyspace.LegacyHints.comparator.make(hintId, version);
        ByteBuffer value = ByteBuffer.wrap(FBUtilities.serialize(mutation, Mutation.serializer, version));
        Cell cell = BufferCell.expiring(SystemKeyspace.LegacyHints.compactValueColumn(),
                                        now,
                                        ttl,
                                        FBUtilities.nowInSeconds(),
                                        value);
        return new Mutation(PartitionUpdate.singleRowUpdate(SystemKeyspace.LegacyHints,
                                                            key,
                                                            BTreeRow.singleCellRow(clustering, cell)));
    }

    private static Mutation createMutation(int index, long timestamp)
    {
        CFMetaData table = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        return new RowUpdateBuilder(table, timestamp, bytes(index))
               .clustering(bytes(index))
               .add("val", bytes(index))
               .build();
    }
}
