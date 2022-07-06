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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotSame;
import static org.apache.cassandra.Util.dk;

public class HintsOrphanModeHintFilesCleanupTest
{
    private static final String KEYSPACE = "hint_test";
    private static final String TABLE0 = "table_0";
    private static final String TABLE1 = "table_1";
    private static final String TABLE2 = "table_2";
    static final int WRITE_BUFFER_SIZE = 256 << 10;
    private static HintsDispatchExecutor hintsDispatchExecutor;
    private static int expectedHintFilesMetrics = 0;
    private static int expectedHintStoresDetected = 0;
    private static int expectedHintStoresPurged = 0;

    @BeforeClass
    public static void defineSchema() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE0),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE2));
        hintsDispatchExecutor = new HintsDispatchExecutor(directory, 100, new AtomicBoolean(false), (addressAndPort) -> true);
    }

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void testExpiredHintStoreOfOrphanNode() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        try
        {
            // set hint store expiry to 16 days (15 is the configured default limit), so hint files should get purged
            invokeHintsDispatchTrigger(directory, Instant.now().minus(Duration.ofDays(16)).getEpochSecond(), 0, 0, true);
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    @Test
    public void testNonExpiredHintStoreOfOrphanNode() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        try
        {
            // set hint store expiry to just one day old, so hint files should not be purged
            invokeHintsDispatchTrigger(directory, Instant.now().minus(Duration.ofDays(1)).getEpochSecond() * 1000, 1, 4, false);
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    @Test
    public void testNodeValidityForPurge() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        try
        {
            long timestamp = Instant.now().minus(Duration.ofDays(16)).getEpochSecond();

            UUID validHostId = UUID.randomUUID();
            createHintFiles(validHostId, directory, timestamp);

            HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
            assertEquals(1, catalog.stores().count());

            HintsStore store = catalog.get(validHostId);

            //should have 4 hint files
            assertEquals(4, store.getDispatchQueueSize());

            HintsDispatchTrigger hintsDispatchTrigger = new HintsDispatchTrigger(catalog,
                                                                                 null,
                                                                                 hintsDispatchExecutor,
                                                                                 new AtomicBoolean(false));
            // a valid node should not be categorized as an orphan
            assertEquals(false, hintsDispatchTrigger.isOrphan(store, new HashSet<>(Arrays.asList(validHostId))));
            // an invalid store should be categorized as an orphan
            UUID someOtherHostIdIsInTheRing = UUID.randomUUID();
            assertNotSame(someOtherHostIdIsInTheRing, store.hostId);
            assertEquals(true, hintsDispatchTrigger.isOrphan(store, new HashSet<>(Arrays.asList(someOtherHostIdIsInTheRing))));
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    @Test
    public void testDurationValidityForPurge() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        try
        {
            long expiredHintStoreDuration = Instant.now().minus(Duration.ofDays(16)).getEpochSecond() * 1000;
            long nonExpiredHintStoreDuration = Instant.now().minus(Duration.ofDays(1)).getEpochSecond() * 1000;

            UUID hostId1 = UUID.randomUUID();
            createHintFiles(hostId1, directory, expiredHintStoreDuration);
            UUID hostId2 = UUID.randomUUID();
            createHintFiles(hostId2, directory, nonExpiredHintStoreDuration);

            HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
            assertEquals(2, catalog.stores().count());

            HintsStore store1 = catalog.get(hostId1);
            HintsStore store2 = catalog.get(hostId2);

            //should have 4 hint files
            assertEquals(4, store1.getDispatchQueueSize());
            assertEquals(4, store2.getDispatchQueueSize());

            HintsDispatchTrigger hintsDispatchTrigger = new HintsDispatchTrigger(catalog,
                                                                                 null,
                                                                                 hintsDispatchExecutor,
                                                                                 new AtomicBoolean(false));
            // store1 should qualify as an orphan as the hint files are meeting the expiry window (16 days old)
            assertEquals(true, hintsDispatchTrigger.isOrphan(store1, new HashSet<>(Arrays.asList())));
            // store2 should not qualify as an orphan as the hint files are not yet meeting the expiry window (just 1 day old)
            assertEquals(false, hintsDispatchTrigger.isOrphan(store2, new HashSet<>(Arrays.asList())));
        }
        finally
        {
            directory.deleteOnExit();
        }
    }

    private static void createHintFiles(UUID hostId, File directory, long timestampInMs) throws IOException
    {
        HintsDescriptor descriptor1 = new HintsDescriptor(hostId, timestampInMs);
        HintsDescriptor descriptor2 = new HintsDescriptor(hostId, timestampInMs + 1);
        HintsDescriptor descriptor3 = new HintsDescriptor(hostId, timestampInMs + 2);
        HintsDescriptor descriptor4 = new HintsDescriptor(hostId, timestampInMs + 3);

        createHintFile(directory, descriptor1);
        createHintFile(directory, descriptor2);
        createHintFile(directory, descriptor3);
        createHintFile(directory, descriptor4);
    }

    private static void invokeHintsDispatchTrigger(File directory, long timestamp, int expectedStoreCount, int expectedHintFilesCount, boolean purgeHintStoreExpected) throws IOException
    {
        UUID hostId = UUID.randomUUID();
        createHintFiles(hostId, directory, timestamp);
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(1, catalog.stores().count());

        HintsStore store = catalog.get(hostId);

        //should have 4 hint files
        assertEquals(4, store.getDispatchQueueSize());

        HintsDispatchTrigger hintsDispatchTrigger = new HintsDispatchTrigger(catalog,
                                                                             null,
                                                                             hintsDispatchExecutor,
                                                                             new AtomicBoolean(false));
        hintsDispatchTrigger.run();
        catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(expectedStoreCount, catalog.stores().count());
        store = catalog.get(hostId);

        //should have <expectedHintFilesCount> hint files now
        assertEquals(expectedHintFilesCount, store.getDispatchQueueSize());
        assertEquals(4, hintsDispatchTrigger.totalHintFiles);
        expectedHintFilesMetrics += 4;
        assertEquals(expectedHintFilesMetrics, StorageMetrics.totalHintFilesPresent.getCount());
        expectedHintStoresDetected += 1;
        if (purgeHintStoreExpected)
        {
            expectedHintStoresPurged += 1;
        }
        assertEquals(expectedHintStoresDetected, StorageMetrics.orphanHintStoresDetected.getCount());
        assertEquals(expectedHintStoresPurged, StorageMetrics.orphanHintStoresPurged.getCount());
    }

    private static Mutation createMutation(String key, long now)
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(KEYSPACE, dk(key));

        builder.update(Schema.instance.getTableMetadata(KEYSPACE, TABLE0))
        .timestamp(now)
        .row("column0")
        .add("val", "value0");

        builder.update(Schema.instance.getTableMetadata(KEYSPACE, TABLE1))
        .timestamp(now + 1)
        .row("column1")
        .add("val", "value1");

        builder.update(Schema.instance.getTableMetadata(KEYSPACE, TABLE2))
        .timestamp(now + 2)
        .row("column2")
        .add("val", "value2");

        return builder.build();
    }

    @SuppressWarnings("EmptyTryBlock")
    private static void createHintFile(File directory, HintsDescriptor descriptor) throws IOException
    {
        try (HintsWriter writer = HintsWriter.create(directory, descriptor))
        {
            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE);
            try (HintsWriter.Session session = writer.newSession(writeBuffer))
            {
                long now = FBUtilities.timestampMicros();
                Mutation mutation = createMutation("testSerializer", now);
                Hint hint = Hint.create(mutation, now / 1000);

                session.append(hint);
            }
        }
    }
}
