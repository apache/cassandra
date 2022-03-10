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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class HintsStoreTest
{
    private static final String KEYSPACE = "hints_store_test";
    private static final String TABLE = "table";
    private File directory;
    private UUID hostId;

    @Before
    public void testSetup() throws IOException
    {
        directory = new File(Files.createTempDirectory(null));
        directory.deleteOnExit();
        hostId = UUID.randomUUID();
    }

    @BeforeClass
    public static void setup()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Test
    public void testDeleteAllExpiredHints() throws IOException
    {
        final long now = System.currentTimeMillis();
        // hints to delete
        writeHints(directory, new HintsDescriptor(hostId, now), 100, now);
        writeHints(directory, new HintsDescriptor(hostId, now + 1000), 1, now);
        HintsStore store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        assertTrue("Hints store should have files", store.hasFiles());
        assertEquals(2, store.getDispatchQueueSize());

        // jump to the future and delete.
        store.deleteExpiredHints(now + TimeUnit.SECONDS.toMillis(Hint.maxHintTTL) + 10);

        assertFalse("All hints files should be deleted", store.hasFiles());
    }

    @Test
    public void testDeleteAllExpiredHintsByHittingExpirationsCache() throws IOException
    {
        final long now = System.currentTimeMillis();
        HintsDescriptor hintsDescriptor = new HintsDescriptor(hostId, now);
        writeHints(directory, hintsDescriptor, 100, now);

        HintsStore store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        assertTrue("Hints store should have files", store.hasFiles());
        assertEquals("Hints store should not have cached expiration yet", 0, store.getHintsExpirationsMapSize());
        assertEquals(1, store.getDispatchQueueSize());

        store.deleteExpiredHints(now + 1); // Not expired yet. Wont delete.
        assertEquals("Hint should not be deleted yet", 1, store.getDispatchQueueSize());
        assertEquals("Found no cached hints expiration", 1, store.getHintsExpirationsMapSize());
        // jump to the future and delete. It should not re-read all the file
        store.deleteExpiredHints(now + TimeUnit.SECONDS.toMillis(Hint.maxHintTTL) + 10);
        assertFalse("All hints files should be deleted", store.hasFiles());
    }

    /**
     * Test multiple threads delete hints files.
     * It could happen when hint service is running a removal process, meanwhile operator issues a NodeTool command to delete.
     *
     * Thread contends and delete part of the files in the store. The final effect should all files get deleted.
     */
    @Test
    public void testConcurrentDeleteExpiredHints() throws Exception
    {
        final long now = System.currentTimeMillis();
        for (int i = 100; i >= 0; i--)
        {
            writeHints(directory, new HintsDescriptor(hostId, now - i), 100, now);
        }

        HintsStore store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        int concurrency = 3;
        CountDownLatch start = new CountDownLatch(1);
        Runnable removal = () -> {
            Uninterruptibles.awaitUninterruptibly(start);
            store.deleteExpiredHints(now + TimeUnit.SECONDS.toMillis(Hint.maxHintTTL) + 10); // jump to the future and delete
        };
        ExecutorService es = Executors.newFixedThreadPool(concurrency);
        try (Closeable ignored = es::shutdown)
        {
            for (int i = 0; i < concurrency; i++)
                es.submit(removal);
            start.countDown();
        }
        assertTrue(es.awaitTermination(2, TimeUnit.SECONDS));
        assertFalse("All hints files should be deleted", store.hasFiles());
    }

    @Test
    public void testPendingHintsInfo() throws Exception
    {
        HintsStore store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        assertNull(store.getPendingHintsInfo());

        final long t1 = 10;
        writeHints(directory, new HintsDescriptor(hostId, t1), 100, t1);
        store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        assertEquals(new PendingHintsInfo(store.hostId, 1, t1, t1),
                     store.getPendingHintsInfo());
        final long t2 = t1 + 1;
        writeHints(directory, new HintsDescriptor(hostId, t2), 100, t2);
        store = HintsCatalog.load(directory, ImmutableMap.of()).get(hostId);
        assertEquals(new PendingHintsInfo(store.hostId, 2, t1, t2),
                     store.getPendingHintsInfo());
    }

    private long writeHints(File directory, HintsDescriptor descriptor, int hintsCount, long hintCreationTime) throws IOException
    {
        try (HintsWriter writer = HintsWriter.create(directory, descriptor))
        {
            ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024);
            try (HintsWriter.Session session = writer.newSession(buffer))
            {
                for (int i = 0; i < hintsCount; i++)
                    session.append(createHint(i, hintCreationTime));
            }
            FileUtils.clean(buffer);
        }
        return new File(directory, descriptor.fileName()).lastModified(); // hint file last modified time
    }

    private Hint createHint(int idx, long creationTime)
    {
        TableMetadata table = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        Mutation mutation = new RowUpdateBuilder(table, creationTime, bytes(idx))
                            .clustering(bytes(idx))
                            .add("val", bytes(idx))
                            .build();

        return Hint.create(mutation, creationTime, 1);
    }
}
