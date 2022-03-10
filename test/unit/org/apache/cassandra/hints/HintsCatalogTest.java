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
import java.util.*;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.Util.dk;

public class HintsCatalogTest
{
    private static final String KEYSPACE = "hint_test";
    private static final String TABLE0 = "table_0";
    private static final String TABLE1 = "table_1";
    private static final String TABLE2 = "table_2";
    private static final int WRITE_BUFFER_SIZE = 256 << 10;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE0),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE1),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE2));
    }

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Test
    public void loadCompletenessAndOrderTest() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        loadCompletenessAndOrderTest(directory);
    }

    private void loadCompletenessAndOrderTest(File directory) throws IOException
    {
        UUID hostId1 = UUID.randomUUID();
        UUID hostId2 = UUID.randomUUID();

        long timestamp1 = Clock.Global.currentTimeMillis();
        long timestamp2 = Clock.Global.currentTimeMillis() + 1;
        long timestamp3 = Clock.Global.currentTimeMillis() + 2;
        long timestamp4 = Clock.Global.currentTimeMillis() + 3;

        HintsDescriptor descriptor1 = new HintsDescriptor(hostId1, timestamp1);
        HintsDescriptor descriptor2 = new HintsDescriptor(hostId2, timestamp3);
        HintsDescriptor descriptor3 = new HintsDescriptor(hostId2, timestamp2);
        HintsDescriptor descriptor4 = new HintsDescriptor(hostId1, timestamp4);

        writeDescriptor(directory, descriptor1);
        writeDescriptor(directory, descriptor2);
        writeDescriptor(directory, descriptor3);
        writeDescriptor(directory, descriptor4);

        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(2, catalog.stores().count());

        HintsStore store1 = catalog.get(hostId1);
        assertNotNull(store1);
        assertEquals(descriptor1, store1.poll());
        assertEquals(descriptor4, store1.poll());
        assertNull(store1.poll());

        HintsStore store2 = catalog.get(hostId2);
        assertNotNull(store2);
        assertEquals(descriptor3, store2.poll());
        assertEquals(descriptor2, store2.poll());
        assertNull(store2.poll());
    }

    @Test
    public void deleteHintsTest() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        UUID hostId1 = UUID.randomUUID();
        UUID hostId2 = UUID.randomUUID();
        long now = Clock.Global.currentTimeMillis();
        writeDescriptor(directory, new HintsDescriptor(hostId1, now));
        writeDescriptor(directory, new HintsDescriptor(hostId1, now + 1));
        writeDescriptor(directory, new HintsDescriptor(hostId2, now + 2));
        writeDescriptor(directory, new HintsDescriptor(hostId2, now + 3));

        // load catalog containing two stores (one for each host)
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(2, catalog.stores().count());
        assertTrue(catalog.hasFiles());

        // delete all hints from store 1
        assertTrue(catalog.get(hostId1).hasFiles());
        catalog.deleteAllHints(hostId1);
        assertFalse(catalog.get(hostId1).hasFiles());
        // stores are still keepts for each host, even after deleting hints
        assertEquals(2, catalog.stores().count());
        assertTrue(catalog.hasFiles());

        // delete all hints from all stores
        catalog.deleteAllHints();
        assertEquals(2, catalog.stores().count());
        assertFalse(catalog.hasFiles());
    }

    @Test
    public void exciseHintFiles() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        exciseHintFiles(directory);
    }

    @Test
    public void hintsTotalSizeTest() throws IOException
    {
        File directory = new File(testFolder.newFolder());
        UUID hostId = UUID.randomUUID();
        long now = Clock.Global.currentTimeMillis();
        long totalSize = 0;
        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        HintsStore store = catalog.get(hostId);
        assertEquals(totalSize, store.getTotalFileSize());
        for (int i = 0; i < 3; i++)
        {
            HintsDescriptor descriptor = new HintsDescriptor(hostId, now + i);
            writeDescriptor(directory, descriptor);
            store.offerLast(descriptor);
            assertTrue("Total file size should increase after writing more hints", store.getTotalFileSize() > totalSize);
            totalSize = store.getTotalFileSize();
        }
    }

    private static void exciseHintFiles(File directory) throws IOException
    {
        UUID hostId = UUID.randomUUID();

        HintsDescriptor descriptor1 = new HintsDescriptor(hostId, Clock.Global.currentTimeMillis());
        HintsDescriptor descriptor2 = new HintsDescriptor(hostId, Clock.Global.currentTimeMillis() + 1);
        HintsDescriptor descriptor3 = new HintsDescriptor(hostId, Clock.Global.currentTimeMillis() + 2);
        HintsDescriptor descriptor4 = new HintsDescriptor(hostId, Clock.Global.currentTimeMillis() + 3);

        createHintFile(directory, descriptor1);
        createHintFile(directory, descriptor2);
        createHintFile(directory, descriptor3);
        createHintFile(directory, descriptor4);

        HintsCatalog catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(1, catalog.stores().count());

        HintsStore store = catalog.get(hostId);

        //should have 4 hint files
        assertEquals(4, store.getDispatchQueueSize());

        //excise store as a result it should remove all the hint files
        catalog.exciseStore(hostId);

        catalog = HintsCatalog.load(directory, ImmutableMap.of());
        assertEquals(0, catalog.stores().count());
        store = catalog.get(hostId);

        //should have 0 hint files now
        assertEquals(0, store.getDispatchQueueSize());
    }

    @SuppressWarnings("EmptyTryBlock")
    private static void writeDescriptor(File directory, HintsDescriptor descriptor) throws IOException
    {
        try (HintsWriter ignored = HintsWriter.create(directory, descriptor))
        {
        }
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
