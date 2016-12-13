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
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;

import static junit.framework.Assert.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.FBUtilities.updateChecksum;

public class HintsBufferTest
{
    private static final String KEYSPACE = "hints_buffer_test";
    private static final String TABLE = "table";

    private static final int HINTS_COUNT = 300_000;
    private static final int HINT_THREADS_COUNT = 10;
    private static final int HOST_ID_COUNT = 10;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Test
    @SuppressWarnings("resource")
    public void testOverlyLargeAllocation()
    {
        // create a small, 128 bytes buffer
        HintsBuffer buffer = HintsBuffer.create(128);

        // try allocating an entry of 65 bytes (53 bytes hint + 12 bytes of overhead)
        try
        {
            buffer.allocate(65 - HintsBuffer.ENTRY_OVERHEAD_SIZE);
            fail("Allocation of the buffer should have failed but hasn't");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(String.format("Hint of %s bytes is too large - the maximum size is 64", 65 - HintsBuffer.ENTRY_OVERHEAD_SIZE),
                         e.getMessage());
        }

        // assert that a 1-byte smaller allocation fits properly
        try (HintsBuffer.Allocation allocation = buffer.allocate(64 - HintsBuffer.ENTRY_OVERHEAD_SIZE))
        {
            assertNotNull(allocation);
        }
    }

    @Test
    public void testWrite() throws IOException, InterruptedException
    {
        // generate 10 random host ids to choose from
        UUID[] hostIds = new UUID[HOST_ID_COUNT];
        for (int i = 0; i < hostIds.length; i++)
            hostIds[i] = UUID.randomUUID();

        // map each index to one random UUID from the previously created UUID array
        Random random = new Random(System.currentTimeMillis());
        UUID[] load = new UUID[HINTS_COUNT];
        for (int i = 0; i < load.length; i++)
            load[i] = hostIds[random.nextInt(HOST_ID_COUNT)];

        // calculate the size of a single hint (they will all have an equal size in this test)
        int hintSize = (int) Hint.serializer.serializedSize(createHint(0, System.currentTimeMillis()), MessagingService.current_version);
        int entrySize = hintSize + HintsBuffer.ENTRY_OVERHEAD_SIZE;

        // allocate a slab to fit *precisely* HINTS_COUNT hints
        int slabSize = entrySize * HINTS_COUNT;
        HintsBuffer buffer = HintsBuffer.create(slabSize);

        // use a fixed timestamp base for all mutation timestamps
        long baseTimestamp = System.currentTimeMillis();

        // create HINT_THREADS_COUNT, start them, and wait for them to finish
        List<Thread> threads = new ArrayList<>(HINT_THREADS_COUNT);
        for (int i = 0; i < HINT_THREADS_COUNT; i ++)
            threads.add(NamedThreadFactory.createThread(new Writer(buffer, load, hintSize, i, baseTimestamp)));
        threads.forEach(java.lang.Thread::start);
        for (Thread thread : threads)
            thread.join();

        // sanity check that we are full
        assertEquals(slabSize, buffer.capacity());
        assertEquals(0, buffer.remaining());

        // try to allocate more bytes, ensure that the allocation fails
        assertNull(buffer.allocate(1));

        // a failed allocation should automatically close the oporder
        buffer.waitForModifications();

        // a failed allocation should also automatically make the buffer as closed
        assertTrue(buffer.isClosed());

        // assert that host id set in the buffer equals to hostIds
        assertEquals(HOST_ID_COUNT, buffer.hostIds().size());
        assertEquals(new HashSet<>(Arrays.asList(hostIds)), buffer.hostIds());

        // iterate over *every written hint*, validate its content
        for (UUID hostId : hostIds)
        {
            Iterator<ByteBuffer> iter = buffer.consumingHintsIterator(hostId);
            while (iter.hasNext())
            {
                int idx = validateEntry(hostId, iter.next(), baseTimestamp, load);
                load[idx] = null; // nullify each visited entry
            }
        }

        // assert that all the entries in load array have been visited and nullified
        for (UUID hostId : load)
            assertNull(hostId);

        // free the buffer
        buffer.free();
    }

    private static int validateEntry(UUID hostId, ByteBuffer buffer, long baseTimestamp, UUID[] load) throws IOException
    {
        CRC32 crc = new CRC32();
        DataInputPlus di = new DataInputBuffer(buffer, true);

        // read and validate size
        int hintSize = di.readInt();
        assertEquals(hintSize + HintsBuffer.ENTRY_OVERHEAD_SIZE, buffer.remaining());

        // read and validate size crc
        updateChecksum(crc, buffer, buffer.position(), 4);
        assertEquals((int) crc.getValue(), di.readInt());

        // read the hint and update/validate overall crc
        Hint hint = Hint.serializer.deserialize(di, MessagingService.current_version);
        updateChecksum(crc, buffer, buffer.position() + 8, hintSize);
        assertEquals((int) crc.getValue(), di.readInt());

        // further validate hint correctness
        int idx = (int) (hint.creationTime - baseTimestamp);
        assertEquals(hostId, load[idx]);

        Row row = hint.mutation.getPartitionUpdates().iterator().next().iterator().next();
        assertEquals(1, Iterables.size(row.cells()));

        assertEquals(bytes(idx), row.clustering().get(0));
        Cell cell = row.cells().iterator().next();
        assertEquals(TimeUnit.MILLISECONDS.toMicros(baseTimestamp + idx), cell.timestamp());
        assertEquals(bytes(idx), cell.value());

        return idx;
    }

    static Hint createHint(int idx, long baseTimestamp)
    {
        long timestamp = baseTimestamp + idx;
        return Hint.create(createMutation(idx, TimeUnit.MILLISECONDS.toMicros(timestamp)), timestamp);
    }

    private static Mutation createMutation(int index, long timestamp)
    {
        CFMetaData table = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        return new RowUpdateBuilder(table, timestamp, bytes(index))
                   .clustering(bytes(index))
                   .add("val", bytes(index))
                   .build();
    }

    static class Writer implements Runnable
    {
        final HintsBuffer buffer;
        final UUID[] load;
        final int hintSize;
        final int index;
        final long baseTimestamp;

        Writer(HintsBuffer buffer, UUID[] load, int hintSize, int index, long baseTimestamp)
        {
            this.buffer = buffer;
            this.load = load;
            this.hintSize = hintSize;
            this.index = index;
            this.baseTimestamp = baseTimestamp;
        }

        public void run()
        {
            int hintsPerThread = HINTS_COUNT / HINT_THREADS_COUNT;
            for (int i = index * hintsPerThread; i < (index + 1) * hintsPerThread; i++)
            {
                try (HintsBuffer.Allocation allocation = buffer.allocate(hintSize))
                {
                    Hint hint = createHint(i, baseTimestamp);
                    allocation.write(Collections.singleton(load[i]), hint);
                }
            }
        }
    }
}
