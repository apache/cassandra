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
package org.apache.cassandra.journal;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.*;

public class SegmentTest
{
    @Test
    public void testWriteReadActiveSegment() throws IOException
    {
        TimeUUID id1 = nextTimeUUID();
        TimeUUID id2 = nextTimeUUID();
        TimeUUID id3 = nextTimeUUID();
        TimeUUID id4 = nextTimeUUID();

        ByteBuffer record1 = ByteBufferUtil.bytes("sample record 1");
        ByteBuffer record2 = ByteBufferUtil.bytes("sample record 2");
        ByteBuffer record3 = ByteBufferUtil.bytes("sample record 3");
        ByteBuffer record4 = ByteBufferUtil.bytes("sample record 4");

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        ActiveSegment<TimeUUID, ByteBuffer> segment = ActiveSegment.create(descriptor, params(), TimeUUIDKeySupport.INSTANCE);

        segment.allocate(record1.remaining()).write(id1, record1);
        segment.allocate(record2.remaining()).write(id2, record2);
        segment.allocate(record3.remaining()).write(id3, record3);
        segment.allocate(record4.remaining()).write(id4, record4);

        // read all 4 entries by id and compare with originals
        EntrySerializer.EntryHolder<TimeUUID> holder = new EntrySerializer.EntryHolder<>();

        segment.readLast(id1, holder);
        assertEquals(id1, holder.key);
        assertEquals(record1, holder.value);

        segment.readLast(id2, holder);
        assertEquals(id2, holder.key);
        assertEquals(record2, holder.value);

        segment.readLast(id3, holder);
        assertEquals(id3, holder.key);
        assertEquals(record3, holder.value);

        segment.readLast(id4, holder);
        assertEquals(id4, holder.key);
        assertEquals(record4, holder.value);
    }

    @Test
    public void testReadClosedSegmentByID() throws IOException
    {
        DatabaseDescriptor.daemonInitialization();

        TimeUUID id1 = nextTimeUUID();
        TimeUUID id2 = nextTimeUUID();
        TimeUUID id3 = nextTimeUUID();
        TimeUUID id4 = nextTimeUUID();

        ByteBuffer record1 = ByteBufferUtil.bytes("sample record 1");
        ByteBuffer record2 = ByteBufferUtil.bytes("sample record 2");
        ByteBuffer record3 = ByteBufferUtil.bytes("sample record 3");
        ByteBuffer record4 = ByteBufferUtil.bytes("sample record 4");

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        ActiveSegment<TimeUUID, ByteBuffer> activeSegment = ActiveSegment.create(descriptor, params(), TimeUUIDKeySupport.INSTANCE);

        activeSegment.allocate(record1.remaining()).write(id1, record1);
        activeSegment.allocate(record2.remaining()).write(id2, record2);
        activeSegment.allocate(record3.remaining()).write(id3, record3);
        activeSegment.allocate(record4.remaining()).write(id4, record4);

        activeSegment.close(null);

        StaticSegment<TimeUUID, ByteBuffer> staticSegment = StaticSegment.open(descriptor, TimeUUIDKeySupport.INSTANCE);

        // read all 4 entries by id and compare with originals
        EntrySerializer.EntryHolder<TimeUUID> holder = new EntrySerializer.EntryHolder<>();

        staticSegment.readLast(id1, holder);
        assertEquals(id1, holder.key);
        assertEquals(record1, holder.value);

        staticSegment.readLast(id2, holder);
        assertEquals(id2, holder.key);
        assertEquals(record2, holder.value);

        staticSegment.readLast(id3, holder);
        assertEquals(id3, holder.key);
        assertEquals(record3, holder.value);

        staticSegment.readLast(id4, holder);
        assertEquals(id4, holder.key);
        assertEquals(record4, holder.value);
    }

    @Test
    public void testReadClosedSegmentSequentially() throws IOException
    {
        TimeUUID id1 = nextTimeUUID();
        TimeUUID id2 = nextTimeUUID();
        TimeUUID id3 = nextTimeUUID();
        TimeUUID id4 = nextTimeUUID();

        ByteBuffer record1 = ByteBufferUtil.bytes("sample record 1");
        ByteBuffer record2 = ByteBufferUtil.bytes("sample record 2");
        ByteBuffer record3 = ByteBufferUtil.bytes("sample record 3");
        ByteBuffer record4 = ByteBufferUtil.bytes("sample record 4");

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        ActiveSegment<TimeUUID, ByteBuffer> activeSegment = ActiveSegment.create(descriptor, params(), TimeUUIDKeySupport.INSTANCE);

        activeSegment.allocate(record1.remaining()).write(id1, record1);
        activeSegment.allocate(record2.remaining()).write(id2, record2);
        activeSegment.allocate(record3.remaining()).write(id3, record3);
        activeSegment.allocate(record4.remaining()).write(id4, record4);

        Segment.Tidier tidier = (Segment.Tidier)activeSegment.selfRef().tidier();
        tidier.executor = ImmediateExecutor.INSTANCE;
        OpOrder opOrder = new OpOrder();
        tidier.await = opOrder.newBarrier();
        tidier.await.issue();
        activeSegment.close(null);

        StaticSegment.SequentialReader<TimeUUID> reader = StaticSegment.sequentialReader(descriptor, TimeUUIDKeySupport.INSTANCE, activeSegment.metadata.fsyncLimit());

        // read all 4 entries sequentially and compare with originals
        assertTrue(reader.advance());
        assertEquals(id1, reader.key());
        assertEquals(record1, reader.record());

        assertTrue(reader.advance());
        assertEquals(id2, reader.key());
        assertEquals(record2, reader.record());

        assertTrue(reader.advance());
        assertEquals(id3, reader.key());
        assertEquals(record3, reader.record());

        assertTrue(reader.advance());
        assertEquals(id4, reader.key());
        assertEquals(record4, reader.record());

        assertFalse(reader.advance());
    }

    @Test
    public void testAllZeroBytes()
    {
        for (int size = 1; size < 200; size++)
        {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            withRandom(rng -> {
                buffer.put(rng.nextInt(buffer.limit()), (byte) 0xff);
                Assert.assertFalse(StaticSegment.areAllBytesZero(buffer, 0, buffer.limit()));
            });
        }
    }

    private static Params params()
    {
        return TestParams.INSTANCE;
    }
}
