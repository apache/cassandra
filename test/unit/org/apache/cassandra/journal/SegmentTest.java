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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

        Random rng = new Random();

        int host1 = rng.nextInt();
        int host2 = rng.nextInt();
        int host3 = rng.nextInt();
        int host4 = rng.nextInt();

        Set<Integer> hosts1 = set(host1);
        Set<Integer> hosts2 = set(host1, host2);
        Set<Integer> hosts3 = set(host1, host2, host3);
        Set<Integer> hosts4 = set(host4);

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        ActiveSegment<TimeUUID> segment = ActiveSegment.create(descriptor, params(), TimeUUIDKeySupport.INSTANCE);

        segment.allocate(record1.remaining(), hosts1).write(id1, record1, hosts1);
        segment.allocate(record2.remaining(), hosts2).write(id2, record2, hosts2);
        segment.allocate(record3.remaining(), hosts3).write(id3, record3, hosts3);
        segment.allocate(record4.remaining(), hosts4).write(id4, record4, hosts4);

        // read all 4 entries by id and compare with originals
        EntrySerializer.EntryHolder<TimeUUID> holder = new EntrySerializer.EntryHolder<>();

        segment.readFirst(id1, holder);
        assertEquals(id1, holder.key);
        assertEquals(hosts1, holder.hosts);
        assertEquals(record1, holder.value);

        segment.readFirst(id2, holder);
        assertEquals(id2, holder.key);
        assertEquals(hosts2, holder.hosts);
        assertEquals(record2, holder.value);

        segment.readFirst(id3, holder);
        assertEquals(id3, holder.key);
        assertEquals(hosts3, holder.hosts);
        assertEquals(record3, holder.value);

        segment.readFirst(id4, holder);
        assertEquals(id4, holder.key);
        assertEquals(hosts4, holder.hosts);
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

        Random rng = new Random();

        int host1 = rng.nextInt();
        int host2 = rng.nextInt();
        int host3 = rng.nextInt();
        int host4 = rng.nextInt();

        Set<Integer> hosts1 = set(host1);
        Set<Integer> hosts2 = set(host1, host2);
        Set<Integer> hosts3 = set(host1, host2, host3);
        Set<Integer> hosts4 = set(host4);

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        ActiveSegment<TimeUUID> activeSegment = ActiveSegment.create(descriptor, params(), TimeUUIDKeySupport.INSTANCE);

        activeSegment.allocate(record1.remaining(), hosts1).write(id1, record1, hosts1);
        activeSegment.allocate(record2.remaining(), hosts2).write(id2, record2, hosts2);
        activeSegment.allocate(record3.remaining(), hosts3).write(id3, record3, hosts3);
        activeSegment.allocate(record4.remaining(), hosts4).write(id4, record4, hosts4);

        activeSegment.close();

        StaticSegment<TimeUUID> staticSegment = StaticSegment.open(descriptor, TimeUUIDKeySupport.INSTANCE);

        // read all 4 entries by id and compare with originals
        EntrySerializer.EntryHolder<TimeUUID> holder = new EntrySerializer.EntryHolder<>();

        staticSegment.readFirst(id1, holder);
        assertEquals(id1, holder.key);
        assertEquals(hosts1, holder.hosts);
        assertEquals(record1, holder.value);

        staticSegment.readFirst(id2, holder);
        assertEquals(id2, holder.key);
        assertEquals(hosts2, holder.hosts);
        assertEquals(record2, holder.value);

        staticSegment.readFirst(id3, holder);
        assertEquals(id3, holder.key);
        assertEquals(hosts3, holder.hosts);
        assertEquals(record3, holder.value);

        staticSegment.readFirst(id4, holder);
        assertEquals(id4, holder.key);
        assertEquals(hosts4, holder.hosts);
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

        Random rng = new Random();

        int host1 = rng.nextInt();
        int host2 = rng.nextInt();
        int host3 = rng.nextInt();
        int host4 = rng.nextInt();

        Set<Integer> hosts1 = set(host1);
        Set<Integer> hosts2 = set(host1, host2);
        Set<Integer> hosts3 = set(host1, host2, host3);
        Set<Integer> hosts4 = set(host4);

        File directory = new File(Files.createTempDirectory(null));
        directory.deleteRecursiveOnExit();

        Descriptor descriptor = Descriptor.create(directory, System.currentTimeMillis(), 1);

        ActiveSegment<TimeUUID> activeSegment = ActiveSegment.create(descriptor, params(), TimeUUIDKeySupport.INSTANCE);

        activeSegment.allocate(record1.remaining(), hosts1).write(id1, record1, hosts1);
        activeSegment.allocate(record2.remaining(), hosts2).write(id2, record2, hosts2);
        activeSegment.allocate(record3.remaining(), hosts3).write(id3, record3, hosts3);
        activeSegment.allocate(record4.remaining(), hosts4).write(id4, record4, hosts4);

        activeSegment.close();

        StaticSegment.SequentialReader<TimeUUID> reader = StaticSegment.reader(descriptor, TimeUUIDKeySupport.INSTANCE, 0);

        // read all 4 entries sequentially and compare with originals
        assertTrue(reader.advance());
        assertEquals(id1, reader.id());
        assertEquals(hosts1, reader.hosts());
        assertEquals(record1, reader.record());

        assertTrue(reader.advance());
        assertEquals(id2, reader.id());
        assertEquals(hosts2, reader.hosts());
        assertEquals(record2, reader.record());

        assertTrue(reader.advance());
        assertEquals(id3, reader.id());
        assertEquals(hosts3, reader.hosts());
        assertEquals(record3, reader.record());

        assertTrue(reader.advance());
        assertEquals(id4, reader.id());
        assertEquals(hosts4, reader.hosts());
        assertEquals(record4, reader.record());

        assertFalse(reader.advance());
    }

    private static Set<Integer> set(Integer... ids)
    {
        return new HashSet<>(Arrays.asList(ids));
    }

    private static Params params()
    {
        return TestParams.INSTANCE;
    }
}
