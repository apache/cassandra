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

package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import com.google.common.primitives.Ints;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ChecksumType;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MmappedRegionsTest
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedRegionsTest.class);

    private static ByteBuffer allocateBuffer(int size)
    {
        ByteBuffer ret = ByteBuffer.allocate(Ints.checkedCast(size));
        long seed = System.nanoTime();
        //seed = 365238103404423L;
        logger.info("Seed {}", seed);

        new Random(seed).nextBytes(ret.array());
        return ret;
    }

    private static File writeFile(String fileName, ByteBuffer buffer) throws IOException
    {
        File ret = File.createTempFile(fileName, "1");
        ret.deleteOnExit();

        try (SequentialWriter writer = SequentialWriter.open(ret))
        {
            writer.write(buffer);
            writer.finish();
        }

        assert ret.exists();
        assert ret.length() >= buffer.capacity();
        return ret;

    }

    @Test
    public void testEmpty() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testEmpty", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            assertTrue(regions.isEmpty());
            assertTrue(regions.isValid(channel));
        }
    }

    @Test
    public void testTwoSegments() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(2048);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testTwoSegments", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(1024);
            for (int i = 0; i < 1024; i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(0, region.bottom());
                assertEquals(1024, region.top());
            }

            regions.extend(2048);
            for (int i = 0; i < 2048; i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                if (i < 1024)
                {
                    assertEquals(0, region.bottom());
                    assertEquals(1024, region.top());
                }
                else
                {
                    assertEquals(1024, region.bottom());
                    assertEquals(2048, region.top());
                }
            }
        }
    }

    @Test
    public void testSmallSegmentSize() throws Exception
    {
        int OLD_MAX_SEGMENT_SIZE = MmappedRegions.MAX_SEGMENT_SIZE;
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(4096);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testSmallSegmentSize", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(1024);
            regions.extend(2048);
            regions.extend(4096);

            final int SIZE = MmappedRegions.MAX_SEGMENT_SIZE;
            for (int i = 0; i < buffer.capacity(); i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(SIZE * (i / SIZE), region.bottom());
                assertEquals(SIZE + (SIZE * (i / SIZE)), region.top());
            }
        }
        finally
        {
            MmappedRegions.MAX_SEGMENT_SIZE = OLD_MAX_SEGMENT_SIZE;
        }
    }

    @Test
    public void testAllocRegions() throws Exception
    {
        int OLD_MAX_SEGMENT_SIZE = MmappedRegions.MAX_SEGMENT_SIZE;
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(MmappedRegions.MAX_SEGMENT_SIZE * MmappedRegions.REGION_ALLOC_SIZE * 3);

        try(ChannelProxy channel = new ChannelProxy(writeFile("testAllocRegions", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(buffer.capacity());

            final int SIZE = MmappedRegions.MAX_SEGMENT_SIZE;
            for (int i = 0; i < buffer.capacity(); i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(SIZE * (i / SIZE), region.bottom());
                assertEquals(SIZE + (SIZE * (i / SIZE)), region.top());
            }
        }
        finally
        {
            MmappedRegions.MAX_SEGMENT_SIZE = OLD_MAX_SEGMENT_SIZE;
        }
    }

    @Test
    public void testCopy() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(128 * 1024);

        MmappedRegions snapshot;
        ChannelProxy channelCopy;

        try(ChannelProxy channel = new ChannelProxy(writeFile("testSnapshot", buffer));
            MmappedRegions regions = MmappedRegions.map(channel, buffer.capacity() / 4))
        {
            // create 3 more segments, one per quater capacity
            regions.extend(buffer.capacity() / 2);
            regions.extend(3 * buffer.capacity() / 4);
            regions.extend(buffer.capacity());

            // make a snapshot
            snapshot = regions.sharedCopy();

            // keep the channel open
            channelCopy = channel.sharedCopy();
        }

        assertFalse(snapshot.isCleanedUp());

        final int SIZE = buffer.capacity() / 4;
        for (int i = 0; i < buffer.capacity(); i++)
        {
            MmappedRegions.Region region = snapshot.floor(i);
            assertNotNull(region);
            assertEquals(SIZE * (i / SIZE), region.bottom());
            assertEquals(SIZE + (SIZE * (i / SIZE)), region.top());

            // check we can access the buffer
            assertNotNull(region.buffer.duplicate().getInt());
        }

        assertNull(snapshot.close(null));
        assertNull(channelCopy.close(null));
        assertTrue(snapshot.isCleanedUp());
    }

    @Test(expected = AssertionError.class)
    public void testCopyCannotExtend() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(128 * 1024);

        MmappedRegions snapshot;
        ChannelProxy channelCopy;

        try(ChannelProxy channel = new ChannelProxy(writeFile("testSnapshotCannotExtend", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(buffer.capacity() / 2);

            // make a snapshot
            snapshot = regions.sharedCopy();

            // keep the channel open
            channelCopy = channel.sharedCopy();
        }

        try
        {
            snapshot.extend(buffer.capacity());
        }
        finally
        {
            assertNull(snapshot.close(null));
            assertNull(channelCopy.close(null));
        }
    }

    @Test
    public void testExtendOutOfOrder() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(4096);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testExtendOutOfOrder", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(4096);
            regions.extend(1024);
            regions.extend(2048);

            for (int i = 0; i < buffer.capacity(); i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(0, region.bottom());
                assertEquals(4096, region.top());
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeExtend() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testNegativeExtend", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(-1);
        }
    }

    @Test
    public void testMapForCompressionMetadata() throws Exception
    {
        int OLD_MAX_SEGMENT_SIZE = MmappedRegions.MAX_SEGMENT_SIZE;
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(128 * 1024);
        File f = File.createTempFile("testMapForCompressionMetadata", "1");
        f.deleteOnExit();

        File cf = File.createTempFile(f.getName() + ".metadata", "1");
        cf.deleteOnExit();

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance))
                                                     .replayPosition(null);
        try(SequentialWriter writer = new CompressedSequentialWriter(f,
                                                                     cf.getAbsolutePath(),
                                                                     CompressionParams.snappy(),
                                                                     sstableMetadataCollector))
        {
            writer.write(buffer);
            writer.finish();
        }

        CompressionMetadata metadata = new CompressionMetadata(cf.getAbsolutePath(), f.length(), ChecksumType.CRC32);
        try(ChannelProxy channel = new ChannelProxy(f);
            MmappedRegions regions = MmappedRegions.map(channel, metadata))
        {

            assertFalse(regions.isEmpty());
            int i = 0;
            while(i < buffer.capacity())
            {
                CompressionMetadata.Chunk chunk = metadata.chunkFor(i);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                assertNotNull(region);

                ByteBuffer compressedChunk = region.buffer.duplicate();
                assertNotNull(compressedChunk);
                assertEquals(chunk.length + 4, compressedChunk.capacity());

                assertEquals(chunk.offset, region.bottom());
                assertEquals(chunk.offset + chunk.length + 4, region.top());

                i += metadata.chunkLength();
            }
        }
        finally
        {
            MmappedRegions.MAX_SEGMENT_SIZE = OLD_MAX_SEGMENT_SIZE;
            metadata.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap1() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap1", buffer));
            MmappedRegions regions = MmappedRegions.map(channel, 0))
        {
            assertTrue(regions.isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap2() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap2", buffer));
            MmappedRegions regions = MmappedRegions.map(channel, -1L))
        {
            assertTrue(regions.isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap3() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap3", buffer));
            MmappedRegions regions = MmappedRegions.map(channel, null))
        {
            assertTrue(regions.isEmpty());
        }
    }

}
