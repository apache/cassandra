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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import com.google.common.primitives.Ints;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.compress.CompressionMetadata.Chunk;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MmappedRegionsTest
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedRegionsTest.class);

    private final int OLD_MAX_SEGMENT_SIZE = MmappedRegions.MAX_SEGMENT_SIZE;

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void resetMaxSegmentSize()
    {
        MmappedRegions.MAX_SEGMENT_SIZE = OLD_MAX_SEGMENT_SIZE;
    }

    private static ByteBuffer allocateBuffer(int size)
    {
        ByteBuffer ret = ByteBuffer.allocate(Ints.checkedCast(size));
        long seed = nanoTime();
        //seed = 365238103404423L;
        logger.info("Seed {}", seed);

        new Random(seed).nextBytes(ret.array());
        byte[] arr = ret.array();
        for (int i = 0; i < arr.length; i++)
        {
            arr[i] = (byte) (arr[i] & 0xf);
        }
        return ret;
    }

    private static File writeFile(String fileName, ByteBuffer buffer) throws IOException
    {
        File ret = FileUtils.createTempFile(fileName, "1");
        ret.deleteOnExit();

        try (SequentialWriter writer = new SequentialWriter(ret))
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
        try (ChannelProxy channel = new ChannelProxy(writeFile("testEmpty", buffer));
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
        try (ChannelProxy channel = new ChannelProxy(writeFile("testTwoSegments", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(1024);
            for (int i = 0; i < 1024; i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(0, region.offset());
                assertEquals(1024, region.end());
            }

            regions.extend(2048);
            for (int i = 0; i < 2048; i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                if (i < 1024)
                {
                    assertEquals(0, region.offset());
                    assertEquals(1024, region.end());
                }
                else
                {
                    assertEquals(1024, region.offset());
                    assertEquals(2048, region.end());
                }
            }
        }
    }

    @Test
    public void testSmallSegmentSize() throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(4096);
        try (ChannelProxy channel = new ChannelProxy(writeFile("testSmallSegmentSize", buffer));
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
                assertEquals(SIZE * (i / SIZE), region.offset());
                assertEquals(SIZE + (SIZE * (i / SIZE)), region.end());
            }
        }
    }

    @Test
    public void testAllocRegions() throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(MmappedRegions.MAX_SEGMENT_SIZE * MmappedRegions.REGION_ALLOC_SIZE * 3);

        try (ChannelProxy channel = new ChannelProxy(writeFile("testAllocRegions", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(buffer.capacity());

            final int SIZE = MmappedRegions.MAX_SEGMENT_SIZE;
            for (int i = 0; i < buffer.capacity(); i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(SIZE * (i / SIZE), region.offset());
                assertEquals(SIZE + (SIZE * (i / SIZE)), region.end());
            }
        }
    }

    @Test
    public void testCopy() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(128 * 1024);

        MmappedRegions snapshot;
        ChannelProxy channelCopy;

        try (ChannelProxy channel = new ChannelProxy(writeFile("testSnapshot", buffer));
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
            assertEquals(SIZE * (i / SIZE), region.offset());
            assertEquals(SIZE + (SIZE * (i / SIZE)), region.end());

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

        try (ChannelProxy channel = new ChannelProxy(writeFile("testSnapshotCannotExtend", buffer));
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
        try (ChannelProxy channel = new ChannelProxy(writeFile("testExtendOutOfOrder", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(4096);
            regions.extend(1024);
            regions.extend(2048);

            for (int i = 0; i < buffer.capacity(); i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(0, region.offset());
                assertEquals(4096, region.end());
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeExtend() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try (ChannelProxy channel = new ChannelProxy(writeFile("testNegativeExtend", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(-1);
        }
    }

    @Test
    public void testMapForCompressionMetadata() throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(128 * 1024);
        File f = FileUtils.createTempFile("testMapForCompressionMetadata", "1");
        f.deleteOnExit();

        File cf = FileUtils.createTempFile(f.name() + ".metadata", "1");
        cf.deleteOnExit();

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try (SequentialWriter writer = new CompressedSequentialWriter(f, cf,
                                                                      null, SequentialWriterOption.DEFAULT,
                                                                      CompressionParams.snappy(), sstableMetadataCollector))
        {
            writer.write(buffer);
            writer.finish();
        }

        CompressionMetadata metadata = CompressionMetadata.open(cf, f.length(), true);
        try (ChannelProxy channel = new ChannelProxy(f);
             MmappedRegions regions = MmappedRegions.map(channel, metadata))
        {

            assertFalse(regions.isEmpty());
            int dataOffset = 0;
            while (dataOffset < buffer.capacity())
            {
                verifyChunks(f, metadata, dataOffset, regions);
                dataOffset += metadata.chunkLength();
            }
        }
        finally
        {
            metadata.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap1() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try (ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap1", buffer));
             MmappedRegions regions = MmappedRegions.map(channel, 0))
        {
            assertTrue(regions.isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap2() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try (ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap2", buffer));
             MmappedRegions regions = MmappedRegions.map(channel, -1L))
        {
            assertTrue(regions.isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap3() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        try (ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap3", buffer));
             MmappedRegions regions = MmappedRegions.map(channel, null))
        {
            assertTrue(regions.isEmpty());
        }
    }

    @Test
    public void testExtendForCompressionMetadata() throws Exception
    {
        testExtendForCompressionMetadata(8, 4, 4, 8, 12);
        testExtendForCompressionMetadata(4, 4, 4, 8, 12);
        testExtendForCompressionMetadata(2, 4, 4, 8, 12);
    }

    public void testExtendForCompressionMetadata(int maxSegmentSize, int chunkSize, int... writeSizes) throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = maxSegmentSize << 10;
        int size = Arrays.stream(writeSizes).sum() << 10;

        ByteBuffer buffer = allocateBuffer(size);
        File f = FileUtils.createTempFile("testMapForCompressionMetadata", "1");
        f.deleteOnExit();

        File cf = FileUtils.createTempFile(f.name() + ".metadata", "1");
        cf.deleteOnExit();

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, cf, null,
                                                                                SequentialWriterOption.DEFAULT,
                                                                                CompressionParams.deflate(chunkSize << 10),
                                                                                sstableMetadataCollector))
        {
            ByteBuffer slice = buffer.slice();
            slice.limit(writeSizes[0] << 10);
            writer.write(slice);
            writer.sync();

            try (ChannelProxy channel = new ChannelProxy(f);
                 CompressionMetadata metadata = writer.open(writer.getLastFlushOffset());
                 MmappedRegions regions = MmappedRegions.map(channel, metadata))
            {
                assertFalse(regions.isEmpty());
                int dataOffset = 0;
                while (dataOffset < metadata.dataLength)
                {
                    verifyChunks(f, metadata, dataOffset, regions);
                    dataOffset += metadata.chunkLength();
                }

                int idx = 1;
                int pos = writeSizes[0] << 10;
                while (idx < writeSizes.length)
                {
                    slice = buffer.slice();
                    slice.position(pos).limit(pos + (writeSizes[idx] << 10));
                    writer.write(slice);
                    writer.sync();

                    // verify that calling extend for the same (first iteration) or some previous metadata (further iterations) has no effect
                    assertFalse(regions.extend(metadata));

                    logger.info("Checking extend on compressed chunk for range={} {}..{} / {}", idx, pos, pos + (writeSizes[idx] << 10), size);
                    checkExtendOnCompressedChunks(f, writer, regions);
                    pos += writeSizes[idx] << 10;
                    idx++;
                }
            }
        }
    }

    private void checkExtendOnCompressedChunks(File f, CompressedSequentialWriter writer, MmappedRegions regions)
    {
        int dataOffset;
        try (CompressionMetadata metadata = writer.open(writer.getLastFlushOffset()))
        {
            regions.extend(metadata);
            assertFalse(regions.isEmpty());
            dataOffset = 0;
            while (dataOffset < metadata.dataLength)
            {
                logger.info("Checking chunk {}..{}", dataOffset, dataOffset + metadata.chunkLength());
                verifyChunks(f, metadata, dataOffset, regions);
                dataOffset += metadata.chunkLength();
            }
        }
    }

    private ByteBuffer fromRegions(MmappedRegions regions, int offset, int size)
    {
        ByteBuffer buf = ByteBuffer.allocate(size);

        while (buf.remaining() > 0)
        {
            MmappedRegions.Region region = regions.floor(offset);
            ByteBuffer regBuf = region.buffer.slice();
            int regBufOffset = (int) (offset - region.offset);
            regBuf.position(regBufOffset);
            regBuf.limit(regBufOffset + Math.min(buf.remaining(), regBuf.remaining()));
            offset += regBuf.remaining();
            buf.put(regBuf);
        }

        buf.flip();
        return buf;
    }

    private Chunk verifyChunks(File f, CompressionMetadata metadata, long dataOffset, MmappedRegions regions)
    {
        Chunk chunk = metadata.chunkFor(dataOffset);

        ByteBuffer compressedChunk = fromRegions(regions, (int) chunk.offset, chunk.length + 4);
        assertThat(compressedChunk.capacity()).isEqualTo(chunk.length + 4);

        try (ChannelProxy channel = new ChannelProxy(f))
        {
            ByteBuffer buf = ByteBuffer.allocate(compressedChunk.remaining());
            long len = channel.read(buf, chunk.offset);
            assertThat(len).isEqualTo(chunk.length + 4);
            buf.flip();
            String mmappedHex = ByteBufferUtil.bytesToHex(compressedChunk);
            String fileHex = ByteBufferUtil.bytesToHex(buf);
            assertThat(fileHex).isEqualTo(mmappedHex);
        }

        return chunk;
    }
}
