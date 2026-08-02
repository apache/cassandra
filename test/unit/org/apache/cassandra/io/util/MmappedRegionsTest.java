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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        int bufSize = 1024;
        try (ChannelProxy channel = new ChannelProxy(writeFile("testTwoSegments", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(1024, bufSize);
            for (int i = 0; i < 1024; i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(0, region.offset());
                assertEquals(1024, region.end());
            }

            regions.extend(2048, bufSize);
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
        int bufSize = 1024;

        ByteBuffer buffer = allocateBuffer(4096);
        try (ChannelProxy channel = new ChannelProxy(writeFile("testSmallSegmentSize", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(1024, bufSize);
            regions.extend(2048, bufSize);
            regions.extend(4096, bufSize);

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
    public void testSizeIsChunkMultiple() throws Exception
    {
        final int oldMaxSegmentSize = MmappedRegions.MAX_SEGMENT_SIZE;
        final int bufSize = 1024;
        MmappedRegions.MAX_SEGMENT_SIZE = 2047;
        ByteBuffer buffer = allocateBuffer(4096);
        try(ChannelProxy channel = new ChannelProxy(writeFile("testSmallSegmentSize", buffer));
            MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(1024, bufSize);
            regions.extend(2048, bufSize);
            regions.extend(4096, bufSize);
            for (int i = 0; i < buffer.capacity(); i++)
            {
                MmappedRegions.Region region = regions.floor(i);
                assertNotNull(region);
                assertEquals(bufSize * (i / bufSize), region.offset());
                assertEquals(bufSize + (bufSize * (i / bufSize)), region.end());
            }
        }
        finally
        {
            MmappedRegions.MAX_SEGMENT_SIZE = oldMaxSegmentSize;
        }
    }

    @Test
    public void testAllocRegions() throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        ByteBuffer buffer = allocateBuffer(MmappedRegions.MAX_SEGMENT_SIZE * MmappedRegions.REGION_ALLOC_SIZE * 3);
        int bufSize = 1024;

        try (ChannelProxy channel = new ChannelProxy(writeFile("testAllocRegions", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(buffer.capacity(), bufSize);

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
        int bufSize = 4096;

        MmappedRegions snapshot;
        ChannelProxy channelCopy;

        try (ChannelProxy channel = new ChannelProxy(writeFile("testSnapshot", buffer));
             MmappedRegions regions = MmappedRegions.map(channel, buffer.capacity() / 4, bufSize))
        {
            // create 3 more segments, one per quater capacity
            regions.extend(buffer.capacity() / 2, bufSize);
            regions.extend(3 * buffer.capacity() / 4, bufSize);
            regions.extend(buffer.capacity(), bufSize);

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
        int bufSize = 1024;

        MmappedRegions snapshot;
        ChannelProxy channelCopy;

        try (ChannelProxy channel = new ChannelProxy(writeFile("testSnapshotCannotExtend", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(buffer.capacity() / 2, bufSize);

            // make a snapshot
            snapshot = regions.sharedCopy();

            // keep the channel open
            channelCopy = channel.sharedCopy();
        }

        try
        {
            snapshot.extend(buffer.capacity(), bufSize);
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
        int bufSize = 1024;
        try (ChannelProxy channel = new ChannelProxy(writeFile("testExtendOutOfOrder", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(4096, bufSize);
            regions.extend(1024, bufSize);
            regions.extend(2048, bufSize);

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
        int bufSize = 1024;
        try (ChannelProxy channel = new ChannelProxy(writeFile("testNegativeExtend", buffer));
             MmappedRegions regions = MmappedRegions.empty(channel))
        {
            regions.extend(-1, bufSize);
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

    /**
     * A compressed file whose first chunk does NOT start at physical 0, i.e. one carrying leading bytes that
     * belong to no chunk. {@link org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter} produces exactly this
     * when it aligns a child's Data.db so that its extents can be shared with the parent's: up to 64 KiB of the
     * parent's previous chunk sits at the head and {@code offsets[0]} is that pad rather than 0.
     * <p>
     * Segments are placed at a cumulative sum of {@code chunk.length + 4}, so seeding that sum at 0 rather than
     * at the first chunk's offset mapped every region {@code pad} bytes too early and left the last {@code pad}
     * bytes of the file unmapped. With MAX_SEGMENT_SIZE forced down to one chunk each, that shows up as every
     * region after the first being offset by the pad -- so this is the multi-region form of the bug, which a
     * split of a test-sized sstable (one 2 GiB region) cannot reach.
     */
    @Test
    public void testMapForCompressionMetadataWithFrontPad() throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        int pad = 12345;
        ByteBuffer buffer = allocateBuffer(128 * 1024);
        File f = FileUtils.createTempFile("testMapForCompressionMetadataWithFrontPad", "1");
        f.deleteOnExit();
        File cf = FileUtils.createTempFile(f.name() + ".metadata", "1");
        cf.deleteOnExit();

        // Write an ordinary compressed file, then rebuild it with `pad` junk bytes in front and shift every
        // chunk offset by the same amount -- byte for byte what the splitter's aligned copy produces.
        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try (SequentialWriter writer = new CompressedSequentialWriter(f, cf,
                                                                      null, SequentialWriterOption.DEFAULT,
                                                                      CompressionParams.snappy(), sstableMetadataCollector))
        {
            writer.write(buffer);
            writer.finish();
        }

        byte[] unpadded = java.nio.file.Files.readAllBytes(f.toPath());
        byte[] padded = new byte[pad + unpadded.length];
        new Random(1).nextBytes(padded);                              // the pad is junk, and must never be read
        System.arraycopy(unpadded, 0, padded, pad, unpadded.length);
        java.nio.file.Files.write(f.toPath(), padded);

        Memory offsets;
        int chunkCount;
        try (CompressionMetadata unshifted = CompressionMetadata.open(cf, unpadded.length, true))
        {
            chunkCount = Ints.checkedCast((unshifted.dataLength + unshifted.chunkLength() - 1) / unshifted.chunkLength());
            offsets = Memory.allocate(chunkCount * 8L);
            for (int k = 0; k < chunkCount; k++)
                offsets.setLong(k * 8L, unshifted.chunkFor((long) k * unshifted.chunkLength()).offset + pad);
        }

        CompressionMetadata metadata = new CompressionMetadata(cf, CompressionParams.snappy(),
                                                               offsets, chunkCount * 8L,
                                                               128 * 1024, padded.length, null);
        try (ChannelProxy channel = new ChannelProxy(f);
             MmappedRegions regions = MmappedRegions.map(channel, metadata))
        {
            assertFalse(regions.isEmpty());
            int i = 0;
            while (i < buffer.capacity())
            {
                Chunk chunk = metadata.chunkFor(i);
                assertTrue("every chunk must sit past the pad", chunk.offset >= pad);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                assertNotNull(region);

                // one chunk per region, so the region must BE the chunk: this is the assertion that fails when
                // the segment placement ignores the pad, and it fails for every region but the first
                assertEquals(chunk.offset, region.offset());
                assertEquals(chunk.offset + chunk.length + 4, region.end());
                assertEquals(chunk.length + 4, region.buffer.duplicate().capacity());

                // and the mapped bytes must be the file's bytes at that offset, not shifted by the pad
                ByteBuffer mapped = region.buffer();
                assertEquals("mapped byte 0 of the chunk at " + chunk.offset,
                             padded[Ints.checkedCast(chunk.offset)], mapped.get(0));
                assertEquals("mapped last byte of the chunk at " + chunk.offset,
                             padded[Ints.checkedCast(chunk.offset + chunk.length + 3)],
                             mapped.get(chunk.length + 3));

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
        int bufSize = 1024;
        try (ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap1", buffer));
             MmappedRegions regions = MmappedRegions.map(channel, 0, bufSize))
        {
            assertTrue(regions.isEmpty());
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArgForMap2() throws Exception
    {
        ByteBuffer buffer = allocateBuffer(1024);
        int bufSize = 1024;
        try (ChannelProxy channel = new ChannelProxy(writeFile("testIllegalArgForMap2", buffer));
             MmappedRegions regions = MmappedRegions.map(channel, -1L, bufSize))
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
        int bufSize = 4096;

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
                    assertFalse(regions.extend(metadata, bufSize));

                    logger.info("Checking extend on compressed chunk for range={} {}..{} / {}", idx, pos, pos + (writeSizes[idx] << 10), size);
                    checkExtendOnCompressedChunks(f, writer, regions, bufSize);
                    pos += writeSizes[idx] << 10;
                    idx++;
                }
            }
        }
    }

    /**
     * Growing a NON-padded compressed file one flush at a time must produce exactly the regions that mapping the
     * finished file in one go produces.
     * <p>
     * {@code updateState(CompressionMetadata)} seeds its running sum from the first chunk's physical offset rather
     * than from 0, so that a Data.db carrying leading bytes that belong to no chunk (see
     * {@link #testMapForCompressionMetadataWithFrontPad()}) maps its tail. The obvious way to write that -- always
     * seed from {@code chunkFor(0).offset} and always start the walk at uncompressed 0 -- silently breaks
     * {@link MmappedRegions#extend(CompressionMetadata, int)}, which has to resume from the current end instead:
     * it would re-map every chunk from the beginning and append a second, overlapping copy of every existing
     * region. Nothing in the padded test can see that, because it never extends. This is the guard.
     * <p>
     * The growth per step is deliberately larger than MAX_SEGMENT_SIZE, because {@code extend} short-circuits to
     * the plain-length {@code updateState(long, int)} for smaller growth and would never reach the seed at all.
     */
    @Test
    public void testExtendForCompressionMetadataMatchesOneShotMap() throws Exception
    {
        MmappedRegions.MAX_SEGMENT_SIZE = 1024;

        int chunkLength = 4 << 10;
        int bufSize = 4096;
        int[] writeSizes = { 16 << 10, 16 << 10, 16 << 10 };

        ByteBuffer buffer = allocateBuffer(Arrays.stream(writeSizes).sum());
        File f = FileUtils.createTempFile("testExtendForCompressionMetadataMatchesOneShotMap", "1");
        f.deleteOnExit();

        File cf = FileUtils.createTempFile(f.name() + ".metadata", "1");
        cf.deleteOnExit();

        MetadataCollector sstableMetadataCollector = new MetadataCollector(new ClusteringComparator(BytesType.instance));
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, cf, null,
                                                                                SequentialWriterOption.DEFAULT,
                                                                                CompressionParams.deflate(chunkLength),
                                                                                sstableMetadataCollector))
        {
            ByteBuffer slice = buffer.slice();
            slice.limit(writeSizes[0]);
            writer.write(slice);
            writer.sync();

            int pos = writeSizes[0];

            try (ChannelProxy channel = new ChannelProxy(f);
                 CompressionMetadata firstMetadata = writer.open(writer.getLastFlushOffset());
                 MmappedRegions regions = MmappedRegions.map(channel, firstMetadata))
            {
                long mappedCompressedLength = firstMetadata.compressedFileLength;

                for (int idx = 1; idx < writeSizes.length; idx++)
                {
                    slice = buffer.slice();
                    slice.position(pos).limit(pos + writeSizes[idx]);
                    writer.write(slice);
                    writer.sync();
                    pos += writeSizes[idx];

                    try (CompressionMetadata grown = writer.open(writer.getLastFlushOffset()))
                    {
                        assertTrue("the growth has to exceed one segment, otherwise extend() takes the plain-length" +
                                   " path and the CompressionMetadata seed is never exercised",
                                   grown.compressedFileLength - mappedCompressedLength > MmappedRegions.MAX_SEGMENT_SIZE);
                        regions.extend(grown, bufSize);
                        mappedCompressedLength = grown.compressedFileLength;
                    }
                }

                try (CompressionMetadata full = writer.open(writer.getLastFlushOffset()))
                {
                    List<String> incremental = regionLayout(regions, full);
                    assertTrue("the file has to span several regions for this to mean anything",
                               incremental.size() > 1);

                    // the bytes have to be there too, not just the region bookkeeping
                    for (long dataOffset = 0; dataOffset < full.dataLength; dataOffset += full.chunkLength())
                        verifyChunks(f, full, dataOffset, regions);

                    try (ChannelProxy oneShotChannel = new ChannelProxy(f);
                         MmappedRegions oneShot = MmappedRegions.map(oneShotChannel, full))
                    {
                        assertEquals("extending must land on the same regions as mapping the finished file",
                                     regionLayout(oneShot, full), incremental);
                    }
                }
            }
        }
    }

    /**
     * The ordered, de-duplicated list of regions covering every chunk of {@code metadata}, as {@code offset..end}
     * strings so that a mismatch reads usefully. Also asserts that each chunk is wholly inside its region, which
     * is what a short final mapping breaks.
     */
    private static List<String> regionLayout(MmappedRegions regions, CompressionMetadata metadata)
    {
        List<String> layout = new ArrayList<>();
        for (long dataOffset = 0; dataOffset < metadata.dataLength; dataOffset += metadata.chunkLength())
        {
            Chunk chunk = metadata.chunkFor(dataOffset);
            MmappedRegions.Region region = regions.floor(chunk.offset);
            assertNotNull("no region covers the chunk at " + chunk.offset, region);
            assertTrue("the chunk at " + chunk.offset + " runs past the end of its region " + region.end(),
                       chunk.offset + chunk.length + 4 <= region.end());

            String descriptor = region.offset() + ".." + region.end();
            if (layout.isEmpty() || !layout.get(layout.size() - 1).equals(descriptor))
                layout.add(descriptor);
        }
        return layout;
    }

    private void checkExtendOnCompressedChunks(File f, CompressedSequentialWriter writer, MmappedRegions regions, int bufSize)
    {
        int dataOffset;
        try (CompressionMetadata metadata = writer.open(writer.getLastFlushOffset()))
        {
            regions.extend(metadata, bufSize);
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
