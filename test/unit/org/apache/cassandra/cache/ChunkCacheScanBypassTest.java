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

package org.apache.cassandra.cache;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.CompressedChunkReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileSystems;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunkCacheScanBypassTest
{
    /** The minimum legal read-ahead buffer size. */
    private static final int READ_AHEAD_BUFFER_SIZE_IN_KB = 256;

    static
    {
        DatabaseDescriptor.clientInitialization();
        // Client initialization leaves the cache size at zero, which would make a point read spin forever.
        DatabaseDescriptor.getRawConfig().file_cache_size = new DataStorageSpec.IntMebibytesBound(64);
    }

    private final int originalReadAheadBufferSizeInKb = DatabaseDescriptor.getCompressedReadAheadBufferSizeInKB();

    @After
    public void restoreReadAhead()
    {
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(originalReadAheadBufferSizeInKb);
    }

    @Test(timeout = 60_000)
    public void scanBypassesCacheWhilePointReadPopulatesIt() throws Exception
    {
        FileSystems.newGlobalInMemoryFileSystem();
        File f = new File("/chunk_cache_scan_bypass.db");
        CompressionMetadata metadata = writeCompressedFile(f, new File("/chunk_cache_scan_bypass.offset"), new File("/chunk_cache_scan_bypass.digest"), 200_000);
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(256);

        ChunkCache cache = newChunkCache();
        try (ChannelProxy channel = new ChannelProxy(f);
             CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1d);
             CompressionMetadata ignored = metadata)
        {
            RebuffererFactory factory = cache.wrap(reader);

            Rebufferer scan = factory.instantiateRebufferer(true);
            assertThat(scan).isNotSameAs(factory);
            try
            {
                rebufferWholeFile(scan, reader.fileLength(), reader.chunkSize());
                assertThat(cache.size()).as("a scan must not populate the chunk cache").isZero();
            }
            finally
            {
                // Only release the scan's own buffer. The shared reader stays open for the point read below.
                scan.closeReader();
            }

            Rebufferer point = factory.instantiateRebufferer(false);
            assertThat(point).isSameAs(factory);
            rebufferWholeFile(point, reader.fileLength(), reader.chunkSize());
            assertThat(cache.size()).as("a point read must populate the chunk cache").isGreaterThan(0);
        }
        finally
        {
            cache.clear();
        }
    }

    /**
     * A scan bypasses the cache, but it must still batch its reads through its own read-ahead buffer.
     */
    @Test(timeout = 60_000)
    public void scanUnderCacheUsesReadAheadBuffer() throws Exception
    {
        ListenableFileSystem fs = FileSystems.newGlobalInMemoryFileSystem();
        File f = new File("/chunk_cache_scan_readahead.db");
        CompressionMetadata metadata = writeCompressedFile(f, new File("/chunk_cache_scan_readahead.offset"), new File("/chunk_cache_scan_readahead.digest"), 200_000);
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(256);

        AtomicInteger reads = new AtomicInteger();
        fs.onPostRead(f.toPath()::equals, (p, c, pos, dst, r) -> reads.incrementAndGet());

        ChunkCache cache = newChunkCache();
        try (ChannelProxy channel = new ChannelProxy(f);
             CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1d);
             CompressionMetadata ignored = metadata)
        {
            RebuffererFactory factory = cache.wrap(reader);

            long fileLength = reader.fileLength();
            int chunkSize = reader.chunkSize();
            long chunkCount = (fileLength + chunkSize - 1) / chunkSize;

            Rebufferer scan = factory.instantiateRebufferer(true);
            try
            {
                rebufferWholeFile(scan, fileLength, chunkSize);
            }
            finally
            {
                scan.closeReader();
            }

            assertThat((long) reads.get()).as("the scan must batch reads through the read-ahead buffer").isLessThan(chunkCount);
            assertThat(cache.size()).as("a scan must not populate the chunk cache").isZero();
        }
        finally
        {
            cache.clear();
        }
    }

    @Test
    public void scanBypassesCacheEvenWithoutReadAhead() throws Exception
    {
        // 0 disables read-ahead outright.
        assertScanBypassesCache("/chunk_cache_scan_no_readahead", 0, CompressionParams.snappy(4096, 1.1));

        // Standard requires the buffer to be strictly larger than a chunk, so a chunk length equal to the
        // read-ahead buffer size also gets no read-ahead, even with the setting untouched.
        assertScanBypassesCache("/chunk_cache_scan_big_chunks", READ_AHEAD_BUFFER_SIZE_IN_KB, CompressionParams.snappy(READ_AHEAD_BUFFER_SIZE_IN_KB * 1024, 1.1));
    }

    private void assertScanBypassesCache(String fileNamePrefix, int readAheadBufferSizeInKb, CompressionParams params) throws Exception
    {
        FileSystems.newGlobalInMemoryFileSystem();
        File f = new File(fileNamePrefix + ".db");
        CompressionMetadata metadata = writeCompressedFile(f, new File(fileNamePrefix + ".offset"), new File(fileNamePrefix + ".digest"), 200_000, params);
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(readAheadBufferSizeInKb);

        ChunkCache cache = newChunkCache();
        try (ChannelProxy channel = new ChannelProxy(f);
             CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1d);
             CompressionMetadata ignored = metadata)
        {
            RebuffererFactory factory = cache.wrap(reader);
            Rebufferer scan = factory.instantiateRebufferer(true);
            assertThat(scan).as("a scan must bypass the chunk cache").isNotSameAs(factory);

            rebufferWholeFile(scan, reader.fileLength(), reader.chunkSize());
            assertThat(cache.size()).as("a scan must not populate the chunk cache").isZero();
        }
        finally
        {
            cache.clear();
        }
    }

    private static ChunkCache newChunkCache() throws Exception
    {
        Constructor<ChunkCache> ctor = ChunkCache.class.getDeclaredConstructor(BufferPool.class);
        ctor.setAccessible(true);
        return ctor.newInstance(BufferPools.forChunkCache());
    }

    private static CompressionMetadata writeCompressedFile(File f, File offsets, File digest, long longsToWrite) throws Exception
    {
        return writeCompressedFile(f, offsets, digest, longsToWrite, CompressionParams.snappy(4096, 1.1));
    }

    private static CompressionMetadata writeCompressedFile(File f, File offsets, File digest, long longsToWrite, CompressionParams params) throws Exception
    {
        SequentialWriterOption option = SequentialWriterOption.newBuilder().finishOnClose(false).bufferSize(1 << 10).build();

        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, offsets, digest, option, params, new MetadataCollector(new ClusteringComparator())))
        {
            for (long i = 0; i < longsToWrite; i++)
                writer.writeLong(i);

            writer.sync();
            return writer.open(0);
        }
    }

    private static void rebufferWholeFile(Rebufferer rebufferer, long fileLength, int chunkSize)
    {
        long position = 0;
        do
        {
            Rebufferer.BufferHolder holder = rebufferer.rebuffer(position);
            holder.release();
            position += chunkSize;
        }
        while (position < fileLength);
    }
}
