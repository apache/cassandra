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
import org.apache.cassandra.io.util.ReadPattern;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.RebuffererFactory;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.BufferPool;
import org.apache.cassandra.utils.memory.BufferPools;

import static org.assertj.core.api.Assertions.assertThat;

public class ChunkCacheScanBypassTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        // Client init leaves file_cache_size null, which gives the chunk cache a maximum weight of 0 and a
        // 0-byte buffer pool. Set a real size before ChunkCache and the buffer pool load so the cache can
        // actually hold entries; otherwise a point read spins forever in Caffeine's degenerate maintenance.
        DatabaseDescriptor.getRawConfig().file_cache_size = new DataStorageSpec.IntMebibytesBound(64);
    }

    /**
     * A scan (ex. compaction) should not populate the chunk cache, since it reads each chunk once
     * and would only evict hot data.
     */
    @Test
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

            // A SCAN gets a distinct, non-caching rebufferer and must not add anything to the cache.
            Rebufferer scan = factory.instantiateRebufferer(ReadPattern.SCAN);
            assertThat(scan).isNotSameAs(factory);
            try
            {
                rebufferWholeFile(scan, reader.fileLength(), reader.chunkSize());
                assertThat(cache.size()).as("a scan must not populate the chunk cache").isZero();
            }
            finally
            {
                // Release the scan's read-ahead buffer only. close() would close the shared underlying reader,
                // which the point read below still needs; the parent reader's try-with-resources closes it.
                scan.closeReader();
            }

            // A PARTITION_READ reuses the caching rebufferer and populates the cache, so a repeated partition-range
            // query re-reads hot data. This is the CASSANDRA-21671 regression: it must not bypass the cache.
            Rebufferer partitionRead = factory.instantiateRebufferer(ReadPattern.PARTITION_READ);
            assertThat(partitionRead).isSameAs(factory);
            rebufferWholeFile(partitionRead, reader.fileLength(), reader.chunkSize());
            assertThat(cache.size()).as("a partition read must populate the chunk cache").isGreaterThan(0);

            cache.close();

            // A point read reuses the caching rebufferer and populates the cache, proving the cache works.
            Rebufferer point = factory.instantiateRebufferer(ReadPattern.ROW_READ);
            assertThat(point).isSameAs(factory);
            rebufferWholeFile(point, reader.fileLength(), reader.chunkSize());
            assertThat(cache.size()).as("a point read must populate the chunk cache").isGreaterThan(0);
        }
        finally
        {
            cache.close();
        }
    }

    /**
     * A can bypasses the chunk cache entirely (its rebufferer delegates straight to the uncompressed source
     * instead of the cache), so it must still batch its reads through its own read-ahead buffer rather than
     * issuing one physical read per chunk.
     */
    @Test
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

            Rebufferer scan = factory.instantiateRebufferer(ReadPattern.SCAN);
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
            cache.close();
        }
    }

    private static ChunkCache newChunkCache() throws Exception
    {
        // The shared ChunkCache.instance is null when the file cache is disabled (the default). Build a private
        // instance directly so the test controls the cache regardless of the file_cache_enabled setting.
        Constructor<ChunkCache> ctor = ChunkCache.class.getDeclaredConstructor(BufferPool.class);
        ctor.setAccessible(true);
        return ctor.newInstance(BufferPools.forChunkCache());
    }

    private static CompressionMetadata writeCompressedFile(File f, File offsets, File digest, long longsToWrite) throws Exception
    {
        SequentialWriterOption option = SequentialWriterOption.newBuilder().finishOnClose(false).bufferSize(1 << 10).build();
        CompressionParams params = CompressionParams.snappy(4096, 1.1);
        
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
        // Rebuffer every chunk-aligned position over the whole file and release each holder. This is enough to
        // drive cache population (a point read) or a read-ahead scan.
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
