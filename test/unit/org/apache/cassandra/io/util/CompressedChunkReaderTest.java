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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;

import static accord.utils.Property.qt;

public class CompressedChunkReaderTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void scanReaderIssuesFewerPhysicalReadsThanRandomAccessReader()
    {
        var optionGen = options();
        var paramsGen = params();
        var lengthGen = Gens.longs().between(1, 1 << 16);

        qt().forAll(Gens.random(), optionGen, paramsGen).check((rs, option, params) -> {
            ListenableFileSystem fs = FileSystems.newGlobalInMemoryFileSystem();

            File f = new File("/file.bin");
            AtomicInteger reads = new AtomicInteger();
            fs.onPostRead(f.path::equals, (p, c, pos, dst, r) -> reads.incrementAndGet());
            long length = lengthGen.nextLong(rs);
            CompressionMetadata metadata1, metadata2;
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, new File("/file.offset"), new File("/file.digest"), option, params, new MetadataCollector(new ClusteringComparator())))
            {
                for (long i = 0; i < length; i++)
                    writer.writeLong(i);

                writer.sync();
                metadata1 = writer.open(0);
                metadata2 = writer.open(0);
            }

            doReads(f, metadata1, length, false);
            int randomAccessReads = reads.getAndSet(0);

            doReads(f, metadata2, length, true);
            int scanReads = reads.getAndSet(0);

            if (Files.size(f.toPath()) > DatabaseDescriptor.getCompressedReadAheadBufferSize())
                Assert.assertTrue(scanReads <= randomAccessReads);
        });
    }

    private void doReads(File f, CompressionMetadata metadata, long length, boolean useReadAhead)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.chunkLength());

        try (ChannelProxy channel = new ChannelProxy(f))
        {
            try (CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1d);
                 metadata)
            {
                CompressedChunkReader scanReader = useReadAhead ? reader.forScan() : reader;
                try
                {
                    long offset = 0;
                    long maxOffset = length * Long.BYTES;
                    do
                    {
                        scanReader.readChunk(offset, buffer);
                        for (long expected = offset / Long.BYTES; buffer.hasRemaining(); expected++)
                            Assertions.assertThat(buffer.getLong()).isEqualTo(expected);

                        offset += metadata.chunkLength();
                    }
                    while (offset < maxOffset);
                }
                finally
                {
                    if (scanReader != reader)
                        scanReader.close();
                }
            }
        }
        finally
        {
            FileUtils.clean(buffer);
        }
    }

    private static Gen<SequentialWriterOption> options()
    {
        Gen<Integer> bufferSizes = Gens.constant(1 << 10);
        return rs -> SequentialWriterOption.newBuilder()
                                           .finishOnClose(false)
                                           .bufferSize(bufferSizes.next(rs))
                                           .build();
    }

    private enum CompressionKind { Noop, Snappy, Deflate, Lz4, Zstd }

    private static Gen<CompressionParams> params()
    {
        Gen<Integer> chunkLengths = Gens.constant(CompressionParams.DEFAULT_CHUNK_LENGTH);
        Gen<Double> compressionRatio = Gens.pick(1.1D);
        return rs -> {
            CompressionKind kind = rs.pick(CompressionKind.values());
            switch (kind)
            {
                case Noop: return CompressionParams.noop();
                case Snappy: return CompressionParams.snappy(chunkLengths.next(rs), compressionRatio.next(rs));
                case Deflate: return CompressionParams.deflate(chunkLengths.next(rs));
                case Lz4: return CompressionParams.lz4(chunkLengths.next(rs));
                case Zstd: return CompressionParams.zstd(chunkLengths.next(rs));
                default: throw new UnsupportedOperationException(kind.name());
            }
        };
    }

    @Test(timeout = 10_000)
    public void scanReaderShouldNotHangOnTruncatedFile() throws Exception
    {
        SequentialWriterOption writerOption = SequentialWriterOption.newBuilder().finishOnClose(false).bufferSize(1 << 10).build();
        CompressionParams params = CompressionParams.snappy(4096, 1.1);

        FileSystems.newGlobalInMemoryFileSystem();
        File f = new File("/truncated_hang_repro.db");
        File offsets = new File("/truncated_hang_repro.offset");
        File digest = new File("/truncated_hang_repro.digest");

        long longsToWrite = 600; // 4800 uncompressed bytes -> 2 compressed chunks (second one partial)
        CompressionMetadata metadata;
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, offsets, digest, writerOption, params, new MetadataCollector(new ClusteringComparator())))
        {
            for (long i = 0; i < longsToWrite; i++)
                writer.writeLong(i);

            writer.sync();
            metadata = writer.open(0);
        }

        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(256);

        // Truncate file so that chunk metadata expects a chunk to extend further than the actual file size
        long originalSize = Files.size(f.toPath());
        long truncatedSize = originalSize - (params.chunkLength() / 2);
        try (FileChannel fc = FileChannel.open(f.toPath(), StandardOpenOption.WRITE))
        {
            fc.truncate(truncatedSize);
        }
        long uncompressedTotal = longsToWrite * Long.BYTES;
        long lastChunkUncompressedStart = ((uncompressedTotal - 1) / metadata.chunkLength()) * metadata.chunkLength();

        ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.chunkLength());
        try (ChannelProxy channel = new ChannelProxy(f);
             CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1.1);
             metadata)
        {
            CompressedChunkReader scanReader = reader.forScan();
            try
            {
                Assertions.assertThatThrownBy(() -> scanReader.readChunk(lastChunkUncompressedStart, buffer))
                          .as("readChunk() reading past truncated EOF via the scan path")
                          .isInstanceOf(CorruptSSTableException.class);
            }
            finally
            {
                if (scanReader != reader)
                    scanReader.close();
            }
        }
        finally
        {
            FileUtils.clean(buffer);
        }
    }

    /*
     * Two or more concurrent scans of one file must not corrupt each other or double-free a shared buffer.
     */
    @Test(timeout = 60_000)
    public void concurrentScansOfOneReaderAreIndependent() throws Exception
    {
        
        SequentialWriterOption writerOption = SequentialWriterOption.newBuilder().finishOnClose(false).bufferSize(1 << 10).build();
        CompressionParams params = CompressionParams.snappy(4096, 1.1);

        FileSystems.newGlobalInMemoryFileSystem();
        File f = new File("/concurrent_scans.db");
        File offsets = new File("/concurrent_scans.offset");
        File digest = new File("/concurrent_scans.digest");

        long longsToWrite = 200_000; // spans many read-ahead blocks
        CompressionMetadata metadata;
        try (CompressedSequentialWriter writer = new CompressedSequentialWriter(f, offsets, digest, writerOption, params, new MetadataCollector(new ClusteringComparator())))
        {
            for (long i = 0; i < longsToWrite; i++)
                writer.writeLong(i);
            writer.sync();
            metadata = writer.open(0);
        }

        // Minium legal buffer size. (This should span many blocks over the test file.)
        DatabaseDescriptor.setCompressedReadAheadBufferSizeInKb(256);

        int threads = 4;
        long maxOffset = longsToWrite * Long.BYTES;
        try (ChannelProxy channel = new ChannelProxy(f);
             CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1d);
             metadata)
        {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            try
            {
                for (int t = 0; t < threads; t++)
                {
                    futures.add(pool.submit(() -> {
                        ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.chunkLength());
                        CompressedChunkReader scanReader = reader.forScan();
                        try
                        {
                            barrier.await(); // start all scans together to maximise overlap
                            long offset = 0;
                            do
                            {
                                scanReader.readChunk(offset, buffer);
                                for (long expected = offset / Long.BYTES; buffer.hasRemaining(); expected++)
                                    Assertions.assertThat(buffer.getLong()).isEqualTo(expected);

                                offset += metadata.chunkLength();
                            }
                            while (offset < maxOffset);
                            return null;
                        }
                        finally
                        {
                            if (scanReader != reader)
                                scanReader.close();
                            FileUtils.clean(buffer);
                        }
                    }));
                }

                for (Future<?> future : futures)
                    future.get(45, TimeUnit.SECONDS);
            }
            finally
            {
                pool.shutdownNow();
            }
        }
    }
}
