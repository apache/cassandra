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
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

import accord.utils.Gens;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static accord.utils.Property.qt;

public class StandardCompressedChunkReaderTest extends CompressedChunkReaderTestBase
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void scanReaderReadsLessThanRAReader()
    {
        var optionGen = writerOptions();
        var paramsGen = compressionParams(Gens.constant(CompressionParams.DEFAULT_CHUNK_LENGTH));
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
            int raReads = reads.getAndSet(0);

            doReads(f, metadata2, length, true);
            int scanReads = reads.getAndSet(0);

            if (Files.size(f.toPath()) > DatabaseDescriptor.getCompressedReadAheadBufferSize())
                Assert.assertTrue(scanReads <= raReads);
        });
    }

    protected void doReads(File f, CompressionMetadata metadata, long length, boolean useReadAhead)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.chunkLength());

        try (ChannelProxy channel = new ChannelProxy(f))
        {
            try (CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1d);
                 metadata)
            {
                if (useReadAhead)
                    reader.forScan();

                long offset = 0;
                long maxOffset = length * Long.BYTES;
                do
                {
                    reader.readChunk(offset, buffer);
                    for (long expected = offset / Long.BYTES; buffer.hasRemaining(); expected++)
                        Assertions.assertThat(buffer.getLong()).isEqualTo(expected);

                    offset += metadata.chunkLength();
                }
                while (offset < maxOffset);
            }
        }
        finally
        {
            MemoryUtil.clean(buffer);
        }
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
            reader.forScan();

            Assertions.assertThatThrownBy(() -> reader.readChunk(lastChunkUncompressedStart, buffer))
                      .as("readChunk() reading past truncated EOF via the scan path")
                      .isInstanceOf(CorruptSSTableException.class);
        }
        finally
        {
            MemoryUtil.clean(buffer);
        }
    }
}