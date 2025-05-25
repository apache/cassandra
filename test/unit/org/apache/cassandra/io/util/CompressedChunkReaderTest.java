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

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.filesystem.ListenableFileSystem;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.assertj.core.api.Assertions;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

import static accord.utils.Property.qt;

public class CompressedChunkReaderTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void scanReaderReadsLessThanRAReader()
    {
        var optionGen = options();
        var paramsGen = params();
        var lengthGen = Gens.longs().between(1, 1 << 16);
        qt().withSeed(-1871070464864118891L).forAll(Gens.random(), optionGen, paramsGen).check((rs, option, params) -> {
            ListenableFileSystem fs = FileSystems.newGlobalInMemoryFileSystem();

            File f = new File("/file.db");
            AtomicInteger reads = new AtomicInteger();
            fs.onPostRead(f.path::equals, (p, c, pos, dst, r) -> {
                reads.incrementAndGet();
            });
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

            doReads(f, metadata1, length, true);
            int scanReads = reads.getAndSet(0);

            doReads(f, metadata2, length, false);
            int raReads = reads.getAndSet(0);
            
            if (Files.size(f.toPath()) > DatabaseDescriptor.getCompressedReadAheadBufferSize())
                Assert.assertTrue(scanReads < raReads);
        });
    }

    private void doReads(File f, CompressionMetadata metadata, long length, boolean useReadAhead)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(metadata.chunkLength());

        try (ChannelProxy channel = new ChannelProxy(f);
             CompressedChunkReader reader = new CompressedChunkReader.Standard(channel, metadata, () -> 1.1);
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
        finally
        {
            MemoryUtil.clean(buffer);
        }}

    private static Gen<SequentialWriterOption> options()
    {
        Gen<Integer> bufferSizes = Gens.constant(1 << 10); //.pickInt(1 << 4, 1 << 10, 1 << 15);
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
}