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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static accord.utils.Property.qt;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_IO_TMPDIR;
import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;

public class DirectCompressedChunkReaderTest extends CompressedChunkReaderTestBase
{
    private static final int CHECKSUM_LENGTH = Integer.BYTES; // CRC32 checksum size

    private static final Logger logger = LoggerFactory.getLogger(DirectCompressedChunkReaderTest.class);
    private static int seed;

    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @BeforeClass
    public static void setup()
    {
        seed = new Random().nextInt();
        logger.info("Seed: {}", seed);
    }

    private static Gen<Integer> mixedChunkLengths()
    {
        int minLength = 1024;
        int maxLength = 1024 * 64;
        return Gens.pick(Stream.iterate(minLength, n -> n <= maxLength, n -> n * 2)
                               .collect(Collectors.toList()));
    }

    @Test
    public void compressedReads()
    {
        var optionGen = writerOptions();
        var paramsGen = compressionParams(mixedChunkLengths());
        var fileLengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, fileLengthGen, false);
    }

    @Test
    public void scanCompressedReads()
    {
        var optionGen = writerOptions();
        var paramsGen = compressionParams(mixedChunkLengths());
        var fileLengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, fileLengthGen, true);
    }

    @Test
    public void compressedReads_edgeCases()
    {
        var optionGen = writerOptions();
        int chunkLength = 4096;
        var paramsGen = compressionParams(Gens.constant(chunkLength));

        // Test file smaller than chunk length
        testReads(optionGen, paramsGen, Gens.longs().of(1024), true);

        // Test partial trailing chunk
        testReads(optionGen, paramsGen, Gens.longs().of(chunkLength + 96), true);
    }

    @Test
    public void corruptBlock()
    {
        Gen<Integer> chunkLength = Gens.constant(4096);
        Gen<CompressionParams> compressionGen = compressionParams(chunkLength);

        qt().withSeed(seed).forAll(Gens.random(), compressionGen).check((rs, params) -> {
            long totalBytesToWrite = params.chunkLength() * 2L; // two chunks

            List<ByteBuffer> chunks = generateRandomChunks(rs, params, totalBytesToWrite);

            File dataFile = new File(JAVA_IO_TMPDIR.getString() + "data_corrupt_block.bin");

            final CompressionMetadata metadata;

            try (CompressedSequentialWriter writer = getCompressedSequentialWriter(writerOption(1 << 10), params, dataFile))
            {
                for (ByteBuffer chunk : chunks)
                    writer.write(chunk.duplicate());
                writer.sync();
                metadata = writer.open(0);
            }

            CompressionMetadata.Chunk firstChunkMeta = metadata.chunkFor(0);

            long truncatePoint = firstChunkMeta.offset + (long) firstChunkMeta.length / 2; // Halfway into the chunk

            try (FileChannel fileChannel = FileChannel.open(dataFile.toPath(), StandardOpenOption.WRITE))
            {
                fileChannel.truncate(truncatePoint);
            }

            boolean exceptionThrown = false;
            try
            {
                readAndVerifyChunks(dataFile, metadata, chunks, totalBytesToWrite, false);
            }
            catch (CorruptSSTableException exc)
            {
                exceptionThrown = true;
                Assert.assertTrue("Exception message should indicate corrupt SSTable", exc.getMessage().contains(dataFile.name()));
            }
            finally
            {
                metadata.close();
                Files.deleteIfExists(dataFile.toPath());
            }
            Assert.assertTrue("Expected CorruptSSTableException for truncated chunk, but none was thrown.", exceptionThrown);
        });
    }

    @Test
    public void uncompressedReads()
    {
        testUncompressedReads(false);
    }

    @Test
    public void scanUncompressedReads()
    {
        testUncompressedReads(true);
    }

    private void testUncompressedReads(boolean forScan)
    {
        int maxCompressedLength = 0; // force uncompressed path (chunk.length > maxCompressedLength)
        CompressionParams uncompressed = CompressionParams.lz4(DEFAULT_CHUNK_LENGTH, maxCompressedLength);

        var optionGen = writerOptions();
        var paramsGen = Gens.constant(uncompressed);
        var fileLengthGen = Gens.longs().between(1, 1 << 16);

        testReads(optionGen, paramsGen, fileLengthGen, forScan);
    }

    @Test
    public void largeFilePositionReads() throws Exception
    {
        long FOUR_GB = 1L << 32;
        int chunkLength = DEFAULT_CHUNK_LENGTH;

        // Test critical boundaries for compressed file offsets
        long[] testOffsets = {
            FOUR_GB - 1024,                  // Just below 4GB
            FOUR_GB,                         // Exactly 4GB
            FOUR_GB + 1024,                  // Just above 4GB
            FOUR_GB + (1L << 30),            // 5GB (4GB + 1GB)
            FOUR_GB * 2                      // 8GB
        };

        CompressionParams params = CompressionParams.lz4(chunkLength); // Production default compressor

        for (long offset : testOffsets)
            testReadAtLargeOffset(offset, chunkLength, params);
    }

    private static void testReads(Gen<SequentialWriterOption> optionGen, Gen<CompressionParams> paramsGen, Gen.LongGen totalBytesGen, boolean forScan)
    {
        qt().withSeed(seed).forAll(Gens.random(), optionGen, paramsGen).check((rs, option, params) -> {
            long totalBytesToWrite = totalBytesGen.nextLong(rs);
            List<ByteBuffer> chunks = generateRandomChunks(rs, params, totalBytesToWrite);

            File file = new File(JAVA_IO_TMPDIR.getString() + "data.bin");
            try (CompressedSequentialWriter writer = getCompressedSequentialWriter(option, params, file))
            {
                for (ByteBuffer chunk : chunks)
                    writer.write(chunk.duplicate());

                writer.sync();

                CompressionMetadata metadata = writer.open(0);
                readAndVerifyChunks(file, metadata, chunks, totalBytesToWrite, forScan);
            }
            finally
            {
                Files.deleteIfExists(file.toPath());
            }
        });
    }

    private static void readAndVerifyChunks(File file, CompressionMetadata metadata, List<ByteBuffer> expectedChunksData, long totalBytesExpected,
                                            boolean forScan)
    {
        ByteBuffer readBuffer = ByteBuffer.allocateDirect(metadata.chunkLength());

        try (ChannelProxy channel = new ChannelProxy(file, ChannelProxy.IOMode.DIRECT);
             CompressedChunkReader reader = new CompressedChunkReader.Direct(channel, metadata, () -> 1d);
             metadata)
        {
            if (forScan)
                reader.forScan();

            long currentFileOffset = 0;
            long totalBytesRead = 0;
            int currentChunkIndex = 0;

            while (totalBytesRead < totalBytesExpected)
            {
                ByteBuffer currentExpectedChunk = expectedChunksData.get(currentChunkIndex);

                readBuffer.clear();
                reader.readChunk(currentFileOffset, readBuffer);

                Assert.assertTrue("Read buffer is empty unexpectedly at offset " + currentFileOffset, readBuffer.hasRemaining());

                int actualBytesRead = readBuffer.remaining();
                int expectedBytes = currentExpectedChunk.remaining();

                Assert.assertTrue("Read buffer remaining (" + actualBytesRead + ") is less than expected (" + expectedBytes + ") at offset " + currentFileOffset,
                                  actualBytesRead >= expectedBytes);

                int originalReadBufferLimit = readBuffer.limit();
                readBuffer.limit(expectedBytes);
                Assert.assertEquals("Mismatched data at offset " + currentFileOffset, currentExpectedChunk, readBuffer);
                readBuffer.limit(originalReadBufferLimit);

                totalBytesRead += expectedBytes;
                currentFileOffset += metadata.chunkLength();
                currentChunkIndex++;
            }
        }
        finally
        {
            MemoryUtil.clean(readBuffer);
        }
    }

    private static List<ByteBuffer> generateRandomChunks(RandomSource rs, CompressionParams params, long bytesToWrite)
    {
        List<ByteBuffer> chunks = new ArrayList<>();
        long bytesGenerated = 0;
        while (bytesGenerated < bytesToWrite)
        {
            ByteBuffer chunkBuffer = ByteBuffer.allocate(params.chunkLength());
            int bytesToFill = (int) Math.min(chunkBuffer.capacity(), bytesToWrite - bytesGenerated);
            byte[] tempBytes = new byte[bytesToFill];
            rs.nextBytes(tempBytes);
            chunkBuffer.put(tempBytes);
            chunkBuffer.flip();
            chunks.add(chunkBuffer);
            bytesGenerated += bytesToFill;
        }
        return chunks;
    }

    private static CompressedSequentialWriter getCompressedSequentialWriter(SequentialWriterOption option, CompressionParams params, File dataFile)
    {
        return new CompressedSequentialWriter(dataFile, new File("file.offset"), new File("file.digest"), option, params, new MetadataCollector(new ClusteringComparator()));
    }

    /**
     * Test reading from a large file offset to verify no integer overflow.
     * Creates a simulated compressed file with a chunk at a very large offset (>4GB)
     */
    private static void testReadAtLargeOffset(long chunkOffset, int chunkLength, CompressionParams params) throws Exception
    {
        byte[] uncompressedData = new byte[chunkLength];
        for (int i = 0; i < chunkLength; i++)
            uncompressedData[i] = (byte) (i % 256);  // Repeating pattern for realistic compression
        ByteBuffer uncompressedBuf = ByteBuffer.wrap(uncompressedData);

        // Compress the data
        ByteBuffer compressedBuf = ByteBuffer.allocate(chunkLength * 2);
        params.getSstableCompressor().compress(uncompressedBuf, compressedBuf);
        compressedBuf.flip();
        byte[] compressedData = new byte[compressedBuf.remaining()];
        compressedBuf.get(compressedData);

        // Create a fake channel that simulates a compressed file with data at a large offset
        FakeLargeFileChannel fakeChannel = new FakeLargeFileChannel(compressedData, chunkOffset);

        // Create a temp file for ChannelProxy (the file itself isn't used, only the fake channel)
        File tmpFile = FileUtils.createTempFile("large_file_test", ".bin");

        try
        {
            // Create metadata indicating uncompressed position 0 maps to compressed offset at chunkOffset
            try (ChannelProxy channelProxy = new ChannelProxy(tmpFile, fakeChannel); CompressionMetadata metadata = createLargeFileMetadata(0, chunkLength, chunkOffset, compressedData.length, params); CompressedChunkReader directReader = new CompressedChunkReader.Direct(channelProxy, metadata, () -> 0d))
            {
                // Read uncompressed position 0, which will read from the large compressed file offset
                ByteBuffer readBuffer = ByteBuffer.allocateDirect(chunkLength);
                try
                {
                    directReader.readChunk(0, readBuffer);

                    // Verify we got a full chunk back
                    Assert.assertTrue("Read buffer should have data for chunk at offset " + chunkOffset, readBuffer.hasRemaining());
                    Assert.assertEquals("Read buffer should contain full chunk", chunkLength, readBuffer.remaining());

                    // Verify decompressed data matches original
                    byte[] readData = new byte[chunkLength];
                    readBuffer.get(readData);
                    Assert.assertArrayEquals("Data mismatch for chunk at offset " + chunkOffset,
                                             uncompressedData,
                                             readData);
                }
                finally
                {
                    MemoryUtil.clean(readBuffer);
                }
            }
        }
        finally
        {
            Files.deleteIfExists(tmpFile.toPath());
        }
    }

    private static CompressionMetadata createLargeFileMetadata(long chunkIndex, int chunkLength, long chunkOffset,
                                                                int compressedLength, CompressionParams params)
    {
        // Allocate space for chunk offset index: need current offset + next offset to calculate length
        Memory chunkOffsets = Memory.allocate(2 * Long.BYTES);

        // Chunk offset index stores where each compressed chunk starts in the file
        chunkOffsets.setLong(0, chunkOffset);                                    // This chunk's compressed offset
        chunkOffsets.setLong(8, chunkOffset + compressedLength + CHECKSUM_LENGTH); // Next chunk offset (current + data + checksum)

        long dataLength = (chunkIndex + 1) * chunkLength;                 // Total uncompressed data (one chunk = 16KB)
        long compressedFileLength = chunkOffset + compressedLength + CHECKSUM_LENGTH; // Total compressed file size

        File chunksIndexFile = new File(JAVA_IO_TMPDIR.getString() + "large_file_chunks.index");

        return new CompressionMetadata(chunksIndexFile,
                                       params,
                                       chunkOffsets,
                                       chunkOffsets.size(),
                                       dataLength,
                                       compressedFileLength,
                                       null);
    }

    /**
     * Fake FileChannel that simulates a large compressed file (>4GB) without requiring actual disk space.
     * This allows testing integer overflow scenarios by:
     * - Reporting a large file size (e.g., 8GB) via size()
     * - Storing only the actual compressed chunk data in memory (a few KB)
     * - Mapping reads at high offsets (>4GB) to the in-memory data
     * - Returning zeros for reads outside the compressed data region
     * This approach enables efficient testing of large file position handling without creating multi-GB test files.
     */
    private static class FakeLargeFileChannel extends FileChannel
    {
        private final byte[] compressedData;
        private final long chunkOffset;
        private final long fileSize;
        private long position;

        FakeLargeFileChannel(byte[] compressedData, long chunkOffset)
        {
            this.compressedData = compressedData;
            this.chunkOffset = chunkOffset;
            // File size = offset to chunk + compressed chunk data + CRC32 checksum
            // This represents the minimum file size needed to contain the chunk at the specified offset
            this.fileSize = chunkOffset + compressedData.length + CHECKSUM_LENGTH;
        }

        @Override
        public int read(ByteBuffer dst, long readPosition)
        {
            int bytesToRead = dst.remaining();

            // Simulate reading from file: return actual data at chunk offset, zeros elsewhere
            for (int i = 0; i < bytesToRead; i++)
            {
                long fileOffset = readPosition + i;

                if (fileOffset >= chunkOffset && fileOffset < chunkOffset + compressedData.length)
                {
                    // Reading from compressed data region
                    int dataOffset = (int) (fileOffset - chunkOffset);
                    dst.put(compressedData[dataOffset]);
                }
                else if (fileOffset >= chunkOffset + compressedData.length && fileOffset < fileSize)
                {
                    // Reading from checksum region (4 bytes after compressed data)
                    dst.put((byte) 0); // Dummy checksum
                }
                else
                {
                    // Reading outside our chunk - return padding
                    dst.put((byte) 0);
                }
            }

            return bytesToRead;
        }

        @Override
        public int read(ByteBuffer dst)
        {
            int read = read(dst, position);
            position += read;
            return read;
        }

        @Override
        public long size()
        {
            return fileSize;
        }

        @Override
        public long position()
        {
            return position;
        }

        @Override
        public FileChannel position(long newPosition)
        {
            this.position = newPosition;
            return this;
        }

        // Unsupported operations
        @Override
        public long read(ByteBuffer[] dsts, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int write(ByteBuffer src, long position)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileChannel truncate(long size)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void force(boolean metaData)
        {
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public MappedByteBuffer map(FileChannel.MapMode mode, long position, long size)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock lock(long position, long size, boolean shared)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void implCloseChannel()
        {
        }
    }
}