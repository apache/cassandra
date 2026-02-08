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
package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Random;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DirectCompressedSequentialWriterTest
{
    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMultipleChunks() throws IOException
    {
        testWriteAndRead("multiChunk", DEFAULT_CHUNK_LENGTH * 3 + 500, CompressionParams.lz4());
    }

    @Test
    public void testLargeData() throws IOException
    {
        // Write 1MB of data
        testWriteAndRead("largeData", 1024 * 1024, CompressionParams.lz4());
    }

    @Test
    public void testBufferSizedForWorstCaseCompressedOutput() throws Exception
    {
        // The write buffer must be large enough for the compressor's worst-case output
        // (initialCompressedBufferLength), not just chunkLength.  Use a chunk size large enough
        // that minRequiredSize dominates over the default 256KiB configured buffer, so the
        // buffer sizing formula is actually exercised rather than being masked by the config.
        int largeChunk = 1024 * 1024;
        CompressionParams params = CompressionParams.lz4(largeChunk, Integer.MAX_VALUE);

        File dataFile = FileUtils.createTempFile("bufferSize", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, collector, null))
            {
                int blockSize = FileUtils.getBlockSize(dataFile.parent());
                int maxChunkWrite = params.getSstableCompressor().initialCompressedBufferLength(largeChunk);

                java.lang.reflect.Field f = DirectCompressedSequentialWriter.class.getDeclaredField("writeBuffer");
                f.setAccessible(true);
                int bufferCapacity = ((java.nio.ByteBuffer) f.get(writer)).capacity();

                // After flushCompleteBlocks(), up to (blockSize - 1) bytes of leftover can remain.
                // The buffer must still have room for the worst-case chunk write + 4-byte CRC.
                int requiredCapacity = maxChunkWrite + 4 + blockSize;
                Assert.assertTrue(
                String.format("Write buffer (%d) too small for worst-case chunk write (%d) + CRC (4) + blockSize (%d) = %d",
                              bufferCapacity, maxChunkWrite, blockSize, requiredCapacity),
                bufferCapacity >= requiredCapacity);
            }
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testAllCompressors() throws IOException
    {
        for (CompressionParams params : new CompressionParams[]{ CompressionParams.lz4(),
                                                                 CompressionParams.zstd(),
                                                                 CompressionParams.snappy(),
                                                                 CompressionParams.deflate() })
            testWriteAndRead(params.getSstableCompressor().getClass().getSimpleName(), DEFAULT_CHUNK_LENGTH * 2, params);
    }

    @Test
    public void testOutputMatchesStandardWriter() throws IOException
    {
        int dataSize = DEFAULT_CHUNK_LENGTH * 2 + 100;
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        CompressionParams params = CompressionParams.lz4();

        // Write with standard writer
        File standardFile = FileUtils.createTempFile("standard_compare", ".db");
        File standardMetadata = new File(standardFile.absolutePath() + ".metadata");
        byte[] standardContent;

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(
            standardFile, standardMetadata, null,
            SequentialWriterOption.DEFAULT, params, collector))
            {
                writer.write(testData);
                writer.finish();
            }

            // Read back the data
            try (CompressionMetadata metadata = CompressionMetadata.open(standardMetadata, standardFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(standardFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                standardContent = new byte[(int) reader.length()];
                reader.readFully(standardContent);
            }
        }
        finally
        {
            standardFile.tryDelete();
            standardMetadata.tryDelete();
        }

        // Write with Direct IO writer
        File directFile = FileUtils.createTempFile("direct_compare", ".db");
        File directMetadata = new File(directFile.absolutePath() + ".metadata");
        byte[] directContent;

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            directFile, directMetadata, null,
            SequentialWriterOption.DEFAULT, params, collector, null))
            {
                writer.write(testData);
                writer.finish();
            }

            // Read back the data
            try (CompressionMetadata metadata = CompressionMetadata.open(directMetadata, directFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(directFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                directContent = new byte[(int) reader.length()];
                reader.readFully(directContent);
            }
        }
        finally
        {
            directFile.tryDelete();
            directMetadata.tryDelete();
        }

        // Data read back should be identical
        assertArrayEquals("Direct IO output should match standard writer output", standardContent, directContent);
    }

    @Test
    public void testSingleByteWrite() throws IOException
    {
        testWriteAndRead("singleByte", 1, CompressionParams.lz4());
    }

    @Test
    public void testMarkThrowsUnsupportedOperationException() throws IOException
    {
        File dataFile = FileUtils.createTempFile("mark_test", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, CompressionParams.lz4(), collector, null))
            {
                writer.write(new byte[100]);
                try
                {
                    writer.mark();
                    fail("Expected UnsupportedOperationException");
                }
                catch (UnsupportedOperationException expected)
                {
                }
            }
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testResetAndTruncateThrowsUnsupportedOperationException() throws IOException
    {
        File dataFile = FileUtils.createTempFile("reset_test", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, CompressionParams.lz4(), collector, null))
            {
                writer.write(new byte[100]);
                try
                {
                    writer.resetAndTruncate(null);
                    fail("Expected UnsupportedOperationException");
                }
                catch (UnsupportedOperationException expected)
                {
                }
            }
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testAbortCleansUpResources() throws IOException
    {
        File dataFile = FileUtils.createTempFile("abort_test", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        try
        {
            byte[] testData = new byte[1024];
            new Random(99).nextBytes(testData);

            MetadataCollector collector = new MetadataCollector(
            new ClusteringComparator(Collections.singletonList(BytesType.instance)));

            // finishOnClose(false) ensures close() triggers the abort path, not finish()
            SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                              .finishOnClose(false)
                                                                              .build();

            // Write data but do NOT call finish() -- close triggers abort path
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            abortOnCloseOption, CompressionParams.lz4(), collector, null))
            {
                writer.write(testData);
                // No writer.finish() -- this is intentional
            }
            // If we reach here, abort completed without throwing.
            // The aligned buffer, compressed buffer, and channel were all cleaned up.
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testDigestFileValidation() throws IOException
    {
        CompressionParams params = CompressionParams.lz4();
        int chunkLength = params.chunkLength();

        for (int dataSize : new int[]{ 100, chunkLength, chunkLength * 3 + 500 })
        {
            File dataFile = FileUtils.createTempFile("digest_validate_" + dataSize, ".db");
            File metadataFile = new File(dataFile.absolutePath() + ".metadata");
            File digestFile = FileUtils.createTempFile("digest_validate_" + dataSize, ".digest");

            try
            {
                byte[] testData = new byte[dataSize];
                new Random(42).nextBytes(testData);

                MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                dataFile, metadataFile, digestFile,
                SequentialWriterOption.DEFAULT, params, collector, null))
                {
                    writer.write(testData);
                    writer.finish();
                }

                // Validate the digest matches the data file
                DataIntegrityMetadata.FileDigestValidator validator =
                    new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile);
                validator.validate();
            }
            finally
            {
                dataFile.tryDelete();
                metadataFile.tryDelete();
                digestFile.tryDelete();
            }
        }
    }

    @Test
    public void testDigestMatchesBetweenDirectAndStandardWriter() throws IOException
    {
        int dataSize = DEFAULT_CHUNK_LENGTH * 2 + 100;
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        CompressionParams params = CompressionParams.lz4();

        // Write with standard writer
        File standardFile = FileUtils.createTempFile("standard_digest", ".db");
        File standardMetadata = new File(standardFile.absolutePath() + ".metadata");
        File standardDigest = FileUtils.createTempFile("standard_digest", ".digest");

        // Write with direct writer
        File directFile = FileUtils.createTempFile("direct_digest", ".db");
        File directMetadata = new File(directFile.absolutePath() + ".metadata");
        File directDigest = FileUtils.createTempFile("direct_digest", ".digest");

        try
        {
            MetadataCollector standardCollector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(
            standardFile, standardMetadata, standardDigest,
            SequentialWriterOption.DEFAULT, params, standardCollector))
            {
                writer.write(testData);
                writer.finish();
            }

            MetadataCollector directCollector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            directFile, directMetadata, directDigest,
            SequentialWriterOption.DEFAULT, params, directCollector, null))
            {
                writer.write(testData);
                writer.finish();
            }

            // Digest files should contain identical values
            String standardDigestContent = new String(Files.readAllBytes(standardDigest.toPath())).trim();
            String directDigestContent = new String(Files.readAllBytes(directDigest.toPath())).trim();
            assertEquals("Digest values should match between standard and direct writer",
                         standardDigestContent, directDigestContent);
        }
        finally
        {
            standardFile.tryDelete();
            standardMetadata.tryDelete();
            standardDigest.tryDelete();
            directFile.tryDelete();
            directMetadata.tryDelete();
            directDigest.tryDelete();
        }
    }

    /**
     * Write incompressible (random) data with maxCompressedLength == chunkLength, forcing the
     * uncompressed fallback path (Case B) for every full chunk. Verify correct read-back.
     */
    @Test
    public void testCompressionFailureFallback() throws IOException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        // maxCompressedLength == chunkLength forces uncompressed storage when compression doesn't help
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);

        // Multiple full chunks of random (incompressible) data
        int dataSize = chunkLength * 3;
        testWriteAndRead("compressionFailure", dataSize, params);
    }

    /**
     * Write data where the last chunk is partial (dataSize % chunkLength != 0) with
     * maxCompressedLength == chunkLength. The last chunk hits the padding path (Case C)
     * where uncompressed data smaller than maxCompressedLength is padded with zeroes.
     */
    @Test
    public void testPartialLastChunkPadding() throws IOException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);

        // 2 full chunks + a partial last chunk (500 bytes)
        int dataSize = chunkLength * 2 + 500;
        testWriteAndRead("partialChunkPadding", dataSize, params);
    }

    /**
     * Compare Direct and Standard writer output when compression fails (minCompressRatio = 1.0,
     * i.e. maxCompressedLength == chunkLength). Both writers should produce identical read-back
     * data and identical digest values.
     */
    @Test
    public void testCompressionFailureMatchesStandardWriter() throws IOException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);

        // Partial last chunk to also exercise the padding path
        int dataSize = chunkLength * 2 + 100;
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        // Write with standard writer
        File standardFile = FileUtils.createTempFile("std_fail", ".db");
        File standardMetadata = new File(standardFile.absolutePath() + ".metadata");
        File standardDigest = FileUtils.createTempFile("std_fail", ".digest");
        byte[] standardContent;
        String standardDigestValue;

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(
            standardFile, standardMetadata, standardDigest,
            SequentialWriterOption.DEFAULT, params, collector))
            {
                writer.write(testData);
                writer.finish();
            }

            standardDigestValue = new String(Files.readAllBytes(standardDigest.toPath())).trim();

            try (CompressionMetadata metadata = CompressionMetadata.open(standardMetadata, standardFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(standardFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                standardContent = new byte[(int) reader.length()];
                reader.readFully(standardContent);
            }
        }
        finally
        {
            standardFile.tryDelete();
            standardMetadata.tryDelete();
            standardDigest.tryDelete();
        }

        // Write with Direct IO writer
        File directFile = FileUtils.createTempFile("direct_fail", ".db");
        File directMetadata = new File(directFile.absolutePath() + ".metadata");
        File directDigest = FileUtils.createTempFile("direct_fail", ".digest");
        byte[] directContent;
        String directDigestValue;

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            directFile, directMetadata, directDigest,
            SequentialWriterOption.DEFAULT, params, collector, null))
            {
                writer.write(testData);
                writer.finish();
            }

            directDigestValue = new String(Files.readAllBytes(directDigest.toPath())).trim();

            try (CompressionMetadata metadata = CompressionMetadata.open(directMetadata, directFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(directFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                directContent = new byte[(int) reader.length()];
                reader.readFully(directContent);
            }
        }
        finally
        {
            directFile.tryDelete();
            directMetadata.tryDelete();
            directDigest.tryDelete();
        }

        assertArrayEquals("Data should match between standard and direct writer with compression failure", standardContent, directContent);
        assertEquals("Digest should match between standard and direct writer with compression failure", standardDigestValue, directDigestValue);
    }

    private void testWriteAndRead(String testName, int dataSize, CompressionParams params) throws IOException
    {
        File dataFile = FileUtils.createTempFile(testName + "_direct", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        try
        {
            // Write
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, collector, null))
            {
                writer.write(testData);
                writer.finish();
            }

            // Read and verify
            try (CompressionMetadata metadata = CompressionMetadata.open(metadataFile, dataFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(dataFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                assertEquals("Length should match", dataSize, reader.length());

                byte[] readBack = new byte[dataSize];
                reader.readFully(readBack);

                assertArrayEquals("Data should match", testData, readBack);
            }
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }
}
