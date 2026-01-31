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
import java.util.Collections;
import java.util.Random;

import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for DirectIOCompressedSequentialWriter.
 *
 * Tests verify:
 * - Basic write and read functionality
 * - File integrity and checksum correctness
 * - Various data sizes (small, chunk-aligned, large)
 * - Output matches standard CompressedSequentialWriter
 */
public class DirectCompressedSequentialWriterTest
{
    private static boolean directIOSupported = false;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();

        // Check if Direct IO is supported on the test filesystem
        try
        {
            File tempDir = FileUtils.createTempFile("direct-io-test", ".tmp").parent();
            int blockSize = FileUtils.getBlockSize(tempDir);
            directIOSupported = blockSize > 0;
        }
        catch (Exception e)
        {
            directIOSupported = false;
        }
    }

    @After
    public void cleanup()
    {
        // Reset to standard mode after each test
        DatabaseDescriptor.setCompactionWriteDiskAccessMode(DiskAccessMode.standard);
    }

    /**
     * Skip test if Direct IO is not supported on this filesystem.
     */
    private void assumeDirectIOSupported()
    {
        Assume.assumeTrue("Direct IO not supported on this filesystem", directIOSupported);
    }

    @Test
    public void testBasicWriteAndRead() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("basicTest", 1024, CompressionParams.lz4());
    }

    @Test
    public void testSmallDataLessThanChunk() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("smallData", 100, CompressionParams.lz4());
    }

    @Test
    public void testChunkAlignedData() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("chunkAligned", DEFAULT_CHUNK_LENGTH, CompressionParams.lz4());
    }

    @Test
    public void testMultipleChunks() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("multiChunk", DEFAULT_CHUNK_LENGTH * 3 + 500, CompressionParams.lz4());
    }

    @Test
    public void testLargeData() throws IOException
    {
        assumeDirectIOSupported();
        // Write 1MB of data
        testWriteAndRead("largeData", 1024 * 1024, CompressionParams.lz4());
    }

    @Test
    public void testWithZstdCompression() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("zstdTest", DEFAULT_CHUNK_LENGTH * 2, CompressionParams.zstd());
    }

    @Test
    public void testWithSnappyCompression() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("snappyTest", DEFAULT_CHUNK_LENGTH * 2, CompressionParams.snappy());
    }

    @Test
    public void testWithDeflateCompression() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("deflateTest", DEFAULT_CHUNK_LENGTH * 2, CompressionParams.deflate());
    }

    @Test
    public void testOutputMatchesStandardWriter() throws IOException
    {
        assumeDirectIOSupported();

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
    public void testFileIntegrity() throws IOException
    {
        assumeDirectIOSupported();

        File dataFile = FileUtils.createTempFile("integrity_test", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        int dataSize = DEFAULT_CHUNK_LENGTH * 5 + 1234;  // Multiple chunks with remainder
        byte[] testData = new byte[dataSize];
        new Random(123).nextBytes(testData);

        try
        {
            // Write data
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                dataFile, metadataFile, null,
                SequentialWriterOption.DEFAULT, CompressionParams.lz4(), collector, null))
            {
                writer.write(testData);
                writer.finish();
            }

            // Verify file exists and has correct metadata
            assertTrue("Data file should exist", dataFile.exists());
            assertTrue("Metadata file should exist", metadataFile.exists());

            // Read and verify data
            try (CompressionMetadata metadata = CompressionMetadata.open(metadataFile, dataFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(dataFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                assertEquals("Uncompressed length should match", dataSize, reader.length());

                byte[] readBack = new byte[dataSize];
                reader.readFully(readBack);

                assertTrue("Should be at EOF", reader.isEOF());
                assertArrayEquals("Data should match exactly", testData, readBack);
            }
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testSingleByteWrite() throws IOException
    {
        assumeDirectIOSupported();
        testWriteAndRead("singleByte", 1, CompressionParams.lz4());
    }

    @Test
    public void testExactlyOneBlock() throws IOException
    {
        assumeDirectIOSupported();
        // Write exactly 4096 bytes (typical block size)
        testWriteAndRead("oneBlock", 4096, CompressionParams.lz4());
    }

    @Test
    public void testMarkThrowsUnsupportedOperationException() throws IOException
    {
        assumeDirectIOSupported();

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
        assumeDirectIOSupported();

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
    public void testConfiguredBufferSize() throws IOException
    {
        assumeDirectIOSupported();

        // Verify buffer size getter works
        int bufferSize = DatabaseDescriptor.getCompactionDirectIOWriteBufferSize();
        assertTrue("Buffer size should be positive", bufferSize > 0);
        assertTrue("Buffer size should be at least 4KB", bufferSize >= 4096);
    }

    /**
     * Helper method to test write and read cycle.
     */
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
