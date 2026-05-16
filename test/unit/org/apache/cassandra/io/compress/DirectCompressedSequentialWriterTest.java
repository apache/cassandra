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
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

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
        testWriteAndRead("largeData", 1024 * 1024, CompressionParams.lz4());
    }

    @Test
    public void testBufferSizedForWorstCaseCompressedOutput() throws Exception
    {
        // Chunk large enough that minRequiredSize dominates the default 256KiB configured buffer,
        // so the buffer sizing formula is exercised rather than masked by the config.
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

    /**
     * NoopCompressor's {@code initialCompressedBufferLength} is identity, so the worst-case
     * buffer-sizing branch differs from real compressors. FLUSH (the only path that produces NOOP)
     * is gated out of DIO today; this guards against future breakage if the gating changes.
     */
    @Test
    public void testNoopCompressor() throws IOException
    {
        int chunk = DEFAULT_CHUNK_LENGTH;
        for (int dataSize : new int[]{ 1, chunk - 1, chunk, chunk + 1, chunk * 3 + 137 })
            testWriteAndRead("noop_" + dataSize, dataSize, CompressionParams.NOOP);
    }

    @Test
    public void testOutputMatchesStandardWriter() throws IOException
    {
        int dataSize = DEFAULT_CHUNK_LENGTH * 2 + 100;
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        CompressionParams params = CompressionParams.lz4();

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

        assertArrayEquals("Direct IO output should match standard writer output", standardContent, directContent);
    }

    @Test
    public void testDigestMatchesStandardWriter() throws IOException
    {
        CompressionParams[] paramSet = { CompressionParams.lz4(),
                                         CompressionParams.zstd(),
                                         CompressionParams.snappy(),
                                         CompressionParams.deflate() };
        int chunk = DEFAULT_CHUNK_LENGTH;
        int[] sizes = { 1, chunk - 1, chunk, chunk + 1, chunk * 3 + 137 };

        for (CompressionParams params : paramSet)
        {
            for (int dataSize : sizes)
            {
                byte[] testData = new byte[dataSize];
                new Random(0xC0FFEEL ^ dataSize).nextBytes(testData);

                byte[] standardDigest = writeAndReadDigest(testData, params, false);
                byte[] directDigest = writeAndReadDigest(testData, params, true);

                String label = params.getSstableCompressor().getClass().getSimpleName() + "/" + dataSize;
                assertArrayEquals("Digest mismatch for " + label, standardDigest, directDigest);
            }
        }
    }

    private byte[] writeAndReadDigest(byte[] data, CompressionParams params, boolean direct) throws IOException
    {
        String prefix = (direct ? "direct" : "standard") + "_digest_";
        File dataFile = FileUtils.createTempFile(prefix, ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");
        File digestFile = FileUtils.createTempFile(prefix, ".digest");
        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (SequentialWriter writer = direct
                                           ? new DirectCompressedSequentialWriter(dataFile, metadataFile, digestFile,
                                                                                  SequentialWriterOption.DEFAULT, params, collector, null)
                                           : new CompressedSequentialWriter(dataFile, metadataFile, digestFile,
                                                                            SequentialWriterOption.DEFAULT, params, collector))
            {
                writer.write(data);
                writer.finish();
            }
            return Files.readAllBytes(digestFile.toPath());
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
            digestFile.tryDelete();
        }
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

            // finishOnClose(false) routes close() through the abort path instead of finish().
            SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                              .finishOnClose(false)
                                                                              .build();

            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            abortOnCloseOption, CompressionParams.lz4(), collector, null))
            {
                writer.write(testData);
            }
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testDirectMemoryIsCleanedOnClose() throws IOException
    {
        // Sized to dominate baseline allocator noise; matches DirectThreadLocalReadAheadBufferTest.
        int bufferSize = 64 * 1024 * 1024;
        Config conf = DatabaseDescriptor.getRawConfig();
        DataStorageSpec.IntKibibytesBound savedBufferSize = conf.direct_write_buffer_size;

        File dataFile = FileUtils.createTempFile("direct_mem_clean", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");
        try
        {
            conf.direct_write_buffer_size = new DataStorageSpec.IntKibibytesBound(bufferSize / 1024 + "KiB");

            BufferPoolMXBean directPool = getDirectBufferPool();
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            SequentialWriterOption abortOnClose = SequentialWriterOption.newBuilder().finishOnClose(false).build();

            long memoryUsedBefore;
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                 dataFile, metadataFile, null, abortOnClose, CompressionParams.lz4(), collector, null))
            {
                writer.write(new byte[1024]);
                memoryUsedBefore = directPool.getMemoryUsed();
            }
            long memoryUsedAfter = directPool.getMemoryUsed();
            long actualDecrease = memoryUsedBefore - memoryUsedAfter;

            Assert.assertTrue("Direct memory should drop by ~bufferSize on close. before=" + memoryUsedBefore
                              + ", after=" + memoryUsedAfter + ", decrease=" + actualDecrease + ", expected~=" + bufferSize,
                              actualDecrease >= bufferSize * 0.9); // 10% tolerance for alignment overhead
        }
        finally
        {
            conf.direct_write_buffer_size = savedBufferSize;
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    private static BufferPoolMXBean getDirectBufferPool()
    {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
            if (pool.getName().equals("direct"))
                return pool;
        throw new IllegalStateException("Direct buffer pool not found");
    }

    @Test
    public void testConstructorFailureClosesParentChannel() throws IOException
    {
        File dataFile = FileUtils.createTempFile("ctor_leak", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));

            // Force getBlockSize() to 0 so the constructor throws after super() has opened
            // the channel and allocated parent buffers, but before writeBuffer is assigned.
            ChannelCapturingWriter.lastCapturedChannel = null;
            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(0);

                try
                {
                    new ChannelCapturingWriter(dataFile, metadataFile, SequentialWriterOption.DEFAULT,
                                               CompressionParams.lz4(), collector);
                    fail("Expected IllegalStateException from constructor");
                }
                catch (IllegalStateException expected)
                {
                    Assert.assertTrue("expected block-size message, got: " + expected.getMessage(),
                                      expected.getMessage().contains("block size"));
                }
            }

            FileChannel parentChannel = ChannelCapturingWriter.lastCapturedChannel;
            assertNotNull("test subclass should have captured the parent FileChannel", parentChannel);
            assertFalse("parent FileChannel must be closed after constructor failure",
                        parentChannel.isOpen());
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    @Test
    public void testConstructorRejectsNonPowerOfTwoBlockSize() throws IOException
    {
        File dataFile = FileUtils.createTempFile("nonpow2_blocksize", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");

        try
        {
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));

            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(4097);

                try
                {
                    new DirectCompressedSequentialWriter(dataFile, metadataFile, null,
                                                         SequentialWriterOption.DEFAULT,
                                                         CompressionParams.lz4(), collector, null);
                    fail("Expected IllegalStateException for non-power-of-two block size");
                }
                catch (IllegalStateException expected)
                {
                    Assert.assertTrue("expected power-of-two message, got: " + expected.getMessage(),
                                      expected.getMessage().contains("power of two"));
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
    public void testUndersizedBufferLogsWarningOnce() throws Exception
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        DataStorageSpec.IntKibibytesBound savedBufferSize = conf.direct_write_buffer_size;

        Logger writerLogger = (Logger) LoggerFactory.getLogger(DirectCompressedSequentialWriter.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        writerLogger.addAppender(appender);

        // Reset the once-per-JVM guard so the test outcome is independent of execution order.
        Field warnedField = DirectCompressedSequentialWriter.class.getDeclaredField("undersizedBufferWarned");
        warnedField.setAccessible(true);
        AtomicBoolean warned = (AtomicBoolean) warnedField.get(null);
        warned.set(false);

        File file1 = FileUtils.createTempFile("undersized_1", ".db");
        File meta1 = new File(file1.absolutePath() + ".metadata");
        File file2 = FileUtils.createTempFile("undersized_2", ".db");
        File meta2 = new File(file2.absolutePath() + ".metadata");
        try
        {
            // 1 KiB is well below lz4's minRequiredSize (worst-case 64 KiB chunk + 4 + 4 KiB block).
            conf.direct_write_buffer_size = new DataStorageSpec.IntKibibytesBound("1KiB");

            MetadataCollector c1 = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter w = new DirectCompressedSequentialWriter(
                 file1, meta1, null, SequentialWriterOption.DEFAULT, CompressionParams.lz4(), c1, null))
            {
                // construction is enough to trigger the warning
            }

            MetadataCollector c2 = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter w = new DirectCompressedSequentialWriter(
                 file2, meta2, null, SequentialWriterOption.DEFAULT, CompressionParams.lz4(), c2, null))
            {
                // second construction must not re-fire the warning
            }

            List<ILoggingEvent> warnings = appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .filter(e -> e.getFormattedMessage().contains("direct_write_buffer_size"))
                .collect(java.util.stream.Collectors.toList());
            assertEquals("Expected exactly one undersized-buffer warning across two writers, got: " + warnings,
                         1, warnings.size());
        }
        finally
        {
            conf.direct_write_buffer_size = savedBufferSize;
            writerLogger.detachAppender(appender);
            warned.set(false);
            file1.tryDelete();
            meta1.tryDelete();
            file2.tryDelete();
            meta2.tryDelete();
        }
    }

    /**
     * Captures the parent {@code FileChannel} via the overridden {@code txnProxy()}, which
     * {@code SequentialWriter}'s field initializer invokes before this subclass's instance
     * fields are assigned — hence the static slot.
     */
    private static class ChannelCapturingWriter extends DirectCompressedSequentialWriter
    {
        static volatile FileChannel lastCapturedChannel;

        ChannelCapturingWriter(File file,
                               File offsetsFile,
                               SequentialWriterOption option,
                               CompressionParams parameters,
                               MetadataCollector collector)
        {
            super(file, offsetsFile, null, option, parameters, collector, null);
        }

        @Override
        protected SequentialWriter.TransactionalProxy txnProxy()
        {
            lastCapturedChannel = (FileChannel) channel;
            return super.txnProxy();
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

        File standardFile = FileUtils.createTempFile("standard_digest", ".db");
        File standardMetadata = new File(standardFile.absolutePath() + ".metadata");
        File standardDigest = FileUtils.createTempFile("standard_digest", ".digest");

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

    @Test
    public void testCompressionFailureFallback() throws IOException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);

        int dataSize = chunkLength * 3;
        testWriteAndRead("compressionFailure", dataSize, params);
    }

    @Test
    public void testPartialLastChunkPadding() throws IOException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);

        int dataSize = chunkLength * 2 + 500;
        testWriteAndRead("partialChunkPadding", dataSize, params);
    }

    @Test
    public void testCompressionFailureMatchesStandardWriter() throws IOException
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);

        // Partial last chunk also exercises the padding path.
        int dataSize = chunkLength * 2 + 100;
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

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
            MetadataCollector collector = new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, collector, null))
            {
                writer.write(testData);
                writer.finish();
            }

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
