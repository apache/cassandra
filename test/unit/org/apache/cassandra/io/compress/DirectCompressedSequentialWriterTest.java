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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.CompressionParams;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import static accord.utils.Property.qt;
import static org.apache.cassandra.schema.CompressionParams.DEFAULT_CHUNK_LENGTH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

public class DirectCompressedSequentialWriterTest
{
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DirectCompressedSequentialWriterTest.class);

    // Logged in @BeforeClass so a property-test failure in CI can be reproduced locally.
    private static long seed;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
        seed = new Random().nextLong();
        logger.info("DirectCompressedSequentialWriterTest property-based seed: {}", seed);
    }

    private static MetadataCollector newCollector()
    {
        return new MetadataCollector(new ClusteringComparator(Collections.singletonList(BytesType.instance)));
    }

    @FunctionalInterface
    private interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    @FunctionalInterface
    private interface DataFileTest
    {
        void accept(File dataFile, File metadataFile) throws Exception;
    }

    private static void withTempDataFile(String prefix, DataFileTest body) throws Exception
    {
        File dataFile = FileUtils.createTempFile(prefix, ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");
        try
        {
            body.accept(dataFile, metadataFile);
        }
        finally
        {
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    private static void withDirectWriteBufferSize(int kib, ThrowingRunnable body) throws Exception
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        DataStorageSpec.IntKibibytesBound saved = conf.direct_write_buffer_size;
        conf.direct_write_buffer_size = new DataStorageSpec.IntKibibytesBound(kib + "KiB");
        try
        {
            body.run();
        }
        finally
        {
            conf.direct_write_buffer_size = saved;
        }
    }

    private static void readBackVerify(File dataFile, File metadataFile, byte[] expected) throws IOException
    {
        try (CompressionMetadata metadata = CompressionMetadata.open(metadataFile, dataFile.length(), true);
             FileHandle fh = new FileHandle.Builder(dataFile).withCompressionMetadata(metadata).complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(expected.length, reader.length());
            byte[] readBack = new byte[expected.length];
            reader.readFully(readBack);
            assertArrayEquals(expected, readBack);
        }
    }

    @Test
    public void testBufferSizedForWorstCaseCompressedOutput() throws Exception
    {
        // Large chunk so minRequiredSize dominates the default configured buffer — exercises the
        // sizing formula directly rather than letting the config mask it.
        assertBufferSizedForWorstCaseChunk(CompressionParams.lz4(1024 * 1024, Integer.MAX_VALUE));

        // Same invariant at default chunk length for every supported compressor: worst-case
        // initialCompressedBufferLength differs per compressor.
        assertBufferSizedForWorstCaseChunk(CompressionParams.lz4());
        assertBufferSizedForWorstCaseChunk(CompressionParams.zstd());
        assertBufferSizedForWorstCaseChunk(CompressionParams.snappy());
        assertBufferSizedForWorstCaseChunk(CompressionParams.deflate());
    }

    // writeBuffer must hold a worst-case compressed chunk + its CRC trailer plus up to (blockSize - 1)
    // leftover bytes that survive a flushCompleteBlocks. If this fails, the SUT's mid-stream flush
    // guards become reachable — and nothing tests them.
    private static void assertBufferSizedForWorstCaseChunk(CompressionParams params) throws Exception
    {
        withTempDataFile("bufferSize", (dataFile, metadataFile) ->
        {
            MetadataCollector collector = newCollector();
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, collector, null))
            {
                int blockSize = FileUtils.getBlockSize(dataFile.parent());
                int maxChunkWrite = params.getSstableCompressor().initialCompressedBufferLength(params.chunkLength());

                int bufferCapacity = writer.writeBuffer().capacity();

                int requiredCapacity = maxChunkWrite + Integer.BYTES + blockSize;
                Assert.assertTrue(
                String.format("%s: write buffer (%d) too small for worst-case chunk write (%d) + CRC (%d) + blockSize (%d) = %d",
                              params.getSstableCompressor().getClass().getSimpleName(),
                              bufferCapacity, maxChunkWrite, Integer.BYTES, blockSize, requiredCapacity),
                bufferCapacity >= requiredCapacity);
            }
        });
    }

    @Test
    public void testDirectWriteBufferMetricBalances() throws Exception
    {
        long bytes0 = StorageMetrics.directWriteBufferBytes.getCount();
        long alloc0 = StorageMetrics.directWriteBuffersAllocated.getCount();

        withTempDataFile("metric", (dataFile, metadataFile) ->
        {
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null, SequentialWriterOption.DEFAULT, CompressionParams.lz4(), newCollector(), null))
            {
                assertEquals("byte gauge must rise by the allocated buffer capacity",
                             bytes0 + writer.writeBuffer().capacity(), StorageMetrics.directWriteBufferBytes.getCount());
                assertEquals("allocation meter must be marked exactly once per writer",
                             alloc0 + 1, StorageMetrics.directWriteBuffersAllocated.getCount());
            }

            assertEquals("byte gauge must return to baseline once the writer is cleaned up",
                         bytes0, StorageMetrics.directWriteBufferBytes.getCount());
        });
    }

    /**
     * The only guaranteed NOOP round-trip: testDigestMatchesStandardWriter omits NOOP and the
     * randomized sweep only samples it, so a given seed can skip NOOP entirely. Worth a named test
     * because NOOP is the identity path (every chunk stored verbatim + CRC) and never reaches this
     * writer in prod — it comes only from FLUSH, which is gated out of O_DIRECT (the gate itself is
     * asserted separately, not here).
     */
    @Test
    public void testNoopCompressor() throws Exception
    {
        testWriteAndRead("noop", DEFAULT_CHUNK_LENGTH * 3 + 137, CompressionParams.NOOP);
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
            MetadataCollector collector = newCollector();
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
    public void testMarkThrowsUnsupportedOperationException() throws Exception
    {
        withTempDataFile("mark_test", (dataFile, metadataFile) ->
        {
            MetadataCollector collector = newCollector();
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
        });
    }

    @Test
    public void testResetAndTruncateThrowsUnsupportedOperationException() throws Exception
    {
        withTempDataFile("reset_test", (dataFile, metadataFile) ->
        {
            MetadataCollector collector = newCollector();
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
        });
    }

    @Test
    public void testAbortBeforeFlushCleansUpResources() throws Exception
    {
        withTempDataFile("abort_test", (dataFile, metadataFile) ->
        {
            byte[] testData = new byte[1024];
            new Random(99).nextBytes(testData);

            MetadataCollector collector = newCollector();

            // finishOnClose(false) routes close() through the abort path instead of finish().
            SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                              .finishOnClose(false)
                                                                              .build();

            DirectCompressedSequentialWriter writerRef;
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            abortOnCloseOption, CompressionParams.lz4(), collector, null))
            {
                writerRef = writer;
                writer.write(testData);
            }

            // Sub-block write (1024 B < blockSize ≥4096): flushCompleteBlocks never had a full
            // block to flush, so nothing reached the channel BEFORE the abort. doPrepare (where
            // flushFinalWithPadding lives) does not run on the abort path either, so the file is
            // empty. Sibling testAbortAfterFlushCleansUpResources covers abort AFTER a flush.
            assertEquals("aborted data file should be empty (abort before any flush, no doPrepare)",
                         0L, dataFile.length());

            // doPreCleanup frees and nulls writeBuffer even when nothing was ever flushed.
            assertNull("writeBuffer must be released by doPreCleanup on abort", writerRef.writeBuffer());
        });
    }

    @Test
    public void testAbortAfterFlushCleansUpResources() throws Exception
    {
        CompressionParams params = CompressionParams.lz4();
        int chunk = params.chunkLength();

        // Smallest warning-free buffer (worst-case chunk + CRC + one block), so a few incompressible
        // chunks overflow it and force flushCompleteBlocks to write whole blocks to the channel.
        int max = params.getSstableCompressor().initialCompressedBufferLength(chunk);
        int minRequired = max + Integer.BYTES + 4096;
        int bufferKiB = roundUpToKiB(minRequired) / 1024;

        withDirectWriteBufferSize(bufferKiB, () ->
        withTempDataFile("abort_after_flush", (dataFile, metadataFile) ->
        {
            int blockSize = FileUtils.getBlockSize(dataFile.parent());

            // Random bytes don't compress under LZ4, so each chunk stays ~chunk-sized and
            // several of them overflow the small buffer, forcing flushCompleteBlocks.
            byte[] testData = new byte[chunk * 4];
            new Random(99).nextBytes(testData);

            MetadataCollector collector = newCollector();

            // finishOnClose(false) routes close() through the abort path instead of finish().
            SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                              .finishOnClose(false)
                                                                              .build();

            DirectCompressedSequentialWriter writerRef;
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            abortOnCloseOption, params, collector, null))
            {
                writerRef = writer;
                writer.write(testData);

                // While still OPEN: flushCompleteBlocks must already have written whole blocks.
                // flushFinalWithPadding runs only on commit (doPrepare), never on abort, so any
                // on-disk bytes at this point can only have come from flushCompleteBlocks.
                long flushedBeforeAbort = dataFile.length();
                assertTrue("expected flushCompleteBlocks to have written at least one block before abort, got "
                           + flushedBeforeAbort, flushedBeforeAbort > 0);
                assertEquals("flushed bytes must be a whole multiple of blockSize under O_DIRECT",
                             0L, flushedBeforeAbort % blockSize);
            }

            // Abort neither truncates nor deletes: doPrepare (flushFinalWithPadding + truncate) runs
            // only on commit, so blocks flushed by flushCompleteBlocks stay on disk. That is the
            // contract — abort, like the base SequentialWriter's, releases only OS/off-heap resources;
            // file cleanup belongs to the SSTable lifecycle (LogTransaction.SSTableTidier). Do not
            // "fix" this to assert the file is gone: length > 0 is proof the after-flush path ran.
            assertTrue("aborted data file should retain the blocks flushed before abort",
                       dataFile.length() > 0);

            // doPreCleanup frees and nulls writeBuffer even on the abort path.
            assertNull("writeBuffer must be released by doPreCleanup on abort", writerRef.writeBuffer());
        }));
    }

    @Test
    public void testDirectMemoryIsCleanedOnClose() throws Exception
    {
        // Sized to dominate baseline allocator noise; matches DirectThreadLocalReadAheadBufferTest.
        int bufferSize = 64 * 1024 * 1024;
        withDirectWriteBufferSize(bufferSize / 1024, () ->
        withTempDataFile("direct_mem_clean", (dataFile, metadataFile) ->
        {
            BufferPoolMXBean directPool = getDirectBufferPool();
            MetadataCollector collector = newCollector();
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
        }));
    }

    private static BufferPoolMXBean getDirectBufferPool()
    {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class))
            if (pool.getName().equals("direct"))
                return pool;
        throw new IllegalStateException("Direct buffer pool not found");
    }

    // The ctor call below intentionally throws before producing a writer; no AutoCloseable to manage.
    @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" })
    @Test
    public void testConstructorFailureClosesParentChannel() throws Exception
    {
        withTempDataFile("ctor_leak", (dataFile, metadataFile) ->
        {
            MetadataCollector collector = newCollector();

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
        });
    }

    // The ctor call below intentionally throws before producing a writer; no AutoCloseable to manage.
    @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" })
    @Test
    public void testConstructorRejectsNonPowerOfTwoBlockSize() throws Exception
    {
        withTempDataFile("nonpow2_blocksize", (dataFile, metadataFile) ->
        {
            MetadataCollector collector = newCollector();

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
        });
    }

    @Test
    public void testUndersizedBufferLogsWarningOnce() throws Exception
    {
        // Reset the once-per-JVM guard so the test outcome is independent of execution order.
        DirectCompressedSequentialWriter.resetUndersizedBufferWarned();

        Logger writerLogger = (Logger) LoggerFactory.getLogger(DirectCompressedSequentialWriter.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        writerLogger.addAppender(appender);
        try
        {
            // 1 KiB is well below lz4's minRequiredSize (worst-case 64 KiB chunk + 4 + 4 KiB block).
            withDirectWriteBufferSize(1, () ->
            {
                for (String prefix : new String[]{ "undersized_1", "undersized_2" })
                    withTempDataFile(prefix, (dataFile, metadataFile) ->
                    {
                        try (DirectCompressedSequentialWriter w = new DirectCompressedSequentialWriter(
                             dataFile, metadataFile, null, SequentialWriterOption.DEFAULT, CompressionParams.lz4(), newCollector(), null))
                        {
                        }
                    });
            });

            List<ILoggingEvent> warnings = appender.list.stream()
                .filter(e -> e.getLevel() == Level.WARN)
                .filter(e -> e.getFormattedMessage().contains("direct_write_buffer_size"))
                .collect(Collectors.toList());
            assertEquals("Expected exactly one undersized-buffer warning across two writers, got: " + warnings,
                         1, warnings.size());
        }
        finally
        {
            writerLogger.detachAppender(appender);
            DirectCompressedSequentialWriter.resetUndersizedBufferWarned();
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
    public void testDigestFileValidation() throws Exception
    {
        CompressionParams params = CompressionParams.lz4();
        int chunkLength = params.chunkLength();

        for (int dataSize : new int[]{ 100, chunkLength, chunkLength * 3 + 500 })
            withTempDataFile("digest_validate_" + dataSize, (dataFile, metadataFile) ->
            {
                File digestFile = FileUtils.createTempFile("digest_validate_" + dataSize, ".digest");
                try
                {
                    byte[] testData = new byte[dataSize];
                    new Random(42).nextBytes(testData);

                    try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                    dataFile, metadataFile, digestFile,
                    SequentialWriterOption.DEFAULT, params, newCollector(), null))
                    {
                        writer.write(testData);
                        writer.finish();
                    }

                    new DataIntegrityMetadata.FileDigestValidator(dataFile, digestFile).validate();
                }
                finally
                {
                    digestFile.tryDelete();
                }
            });
    }

    @Test
    public void testCompressionFailureFallback() throws Exception
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        // maxCompressedLength == chunkLength forces fallback to the uncompressed path when
        // a chunk doesn't compress smaller than the input.
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);
        int dataSize = chunkLength * 3;

        // Random bytes don't compress under LZ4, so at least one chunk MUST take the fallback.
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        withTempDataFile("compressionFailure_direct", (dataFile, metadataFile) ->
        {
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, newCollector(), null))
            {
                writer.write(testData);
                writer.finish();
            }

            readBackVerify(dataFile, metadataFile, testData);

            try (CompressionMetadata metadata = CompressionMetadata.open(metadataFile, dataFile.length(), true))
            {
                // Fallback chunks have stored length == chunkLength (data stored verbatim);
                // compressed chunks have length < chunkLength.
                int chunks = (dataSize + chunkLength - 1) / chunkLength;
                boolean anyUncompressed = false;
                for (int i = 0; i < chunks; i++)
                    if (metadata.chunkFor((long) i * chunkLength).length == chunkLength)
                    {
                        anyUncompressed = true;
                        break;
                    }
                assertTrue("expected at least one chunk to take the uncompressed fallback path",
                           anyUncompressed);

                // chunkOffsetsSize counts bytes, not chunks: one long offset per chunk.
                assertEquals("metadata chunk count should match expected chunk count",
                             chunks, (int) (metadata.chunkOffsetsSize / Long.BYTES));

                // On-disk chunk offsets must strictly increase: each chunk is written after the prior.
                long prevOffset = -1;
                for (int i = 0; i < chunks; i++)
                {
                    long offset = metadata.chunkFor((long) i * chunkLength).offset;
                    assertTrue("chunk offsets must be strictly increasing: chunk " + i +
                               " offset " + offset + " not greater than previous " + prevOffset,
                               offset > prevOffset);
                    prevOffset = offset;
                }
            }
        });
    }

    /**
     * Byte equivalence with the standard writer in uncompressed-fallback mode (maxCompressedLength ==
     * chunkLength on incompressible data) — the fallback companion to testDigestMatchesStandardWriter.
     * Digest equivalence is implied: the digest is a deterministic function of data + per-chunk CRCs.
     */
    @Test
    public void testCompressionFailureMatchesStandardWriter() throws Exception
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength, chunkLength);
        byte[] payload = new byte[chunkLength * 2 + 100]; // partial last chunk exercises padding
        new Random(42).nextBytes(payload);
        assertWriteAndReadMatchesStandardWriter(payload, params);
    }

    @Test
    public void testPartialChannelWritesAreFullyDrained() throws Exception
    {
        int blockSize = 4096;
        CompressionParams params = CompressionParams.lz4();
        // Incompressible and several chunks, so flushes span multiple blocks — far larger than the cap.
        byte[] payload = new byte[DEFAULT_CHUNK_LENGTH * 3];
        new Random(42).nextBytes(payload);

        withTempDataFile("partialWrite_direct", (dataFile, metadataFile) ->
        {
            PartialWritingFileChannel partial;
            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(blockSize);
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                dataFile, metadataFile, null, SequentialWriterOption.DEFAULT, params, newCollector(), null))
                {
                    // Invariant: cap < blockSize. The channel is handed block-aligned flush buffers
                    // (always >= blockSize), so a sub-block cap forces every flush to split across calls.
                    // The multi-block payload guarantees at least one such flush exists.
                    int writeCap = blockSize / 2;
                    partial = installPartialChannel(writer, dataFile, writeCap);
                    writer.write(payload);
                    writer.finish();
                }
            }

            // Proves the invariant actually engaged the drain loop, rather than trusting the arithmetic.
            assertTrue("cap must force a short write, else the drain loop is untested", partial.shortWrites > 0);
            readBackVerify(dataFile, metadataFile, payload);
        });
    }

    /**
     * Pins the getOnDiskFilePointer() contract: 0 before any write, advancing as chunks flush
     * mid-stream, equal to the file length after finish() — padding is truncated away, so it never counts.
     */
    @Test
    public void testOnDiskFilePointerTracksActualDataSize() throws Exception
    {
        int chunkLength = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunkLength);

        // Incompressible payload of exactly 4 full chunks so every chunk produces real on-disk growth.
        byte[] payload = new byte[chunkLength * 4];
        new Random(42).nextBytes(payload);

        withTempDataFile("onDiskPointer_direct", (dataFile, metadataFile) ->
        {
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, newCollector(), null))
            {
                assertEquals("pointer must be 0 before any write", 0L, writer.getOnDiskFilePointer());

                // chunkLength-sized slices make the parent fill (and flush) a chunk per slice, so the
                // pointer advances mid-stream rather than only at finish().
                ByteBuffer slice = ByteBuffer.wrap(payload);
                long previous = 0L;
                for (int offset = 0; offset < payload.length; offset += chunkLength)
                {
                    slice.position(offset).limit(offset + chunkLength);
                    writer.write(slice);

                    long current = writer.getOnDiskFilePointer();
                    assertTrue("pointer must be monotonic non-decreasing, was " + previous + " now " + current,
                               current >= previous);
                    previous = current;
                }

                assertTrue("pointer must advance above 0 before finish() (mid-stream chunk flushes), was " + previous,
                           previous > 0L);

                writer.finish();

                assertEquals("after finish() the pointer must equal the truncated file length",
                             dataFile.length(), writer.getOnDiskFilePointer());
            }
        });
    }

    /**
     * Regression guard for the openFinalEarly (SYNC) short-read: under O_DIRECT the last chunk's sub-block tail is
     * still buffered after sync(), so a reader bound before commit short-reads past EOF without the
     * tail-flush. Tiny sub-block outputs (e.g. system/IndexInfo) are the prod shape.
     */
    @Test
    public void testEarlyOpenAfterSyncReadsBackData() throws IOException
    {
        for (int size : new int[]{ 1, 64, 298, 1024, 4096, DEFAULT_CHUNK_LENGTH * 3 + 137 })
            assertReadBack(OpenMoment.SYNC, ReaderPath.POINT, compressible(size), CompressionParams.lz4(), BufferRegime.DEFAULT);
    }

    // Compresses well below one block, so no whole block fills and nothing reaches disk before
    // finalization — the worst case for every early-open moment.
    private static byte[] compressible(int size)
    {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte) (i % 7);
        return data;
    }

    /**
     * Regression guard for the preemptive early-open (SSTableRewriter.maybeReopenEarly) short-read: it never calls
     * sync(), so the writer must report the durable (whole-block) offset — a staged offset would let the
     * reader short-read chunks still parked in writeBuffer.
     */
    @Test
    public void testPreemptiveOpenReadsBackSyncedData() throws IOException
    {
        // Many small chunks that compress far below one block: nothing reaches disk before finish, so the
        // writer reports 0 synced (the prod system/IndexInfo shape) and the read is skipped.
        assertReadBack(OpenMoment.PREEMPTIVE, ReaderPath.POINT,
                       compressible(DEFAULT_CHUNK_LENGTH * 4 + 137), CompressionParams.lz4(), BufferRegime.DEFAULT);

        // Several MiB of incompressible data forces real block flushes mid-write, so the synced boundary
        // must advance past zero (preemptive open still works) yet never cross the still-buffered tail.
        long synced = assertReadBack(OpenMoment.PREEMPTIVE, ReaderPath.POINT,
                                     incompressible(4 * 1024 * 1024), CompressionParams.lz4(), BufferRegime.DEFAULT);
        assertTrue("expected the synced boundary to advance for multi-MiB data", synced > 0);
    }

    private static byte[] incompressible(int size)
    {
        byte[] data = new byte[size];
        new Random(42).nextBytes(data);
        return data;
    }

    /**
     * A scanner snapshots the data file's size at open. If commit later truncates the padded early-open file,
     * the live scanner short-reads its last block — the CorruptSSTableException seen on
     * system/local_metadata_log under dtest-latest.
     */
    @Test
    public void testEarlyOpenScanReadsBackAfterCommitTruncate() throws IOException
    {
        for (int size : new int[]{ 1, 298, 1024, DEFAULT_CHUNK_LENGTH * 3 + 137 })
            assertReadBack(OpenMoment.SYNC, ReaderPath.SCAN, compressible(size), CompressionParams.lz4(), BufferRegime.DEFAULT);
    }

    // ---------------------------------------------------------------------------------------------------
    // Read-back matrix for the bug class "a reader binds to a mid-flight O_DIRECT file the writer then mutates".
    // The open-moment and reader-path axes that hid the scan-truncate short-read are enumerated, not
    // sampled — a size-only fuzzer would miss them.
    // ---------------------------------------------------------------------------------------------------

    // The three moments a reader can bind to a live writer's data file (STREAM excluded — streaming receive
    // never early-opens before commit; readers open only after the SSTable is finished).
    private enum OpenMoment { SYNC, PREEMPTIVE, COMMIT }

    // POINT = chunk-metadata-bounded random access; SCAN = read-ahead path that snapshots channel.size() once.
    private enum ReaderPath { POINT, SCAN }

    // DEFAULT = configured 1 MiB write buffer; SMALL = 1 KiB, coerced up to the per-writer minimum so
    // flushCompleteBlocks fires almost every chunk (maximises block-boundary crossings under O_DIRECT).
    private enum BufferRegime { DEFAULT, SMALL }

    /**
     * One driver for every matrix cell. Writes {@code payload} via the O_DIRECT writer, binds a reader at
     * {@code moment} over {@code path}, and asserts the readable prefix round-trips. Returns the number of
     * bytes verified (payload.length for SYNC/COMMIT, the durable offset for PREEMPTIVE).
     *
     * The read/finish order is path-dependent:
     *  - POINT early-open binds to the chunk-metadata length, so it must read BEFORE finish() flushes the
     *    buffered tail — that window is where the SYNC and PREEMPTIVE early-open short-reads occur.
     *  - SCAN early-open snapshots channel.size() at reader construction, so it must read AFTER finish()
     *    truncates the padded file — reading that post-truncate snapshot is where the scan short-read occurs.
     * COMMIT opens only after finish() and is the safe-by-construction control.
     */
    private static long assertReadBack(OpenMoment moment, ReaderPath path, byte[] payload,
                                       CompressionParams params, BufferRegime regime) throws IOException
    {
        // CompressedChunkReader builds the scan reader only when read-ahead exceeds chunkLength; otherwise
        // SCAN silently degrades to a point read and the cell tests nothing.
        if (path == ReaderPath.SCAN)
        {
            int readAhead = DatabaseDescriptor.getCompressedReadAheadBufferSize();
            assertTrue("SCAN cell would silently fall back to a point read: compressed_read_ahead_buffer_size ("
                       + readAhead + ") must exceed chunkLength (" + params.chunkLength()
                       + "). Raise read-ahead or lower chunkLength.",
                       readAhead > params.chunkLength());
        }

        Config conf = DatabaseDescriptor.getRawConfig();
        DataStorageSpec.IntKibibytesBound savedBuffer = conf.direct_write_buffer_size;
        if (regime == BufferRegime.SMALL)
            conf.direct_write_buffer_size = new DataStorageSpec.IntKibibytesBound("1KiB");

        File dataFile = FileUtils.createTempFile("readback_matrix_direct", ".db");
        File metadataFile = new File(dataFile.absolutePath() + ".metadata");
        try
        {
            MetadataCollector collector = newCollector();
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                dataFile, metadataFile, null, SequentialWriterOption.DEFAULT, params, collector, null))
            {
                // PREEMPTIVE binds to whatever offset the writer reports durable via the post-flush listener
                // (the same hook that drives PartitionIndexBuilder.markDataSynced). Register before writing.
                long[] capturedOffset = { 0 };
                if (moment == OpenMoment.PREEMPTIVE)
                    writer.setPostFlushListener(off -> capturedOffset[0] = off);

                writer.write(payload);

                switch (moment)
                {
                    case SYNC:
                        // openFinalEarly: sync() finalizes the file, then open(0) == NO_LENGTH_OVERRIDE.
                        writer.sync();
                        try (CompressionMetadata md = writer.open(0))
                        {
                            readEarlyOpenAndFinish(writer, dataFile, md, path, payload, payload.length, moment, params, regime);
                        }
                        return payload.length;

                    case PREEMPTIVE:
                    {
                        // openEarly: never calls sync(); the reader is bounded by the durable offset only.
                        int readable = (int) capturedOffset[0];
                        assertTrue("preemptive durable offset out of range: " + readable,
                                   readable >= 0 && readable <= payload.length);
                        if (readable == 0)
                        {
                            // Nothing durable yet (whole tail still buffered) — a legitimate prod shape; skip the read.
                            writer.finish();
                            return 0;
                        }
                        try (CompressionMetadata md = writer.open(readable))
                        {
                            readEarlyOpenAndFinish(writer, dataFile, md, path, payload, readable, moment, params, regime);
                        }
                        return readable;
                    }

                    case COMMIT:
                        // Safe-by-construction control: open only after finish().
                        writer.finish();
                        try (CompressionMetadata md = CompressionMetadata.open(metadataFile, dataFile.length(), true);
                             FileHandle fh = new FileHandle.Builder(dataFile).withCompressionMetadata(md).complete();
                             RandomAccessReader reader = fh.createReader(null, path == ReaderPath.SCAN))
                        {
                            assertReadablePrefix(reader, payload, payload.length, moment, path, params, regime);
                        }
                        return payload.length;

                    default:
                        throw new AssertionError(moment);
                }
            }
        }
        finally
        {
            conf.direct_write_buffer_size = savedBuffer;
            dataFile.tryDelete();
            metadataFile.tryDelete();
        }
    }

    // Read/finish order is path-dependent; see assertReadBack's contract.
    private static void readEarlyOpenAndFinish(DirectCompressedSequentialWriter writer, File dataFile,
                                               CompressionMetadata md, ReaderPath path, byte[] payload,
                                               int readableLen, OpenMoment moment, CompressionParams params,
                                               BufferRegime regime) throws IOException
    {
        try (FileHandle fh = new FileHandle.Builder(dataFile).withCompressionMetadata(md).complete();
             RandomAccessReader reader = fh.createReader(null, path == ReaderPath.SCAN))
        {
            if (path == ReaderPath.SCAN)
            {
                writer.finish();
                assertReadablePrefix(reader, payload, readableLen, moment, path, params, regime);
            }
            else
            {
                assertReadablePrefix(reader, payload, readableLen, moment, path, params, regime);
                writer.finish();
            }
        }
    }

    private static void assertReadablePrefix(RandomAccessReader reader, byte[] payload, int readableLen,
                                             OpenMoment moment, ReaderPath path, CompressionParams params,
                                             BufferRegime regime) throws IOException
    {
        String ctx = moment + "/" + path + "/" + regime + " size=" + payload.length + " "
                     + params.getSstableCompressor().getClass().getSimpleName() + " chunk=" + params.chunkLength();
        if (path == ReaderPath.POINT)
            assertEquals("readable length mismatch [" + ctx + ']', readableLen, reader.length());
        byte[] readBack = new byte[readableLen];
        reader.readFully(readBack);
        assertArrayEquals("read-back mismatch [" + ctx + ']', Arrays.copyOf(payload, readableLen), readBack);
    }

    // Boundary shapes that have historically broken flush/early-open (see inline tags). Size 0 is excluded:
    // an empty SSTable is never early-opened (open(0) asserts tCount>0); a COMMIT-only cell covers it.
    private static int[] boundarySizes()
    {
        return new int[]{ 1, 298 /* sub-block */, 4096 /* one block */, 4097 /* one byte over */,
                          DEFAULT_CHUNK_LENGTH - 1, DEFAULT_CHUNK_LENGTH, DEFAULT_CHUNK_LENGTH + 1,
                          DEFAULT_CHUNK_LENGTH * 3 + 137 /* 3 chunks + non-aligned tail */ };
    }

    /**
     * Full open × path × content × boundary-size cross on Lz4. Both content kinds matter: compressible keeps
     * the whole tail buffered; incompressible forces a real mid-stream block flush. Plus one multi-MiB payload.
     */
    @Test
    public void testReadBackAcrossOpenMomentsPathsAndBoundarySizes() throws IOException
    {
        for (OpenMoment moment : OpenMoment.values())
            for (ReaderPath path : ReaderPath.values())
            {
                // Empty payload: only a committed reader can observe a 0-byte SSTable (early-open never fires
                // on empty output).
                if (moment == OpenMoment.COMMIT)
                    assertReadBack(moment, path, new byte[0], CompressionParams.lz4(), BufferRegime.DEFAULT);

                for (int size : boundarySizes())
                {
                    assertReadBack(moment, path, compressible(size), CompressionParams.lz4(), BufferRegime.DEFAULT);
                    assertReadBack(moment, path, incompressible(size), CompressionParams.lz4(), BufferRegime.DEFAULT);
                }
                // Multi-MiB incompressible: forces many real mid-stream flushes so early-open boundaries advance.
                assertReadBack(moment, path, incompressible(4 * 1024 * 1024), CompressionParams.lz4(), BufferRegime.DEFAULT);
            }
    }

    // The early-open moments that bind to a mid-flight file. COMMIT (finished-file read-back) is omitted from
    // the compressor cross below — it is already covered per-compressor by the standard-writer equality test.
    private static final OpenMoment[] EARLY_OPEN_MOMENTS = { OpenMoment.SYNC, OpenMoment.PREEMPTIVE };

    /**
     * Early-open moment × path across all 5 compressors, three chunk lengths (all < 256 KiB read-ahead, so
     * SCAN stays engaged), and both buffer regimes — to surface compressor- or chunk-specific flush bugs.
     * 1 MiB incompressible forces many mid-stream flushes on every compressor.
     */
    @Test
    public void testReadBackAcrossCompressorsChunkLengthsAndBufferRegimes() throws IOException
    {
        for (CompressorKind kind : CompressorKind.values())
            for (int chunkLength : new int[]{ 1024, DEFAULT_CHUNK_LENGTH, 64 * 1024 })
            {
                CompressionParams params = paramsForKind(kind, chunkLength);
                for (BufferRegime regime : BufferRegime.values())
                    for (OpenMoment moment : EARLY_OPEN_MOMENTS)
                        for (ReaderPath path : ReaderPath.values())
                        {
                            // per cell: compressible sub-block, incompressible multi-chunk tail, 1 MiB incompressible
                            assertReadBack(moment, path, compressible(298), params, regime);
                            assertReadBack(moment, path, incompressible(chunkLength * 3 + 137), params, regime);
                            assertReadBack(moment, path, incompressible(1024 * 1024), params, regime);
                        }
            }
    }

    /**
     * Random sizes 1..1 MiB with moment/path/content picked per example, to hit sizes between the enumerated
     * boundaries. Lz4 @ default chunk keeps SCAN engaged.
     */
    @Test
    public void testReadBackRandomSizesAcrossOpenMomentsAndPaths()
    {
        // A PREEMPTIVE example whose tail is still buffered verifies 0 bytes; assert the sweep verified
        // some, so it can't silently degrade to all-vacuous.
        AtomicLong totalVerified = new AtomicLong();
        qt().withSeed(seed).withExamples(120).forAll(Gens.random()).check(rs -> {
            OpenMoment moment = rs.pick(OpenMoment.values());
            ReaderPath path = rs.pick(ReaderPath.values());
            int size = rs.nextInt(1, (1024 * 1024) + 1);
            byte[] payload = rs.nextBoolean() ? compressible(size) : incompressible(size);
            totalVerified.addAndGet(assertReadBack(moment, path, payload, CompressionParams.lz4(), BufferRegime.DEFAULT));
        });
        assertTrue("random read-back sweep verified no bytes; every example degraded to a vacuous pass",
                   totalVerified.get() > 0);
    }

    private void testWriteAndRead(String testName, int dataSize, CompressionParams params) throws Exception
    {
        byte[] testData = new byte[dataSize];
        new Random(42).nextBytes(testData);

        withTempDataFile(testName + "_direct", (dataFile, metadataFile) ->
        {
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
            dataFile, metadataFile, null,
            SequentialWriterOption.DEFAULT, params, newCollector(), null))
            {
                writer.write(testData);
                writer.finish();
            }
            readBackVerify(dataFile, metadataFile, testData);
        });
    }

    private enum CompressorKind { Lz4, Zstd, Snappy, Deflate, Noop }

    // Powers of two 1 KiB → 64 KiB span the below-block, at-block, above-block, and many-blocks-per-chunk regimes.
    private static Gen<Integer> mixedChunkLengths()
    {
        return Gens.pick(Stream.iterate(1024, n -> n <= 64 * 1024, n -> n * 2)
                               .collect(Collectors.toList()));
    }

    private static Gen<CompressionParams> compressionParamsGen(Gen<Integer> chunkLengths)
    {
        return rs -> {
            int chunkLength = chunkLengths.next(rs);
            return paramsForKind(rs.pick(CompressorKind.values()), chunkLength);
        };
    }

    @Test
    public void testRandomizedWriteAndReadMatchesStandardWriter()
    {
        Gen<CompressionParams> paramsGen = compressionParamsGen(mixedChunkLengths());
        // 1 B – 1 MiB covers below-chunk, single-chunk, multi-chunk, many-chunk, and
        // large-file regimes in one sweep.
        Gen.LongGen fileLengthGen = Gens.longs().between(1, 1024 * 1024);

        // 100 examples keeps wall time bounded while sampling the (compressor, chunkLength, fileSize) space.
        qt().withSeed(seed).withExamples(100).forAll(Gens.random(), paramsGen).check((rs, params) -> {
            int dataSize = (int) fileLengthGen.nextLong(rs);
            byte[] payload = new byte[dataSize];
            rs.nextBytes(payload);
            assertWriteAndReadMatchesStandardWriter(payload, params);
        });
    }

    private static void assertWriteAndReadMatchesStandardWriter(byte[] payload, CompressionParams params) throws Exception
    {
        withTempDataFile("rt_direct", (directFile, directMeta) ->
        withTempDataFile("rt_standard", (standardFile, standardMeta) ->
        {
            MetadataCollector dCollector = newCollector();
            try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                 directFile, directMeta, null, SequentialWriterOption.DEFAULT, params, dCollector, null))
            {
                writer.write(payload);
                writer.finish();
            }

            readBackVerify(directFile, directMeta, payload);

            MetadataCollector sCollector = newCollector();
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(
                 standardFile, standardMeta, null, SequentialWriterOption.DEFAULT, params, sCollector))
            {
                writer.write(payload);
                writer.finish();
            }

            byte[] standardBytes = Files.readAllBytes(standardFile.toPath());
            byte[] directBytes = Files.readAllBytes(directFile.toPath());
            assertArrayEquals("Direct vs standard writer on-disk bytes differ for " +
                              params.getSstableCompressor().getClass().getSimpleName() +
                              ", chunkLength=" + params.chunkLength() + ", size=" + payload.length,
                              standardBytes, directBytes);
        }));
    }

    @Test
    public void testWriteAndReadAcrossBufferSizes() throws Exception
    {
        int chunk = DEFAULT_CHUNK_LENGTH;
        CompressionParams params = CompressionParams.lz4(chunk);

        int max = params.getSstableCompressor().initialCompressedBufferLength(chunk);
        // 4 KiB is the common-case filesystem block; the writer rounds up to the actual
        // blockSize at runtime, so these sizes remain valid lower bounds on 8 KiB FSes too.
        int blockProxy = 4096;
        int minRequired = max + Integer.BYTES + blockProxy;

        // Three buffer regimes pin different flushCompleteBlocks branches:
        //   exactly minRequired           → clear path dominates
        //   minRequired + block           → frequent compact() of sub-block tail
        //   1 MiB                          → many chunks accumulate before flush
        // Crossed with four data sizes hitting different final-tail regimes:
        //   chunk - 1                     → flushFinalWithPadding only
        //   chunk * 3                     → 3 full chunks, partial-block tail at end
        //   chunk * 3 + 137               → 3 full chunks + explicit non-aligned tail
        //   1 MiB + chunk + 137           → overflows even the 1 MiB buffer, so
        //                                   flushCompleteBlocks fires mid-stream in every regime
        // Each case is proven byte-identical to the standard CompressedSequentialWriter (not just a
        // self round-trip), so flush-boundary corruption under small buffers cannot slip through.
        int[] bufferSizesBytes = { roundUpToKiB(minRequired),
                                   roundUpToKiB(minRequired + blockProxy),
                                   1024 * 1024 };
        int[] dataSizes = { chunk - 1, chunk * 3, chunk * 3 + 137, 1024 * 1024 + chunk + 137 };

        for (int sz : bufferSizesBytes)
            withDirectWriteBufferSize(sz / 1024, () ->
            {
                for (int dataSize : dataSizes)
                {
                    byte[] payload = new byte[dataSize];
                    new Random(0x5AFEL ^ dataSize).nextBytes(payload);
                    assertWriteAndReadMatchesStandardWriter(payload, params);
                }
            });
    }

    // Round up so we never land below minRequired due to integer division when converting to KiB.
    private static int roundUpToKiB(int bytes)
    {
        return ((bytes + 1023) / 1024) * 1024;
    }

    /**
     * O_DIRECT alignment is the one gap the round-trip and byte-equality tests miss: macOS treats O_DIRECT as
     * advisory, so a misaligned write succeeds silently and no normal test can assert alignment. So observe it:
     * a tiny mocked block size forces mid-stream flushes (real blocks are >= 4096), and a recording channel spy
     * asserts every {@code fchannel.write()} is a blockSize multiple at a blockSize offset.
     */
    @Test
    public void testBlockAlignmentUnderSmallBlocksAndIncrementalWrites() throws Exception
    {
        // 1 KiB is coerced up to the per-writer minimum, so the buffer is as small as allowed and flushes
        // fire almost every chunk.
        withDirectWriteBufferSize(1, () ->
            // 80 examples keeps wall time bounded while sweeping blockSize × compressor × chunkLength ×
            // payload × write-chunking.
            qt().withSeed(seed).withExamples(80).forAll(Gens.random()).check(rs -> {
                int blockSize = rs.pickInt(8, 16, 64, 512);
                int chunkLength = rs.pickInt(1024, 2048, 4096, 8192);
                CompressionParams params = paramsForKind(rs.pick(CompressorKind.values()), chunkLength);
                byte[] payload = new byte[rs.nextInt(1, 64 * 1024 + 1)];
                rs.nextBytes(payload);
                runAlignmentExample(blockSize, params, payload, sliceRandomly(payload, rs));
            }));
    }

    /**
     * Even with no write(), finish() emits one chunk: writeChunk always appends the 4-byte CRC trailer, so the
     * buffer is non-empty when flushFinalWithPadding runs and its {@code logicalPos == 0} early return is
     * unreachable. Pins that premise so nobody "fixes" the guard expecting an empty file to skip the flush.
     */
    @Test
    public void testEmptyFileWritesSingleChunkMatchingStandardWriter() throws Exception
    {
        CompressionParams params = CompressionParams.lz4();
        int blockSize = 8; // mocked small block: the lone padded final write is a clean multiple to assert on.

        // Oracle: the standard writer's empty SSTable (one empty chunk + CRC). Computed before the mock;
        // blockSize only affects O_DIRECT padding, which is truncated away, never the logical bytes.
        byte[] standardBytes = standardWriterBytes(new byte[0], params);

        File dataFile = FileUtils.createTempFile("empty_direct", ".db");
        File metaFile = new File(dataFile.absolutePath() + ".metadata");
        RecordingFileChannel recording = null;
        long pointerAfterFinish = -1;
        try
        {
            MetadataCollector collector = newCollector();
            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(blockSize);
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                     dataFile, metaFile, null, SequentialWriterOption.DEFAULT, params, collector, null))
                {
                    recording = installRecordingChannel(writer, dataFile);
                    writer.finish();
                    pointerAfterFinish = writer.getOnDiskFilePointer();
                }
            }

            // At least one block-aligned O_DIRECT write — the empty-buffer early return is not taken.
            assertFalse("empty file still writes a padded final block (empty-buffer early return is unreachable)",
                        recording.writes.isEmpty());
            for (long[] w : recording.writes)
            {
                assertEquals("channel write offset not block-aligned", 0, w[0] % blockSize);
                assertEquals("channel write length not a block multiple", 0, w[1] % blockSize);
            }

            byte[] directBytes = Files.readAllBytes(dataFile.toPath());
            assertArrayEquals("empty-file on-disk bytes differ from standard writer", standardBytes, directBytes);
            assertEquals("getOnDiskFilePointer should equal final on-disk data size", standardBytes.length, pointerAfterFinish);
            assertEquals("dataFile length should equal actualDataSize after truncate", standardBytes.length, dataFile.length());

            try (CompressionMetadata metadata = CompressionMetadata.open(metaFile, dataFile.length(), true);
                 FileHandle fh = new FileHandle.Builder(dataFile).withCompressionMetadata(metadata).complete();
                 RandomAccessReader reader = fh.createReader())
            {
                assertEquals("empty SSTable should read back zero uncompressed bytes", 0L, reader.length());
            }
        }
        finally
        {
            if (recording != null)
                recording.closeQuietly();
            dataFile.tryDelete();
            metaFile.tryDelete();
        }
    }

    /**
     * At every post-flush callback the reported durable offset must equal the uncompressed end of the
     * last chunk fully on disk, CRC included — more and an early-open reader short-reads; less and
     * early-open is pointlessly conservative. The oracle is rebuilt independently from
     * CompressionMetadata after finish().
     */
    @Test
    public void testDurableOffsetExactlyTracksOnDiskChunks() throws Exception
    {
        // Large-buffer examples legitimately report 0 throughout (nothing reaches disk mid-stream),
        // so require the sweep to hit partial durability somewhere.
        AtomicBoolean sawPartialDurability = new AtomicBoolean();
        qt().withSeed(seed).withExamples(80).forAll(Gens.random()).check(rs -> {
            int blockSize = rs.pickInt(8, 16, 64, 512);
            int chunkLength = rs.pickInt(1024, 2048, 4096, 8192);
            CompressionParams params = paramsForKind(rs.pick(CompressorKind.values()), chunkLength);
            byte[] payload = new byte[rs.nextInt(1, 64 * 1024 + 1)];
            rs.nextBytes(payload);
            // 1 KiB coerces to the per-writer floor: flushes nearly every chunk, draining the queue
            // incrementally. 64 KiB flushes occasionally. 1 MiB never flushes mid-stream: the queue
            // grows past its initial capacity of 8 and nothing is durable until finish.
            int bufferKiB = rs.pickInt(1, 64, 1024);
            withDirectWriteBufferSize(bufferKiB, () ->
                runDurableOffsetExample(blockSize, params, payload, sliceRandomly(payload, rs), sawPartialDurability));
        });
        assertTrue("sweep never observed 0 < durable < total; the partial-durability boundary was not exercised",
                   sawPartialDurability.get());
    }

    private void runDurableOffsetExample(int blockSize, CompressionParams params, byte[] payload,
                                         List<byte[]> slices, AtomicBoolean sawPartialDurability) throws Exception
    {
        File dataFile = FileUtils.createTempFile("durable_direct", ".db");
        File metaFile = new File(dataFile.absolutePath() + ".metadata");
        RecordingFileChannel recording = null;
        try
        {
            MetadataCollector collector = newCollector();
            // Each entry: { reportedDurableUncompressedOffset, onDiskBytesAtCallback }.
            List<long[]> samples = new ArrayList<>();
            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(blockSize);
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                     dataFile, metaFile, null, SequentialWriterOption.DEFAULT, params, collector, null))
                {
                    RecordingFileChannel channel = installRecordingChannel(writer, dataFile);
                    recording = channel;
                    // The listener runs synchronously after the writer computes the durable offset
                    // from this same position — no intervening writes, so the pair is exact.
                    writer.setPostFlushListener(durable -> {
                        try
                        {
                            samples.add(new long[]{ durable, channel.position() });
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException(e);
                        }
                    });
                    for (byte[] slice : slices)
                        writer.write(slice);
                    writer.finish();
                }
            }

            // Oracle: per-chunk {compressedEndWithCrc, uncompressedEnd} reconstructed from the final
            // metadata. Chunk.length excludes the 4-byte CRC, so on-disk end = offset + length + 4.
            int chunkLen = params.chunkLength();
            int chunkCount = (payload.length + chunkLen - 1) / chunkLen;
            long[] compressedEnds = new long[chunkCount];
            long[] uncompressedEnds = new long[chunkCount];
            try (CompressionMetadata metadata = CompressionMetadata.open(metaFile, dataFile.length(), true))
            {
                for (int i = 0; i < chunkCount; i++)
                {
                    CompressionMetadata.Chunk chunk = metadata.chunkFor((long) i * chunkLen);
                    compressedEnds[i] = chunk.offset + chunk.length + 4;
                    uncompressedEnds[i] = Math.min((long) (i + 1) * chunkLen, payload.length);
                }
            }

            long previous = 0;
            for (long[] sample : samples)
            {
                long reported = sample[0];
                long onDisk = sample[1];
                long expected = 0;
                for (int i = 0; i < chunkCount && compressedEnds[i] <= onDisk; i++)
                    expected = uncompressedEnds[i];
                assertEquals("durable offset must be exactly the last chunk fully on disk (onDisk=" + onDisk +
                             ", block=" + blockSize + ", chunkLength=" + chunkLen +
                             ", compressor=" + params.getSstableCompressor().getClass().getSimpleName() +
                             ", payload=" + payload.length + ')',
                             expected, reported);
                assertTrue("durable offset went backwards: " + reported + " < " + previous, reported >= previous);
                previous = reported;
                if (reported > 0 && reported < payload.length)
                    sawPartialDurability.set(true);
            }
        }
        finally
        {
            if (recording != null)
                recording.closeQuietly();
            dataFile.tryDelete();
            metaFile.tryDelete();
        }
    }

    /**
     * Injects an IOException on a random channel write: the writer must surface FSWriteError carrying
     * the injected cause, and the abort that close() runs afterwards must not throw. Fault points
     * beyond the example's write count finish normally and must match the standard writer
     * byte-for-byte; a sweep-level guard ensures some faults fired.
     */
    @Test
    public void testWriteFailureMidStreamThrowsFSWriteErrorAndAbortsCleanly() throws Exception
    {
        AtomicBoolean anyFaultFired = new AtomicBoolean();
        // Floor-sized buffer maximizes mid-stream channel writes, so most fault points land.
        withDirectWriteBufferSize(1, () ->
            qt().withSeed(seed).withExamples(60).forAll(Gens.random()).check(rs -> {
                int blockSize = rs.pickInt(8, 16, 64, 512);
                CompressionParams params = paramsForKind(rs.pick(CompressorKind.values()), rs.pickInt(1024, 2048, 4096));
                byte[] payload = new byte[rs.nextInt(1, 64 * 1024 + 1)];
                rs.nextBytes(payload);
                int failOnWrite = rs.nextInt(1, 9);
                runWriteFaultExample(blockSize, params, payload, failOnWrite, anyFaultFired);
            }));
        assertTrue("no example fired its injected write fault; the sweep degraded to all-success",
                   anyFaultFired.get());
    }

    private void runWriteFaultExample(int blockSize, CompressionParams params, byte[] payload,
                                      int failOnWrite, AtomicBoolean anyFaultFired) throws Exception
    {
        byte[] standardBytes = standardWriterBytes(payload, params);

        File dataFile = FileUtils.createTempFile("fault_direct", ".db");
        File metaFile = new File(dataFile.absolutePath() + ".metadata");
        FaultInjectingFileChannel channel = null;
        try
        {
            MetadataCollector collector = newCollector();
            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(blockSize);
                FSWriteError observed = null;
                // finishOnClose(false): close() runs the abort path when finish() failed — the abort
                // itself must not throw, or try-with-resources surfaces it and fails the example.
                SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                                  .finishOnClose(false)
                                                                                  .build();
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                     dataFile, metaFile, null, abortOnCloseOption, params, collector, null))
                {
                    channel = installFaultChannel(writer, dataFile).failOnWriteIndex(failOnWrite);
                    try
                    {
                        writer.write(payload);
                        writer.finish();
                    }
                    catch (FSWriteError e)
                    {
                        observed = e;
                    }
                }

                if (channel.faultFired)
                {
                    anyFaultFired.set(true);
                    assertNotNull("write fault fired but no FSWriteError surfaced", observed);
                    assertTrue("FSWriteError should carry the injected IOException, got: " + observed.getCause(),
                               observed.getCause() instanceof IOException
                               && FaultInjectingFileChannel.INJECTED_MESSAGE.equals(observed.getCause().getMessage()));
                }
                else
                {
                    assertNull("no fault fired yet an FSWriteError surfaced", observed);
                    assertArrayEquals("fault-free example must match the standard writer",
                                      standardBytes, Files.readAllBytes(dataFile.toPath()));
                }
            }
        }
        finally
        {
            if (channel != null)
                channel.closeQuietly();
            dataFile.tryDelete();
            metaFile.tryDelete();
        }
    }

    /** Truncate happens once, in flushFinalWithPadding: its IOException must surface as FSWriteError. */
    @Test
    public void testTruncateFailureDuringFinishThrowsFSWriteError() throws Exception
    {
        withTempDataFile("truncate_fault", (dataFile, metadataFile) ->
        {
            SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                              .finishOnClose(false)
                                                                              .build();
            FaultInjectingFileChannel channel = null;
            try
            {
                FSWriteError observed = null;
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                     dataFile, metadataFile, null, abortOnCloseOption, CompressionParams.lz4(), newCollector(), null))
                {
                    channel = installFaultChannel(writer, dataFile).failTruncate();
                    writer.write(compressible(1024)); // sub-block tail → finish() must pad, write, truncate
                    try
                    {
                        writer.finish();
                        fail("expected FSWriteError from injected truncate failure");
                    }
                    catch (FSWriteError e)
                    {
                        observed = e;
                    }
                }
                assertTrue("FSWriteError should carry the injected IOException, got: " + observed.getCause(),
                           observed.getCause() instanceof IOException
                           && FaultInjectingFileChannel.INJECTED_MESSAGE.equals(observed.getCause().getMessage()));
            }
            finally
            {
                if (channel != null)
                    channel.closeQuietly();
            }
        });
    }

    /**
     * durableUncompressedOffset is the only position() caller on this path and runs only via the
     * post-flush listener: its IOException must surface as FSReadError out of write(), and the
     * abort must stay clean.
     */
    @Test
    public void testPositionFailureDuringPostFlushThrowsFSReadError() throws Exception
    {
        withTempDataFile("position_fault", (dataFile, metadataFile) ->
        {
            CompressionParams params = CompressionParams.lz4();
            SequentialWriterOption abortOnCloseOption = SequentialWriterOption.newBuilder()
                                                                              .finishOnClose(false)
                                                                              .build();
            FaultInjectingFileChannel channel = null;
            try
            {
                FSReadError observed = null;
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                     dataFile, metadataFile, null, abortOnCloseOption, params, newCollector(), null))
                {
                    channel = installFaultChannel(writer, dataFile).failOnPositionCall(1);
                    writer.setPostFlushListener(offset -> {});
                    try
                    {
                        // Two full chunks guarantee a mid-write chunk flush → post-flush callback →
                        // durableUncompressedOffset → position() → injected IOException.
                        writer.write(incompressible(params.chunkLength() * 2));
                        writer.finish();
                        fail("expected FSReadError from injected position() failure");
                    }
                    catch (FSReadError e)
                    {
                        observed = e;
                    }
                }
                assertTrue("FSReadError should carry the injected IOException, got: " + observed.getCause(),
                           observed.getCause() instanceof IOException
                           && FaultInjectingFileChannel.INJECTED_MESSAGE.equals(observed.getCause().getMessage()));
            }
            finally
            {
                if (channel != null)
                    channel.closeQuietly();
            }
        });
    }

    private static CompressionParams paramsForKind(CompressorKind kind, int chunkLength)
    {
        switch (kind)
        {
            case Lz4:     return CompressionParams.lz4(chunkLength);
            case Zstd:    return CompressionParams.zstd(chunkLength);
            case Snappy:  return CompressionParams.snappy(chunkLength, 1.1); // 1.1 = min compress ratio Snappy requires
            case Deflate: return CompressionParams.deflate(chunkLength);
            case Noop:    return CompressionParams.noop(chunkLength);
            default:      throw new AssertionError();
        }
    }

    // Many small write() calls straddling chunk/block boundaries exercise the accumulation path the
    // single-write fuzz test never reaches. Slices are contiguous and each at least one byte.
    private static List<byte[]> sliceRandomly(byte[] payload, RandomSource rs)
    {
        List<byte[]> slices = new ArrayList<>();
        int pos = 0;
        while (pos < payload.length)
        {
            int len = rs.nextInt(1, payload.length - pos + 1);
            slices.add(Arrays.copyOfRange(payload, pos, pos + len));
            pos += len;
        }
        return slices;
    }

    private void runAlignmentExample(int blockSize, CompressionParams params, byte[] payload, List<byte[]> slices) throws Exception
    {
        // Oracle: the standard writer's on-disk bytes (computed with the real FS block size, before the
        // mock is active — blockSize only affects O_DIRECT padding, never the logical compressed format).
        byte[] standardBytes = standardWriterBytes(payload, params);

        File dataFile = FileUtils.createTempFile("align_direct", ".db");
        File metaFile = new File(dataFile.absolutePath() + ".metadata");
        RecordingFileChannel recording = null;
        long pointerAfterFinish = -1;
        try
        {
            MetadataCollector collector = newCollector();
            try (MockedStatic<FileUtils> mocked = mockStatic(FileUtils.class, CALLS_REAL_METHODS))
            {
                mocked.when(() -> FileUtils.getBlockSize(any())).thenReturn(blockSize);
                try (DirectCompressedSequentialWriter writer = new DirectCompressedSequentialWriter(
                     dataFile, metaFile, null, SequentialWriterOption.DEFAULT, params, collector, null))
                {
                    recording = installRecordingChannel(writer, dataFile);
                    for (byte[] slice : slices)
                        writer.write(slice);
                    writer.finish();
                    // Sample after finish(): the final partial chunk is compressed and written only during
                    // finish(), so actualDataSize isn't final until then.
                    pointerAfterFinish = writer.getOnDiskFilePointer();
                }
            }

            // The alignment assertion macOS cannot enforce: every write is a whole number of blocks at a
            // block-aligned offset.
            assertFalse("expected at least one channel write for a non-empty payload", recording.writes.isEmpty());
            for (long[] w : recording.writes)
            {
                assertEquals("channel write offset not block-aligned (block=" + blockSize + ", offset=" + w[0] + ')',
                             0, w[0] % blockSize);
                assertEquals("channel write length not a block multiple (block=" + blockSize + ", length=" + w[1] + ')',
                             0, w[1] % blockSize);
            }

            assertEquals("getOnDiskFilePointer should equal final on-disk data size", standardBytes.length, pointerAfterFinish);
            assertEquals("dataFile length should equal actualDataSize after truncate", standardBytes.length, dataFile.length());

            byte[] directBytes = Files.readAllBytes(dataFile.toPath());
            assertArrayEquals("Direct vs standard on-disk bytes differ (block=" + blockSize +
                              ", compressor=" + params.getSstableCompressor().getClass().getSimpleName() +
                              ", chunkLength=" + params.chunkLength() + ", size=" + payload.length +
                              ", writes=" + slices.size() + ')',
                              standardBytes, directBytes);

            readBackVerify(dataFile, metaFile, payload);
        }
        finally
        {
            if (recording != null)
                recording.closeQuietly();
            dataFile.tryDelete();
            metaFile.tryDelete();
        }
    }

    private static byte[] standardWriterBytes(byte[] payload, CompressionParams params) throws IOException
    {
        File file = FileUtils.createTempFile("align_standard", ".db");
        File meta = new File(file.absolutePath() + ".metadata");
        try
        {
            MetadataCollector collector = newCollector();
            try (CompressedSequentialWriter writer = new CompressedSequentialWriter(
                 file, meta, null, SequentialWriterOption.DEFAULT, params, collector))
            {
                writer.write(payload);
                writer.finish();
            }
            return Files.readAllBytes(file.toPath());
        }
        finally
        {
            file.tryDelete();
            meta.tryDelete();
        }
    }

    // Swap the writer's fchannel for a wrapper over a plain channel on the same file. A plain backing
    // channel is correct: the SUT's alignment math is OS-independent, and a plain channel won't reject the
    // small mocked-block writes a real Linux O_DIRECT channel would.
    private static void installChannel(DirectCompressedSequentialWriter writer, FileChannel wrapper) throws Exception
    {
        Field fchannel = SequentialWriter.class.getDeclaredField("fchannel");
        fchannel.setAccessible(true);
        fchannel.set(writer, wrapper);
    }

    private static RecordingFileChannel installRecordingChannel(DirectCompressedSequentialWriter writer, File dataFile) throws Exception
    {
        RecordingFileChannel recording = new RecordingFileChannel(FileChannel.open(dataFile.toPath(), StandardOpenOption.WRITE));
        installChannel(writer, recording);
        return recording;
    }

    private static FaultInjectingFileChannel installFaultChannel(DirectCompressedSequentialWriter writer, File dataFile) throws Exception
    {
        FaultInjectingFileChannel faulty = new FaultInjectingFileChannel(FileChannel.open(dataFile.toPath(), StandardOpenOption.WRITE));
        installChannel(writer, faulty);
        return faulty;
    }

    private static PartialWritingFileChannel installPartialChannel(DirectCompressedSequentialWriter writer, File dataFile, int maxBytesPerWrite) throws Exception
    {
        PartialWritingFileChannel partial = new PartialWritingFileChannel(FileChannel.open(dataFile.toPath(), StandardOpenOption.WRITE), maxBytesPerWrite);
        installChannel(writer, partial);
        return partial;
    }

    /** Delegates everything to a plain FileChannel; subclasses observe or perturb specific calls. */
    private static class DelegatingFileChannel extends FileChannel
    {
        protected final FileChannel delegate;

        DelegatingFileChannel(FileChannel delegate)
        {
            this.delegate = delegate;
        }

        void closeQuietly()
        {
            try { delegate.close(); }
            catch (IOException ignored) {}
        }

        @Override public int write(ByteBuffer src) throws IOException { return delegate.write(src); }
        @Override public int read(ByteBuffer dst) throws IOException { return delegate.read(dst); }
        @Override public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return delegate.read(dsts, offset, length); }
        // The SUT writes blocks only via the single-buffer write(ByteBuffer); a gather or positional write
        // would bypass subclass observation and pass their asserts vacuously, so trip loudly instead.
        @Override public long write(ByteBuffer[] srcs, int offset, int length) { throw new AssertionError("unexpected gather write; DelegatingFileChannel subclasses do not observe it"); }
        @Override public int write(ByteBuffer src, long position) { throw new AssertionError("unexpected positional write; DelegatingFileChannel subclasses do not observe it"); }
        @Override public long position() throws IOException { return delegate.position(); }
        @Override public FileChannel position(long newPosition) throws IOException { delegate.position(newPosition); return this; }
        @Override public long size() throws IOException { return delegate.size(); }
        @Override public FileChannel truncate(long size) throws IOException { delegate.truncate(size); return this; }
        @Override public void force(boolean metaData) throws IOException { delegate.force(metaData); }
        @Override public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return delegate.transferTo(position, count, target); }
        @Override public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { return delegate.transferFrom(src, position, count); }
        @Override public int read(ByteBuffer dst, long position) throws IOException { return delegate.read(dst, position); }
        @Override public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return delegate.map(mode, position, size); }
        @Override public FileLock lock(long position, long size, boolean shared) throws IOException { return delegate.lock(position, size, shared); }
        @Override public FileLock tryLock(long position, long size, boolean shared) throws IOException { return delegate.tryLock(position, size, shared); }
        @Override protected void implCloseChannel() throws IOException { delegate.close(); }
    }

    /** Records each {@link #write(ByteBuffer)}'s start offset and byte count, then delegates. */
    private static final class RecordingFileChannel extends DelegatingFileChannel
    {
        // Each entry: { offsetAtWriteStart, bytesWritten }.
        final List<long[]> writes = new ArrayList<>();

        RecordingFileChannel(FileChannel delegate)
        {
            super(delegate);
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            long offset = delegate.position();
            int n = delegate.write(src);
            writes.add(new long[]{ offset, n });
            return n;
        }
    }

    /** Caps each {@link #write(ByteBuffer)} to force the SUT drain loop to re-issue; delegates so bytes still land. */
    private static final class PartialWritingFileChannel extends DelegatingFileChannel
    {
        private final int maxBytesPerWrite;
        int shortWrites; // write() calls that returned fewer bytes than requested

        PartialWritingFileChannel(FileChannel delegate, int maxBytesPerWrite)
        {
            super(delegate);
            this.maxBytesPerWrite = maxBytesPerWrite;
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            int requested = src.remaining();
            if (requested <= maxBytesPerWrite)
                return delegate.write(src);

            int savedLimit = src.limit();
            src.limit(src.position() + maxBytesPerWrite);
            int n = delegate.write(src);
            src.limit(savedLimit);
            if (n < requested)
                shortWrites++;
            return n;
        }
    }

    /**
     * Perturbs specific channel calls to exercise the SUT's IOException handling:
     * fail the nth write, fail truncate, or fail the nth position() query.
     */
    private static final class FaultInjectingFileChannel extends DelegatingFileChannel
    {
        static final String INJECTED_MESSAGE = "injected fault";

        private int failOnWriteIndex = -1;    // 1-based; -1 = off
        private boolean failTruncate;
        private int failOnPositionCall = -1;  // 1-based; -1 = off

        private int writeCalls;
        private int positionCalls;
        boolean faultFired;

        FaultInjectingFileChannel(FileChannel delegate)
        {
            super(delegate);
        }

        FaultInjectingFileChannel failOnWriteIndex(int n)
        {
            failOnWriteIndex = n;
            return this;
        }

        FaultInjectingFileChannel failTruncate()
        {
            failTruncate = true;
            return this;
        }

        FaultInjectingFileChannel failOnPositionCall(int n)
        {
            failOnPositionCall = n;
            return this;
        }

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            if (++writeCalls == failOnWriteIndex)
            {
                faultFired = true;
                throw new IOException(INJECTED_MESSAGE);
            }
            return delegate.write(src);
        }

        @Override
        public FileChannel truncate(long size) throws IOException
        {
            if (failTruncate)
            {
                faultFired = true;
                throw new IOException(INJECTED_MESSAGE);
            }
            return super.truncate(size);
        }

        @Override
        public long position() throws IOException
        {
            if (++positionCalls == failOnPositionCall)
            {
                faultFired = true;
                throw new IOException(INJECTED_MESSAGE);
            }
            return super.position();
        }
    }
}
