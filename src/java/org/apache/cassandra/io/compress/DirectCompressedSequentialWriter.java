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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.sun.nio.file.ExtendedOpenOption;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.collections.LongArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksumWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.memory.MemoryUtil;

import sun.nio.ch.DirectBuffer;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * Uses O_DIRECT to bypass the OS page cache, reducing memory pressure during compaction.
 * <p>
 * O_DIRECT requires all writes to be block-aligned, so compressed chunks are accumulated in an aligned buffer.
 * Only complete blocks are flushed; at finalization, remaining data is padded, written, and the file is
 * truncated to actual size.
 */
public class DirectCompressedSequentialWriter extends CompressedSequentialWriter
{
    private static final Logger logger = LoggerFactory.getLogger(DirectCompressedSequentialWriter.class);

    // Warn once per JVM that an undersized buffer was coerced up — operator-visible without per-SSTable spam.
    private static final AtomicBoolean undersizedBufferWarned = new AtomicBoolean(false);

    private static final int CRC_LENGTH = Integer.BYTES;

    // capacity >= maxChunkWrite + CRC_LENGTH + blockSize: a chunk is staged in a single put, so it must fit
    // contiguously even after a flush carries over up to blockSize - 1 unaligned bytes.
    private ByteBuffer writeBuffer;
    private long actualDataSize = 0;

    private boolean dataFinalized = false;

    // Chunks staged in writeBuffer but not yet on disk, as flat {compressedEnd, uncompressedEnd} pairs in
    // write order: offer 2 in writeChunk, poll 2 in durableUncompressedOffset, nothing else, or pairing
    // breaks. Lets the post-flush listener report a durable offset. Offsets are never negative, so -1 is
    // a safe empty sentinel.
    private final LongArrayQueue stagedChunkBoundaries = new LongArrayQueue(-1L);
    private long durableUncompressedOffset = 0;

    private final int blockSize;

    // Bytes registered with StorageMetrics.directWriteBufferBytes at allocation; subtracted at cleanup so the
    // gauge balances even on the abort path. 0 until writeBuffer is successfully allocated.
    private int directBufferBytes = 0;

    public DirectCompressedSequentialWriter(File file,
                                            File offsetsFile,
                                            @Nullable File digestFile,
                                            SequentialWriterOption option,
                                            CompressionParams parameters,
                                            MetadataCollector sstableMetadataCollector,
                                            @Nullable CompressionDictionaryManager compressionDictionaryManager)
    {
        super(file, offsetsFile, digestFile, option, parameters, sstableMetadataCollector, compressionDictionaryManager, ExtendedOpenOption.DIRECT);

        // super() opened the O_DIRECT FileChannel and allocated parent buffers; if anything below throws
        // the caller never gets a reference to clean them up, so abort the txn proxy ourselves.
        try
        {
            this.blockSize = FileUtils.getBlockSize(file.parent());
            if (blockSize <= 0)
                throw new IllegalStateException("Unable to determine filesystem block size for Direct IO. " +
                                                "Block size: " + blockSize);

            if (!BitUtil.isPowerOfTwo(blockSize))
                throw new IllegalStateException("Filesystem block size must be a power of two for Direct IO. " +
                                                "Block size: " + blockSize);

            int configuredSize = DatabaseDescriptor.getDirectWriteBufferSize().toBytes();
            int maxChunkWrite = parameters.getSstableCompressor().initialCompressedBufferLength(parameters.chunkLength());
            int minRequiredSize = maxChunkWrite + CRC_LENGTH + blockSize;
            if (configuredSize < minRequiredSize && undersizedBufferWarned.compareAndSet(false, true))
                logger.warn("direct_write_buffer_size ({} bytes) is below the minimum required for SSTable {} " +
                            "(worst-case chunk {} + CRC 4 + blockSize {} = {} bytes); using the minimum. " +
                            "Increase direct_write_buffer_size in cassandra.yaml to silence this warning.",
                            configuredSize, file, maxChunkWrite, blockSize, minRequiredSize);
            int bufferSize = BitUtil.align(Math.max(configuredSize, minRequiredSize), blockSize);

            this.writeBuffer = BufferUtil.allocateDirectAligned(bufferSize, blockSize);
            this.directBufferBytes = bufferSize;
            StorageMetrics.directWriteBufferBytes.inc(bufferSize);
            StorageMetrics.directWriteBuffersAllocated.mark();
        }
        catch (Throwable t)
        {
            Throwable merged = t;
            try { merged = abort(t); }
            catch (Throwable t2) { t.addSuppressed(t2); }
            Throwables.maybeFail(merged);
            // Unreachable: maybeFail(non-null) always throws. Present for definite-assignment of `blockSize`.
            throw new AssertionError("Throwables.maybeFail should have thrown", merged);
        }
    }

    // Invoked from super()'s constructor before writeBuffer exists; safe because we only capture the
    // writeCrcToAlignedBuffer reference, which isn't called until the first chunk is written.
    @Override
    protected ChecksumWriter createChecksumWriter()
    {
        return new DirectChecksumWriter(this::writeCrcToAlignedBuffer);
    }

    // Parent reads fchannel.position(), which lags by the bytes staged in writeBuffer.
    // getEstimatedOnDiskBytesWritten is intentionally NOT overridden: parent returns chunkOffset,
    // which already reflects the eventual on-disk size.
    @Override
    public long getOnDiskFilePointer()
    {
        return actualDataSize;
    }

    @Override
    protected void seekToChunkStart()
    {
        // No-op: bytes staged in writeBuffer would be skipped by a seek, leaving a hole.
        // resetAndTruncate (the parent's reason for this seek) is unsupported here.
    }

    // openFinalEarly() publishes a reader right after sync(), but the inherited sync flushes only whole
    // blocks — the last chunk's sub-block tail would stay buffered and the reader would short-read.
    // Finalize first so the reader sees the complete file.
    @Override
    protected void syncInternal()
    {
        finalizeDataFile();
        syncDataOnlyInternal();
    }

    // Brings the file to its final on-disk form exactly once. openFinalEarly's sync and commit's doPrepare
    // both land here; whichever runs first does the work, the other is a no-op. Safe because the writer is
    // switched out right after openFinalEarly, so nothing writes once the file is final.
    private void finalizeDataFile()
    {
        if (dataFinalized)
            return;

        doFlush(0);
        flushFinalWithPadding();
        dataFinalized = true;
    }

    // The parent feeds the post-flush listener getLastFlushOffset() (== uncompressedSize), which counts bytes
    // staged in writeBuffer that O_DIRECT has not yet written. Report instead the uncompressed offset of the
    // last chunk whose compressed bytes are fully on disk, so a preemptive reader never short-reads past EOF.
    @Override
    public void setPostFlushListener(LongConsumer postFlush)
    {
        super.setPostFlushListener(stagedOffset -> postFlush.accept(durableUncompressedOffset()));
    }

    // fchannel.position() counts only whole blocks written, so chunks ending at or below it are durable.
    // Chunks flush in order, so draining from the head suffices and the offset is monotonic.
    private long durableUncompressedOffset()
    {
        long onDisk;
        try
        {
            onDisk = fchannel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        long compressedEnd;
        while ((compressedEnd = stagedChunkBoundaries.peekLong()) != stagedChunkBoundaries.nullValue()
               && compressedEnd <= onDisk)
        {
            stagedChunkBoundaries.pollLong();
            durableUncompressedOffset = stagedChunkBoundaries.pollLong();
        }
        return durableUncompressedOffset;
    }

    @Override
    protected void writeChunk(ByteBuffer toWrite)
    {
        Preconditions.checkState(!dataFinalized, "writeChunk after finalizeDataFile() under O_DIRECT");

        Preconditions.checkArgument(toWrite.position() == 0,
                                    "writeChunk requires a flipped buffer (position == 0), got position=%s",
                                    toWrite.position());

        int chunkLength = toWrite.remaining();

        writeToAlignedBuffer(toWrite);

        // writeToAlignedBuffer drained toWrite; rewind so appendDirect can re-read it for the CRC.
        toWrite.rewind();
        crcMetadata.appendDirect(toWrite, true);

        actualDataSize = chunkOffset + chunkLength + CRC_LENGTH;

        stagedChunkBoundaries.offerLong(actualDataSize);
        stagedChunkBoundaries.offerLong(uncompressedSize);
    }

    private void writeToAlignedBuffer(ByteBuffer data)
    {
        int dataLength = data.remaining();

        // Buffer is sized for worst-case chunk + CRC + blockSize, so a flush always frees enough room.
        if (writeBuffer.position() + dataLength > writeBuffer.capacity())
            flushCompleteBlocks();

        writeBuffer.put(data);
    }

    private void writeCrcToAlignedBuffer(int crcValue)
    {
        // After flush, leftover < blockSize, so there's always room for the CRC trailer.
        if (writeBuffer.position() + CRC_LENGTH > writeBuffer.capacity())
            flushCompleteBlocks();

        writeBuffer.putInt(crcValue);
    }

    // FileChannel.write may write fewer bytes than requested, and a partial write would short the file
    // and desync the callers' leftover/truncate bookkeeping.
    private void writeAlignedBlocks(int flushLimit) throws IOException
    {
        writeBuffer.position(0);
        writeBuffer.limit(flushLimit);
        while (writeBuffer.hasRemaining())
            fchannel.write(writeBuffer);
    }

    private void flushCompleteBlocks()
    {
        // writeAlignedBlocks rewinds position() to 0 to drain, so snapshot the cursor first.
        int logicalPos = writeBuffer.position();

        // Align down: O_DIRECT cannot write partial blocks
        int flushLimit = logicalPos & -blockSize;

        // Callers flush only on overflow, which the sizing floor (maxChunkWrite + CRC + blockSize)
        // guarantees cannot happen before a whole block is buffered.
        Preconditions.checkState(flushLimit > 0,
                                 "flush requested with no complete block buffered; writeBuffer undersized");

        try
        {
            writeAlignedBlocks(flushLimit);

            int leftover = logicalPos - flushLimit;
            if (leftover > 0)
            {
                writeBuffer.limit(logicalPos);
                writeBuffer.position(flushLimit);
                writeBuffer.compact();
            }
            else
            {
                writeBuffer.clear();
            }
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    private void flushFinalWithPadding()
    {
        int logicalPos = writeBuffer.position();

        if (logicalPos == 0)
            return;

        try
        {
            int flushLimit = BitUtil.align(logicalPos, blockSize);

            ByteBufferUtil.writeZeroes(writeBuffer, flushLimit - logicalPos);

            writeAlignedBlocks(flushLimit);

            // O_DIRECT required padding; truncate back to actual data size.
            fchannel.truncate(actualDataSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    // Gated out for SCRUB in DataComponent.buildWriter; these throws are a canary if the gate is bypassed.
    @Override
    public DataPosition mark()
    {
        throw new UnsupportedOperationException("mark() not supported under O_DIRECT");
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        throw new UnsupportedOperationException("resetAndTruncate() not supported under O_DIRECT");
    }

    @VisibleForTesting
    ByteBuffer writeBuffer()
    {
        return writeBuffer;
    }

    @VisibleForTesting
    static void resetUndersizedBufferWarned()
    {
        undersizedBufferWarned.set(false);
    }

    protected class DirectTransactionalProxy extends CompressedSequentialWriter.TransactionalProxy
    {
        @Override
        protected void doPrepare()
        {
            finalizeDataFile();
            syncDataOnlyInternal();
            writeDigestFile();
            sstableMetadataCollector.addCompressionRatio(compressedSize, uncompressedSize);
            metadataWriter.finalizeLength(current(), chunkCount).prepareToCommit();
        }

        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            if (writeBuffer != null)
            {
                try
                {
                    // allocateDirectAligned returns a slice; clean the backing buffer via the attachment.
                    MemoryUtil.clean((ByteBuffer) ((DirectBuffer) writeBuffer).attachment());
                }
                catch (Throwable t)
                {
                    accumulate = merge(accumulate, t);
                }
                StorageMetrics.directWriteBufferBytes.dec(directBufferBytes);
                writeBuffer = null;
            }

            return super.doPreCleanup(accumulate);
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new DirectTransactionalProxy();
    }

    /**
     * Routes the per-chunk CRC into the block-aligned writeBuffer instead of the channel, reusing
     * ChecksumWriter's bookkeeping rather than duplicating it. Only the CRC trailer flows through here;
     * {@link #writeChunk} stages the chunk data.
     */
    private static final class DirectChecksumWriter extends ChecksumWriter
    {
        private final IntConsumer alignedSink;

        DirectChecksumWriter(IntConsumer alignedSink)
        {
            this.alignedSink = alignedSink;
        }

        @Override
        protected void writeIncrementalInt(int value)
        {
            alignedSink.accept(value);
        }

        @Override
        public void writeChunkSize(int length)
        {
            throw new UnsupportedOperationException("writeChunkSize is unused on the compressed O_DIRECT path");
        }
    }
}
