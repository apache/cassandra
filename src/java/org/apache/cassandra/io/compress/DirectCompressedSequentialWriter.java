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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import javax.annotation.Nullable;

import com.sun.nio.file.ExtendedOpenOption;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compression.CompressionDictionaryManager;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
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
 * Only complete blocks are flushed; at close, remaining data is padded, written, and the file is truncated to
 * actual size.
 */
public class DirectCompressedSequentialWriter extends CompressedSequentialWriter
{
    private static final Logger logger = LoggerFactory.getLogger(DirectCompressedSequentialWriter.class);

    // Fires the "configured buffer below minimum required, was coerced" warning at most once
    // per JVM so a misconfiguration is operator-visible without per-SSTable spam.
    private static final AtomicBoolean undersizedBufferWarned = new AtomicBoolean(false);

    // Per-chunk CRC32 trailer width in bytes (CRC32.getValue() stored via putInt).
    private static final int CRC_LENGTH = Integer.BYTES;

    // Sized to hold at least one full chunk + CRC + post-flush leftover:
    //   capacity >= maxChunkWrite + CRC_LENGTH + blockSize
    // writeToAlignedBuffer puts a chunk in a single ByteBuffer.put, so the chunk must
    // fit contiguously. flushCompleteBlocks aligns down to blockSize, leaving up to
    // (blockSize - 1) bytes carried over via compact(); the floor accounts for that.
    private ByteBuffer writeBuffer;
    private int writeBufferPosition = 0;
    private long actualDataSize = 0;

    private final int blockSize;
    // ChecksumWriter writes CRCs directly to the channel, bypassing writeBuffer; track checksums ourselves.
    private final CRC32 fullFileChecksum = new CRC32();
    private final CRC32 chunkChecksum = new CRC32();
    private final ByteBuffer crcBuffer = ByteBuffer.allocate(CRC_LENGTH);

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

            if ((blockSize & (blockSize - 1)) != 0)
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

    // Parent reads fchannel.position(), which lags by writeBuffer contents under O_DIRECT.
    // getEstimatedOnDiskBytesWritten is intentionally NOT overridden: parent returns chunkOffset,
    // which already represents the eventual on-disk size — correct under DIO.
    @Override
    public long getOnDiskFilePointer()
    {
        return actualDataSize;
    }

    @Override
    protected void seekToChunkStart()
    {
        // No-op: bytes staged in writeBuffer would be skipped by a seek, leaving a hole.
        // resetAndTruncate (the parent's reason for this seek) is unsupported under DIO.
    }

    @Override
    protected void writeChunk(ByteBuffer toWrite)
    {
        int chunkLength = toWrite.remaining();

        toWrite.mark();
        chunkChecksum.reset();
        chunkChecksum.update(toWrite);
        int crcValue = (int) chunkChecksum.getValue();
        toWrite.reset();

        writeToAlignedBuffer(toWrite);
        writeCrcToAlignedBuffer(crcValue);

        toWrite.rewind();
        updateFullChecksum(toWrite, crcValue);

        actualDataSize = chunkOffset + chunkLength + CRC_LENGTH;
    }

    private void writeToAlignedBuffer(ByteBuffer data)
    {
        int dataLength = data.remaining();

        // Buffer is sized for worst-case chunk + CRC + blockSize, so a flush always frees enough room.
        if (writeBufferPosition + dataLength > writeBuffer.capacity())
            flushCompleteBlocks();

        writeBuffer.position(writeBufferPosition);
        writeBuffer.put(data);
        writeBufferPosition = writeBuffer.position();
    }

    private void writeCrcToAlignedBuffer(int crcValue)
    {
        // After flush, leftover < blockSize, so there's always room for the CRC trailer.
        if (writeBufferPosition + CRC_LENGTH > writeBuffer.capacity())
            flushCompleteBlocks();

        writeBuffer.position(writeBufferPosition);
        writeBuffer.putInt(crcValue);
        writeBufferPosition = writeBuffer.position();
    }

    private void flushCompleteBlocks()
    {
        // Align down: O_DIRECT cannot write partial blocks
        int flushLimit = writeBufferPosition & -blockSize;

        if (flushLimit == 0)
            return;

        try
        {
            writeBuffer.position(0);
            writeBuffer.limit(flushLimit);
            fchannel.write(writeBuffer);

            int leftover = writeBufferPosition - flushLimit;
            if (leftover > 0)
            {
                writeBuffer.limit(writeBufferPosition);
                writeBuffer.position(flushLimit);
                writeBuffer.compact();
            }
            else
            {
                writeBuffer.clear();
            }
            writeBufferPosition = leftover;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    private void flushFinalWithPadding()
    {
        if (writeBufferPosition == 0)
            return;

        try
        {
            int flushLimit = BitUtil.align(writeBufferPosition, blockSize);

            writeBuffer.position(writeBufferPosition);
            ByteBufferUtil.writeZeroes(writeBuffer, flushLimit - writeBufferPosition);

            writeBuffer.position(0);
            writeBuffer.limit(flushLimit);
            fchannel.write(writeBuffer);

            // O_DIRECT required padding; truncate back to actual data size.
            fchannel.truncate(actualDataSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    private void updateFullChecksum(ByteBuffer data, int crcValue)
    {
        fullFileChecksum.update(data);

        // Include CRC bytes in the full-file checksum to match ChecksumWriter.appendDirect(..., true).
        crcBuffer.clear();
        crcBuffer.putInt(crcValue);
        crcBuffer.flip();
        fullFileChecksum.update(crcBuffer);
    }

    @Override
    protected void writeDigestFile()
    {
        digestFile.ifPresent(file -> {
            try (FileOutputStreamPlus fos = new FileOutputStreamPlus(file))
            {
                fos.write(String.valueOf(fullFileChecksum.getValue()).getBytes(StandardCharsets.UTF_8));
                fos.flush();
                fos.getChannel().force(true);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, file);
            }
        });
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

    protected class DirectTransactionalProxy extends CompressedSequentialWriter.TransactionalProxy
    {
        @Override
        protected void doPrepare()
        {
            doFlush(0);
            // doFlush leaves a partial-block tail in writeBuffer; pad to a block, write, then truncate.
            flushFinalWithPadding();
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
}
