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
import java.util.zip.CRC32;

import javax.annotation.Nullable;

import com.sun.nio.file.ExtendedOpenOption;

import org.agrona.BitUtil;
import org.agrona.BufferUtil;

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
import org.apache.cassandra.utils.memory.MemoryUtil;

import sun.nio.ch.DirectBuffer;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * A CompressedSequentialWriter that uses Direct IO (O_DIRECT) for writes.
 * <p>
 * This writer bypasses the OS page cache by using O_DIRECT, which can improve
 * I/O predictability and reduce memory pressure during compaction.
 * <p>
 * Direct IO requires all writes to be aligned to the filesystem block size.
 * This writer accumulates compressed chunks in an aligned buffer and flushes
 * only complete aligned blocks to disk. At file close, any remaining data is
 * padded, written, and the file is truncated to the actual data size.
 * <p>
 * This flushes only complete blocks keeping leftover data in the buffer until the next flush or final close.
 */
public class DirectCompressedSequentialWriter extends CompressedSequentialWriter
{

    private final int blockSize;

    // Aligned write buffer that accumulates compressed chunks + checksums
    private ByteBuffer writeBuffer;

    private int writeBufferPosition = 0;

    // Track actual data written (before any padding) for final truncation
    private long actualDataSize = 0;

    // Full file checksum for digest file (since we manage our own I/O)
    private final CRC32 fullFileChecksum = new CRC32();
    private final CRC32 chunkChecksum = new CRC32();
    private final ByteBuffer crcBuffer = ByteBuffer.allocate(4);

    public DirectCompressedSequentialWriter(File file,
                                            File offsetsFile,
                                            @Nullable File digestFile,
                                            SequentialWriterOption option,
                                            CompressionParams parameters,
                                            MetadataCollector sstableMetadataCollector,
                                            @Nullable CompressionDictionaryManager compressionDictionaryManager)
    {
        super(file, offsetsFile, digestFile, option, parameters, sstableMetadataCollector, compressionDictionaryManager, ExtendedOpenOption.DIRECT);

        this.blockSize = FileUtils.getBlockSize(file.parent());
        if (blockSize <= 0)
            throw new IllegalStateException("Unable to determine filesystem block size for Direct IO. " +
                                            "Block size: " + blockSize);

        // Buffer must hold at least one chunk + CRC (4 bytes) + blockSize for leftover after partial flush
        // Use configured size or minimum required, whichever is larger.
        int configuredSize = DatabaseDescriptor.getDirectWriteBufferSize().toBytes();
        int minRequiredSize = parameters.chunkLength() + 4 + blockSize;
        int bufferSize = BitUtil.align(Math.max(configuredSize, minRequiredSize), blockSize);

        this.writeBuffer = BufferUtil.allocateDirectAligned(bufferSize, blockSize);
    }

    @Override
    public long getOnDiskFilePointer()
    {
        return actualDataSize;
    }

    @Override
    protected void seekToChunkStart()
    {
        // No-op: Direct IO writes sequentially to the aligned buffer
    }

    /**
     * Write a compressed chunk to the aligned buffer instead of directly to the channel.
     * This is the key override that enables Direct IO - we accumulate chunks in the aligned buffer and flush only
     * when we have complete aligned blocks.
     */
    @Override
    protected void writeChunk(ByteBuffer toWrite)
    {
        int chunkLength = toWrite.remaining();

        // Calculate CRC
        toWrite.mark();
        chunkChecksum.reset();
        chunkChecksum.update(toWrite);
        int crcValue = (int) chunkChecksum.getValue();
        toWrite.reset();

        // Write data to aligned buffer
        writeToAlignedBuffer(toWrite);

        // Write CRC (4 bytes) to aligned buffer
        writeCrcToAlignedBuffer(crcValue);

        // Update full file checksum for digest
        toWrite.rewind();
        updateFullChecksum(toWrite, crcValue);

        // Track actual data size for final truncation
        actualDataSize = chunkOffset + chunkLength + 4;
    }

    private void writeToAlignedBuffer(ByteBuffer data)
    {
        int dataLength = data.remaining();

        // Buffer is sized to chunkLength + CRC + blockSize, so after flush there's always room
        if (writeBufferPosition + dataLength > writeBuffer.capacity())
            flushCompleteBlocks();

        writeBuffer.position(writeBufferPosition);
        writeBuffer.put(data);
        writeBufferPosition = writeBuffer.position();
    }

    private void writeCrcToAlignedBuffer(int crcValue)
    {
        // After flush, leftover is < blockSize, so there's always room for the 4-byte CRC
        if (writeBufferPosition + 4 > writeBuffer.capacity())
            flushCompleteBlocks();

        writeBuffer.position(writeBufferPosition);
        writeBuffer.putInt(crcValue);
        writeBufferPosition = writeBuffer.position();
    }

    private void flushCompleteBlocks()
    {
        // Align DOWN to get only complete blocks
        int flushLimit = writeBufferPosition & -blockSize;

        if (flushLimit == 0)
            return;

        try
        {
            writeBuffer.position(0);
            writeBuffer.limit(flushLimit);
            fchannel.write(writeBuffer);

            // Compact: move leftover to buffer start
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

    /**
     * Flush all remaining data with padding, then truncate to actual size. Called at file close.
     */
    private void flushFinalWithPadding()
    {
        if (writeBufferPosition == 0)
            return;

        try
        {
            // Align UP - pad to block boundary
            int flushLimit = BitUtil.align(writeBufferPosition, blockSize);

            // Zero the padding region
            writeBuffer.position(writeBufferPosition);
            ByteBufferUtil.writeZeroes(writeBuffer, flushLimit - writeBufferPosition);

            // Write with padding
            writeBuffer.position(0);
            writeBuffer.limit(flushLimit);
            fchannel.write(writeBuffer);

            // Truncate to actual size (remove padding)
            fchannel.truncate(actualDataSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    private void updateFullChecksum(ByteBuffer data, int crcValue)
    {
        data.mark();
        fullFileChecksum.update(data);
        data.reset();

        // Include CRC in full checksum (matches ChecksumWriter.appendDirect with checksumIncrementalResult=true)
        crcBuffer.clear();
        crcBuffer.putInt(crcValue);
        crcBuffer.flip();
        fullFileChecksum.update(crcBuffer);
    }

    /**
     * Write our own digest file since we manage the checksum ourselves.
     */
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

    @Override
    public DataPosition mark()
    {
        throw new UnsupportedOperationException(
            "mark() is not supported with Direct IO. The aligned write buffer may contain " +
            "unflushed data, making chunkOffset stale relative to actual channel position.");
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        throw new UnsupportedOperationException(
            "resetAndTruncate() is not supported with Direct IO. O_DIRECT requires " +
            "block-aligned read buffers and the aligned write buffer may contain data " +
            "not yet flushed to disk.");
    }

    protected class DirectTransactionalProxy extends CompressedSequentialWriter.TransactionalProxy
    {
        @Override
        protected void doPrepare()
        {
            syncInternal();
            flushFinalWithPadding();
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
                    // Agrona's allocateDirectAligned returns a slice; clean the backing buffer (attachment)
                    DirectBuffer db = (DirectBuffer) writeBuffer;
                    ByteBuffer attachment = (ByteBuffer) db.attachment();
                    MemoryUtil.clean(attachment != null ? attachment : writeBuffer);
                }
                catch (Throwable t)
                {
                    accumulate = merge(accumulate, t);
                }
                writeBuffer = null;
            }

            // Parent handles: channel close, buffer cleanup, compressed buffer cleanup
            return super.doPreCleanup(accumulate);
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new DirectTransactionalProxy();
    }
}
