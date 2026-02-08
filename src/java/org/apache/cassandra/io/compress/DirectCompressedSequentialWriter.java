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
 * Uses O_DIRECT to bypass the OS page cache, reducing memory pressure during compaction.
 * <p>
 * O_DIRECT requires all writes to be block-aligned, so compressed chunks are accumulated in an aligned buffer.
 * Only complete blocks are flushed; at close, remaining data is padded, written, and the file is truncated to
 * actual size.
 */
public class DirectCompressedSequentialWriter extends CompressedSequentialWriter
{

    private ByteBuffer writeBuffer;
    private int writeBufferPosition = 0;
    private long actualDataSize = 0;

    private final int blockSize;
    // ChecksumWriter writes CRCs directly to the channel, bypassing our aligned buffer. Track checksums ourselves.
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
        // Not needed: writes go to the aligned buffer, not directly to the channel
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

            // O_DIRECT required padding; truncate back to actual data size
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

            return super.doPreCleanup(accumulate);
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new DirectTransactionalProxy();
    }
}
