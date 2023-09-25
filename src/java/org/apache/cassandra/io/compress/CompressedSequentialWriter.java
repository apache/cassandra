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

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Optional;
import java.util.zip.CRC32;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.ChecksumWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Throwables.merge;

public class CompressedSequentialWriter extends SequentialWriter
{
    private final ChecksumWriter crcMetadata;

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    private long chunkOffset = 0;

    // index file writer (random I/O)
    private final CompressionMetadata.Writer metadataWriter;
    private final ICompressor compressor;

    // used to store compressed data
    private ByteBuffer compressed;

    // holds a number of already written chunks
    private int chunkCount = 0;

    private long uncompressedSize = 0, compressedSize = 0;

    private final MetadataCollector sstableMetadataCollector;

    private final ByteBuffer crcCheckBuffer = ByteBuffer.allocate(4);
    private final Optional<File> digestFile;

    private final int maxCompressedLength;

    /**
     * Create CompressedSequentialWriter without digest file.
     *
     * @param file File to write
     * @param offsetsFile File to write compression metadata
     * @param digestFile File to write digest
     * @param option Write option (buffer size and type will be set the same as compression params)
     * @param parameters Compression mparameters
     * @param sstableMetadataCollector Metadata collector
     */
    public CompressedSequentialWriter(File file,
                                      File offsetsFile,
                                      File digestFile,
                                      SequentialWriterOption option,
                                      CompressionParams parameters,
                                      MetadataCollector sstableMetadataCollector)
    {
        super(file, SequentialWriterOption.newBuilder()
                            .bufferSize(option.bufferSize())
                            .bufferType(option.bufferType())
                            .bufferSize(parameters.chunkLength())
                            .bufferType(parameters.getSstableCompressor().preferredBufferType())
                            .finishOnClose(option.finishOnClose())
                            .build());
        this.compressor = parameters.getSstableCompressor();
        this.digestFile = Optional.ofNullable(digestFile);

        // buffer for compression should be the same size as buffer itself
        compressed = compressor.preferredBufferType().allocate(compressor.initialCompressedBufferLength(buffer.capacity()));

        maxCompressedLength = parameters.maxCompressedLength();

        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = CompressionMetadata.Writer.open(parameters, offsetsFile);

        this.sstableMetadataCollector = sstableMetadataCollector;
        crcMetadata = new ChecksumWriter(new DataOutputStream(Channels.newOutputStream(channel)));
    }

    @Override
    public long getOnDiskFilePointer()
    {
        try
        {
            return fchannel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    /**
     * Get a quick estimation on how many bytes have been written to disk
     *
     * It should for the most part be exactly the same as getOnDiskFilePointer()
     */
    @Override
    public long getEstimatedOnDiskBytesWritten()
    {
        return chunkOffset;
    }

    @Override
    public void flush()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData()
    {
        seekToChunkStart(); // why is this necessary? seems like it should always be at chunk start in normal operation

        try
        {
            // compressing data with buffer re-use
            buffer.flip();
            compressed.clear();
            compressor.compress(buffer, compressed);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        int uncompressedLength = buffer.position();
        int compressedLength = compressed.position();
        uncompressedSize += uncompressedLength;
        ByteBuffer toWrite = compressed;
        if (compressedLength >= maxCompressedLength)
        {
            toWrite = buffer;
            if (uncompressedLength >= maxCompressedLength)
            {
                compressedLength = uncompressedLength;
            }
            else
            {
                // Pad the uncompressed data so that it reaches the max compressed length.
                // This could make the chunk appear longer, but this path is only reached at the end of the file, where
                // we use the file size to limit the buffer on reading.
                assert maxCompressedLength <= buffer.capacity();   // verified by CompressionParams.validate
                buffer.limit(maxCompressedLength);
                ByteBufferUtil.writeZeroes(buffer, maxCompressedLength - uncompressedLength);
                compressedLength = maxCompressedLength;
            }
        }
        compressedSize += compressedLength;

        try
        {
            // write an offset of the newly written chunk to the index file
            metadataWriter.addOffset(chunkOffset);
            chunkCount++;

            // write out the compressed data
            toWrite.flip();
            channel.write(toWrite);

            // write corresponding checksum
            toWrite.rewind();
            crcMetadata.appendDirect(toWrite, true);
            lastFlushOffset = uncompressedSize;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
        if (toWrite == buffer)
            buffer.position(uncompressedLength);

        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += compressedLength + 4;
        if (runPostFlush != null)
            runPostFlush.accept(getLastFlushOffset());
    }

    public CompressionMetadata open(long overrideLength)
    {
        if (overrideLength <= 0)
            overrideLength = uncompressedSize;
        return metadataWriter.open(overrideLength, chunkOffset);
    }

    @Override
    public DataPosition mark()
    {
        if (!buffer.hasRemaining())
            doFlush(0);
        return new CompressedFileWriterMark(chunkOffset, current(), buffer.position(), chunkCount + 1);
    }

    @Override
    public synchronized void resetAndTruncate(DataPosition mark)
    {
        assert mark instanceof CompressedFileWriterMark;

        CompressedFileWriterMark realMark = (CompressedFileWriterMark) mark;

        // reset position
        long truncateTarget = realMark.uncDataOffset;

        if (realMark.chunkOffset == chunkOffset)
        {
            // simply drop bytes to the right of our mark
            buffer.position(realMark.validBufferBytes);
            return;
        }

        // synchronize current buffer with disk - we don't want any data loss
        syncInternal();

        chunkOffset = realMark.chunkOffset;

        // compressed chunk size (- 4 bytes reserved for checksum)
        int chunkSize = (int) (metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - chunkOffset - 4);
        if (compressed.capacity() < chunkSize)
        {
            FileUtils.clean(compressed);
            compressed = compressor.preferredBufferType().allocate(chunkSize);
        }

        try
        {
            compressed.clear();
            compressed.limit(chunkSize);
            fchannel.position(chunkOffset);
            fchannel.read(compressed);

            try
            {
                // Repopulate buffer from compressed data
                buffer.clear();
                compressed.flip();
                if (chunkSize < maxCompressedLength)
                    compressor.uncompress(compressed, buffer);
                else
                    buffer.put(compressed);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize, e);
            }

            CRC32 checksum = new CRC32();
            compressed.rewind();
            checksum.update(compressed);

            crcCheckBuffer.clear();
            fchannel.read(crcCheckBuffer);
            crcCheckBuffer.flip();
            if (crcCheckBuffer.getInt() != (int) checksum.getValue())
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize);
        }
        catch (CorruptBlockException e)
        {
            throw new CorruptSSTableException(e, getPath());
        }
        catch (EOFException e)
        {
            throw new CorruptSSTableException(new CorruptBlockException(getPath(), chunkOffset, chunkSize), getPath());
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }

        // Mark as dirty so we can guarantee the newly buffered bytes won't be lost on a rebuffer
        buffer.position(realMark.validBufferBytes);

        bufferOffset = truncateTarget - buffer.position();
        chunkCount = realMark.nextChunkIndex - 1;

        // truncate data and index file
        truncate(chunkOffset, bufferOffset);
        metadataWriter.resetAndTruncate(realMark.nextChunkIndex - 1);
    }

    private void truncate(long toFileSize, long toBufferOffset)
    {
        try
        {
            fchannel.truncate(toFileSize);
            lastFlushOffset = toBufferOffset;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /**
     * Seek to the offset where next compressed data chunk should be stored.
     */
    private void seekToChunkStart()
    {
        if (getOnDiskFilePointer() != chunkOffset)
        {
            try
            {
                fchannel.position(chunkOffset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, getPath());
            }
        }
    }

    // Page management using chunk boundaries

    @Override
    public int maxBytesInPage()
    {
        return buffer.capacity();
    }

    @Override
    public void padToPageBoundary()
    {
        if (buffer.position() == 0)
            return;

        int padLength = bytesLeftInPage();

        // Flush as much as we have
        doFlush(0);
        // But pretend we had a whole chunk
        bufferOffset += padLength;
        lastFlushOffset += padLength;
    }

    @Override
    public int bytesLeftInPage()
    {
        return buffer.remaining();
    }

    @Override
    public long paddedPosition()
    {
        return position() + (buffer.position() == 0 ? 0 : buffer.remaining());
    }

    protected class TransactionalProxy extends SequentialWriter.TransactionalProxy
    {
        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            return super.doCommit(metadataWriter.commit(accumulate));
        }

        @Override
        protected Throwable doAbort(Throwable accumulate)
        {
            return super.doAbort(metadataWriter.abort(accumulate));
        }

        @Override
        protected void doPrepare()
        {
            syncInternal();
            digestFile.ifPresent(crcMetadata::writeFullChecksum);
            sstableMetadataCollector.addCompressionRatio(compressedSize, uncompressedSize);
            metadataWriter.finalizeLength(current(), chunkCount).prepareToCommit();
        }

        @Override
        protected Throwable doPreCleanup(Throwable accumulate)
        {
            accumulate = super.doPreCleanup(accumulate);
            if (compressed != null)
            {
                try { FileUtils.clean(compressed); }
                catch (Throwable t) { accumulate = merge(accumulate, t); }
                compressed = null;
            }

            return accumulate;
        }
    }

    @Override
    protected SequentialWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements DataPosition
    {
        // chunk offset in the compressed file
        final long chunkOffset;
        // uncompressed data offset (real data offset)
        final long uncDataOffset;

        final int validBufferBytes;
        final int nextChunkIndex;

        public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int validBufferBytes, int nextChunkIndex)
        {
            this.chunkOffset = chunkOffset;
            this.uncDataOffset = uncDataOffset;
            this.validBufferBytes = validBufferBytes;
            this.nextChunkIndex = nextChunkIndex;
        }
    }
}