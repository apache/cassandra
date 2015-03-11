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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.io.compress.CompressionMetadata.Writer.OpenType.FINAL;
import static org.apache.cassandra.io.compress.CompressionMetadata.Writer.OpenType.SHARED;
import static org.apache.cassandra.io.compress.CompressionMetadata.Writer.OpenType.SHARED_FINAL;

public class CompressedSequentialWriter extends SequentialWriter
{
    private final DataIntegrityMetadata.ChecksumWriter crcMetadata;

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    private long chunkOffset = 0;

    // index file writer (random I/O)
    private final CompressionMetadata.Writer metadataWriter;
    private final ICompressor compressor;

    // used to store compressed data
    private final ICompressor.WrappedByteBuffer compressed;

    // holds a number of already written chunks
    private int chunkCount = 0;

    private long uncompressedSize = 0, compressedSize = 0;

    private final MetadataCollector sstableMetadataCollector;

    private final ByteBuffer crcCheckBuffer = ByteBuffer.allocate(4);

    public CompressedSequentialWriter(File file,
                                      String offsetsPath,
                                      CompressionParameters parameters,
                                      MetadataCollector sstableMetadataCollector)
    {
        super(file, parameters.chunkLength(), parameters.sstableCompressor.useDirectOutputByteBuffers());
        this.compressor = parameters.sstableCompressor;

        // buffer for compression should be the same size as buffer itself
        compressed = compressor.useDirectOutputByteBuffers()
            ? new ICompressor.WrappedByteBuffer(ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(buffer.capacity())))
            : new ICompressor.WrappedByteBuffer(ByteBuffer.allocate(compressor.initialCompressedBufferLength(buffer.capacity())));

        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = CompressionMetadata.Writer.open(parameters, offsetsPath);

        this.sstableMetadataCollector = sstableMetadataCollector;
        crcMetadata = new DataIntegrityMetadata.ChecksumWriter(new DataOutputStreamAndChannel(channel));
    }

    @Override
    public long getOnDiskFilePointer()
    {
        try
        {
            return channel.position();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    @Override
    public void sync()
    {
        throw new UnsupportedOperationException();
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

        int compressedLength;
        try
        {
            // compressing data with buffer re-use
            buffer.flip();
            compressed.buffer.clear();
            compressedLength = compressor.compress(buffer, compressed);

            // Compressors don't modify sentinels in our BB - we rely on buffer.position() for bufferOffset adjustment
            buffer.position(buffer.limit());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Compression exception", e); // shouldn't happen
        }

        uncompressedSize += buffer.position();
        compressedSize += compressedLength;

        try
        {
            // write an offset of the newly written chunk to the index file
            metadataWriter.addOffset(chunkOffset);
            chunkCount++;

            assert compressedLength <= compressed.buffer.capacity();

            // write out the compressed data
            compressed.buffer.flip();
            channel.write(compressed.buffer);

            // write corresponding checksum
            compressed.buffer.rewind();
            crcMetadata.appendDirect(compressed.buffer);
            lastFlushOffset += compressedLength + 4;

            // adjust our bufferOffset to account for the new uncompressed data we've now written out
            resetBuffer();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }

        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += compressedLength + 4;
        if (runPostFlush != null)
            runPostFlush.run();
    }

    public CompressionMetadata open(long overrideLength, boolean isFinal)
    {
        if (overrideLength <= 0)
            return metadataWriter.open(uncompressedSize, chunkOffset, isFinal ? FINAL : SHARED_FINAL);
        // we are early opening the file, make sure we open metadata with the correct size
        assert !isFinal;
        return metadataWriter.open(overrideLength, chunkOffset, SHARED);
    }

    @Override
    public FileMark mark()
    {
        return new CompressedFileWriterMark(chunkOffset, current(), buffer.position(), chunkCount + 1);
    }

    @Override
    public synchronized void resetAndTruncate(FileMark mark)
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
        if (compressed.buffer.capacity() < chunkSize)
            compressed.buffer = compressor.useDirectOutputByteBuffers()
                    ? ByteBuffer.allocateDirect(chunkSize)
                    : ByteBuffer.allocate(chunkSize);

        try
        {
            compressed.buffer.clear();
            compressed.buffer.limit(chunkSize);
            channel.position(chunkOffset);
            channel.read(compressed.buffer);

            try
            {
                // Repopulate buffer from compressed data
                buffer.clear();
                compressed.buffer.flip();
                compressor.uncompress(compressed.buffer, buffer);
            }
            catch (IOException e)
            {
                throw new CorruptBlockException(getPath(), chunkOffset, chunkSize);
            }

            Adler32 checksum = new Adler32();

            FBUtilities.directCheckSum(checksum, compressed.buffer);

            crcCheckBuffer.clear();
            channel.read(crcCheckBuffer);
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
        isDirty = true;

        bufferOffset = truncateTarget - buffer.position();
        chunkCount = realMark.nextChunkIndex - 1;

        // truncate data and index file
        truncate(chunkOffset);
        metadataWriter.resetAndTruncate(realMark.nextChunkIndex - 1);
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
                channel.position(chunkOffset);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, getPath());
            }
        }
    }

    @Override
    public void close()
    {
        if (buffer == null)
            return;

        long finalPosition = current();

        super.close();
        sstableMetadataCollector.addCompressionRatio(compressedSize, uncompressedSize);
        try
        {
            metadataWriter.close(finalPosition, chunkCount);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    public void abort()
    {
        super.abort();
        metadataWriter.abort();
    }

    @Override
    public void writeFullChecksum(Descriptor descriptor)
    {
        crcMetadata.writeFullChecksum(descriptor);
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements FileMark
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
