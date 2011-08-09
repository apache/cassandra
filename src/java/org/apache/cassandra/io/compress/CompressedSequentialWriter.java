/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.io.compress;

import java.io.File;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.SequentialWriter;

import org.xerial.snappy.Snappy;

public class CompressedSequentialWriter extends SequentialWriter
{
    public static final int CHUNK_LENGTH = 65536;

    public static SequentialWriter open(String dataFilePath, String indexFilePath, boolean skipIOCache) throws IOException
    {
        return new CompressedSequentialWriter(new File(dataFilePath), indexFilePath, skipIOCache);
    }

    // holds offset in the file where current chunk should be written
    // changed only by flush() method where data buffer gets compressed and stored to the file
    private long chunkOffset = 0;

    // index file writer (random I/O)
    private final CompressionMetadata.Writer metadataWriter;

    // used to store compressed data
    private final byte[] compressed;

    // holds a number of already written chunks
    private int chunkCount = 0;

    private final Checksum checksum = new CRC32();

    public CompressedSequentialWriter(File file, String indexFilePath, boolean skipIOCache) throws IOException
    {
        super(file, CHUNK_LENGTH, skipIOCache);

        // buffer for compression should be the same size as buffer itself
        compressed = new byte[Snappy.maxCompressedLength(buffer.length)];

        /* Index File (-CompressionInfo.db component) and it's header */
        metadataWriter = new CompressionMetadata.Writer(indexFilePath);
        metadataWriter.writeHeader(Snappy.class.getSimpleName(), CHUNK_LENGTH);
    }

    @Override
    public void sync() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void flushData() throws IOException
    {
        seekToChunkStart();

        // compressing data with buffer re-use
        int compressedLength = Snappy.rawCompress(buffer, 0, validBufferBytes, compressed, 0);

        // update checksum
        checksum.update(buffer, 0, validBufferBytes);

        // write an offset of the newly written chunk to the index file
        metadataWriter.writeLong(chunkOffset);
        chunkCount++;

        assert compressedLength <= compressed.length;

        // write data itself
        out.write(compressed, 0, compressedLength);
        // write corresponding checksum
        out.writeInt((int) checksum.getValue());

        // reset checksum object to the blank state for re-use
        checksum.reset();

        // next chunk should be written right after current + length of the checksum (int)
        chunkOffset += compressedLength + 4;
    }

    @Override
    public FileMark mark()
    {
        return new CompressedFileWriterMark(chunkOffset, current, validBufferBytes, chunkCount + 1);
    }

    @Override
    public synchronized void resetAndTruncate(FileMark mark) throws IOException
    {
        assert mark instanceof CompressedFileWriterMark;

        CompressedFileWriterMark realMark = ((CompressedFileWriterMark) mark);

        // reset position
        current = realMark.uncDataOffset;

        if (realMark.chunkOffset == chunkOffset) // current buffer
        {
            // just reset a buffer offset and return
            validBufferBytes = realMark.bufferOffset;
            return;
        }

        // synchronize current buffer with disk
        // because we don't want any data loss
        syncInternal();

        // setting marker as a current offset
        chunkOffset = realMark.chunkOffset;

        // compressed chunk size (- 4 bytes reserved for checksum)
        int chunkSize = (int) (metadataWriter.chunkOffsetBy(realMark.nextChunkIndex) - chunkOffset - 4);

        out.seek(chunkOffset);
        out.read(compressed, 0, chunkSize);

        // decompress data chunk and store its length
        int validBytes = Snappy.rawUncompress(compressed, 0, chunkSize, buffer, 0);

        checksum.update(buffer, 0, validBytes);

        if (out.readInt() != (int) checksum.getValue())
            throw new CorruptedBlockException(getPath(), chunkOffset, chunkSize);

        checksum.reset();

        // reset buffer
        validBufferBytes = realMark.bufferOffset;
        bufferOffset = current - validBufferBytes;
        chunkCount = realMark.nextChunkIndex - 1;

        // truncate data and index file
        truncate(chunkOffset);
        metadataWriter.resetAndTruncate(realMark.nextChunkIndex);
    }

    /**
     * Seek to the offset where next compressed data chunk should be stored.
     *
     * @throws IOException on any I/O error.
     */
    private void seekToChunkStart() throws IOException
    {
        if (out.getFilePointer() != chunkOffset)
            out.seek(chunkOffset);
    }

    @Override
    public void close() throws IOException
    {
        if (buffer == null)
            return; // already closed

        super.close();

        metadataWriter.finalizeHeader(current, chunkCount);
        metadataWriter.close();
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class CompressedFileWriterMark implements FileMark
    {
        // chunk offset in the compressed file
        long chunkOffset;
        // uncompressed data offset (real data offset)
        long uncDataOffset;

        int bufferOffset;
        int nextChunkIndex;

        public CompressedFileWriterMark(long chunkOffset, long uncDataOffset, int bufferOffset, int nextChunkIndex)
        {
            this.chunkOffset = chunkOffset;
            this.uncDataOffset = uncDataOffset;
            this.bufferOffset = bufferOffset;
            this.nextChunkIndex = nextChunkIndex;
        }
    }
}
