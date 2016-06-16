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

package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.hints.ChecksummedDataInput.Position;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.utils.memory.BufferPool;

public final class CompressedChecksummedDataInput extends ChecksummedDataInput
{
    private final ICompressor compressor;
    private volatile long filePosition = 0;     // Current position in file, advanced when reading chunk.
    private volatile long sourcePosition = 0;   // Current position in file to report, advanced after consuming chunk.
    private volatile ByteBuffer compressedBuffer = null;
    private final ByteBuffer metadataBuffer = ByteBuffer.allocate(CompressedHintsWriter.METADATA_SIZE);

    public CompressedChecksummedDataInput(ChannelProxy channel, ICompressor compressor, long filePosition)
    {
        super(channel, compressor.preferredBufferType());
        this.compressor = compressor;
        this.sourcePosition = this.filePosition = filePosition;
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return filePosition == channel.size() && buffer.remaining() == 0;
    }

    public long getSourcePosition()
    {
        return sourcePosition;
    }

    static class Position extends ChecksummedDataInput.Position
    {
        final long bufferStart;
        final int bufferPosition;

        public Position(long sourcePosition, long bufferStart, int bufferPosition)
        {
            super(sourcePosition);
            this.bufferStart = bufferStart;
            this.bufferPosition = bufferPosition;
        }

        @Override
        public long subtract(InputPosition o)
        {
            Position other = (Position) o;
            return bufferStart - other.bufferStart + bufferPosition - other.bufferPosition;
        }
    }

    public InputPosition getSeekPosition()
    {
        return new Position(sourcePosition, bufferOffset, buffer.position());
    }

    public void seek(InputPosition p)
    {
        Position pos = (Position) p;
        bufferOffset = pos.bufferStart;
        filePosition = pos.sourcePosition;
        buffer.position(0).limit(0);
        resetCrc();
        reBuffer();
        buffer.position(pos.bufferPosition);
        assert sourcePosition == pos.sourcePosition;
        assert bufferOffset == pos.bufferStart;
        assert buffer.position() == pos.bufferPosition;
    }

    @Override
    protected void readBuffer()
    {
        sourcePosition = filePosition;
        if (isEOF())
            return;

        metadataBuffer.clear();
        channel.read(metadataBuffer, filePosition);
        filePosition += CompressedHintsWriter.METADATA_SIZE;
        metadataBuffer.rewind();

        int uncompressedSize = metadataBuffer.getInt();
        int compressedSize = metadataBuffer.getInt();

        if (compressedBuffer == null || compressedSize > compressedBuffer.capacity())
        {
            int bufferSize = compressedSize + (compressedSize / 20);  // allocate +5% to cover variability in compressed size
            if (compressedBuffer != null)
            {
                BufferPool.put(compressedBuffer);
            }
            compressedBuffer = BufferPool.get(bufferSize, compressor.preferredBufferType());
        }

        compressedBuffer.clear();
        compressedBuffer.limit(compressedSize);
        channel.read(compressedBuffer, filePosition);
        compressedBuffer.rewind();
        filePosition += compressedSize;

        if (buffer.capacity() < uncompressedSize)
        {
            int bufferSize = uncompressedSize + (uncompressedSize / 20);
            BufferPool.put(buffer);
            buffer = BufferPool.get(bufferSize, compressor.preferredBufferType());
        }

        buffer.clear();
        buffer.limit(uncompressedSize);
        try
        {
            compressor.uncompress(compressedBuffer, buffer);
            buffer.flip();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, getPath());
        }
    }

    @Override
    public void close()
    {
        BufferPool.put(compressedBuffer);
        super.close();
    }

    @SuppressWarnings("resource") // Closing the ChecksummedDataInput will close the underlying channel.
    public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, ICompressor compressor)
    {
        long position = input.getPosition();
        input.close();

        return new CompressedChecksummedDataInput(new ChannelProxy(input.getPath()), compressor, position);
    }

    @VisibleForTesting
    ICompressor getCompressor()
    {
        return compressor;
    }
}
