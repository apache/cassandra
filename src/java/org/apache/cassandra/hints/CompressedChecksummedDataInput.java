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

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.utils.memory.BufferPool;

public final class CompressedChecksummedDataInput extends ChecksummedDataInput
{
    private final ICompressor compressor;
    private volatile long filePosition = 0;
    private volatile ByteBuffer compressedBuffer = null;
    private final ByteBuffer metadataBuffer = ByteBuffer.allocate(CompressedHintsWriter.METADATA_SIZE);

    public CompressedChecksummedDataInput(CompressedChecksummedDataInputBuilder builder)
    {
        super(builder);
        assert regions == null;  //mmapped regions are not supported

        compressor = builder.compressor;
        filePosition = builder.position;
    }

    /**
     * Since an entire block of compressed data is read off of disk, not just a hint at a time,
     * we don't report EOF until the decompressed data has also been read completely
     */
    public boolean isEOF()
    {
        return filePosition == channel.size() && buffer.remaining() == 0;
    }

    protected void reBufferStandard()
    {
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
            compressedBuffer = allocateBuffer(bufferSize, compressor.preferredBufferType());
        }

        compressedBuffer.clear();
        compressedBuffer.limit(compressedSize);
        channel.read(compressedBuffer, filePosition);
        compressedBuffer.rewind();
        filePosition += compressedSize;

        bufferOffset += buffer.position();
        if (buffer.capacity() < uncompressedSize)
        {
            int bufferSize = uncompressedSize + (uncompressedSize / 20);
            BufferPool.put(buffer);
            buffer = allocateBuffer(bufferSize, compressor.preferredBufferType());
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

    protected void releaseBuffer()
    {
        super.releaseBuffer();
        if (compressedBuffer != null)
        {
            BufferPool.put(compressedBuffer);
            compressedBuffer = null;
        }
    }

    protected void reBufferMmap()
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("resource") // Closing the ChecksummedDataInput will close the underlying channel.
    public static ChecksummedDataInput upgradeInput(ChecksummedDataInput input, ICompressor compressor)
    {
        long position = input.getPosition();
        input.close();

        CompressedChecksummedDataInputBuilder builder = new CompressedChecksummedDataInputBuilder(new ChannelProxy(input.getPath()));
        builder.withPosition(position);
        builder.withCompressor(compressor);
        return builder.build();
    }

    @VisibleForTesting
    ICompressor getCompressor()
    {
        return compressor;
    }
}
