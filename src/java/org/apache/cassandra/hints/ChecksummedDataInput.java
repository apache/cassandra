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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.NativeLibrary;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * A {@link RandomAccessReader} wrapper that calculates the CRC in place.
 *
 * Useful for {@link org.apache.cassandra.hints.HintsReader}, for example, where we must verify the CRC, yet don't want
 * to allocate an extra byte array just that purpose. The CRC can be embedded in the input stream and checked via checkCrc().
 *
 * In addition to calculating the CRC, it allows to enforce a maximum known size. This is needed
 * so that {@link org.apache.cassandra.db.Mutation.MutationSerializer} doesn't blow up the heap when deserializing a
 * corrupted sequence by reading a huge corrupted length of bytes via
 * {@link org.apache.cassandra.utils.ByteBufferUtil#readWithLength(java.io.DataInput)}.
 */
public class ChecksummedDataInput extends RebufferingInputStream
{
    private final CRC32 crc;
    private int crcPosition;
    private boolean crcUpdateDisabled;

    private long limit;
    private long limitMark;

    protected long bufferOffset;
    protected final ChannelProxy channel;

    ChecksummedDataInput(ChannelProxy channel, BufferType bufferType)
    {
        super(BufferPool.get(RandomAccessReader.DEFAULT_BUFFER_SIZE, bufferType));

        crc = new CRC32();
        crcPosition = 0;
        crcUpdateDisabled = false;
        this.channel = channel;
        bufferOffset = 0;
        buffer.limit(0);

        resetLimit();
    }

    ChecksummedDataInput(ChannelProxy channel)
    {
        this(channel, BufferType.OFF_HEAP);
    }

    @SuppressWarnings("resource")
    public static ChecksummedDataInput open(File file)
    {
        return new ChecksummedDataInput(new ChannelProxy(file));
    }

    public boolean isEOF()
    {
        return getPosition() == channel.size();
    }

    static class Position implements InputPosition
    {
        final long sourcePosition;

        public Position(long sourcePosition)
        {
            super();
            this.sourcePosition = sourcePosition;
        }

        @Override
        public long subtract(InputPosition other)
        {
            return sourcePosition - ((Position)other).sourcePosition;
        }
    }

    /**
     * Return a seekable representation of the current position. For compressed files this is chunk position
     * in file and offset within chunk.
     */
    public InputPosition getSeekPosition()
    {
        return new Position(getPosition());
    }

    public void seek(InputPosition pos)
    {
        updateCrc();
        bufferOffset = ((Position) pos).sourcePosition;
        buffer.position(0).limit(0);
    }

    public void resetCrc()
    {
        crc.reset();
        crcPosition = buffer.position();
    }

    public void limit(long newLimit)
    {
        limitMark = getPosition();
        limit = limitMark + newLimit;
    }

    /**
     * Returns the exact position in the uncompressed view of the file.
     */
    protected long getPosition()
    {
        return bufferOffset + buffer.position();
    }

    /**
     * Returns the position in the source file, which is different for getPosition() for compressed/encrypted files
     * and may be imprecise.
     */
    protected long getSourcePosition()
    {
        return bufferOffset;
    }

    public void resetLimit()
    {
        limit = Long.MAX_VALUE;
        limitMark = -1;
    }

    public void checkLimit(int length) throws IOException
    {
        if (getPosition() + length > limit)
            throw new IOException("Digest mismatch exception");
    }

    public long bytesPastLimit()
    {
        assert limitMark != -1;
        return getPosition() - limitMark;
    }

    public boolean checkCrc() throws IOException
    {
        try
        {
            updateCrc();

            // we must disable crc updates in case we rebuffer
            // when called source.readInt()
            crcUpdateDisabled = true;
            return ((int) crc.getValue()) == readInt();
        }
        finally
        {
            crcPosition = buffer.position();
            crcUpdateDisabled = false;
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        checkLimit(b.length);
        super.readFully(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
        checkLimit(len);
        return super.read(b, off, len);
    }

    @Override
    protected void reBuffer()
    {
        Preconditions.checkState(buffer.remaining() == 0);
        updateCrc();
        bufferOffset += buffer.limit();

        readBuffer();

        crcPosition = buffer.position();
    }

    protected void readBuffer()
    {
        buffer.clear();
        while ((channel.read(buffer, bufferOffset)) == 0) {}
        buffer.flip();
    }

    public void tryUncacheRead()
    {
        NativeLibrary.trySkipCache(getChannel().getFileDescriptor(), 0, getSourcePosition(), getPath());
    }

    private void updateCrc()
    {
        if (crcPosition == buffer.position() || crcUpdateDisabled)
            return;

        assert crcPosition >= 0 && crcPosition < buffer.position();

        ByteBuffer unprocessed = buffer.duplicate();
        unprocessed.position(crcPosition)
                   .limit(buffer.position());

        crc.update(unprocessed);
    }

    @Override
    public void close()
    {
        BufferPool.put(buffer);
        channel.close();
    }

    protected String getPath()
    {
        return channel.filePath();
    }

    public ChannelProxy getChannel()
    {
        return channel;
    }
}
