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
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public class RandomAccessReader extends RebufferingInputStream implements FileDataInput
{
    // The default buffer size when the client doesn't specify it
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    // The maximum buffer size, we will never buffer more than this size. Further,
    // when the limiter is not null, i.e. when throttling is enabled, we read exactly
    // this size, since when throttling the intention is to eventually read everything,
    // see CASSANDRA-8630
    // NOTE: this size is chosen both for historical consistency, as a reasonable upper bound,
    //       and because our BufferPool currently has a maximum allocation size of this.
    public static final int MAX_BUFFER_SIZE = 1 << 16; // 64k

    // the IO channel to the file, we do not own a reference to this due to
    // performance reasons (CASSANDRA-9379) so it's up to the owner of the RAR to
    // ensure that the channel stays open and that it is closed afterwards
    protected final ChannelProxy channel;

    // optional memory mapped regions for the channel
    protected final MmappedRegions regions;

    // An optional limiter that will throttle the amount of data we read
    protected final RateLimiter limiter;

    // the file length, this can be overridden at construction to a value shorter
    // than the true length of the file; if so, it acts as an imposed limit on reads,
    // required when opening sstables early not to read past the mark
    private final long fileLength;

    // the buffer size for buffered readers
    protected final int bufferSize;

    // the buffer type for buffered readers
    protected final BufferType bufferType;

    // offset from the beginning of the file
    protected long bufferOffset;

    // offset of the last file mark
    protected long markedPointer;

    protected RandomAccessReader(Builder builder)
    {
        super(builder.createBuffer());

        this.channel = builder.channel;
        this.regions = builder.regions;
        this.limiter = builder.limiter;
        this.fileLength = builder.overrideLength <= 0 ? builder.channel.size() : builder.overrideLength;
        this.bufferSize = builder.bufferSize;
        this.bufferType = builder.bufferType;
        this.buffer = builder.buffer;
    }

    protected static ByteBuffer allocateBuffer(int size, BufferType bufferType)
    {
        return BufferPool.get(size, bufferType).order(ByteOrder.BIG_ENDIAN);
    }

    protected void releaseBuffer()
    {
        if (buffer != null)
        {
            if (regions == null)
                BufferPool.put(buffer);
            buffer = null;
        }
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    public void reBuffer()
    {
        if (isEOF())
            return;

        if (regions == null)
            reBufferStandard();
        else
            reBufferMmap();

        if (limiter != null)
            limiter.acquire(buffer.remaining());

        assert buffer.order() == ByteOrder.BIG_ENDIAN : "Buffer must have BIG ENDIAN byte ordering";
    }

    protected void reBufferStandard()
    {
        bufferOffset += buffer.position();
        assert bufferOffset < fileLength;

        buffer.clear();
        long position = bufferOffset;
        long limit = bufferOffset;

        long pageAligedPos = position & ~4095;
        // Because the buffer capacity is a multiple of the page size, we read less
        // the first time and then we should read at page boundaries only,
        // unless the user seeks elsewhere
        long upperLimit = Math.min(fileLength, pageAligedPos + buffer.capacity());
        buffer.limit((int)(upperLimit - position));
        while (buffer.hasRemaining() && limit < upperLimit)
        {
            int n = channel.read(buffer, position);
            if (n < 0)
                throw new FSReadError(new IOException("Unexpected end of file"), channel.filePath());

            position += n;
            limit = bufferOffset + buffer.position();
        }

        buffer.flip();
    }

    protected void reBufferMmap()
    {
        long position = bufferOffset + buffer.position();
        assert position < fileLength;

        MmappedRegions.Region region = regions.floor(position);
        bufferOffset = region.bottom();
        buffer = region.buffer.duplicate();
        buffer.position(Ints.checkedCast(position - bufferOffset));

        if (limiter != null && bufferSize < buffer.remaining())
        { // ensure accurate throttling
            buffer.limit(buffer.position() + bufferSize);
        }
    }

    @Override
    public long getFilePointer()
    {
        return current();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public String getPath()
    {
        return channel.filePath();
    }

    public ChannelProxy getChannel()
    {
        return channel;
    }

    @Override
    public void reset() throws IOException
    {
        seek(markedPointer);
    }

    @Override
    public boolean markSupported()
    {
        return true;
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public DataPosition mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(DataPosition mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(DataPosition mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return current() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    @Override
    public int available() throws IOException
    {
        return Ints.saturatedCast(bytesRemaining());
    }

    @Override
    public void close()
    {
	    //make idempotent
        if (buffer == null)
            return;

        bufferOffset += buffer.position();
        releaseBuffer();

        //For performance reasons we don't keep a reference to the file
        //channel so we don't close it
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(filePath='" + channel + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements DataPosition
    {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (buffer == null)
            throw new IllegalStateException("Attempted to seek in a closed RAR");

        if (newPosition >= length()) // it is save to call length() in read-only mode
        {
            if (newPosition > length())
                throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getPath(), length()));
            buffer.limit(0);
            bufferOffset = newPosition;
            return;
        }

        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }
        // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
        bufferOffset = newPosition;
        buffer.clear();
        reBuffer();
        assert current() == newPosition;
    }

    /**
     * Reads a line of text form the current position in this file. A line is
     * represented by zero or more characters followed by {@code '\n'}, {@code
     * '\r'}, {@code "\r\n"} or the end of file marker. The string does not
     * include the line terminating sequence.
     * <p>
     * Blocks until a line terminating sequence has been read, the end of the
     * file is reached or an exception is thrown.
     * </p>
     * @return the contents of the line or {@code null} if no characters have
     * been read before the end of the file has been reached.
     * @throws IOException if this file is closed or another I/O error occurs.
     */
    public final String readLine() throws IOException
    {
        StringBuilder line = new StringBuilder(80); // Typical line length
        boolean foundTerminator = false;
        long unreadPosition = -1;
        while (true)
        {
            int nextByte = read();
            switch (nextByte)
            {
                case -1:
                    return line.length() != 0 ? line.toString() : null;
                case (byte) '\r':
                    if (foundTerminator)
                    {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    foundTerminator = true;
                    /* Have to be able to peek ahead one byte */
                    unreadPosition = getPosition();
                    break;
                case (byte) '\n':
                    return line.toString();
                default:
                    if (foundTerminator)
                    {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    line.append((char) nextByte);
            }
        }
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return current();
    }

    public static class Builder
    {
        // The NIO file channel or an empty channel
        public final ChannelProxy channel;

        // We override the file length when we open sstables early, so that we do not
        // read past the early mark
        public long overrideLength;

        // The size of the buffer for buffered readers
        public int bufferSize;

        // The type of the buffer for buffered readers
        public BufferType bufferType;

        // The buffer
        public ByteBuffer buffer;

        // The mmap segments for mmap readers
        public MmappedRegions regions;

        // An optional limiter that will throttle the amount of data we read
        public RateLimiter limiter;

        public Builder(ChannelProxy channel)
        {
            this.channel = channel;
            this.overrideLength = -1L;
            this.bufferSize = DEFAULT_BUFFER_SIZE;
            this.bufferType = BufferType.OFF_HEAP;
            this.regions = null;
            this.limiter = null;
        }

        /** The buffer size is typically already page aligned but if that is not the case
         * make sure that it is a multiple of the page size, 4096. Also limit it to the maximum
         * buffer size unless we are throttling, in which case we may as well read the maximum
         * directly since the intention is to read the full file, see CASSANDRA-8630.
         * */
        private void setBufferSize()
        {
            if (limiter != null)
            {
                bufferSize = MAX_BUFFER_SIZE;
                return;
            }

            if ((bufferSize & ~4095) != bufferSize)
            { // should already be a page size multiple but if that's not case round it up
                bufferSize = (bufferSize + 4095) & ~4095;
            }

            bufferSize = Math.min(MAX_BUFFER_SIZE, bufferSize);
        }

        protected ByteBuffer createBuffer()
        {
            setBufferSize();

            buffer = regions == null
                     ? allocateBuffer(bufferSize, bufferType)
                     : regions.floor(0).buffer.duplicate();

            buffer.limit(0);
            return buffer;
        }

        public Builder overrideLength(long overrideLength)
        {
            this.overrideLength = overrideLength;
            return this;
        }

        public Builder bufferSize(int bufferSize)
        {
            if (bufferSize <= 0)
                throw new IllegalArgumentException("bufferSize must be positive");

            this.bufferSize = bufferSize;
            return this;
        }

        public Builder bufferType(BufferType bufferType)
        {
            this.bufferType = bufferType;
            return this;
        }

        public Builder regions(MmappedRegions regions)
        {
            this.regions = regions;
            return this;
        }

        public Builder limiter(RateLimiter limiter)
        {
            this.limiter = limiter;
            return this;
        }

        public RandomAccessReader build()
        {
            return new RandomAccessReader(this);
        }

        public RandomAccessReader buildWithChannel()
        {
            return new RandomAccessReaderWithOwnChannel(this);
        }
    }

    // A wrapper of the RandomAccessReader that closes the channel when done.
    // For performance reasons RAR does not increase the reference count of
    // a channel but assumes the owner will keep it open and close it,
    // see CASSANDRA-9379, this thin class is just for those cases where we do
    // not have a shared channel.
    public static class RandomAccessReaderWithOwnChannel extends RandomAccessReader
    {
        protected RandomAccessReaderWithOwnChannel(Builder builder)
        {
            super(builder);
        }

        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            finally
            {
                channel.close();
            }
        }
    }

    @SuppressWarnings("resource")
    public static RandomAccessReader open(File file)
    {
        return new Builder(new ChannelProxy(file)).buildWithChannel();
    }

    public static RandomAccessReader open(ChannelProxy channel)
    {
        return new Builder(channel).build();
    }
}
