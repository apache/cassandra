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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.util.Rebufferer.BufferHolder;
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

    // offset of the last file mark
    protected long markedPointer;

    @VisibleForTesting
    final Rebufferer rebufferer;
    BufferHolder bufferHolder = Rebufferer.EMPTY;

    protected RandomAccessReader(Rebufferer rebufferer)
    {
        super(Rebufferer.EMPTY.buffer());
        this.rebufferer = rebufferer;
    }

    public static ByteBuffer allocateBuffer(int size, BufferType bufferType)
    {
        return BufferPool.get(size, bufferType).order(ByteOrder.BIG_ENDIAN);
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    public void reBuffer()
    {
        if (isEOF())
            return;

        reBufferAt(current());
    }

    public void reBufferAt(long position)
    {
        bufferHolder.release();
        bufferHolder = rebufferer.rebuffer(position);
        buffer = bufferHolder.buffer();
        buffer.position(Ints.checkedCast(position - bufferHolder.offset()));

        assert buffer.order() == ByteOrder.BIG_ENDIAN : "Buffer must have BIG ENDIAN byte ordering";
    }

    @Override
    public long getFilePointer()
    {
        if (buffer == null)     // closed already
            return rebufferer.fileLength();
        return current();
    }

    protected long current()
    {
        return bufferHolder.offset() + buffer.position();
    }

    public String getPath()
    {
        return getChannel().filePath();
    }

    public ChannelProxy getChannel()
    {
        return rebufferer.channel();
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
        // close needs to be idempotent.
        if (buffer == null)
            return;

        bufferHolder.release();
        rebufferer.closeReader();
        buffer = null;
        bufferHolder = null;

        //For performance reasons we don't keep a reference to the file
        //channel so we don't close it
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ':' + rebufferer.toString();
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

        long bufferOffset = bufferHolder.offset();
        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }

        if (newPosition > length())
            throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                         newPosition, getPath(), length()));
        reBufferAt(newPosition);
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
        return rebufferer.fileLength();
    }

    public long getPosition()
    {
        return current();
    }

    public double getCrcCheckChance()
    {
        return rebufferer.getCrcCheckChance();
    }

    protected static Rebufferer instantiateRebufferer(RebuffererFactory fileRebufferer, RateLimiter limiter)
    {
        Rebufferer rebufferer = fileRebufferer.instantiateRebufferer();

        if (limiter != null)
            rebufferer = new LimitingRebufferer(rebufferer, limiter, MAX_BUFFER_SIZE);

        return rebufferer;
    }

    public static RandomAccessReader build(SegmentedFile file, RateLimiter limiter)
    {
        return new RandomAccessReader(instantiateRebufferer(file.rebuffererFactory(), limiter));
    }

    public static Builder builder(ChannelProxy channel)
    {
        return new Builder(channel);
    }

    public static class Builder
    {
        // The NIO file channel or an empty channel
        public final ChannelProxy channel;

        // The size of the buffer for buffered readers
        protected int bufferSize;

        // The type of the buffer for buffered readers
        public BufferType bufferType;

        // The buffer
        public ByteBuffer buffer;

        // An optional limiter that will throttle the amount of data we read
        public RateLimiter limiter;

        // The mmap segments for mmap readers
        public MmappedRegions regions;

        // Compression for compressed readers
        public CompressionMetadata compression;

        public Builder(ChannelProxy channel)
        {
            this.channel = channel;
            this.bufferSize = DEFAULT_BUFFER_SIZE;
            this.bufferType = BufferType.OFF_HEAP;
        }

        /** The buffer size is typically already page aligned but if that is not the case
         * make sure that it is a multiple of the page size, 4096. Also limit it to the maximum
         * buffer size unless we are throttling, in which case we may as well read the maximum
         * directly since the intention is to read the full file, see CASSANDRA-8630.
         * */
        private int adjustedBufferSize()
        {
            if (limiter != null)
                return MAX_BUFFER_SIZE;

            // should already be a page size multiple but if that's not case round it up
            int wholePageSize = (bufferSize + 4095) & ~4095;
            return Math.min(MAX_BUFFER_SIZE, wholePageSize);
        }

        protected Rebufferer createRebufferer()
        {
            return instantiateRebufferer(chunkReader(), limiter);
        }

        public RebuffererFactory chunkReader()
        {
            if (compression != null)
                return CompressedSegmentedFile.chunkReader(channel, compression, regions);
            if (regions != null)
                return new MmapRebufferer(channel, -1, regions);

            int adjustedSize = adjustedBufferSize();
            return new SimpleChunkReader(channel, -1, bufferType, adjustedSize);
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

        public Builder compression(CompressionMetadata metadata)
        {
            this.compression = metadata;
            return this;
        }

        public Builder limiter(RateLimiter limiter)
        {
            this.limiter = limiter;
            return this;
        }

        public RandomAccessReader build()
        {
            return new RandomAccessReader(createRebufferer());
        }

        public RandomAccessReader buildWithChannel()
        {
            return new RandomAccessReaderWithOwnChannel(createRebufferer());
        }
    }

    // A wrapper of the RandomAccessReader that closes the channel when done.
    // For performance reasons RAR does not increase the reference count of
    // a channel but assumes the owner will keep it open and close it,
    // see CASSANDRA-9379, this thin class is just for those cases where we do
    // not have a shared channel.
    public static class RandomAccessReaderWithOwnChannel extends RandomAccessReader
    {
        protected RandomAccessReaderWithOwnChannel(Rebufferer rebufferer)
        {
            super(rebufferer);
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
                try
                {
                    rebufferer.close();
                }
                finally
                {
                    getChannel().close();
                }
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
