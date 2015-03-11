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
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.utils.ByteBufferUtil;

public class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    public static final long CACHE_FLUSH_INTERVAL_IN_BYTES = (long) Math.pow(2, 27); // 128mb

    // default buffer size, 64Kb
    public static final int DEFAULT_BUFFER_SIZE = 65536;

    // absolute filesystem path to the file
    private final String filePath;

    // buffer which will cache file blocks
    protected ByteBuffer buffer;

    // `bufferOffset` is the offset of the beginning of the buffer
    // `markedPointer` folds the offset of the last file mark
    protected long bufferOffset, markedPointer;

    // channel linked with the file, used to retrieve data and force updates.
    protected final FileChannel channel;

    // this can be overridden at construction to a value shorter than the true length of the file;
    // if so, it acts as an imposed limit on reads, rather than a convenience property
    private final long fileLength;

    protected final PoolingSegmentedFile owner;

    protected RandomAccessReader(File file, int bufferSize, PoolingSegmentedFile owner) throws FileNotFoundException
    {
        this(file, bufferSize, -1, false, owner);
    }
    protected RandomAccessReader(File file, int bufferSize, long overrideLength, boolean useDirectBuffer, PoolingSegmentedFile owner) throws FileNotFoundException
    {
        this.owner = owner;

        filePath = file.getAbsolutePath();

        try
        {
            channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        }
        catch (IOException e)
        {
            throw new FileNotFoundException(filePath);
        }

        // allocating required size of the buffer
        if (bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");

        // we can cache file length in read-only mode
        long fileLength = overrideLength;
        if (fileLength <= 0)
        {
            try
            {
                fileLength = channel.size();
            }
            catch (IOException e)
            {
                throw new FSReadError(e, filePath);
            }
        }
        this.fileLength = fileLength;
        buffer = allocateBuffer(bufferSize, useDirectBuffer);
        buffer.limit(0);
    }

    protected ByteBuffer allocateBuffer(int bufferSize, boolean useDirectBuffer)
    {
        int size = (int) Math.min(fileLength, bufferSize);
        return useDirectBuffer
                ? ByteBuffer.allocate(size)
                : ByteBuffer.allocateDirect(size);
    }

    public static RandomAccessReader open(File file, long overrideSize, PoolingSegmentedFile owner)
    {
        return open(file, DEFAULT_BUFFER_SIZE, overrideSize, owner);
    }

    public static RandomAccessReader open(File file)
    {
        return open(file, -1L);
    }

    public static RandomAccessReader open(File file, long overrideSize)
    {
        return open(file, DEFAULT_BUFFER_SIZE, overrideSize, null);
    }

    @VisibleForTesting
    static RandomAccessReader open(File file, int bufferSize, PoolingSegmentedFile owner)
    {
        return open(file, bufferSize, -1L, owner);
    }

    private static RandomAccessReader open(File file, int bufferSize, long overrideSize, PoolingSegmentedFile owner)
    {
        try
        {
            return new RandomAccessReader(file, bufferSize, overrideSize, false, owner);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    static RandomAccessReader open(SequentialWriter writer)
    {
        return open(new File(writer.getPath()), DEFAULT_BUFFER_SIZE, null);
    }

    // channel extends FileChannel, impl SeekableByteChannel.  Safe to cast.
    public FileChannel getChannel()
    {
        return channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        bufferOffset += buffer.position();
        buffer.clear();
        assert bufferOffset < fileLength;

        try
        {
            channel.position(bufferOffset); // setting channel position
            long limit = bufferOffset;
            while (buffer.hasRemaining() && limit < fileLength)
            {
                int n = channel.read(buffer);
                if (n < 0)
                    break;
                limit = bufferOffset + buffer.position();
            }
            if (limit > fileLength)
                buffer.position((int)(fileLength - bufferOffset));
            buffer.flip();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
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
        return filePath;
    }

    public int getTotalBufferSize()
    {
        //This may NPE so we make a ref
        //https://issues.apache.org/jira/browse/CASSANDRA-7756
        ByteBuffer ref = buffer;
        return ref != null ? ref.capacity() : 0;
    }

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
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
        return getFilePointer() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    @Override
    public void close()
    {
        if (owner == null || buffer == null)
        {
            // The buffer == null check is so that if the pool owner has deallocated us, calling close()
            // will re-call deallocate rather than recycling a deallocated object.
            // I'd be more comfortable if deallocate didn't have to handle being idempotent like that,
            // but RandomAccessFile.close will call AbstractInterruptibleChannel.close which will
            // re-call RAF.close -- in this case, [C]RAR.close since we are overriding that.
            deallocate();
        }
        else
        {
            owner.recycle(this);
        }
    }

    public void deallocate()
    {
        bufferOffset += buffer.position();
        FileUtils.clean(buffer);

        buffer = null; // makes sure we don't use this after it's ostensibly closed

        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + filePath + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
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

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read()
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (!buffer.hasRemaining())
            reBuffer();

        return (int)buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] buffer)
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length)
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (!buffer.hasRemaining())
            reBuffer();

        int toCopy = Math.min(length, buffer.remaining());
        buffer.get(buff, offset, toCopy);
        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;
        try
        {
            ByteBuffer result = ByteBuffer.allocate(length);
            while (result.hasRemaining())
            {
                if (isEOF())
                    throw new EOFException();
                if (!buffer.hasRemaining())
                    reBuffer();
                ByteBufferUtil.put(buffer, result);
            }
            result.flip();
            return result;
        }
        catch (EOFException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, filePath);
        }
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return bufferOffset + buffer.position();
    }

    public long getPositionLimit()
    {
        return length();
    }
}
