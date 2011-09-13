/**
 *
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
 *
 */
package org.apache.cassandra.io.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.utils.ByteBufferUtil;

public class MappedFileDataInput extends AbstractDataInput implements FileDataInput
{
    private final MappedByteBuffer buffer;
    private final String filename;
    private int position;

    public MappedFileDataInput(FileInputStream stream, String filename, int position) throws IOException
    {
        FileChannel channel = stream.getChannel();
        buffer = channel.map(FileChannel.MapMode.READ_ONLY, position, channel.size());
        this.filename = filename;
        this.position = position;
    }

    public MappedFileDataInput(MappedByteBuffer buffer, String filename, int position)
    {
        assert buffer != null;
        this.buffer = buffer;
        this.filename = filename;
        this.position = position;
    }

    // don't make this public, this is only for seeking WITHIN the current mapped segment
    protected void seekInternal(int pos)
    {
        position = pos;
    }

    protected int getPosition()
    {
        return position;
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }

    public void reset(FileMark mark) throws IOException
    {
        assert mark instanceof MappedFileDataInputMark;
        seekInternal(((MappedFileDataInputMark) mark).position);
    }

    public FileMark mark()
    {
        return new MappedFileDataInputMark(position);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof MappedFileDataInputMark;
        assert position >= ((MappedFileDataInputMark) mark).position;
        return position - ((MappedFileDataInputMark) mark).position;
    }

    public boolean isEOF() throws IOException
    {
        return position == buffer.capacity();
    }

    public long bytesRemaining() throws IOException
    {
        return buffer.capacity() - position;
    }

    public String getPath()
    {
        return filename;
    }

    public int read() throws IOException
    {
        if (isEOF())
            return -1;
        return buffer.get(position++) & 0xFF;
    }

    /**
     * Does the same thing as <code>readFully</code> do but without copying data (thread safe)
     * @param length length of the bytes to read
     * @return buffer with portion of file content
     * @throws IOException on any fail of I/O operation
     */
    public synchronized ByteBuffer readBytes(int length) throws IOException
    {
        int remaining = buffer.remaining() - position;
        if (length > remaining)
            throw new IOException(String.format("mmap segment underflow; remaining is %d but %d requested",
                                                remaining, length));

        if (length == 0)
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        ByteBuffer bytes = buffer.duplicate();
        bytes.position(buffer.position() + position).limit(buffer.position() + position + length);
        position += length;

        // we have to copy the data in case we unreference the underlying sstable.  See CASSANDRA-3179
        ByteBuffer clone = ByteBuffer.allocate(bytes.remaining());
        clone.put(bytes);
        clone.flip();
        return clone;
    }

    @Override
    public final void readFully(byte[] buffer) throws IOException
    {
        throw new UnsupportedOperationException("use readBytes instead");
    }

    @Override
    public final void readFully(byte[] buffer, int offset, int count) throws IOException
    {
        throw new UnsupportedOperationException("use readBytes instead");
    }

    public int skipBytes(int n) throws IOException
    {
        assert n >= 0 : "skipping negative bytes is illegal: " + n;
        if (n == 0)
            return 0;
        int oldPosition = position;
        assert ((long)oldPosition) + n <= Integer.MAX_VALUE;
        position = Math.min(buffer.capacity(), position + n);
        return position - oldPosition;
    }

    private static class MappedFileDataInputMark implements FileMark
    {
        int position;

        MappedFileDataInputMark(int position)
        {
            this.position = position;
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" +
               "filename='" + filename + "'" +
               ", position=" + position +
               ")";
    }
}
