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
package org.apache.cassandra.index.sai.disk.io;


import java.io.IOException;

import org.apache.cassandra.io.compress.CorruptBlockException;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

public class IndexInputReader extends IndexInput
{
    private final RandomAccessReader input;
    private final Runnable doOnClose;

    private IndexInputReader(RandomAccessReader input, Runnable doOnClose)
    {
        super(input.getPath());
        this.input = input;
        this.doOnClose = doOnClose;
    }

    public static IndexInputReader create(RandomAccessReader input)
    {
        return new IndexInputReader(input, () -> {});
    }

    static IndexInputReader create(RandomAccessReader input, Runnable doOnClose)
    {
        return new IndexInputReader(input, doOnClose);
    }

    @SuppressWarnings("resource")
    static IndexInputReader create(FileHandle handle)
    {
        RandomAccessReader reader = handle.createReader();
        return new IndexInputReader(reader, () -> {});
    }

    public RandomAccessReader reader()
    {
        return input;
    }

    @Override
    public byte readByte() throws IOException
    {
        return input.readByte();
    }

    @Override
    public void readBytes(byte[] bytes, int off, int len) throws IOException
    {
        try
        {
            input.readFully(bytes, off, len);
        }
        catch (CorruptBlockException ex)
        {
            throw new CorruptIndexException(input.getPath(), "Corrupted block", ex);
        }
    }

    /**
     * Using {@link RandomAccessReader#readShort()} directly is faster than {@link DataInput#readShort()} which calls
     * {@link DataInput#readByte()} one by one
     */
    @Override
    public short readShort() throws IOException
    {
        try
        {
            return input.readShort();
        }
        catch (CorruptBlockException ex)
        {
            throw new CorruptIndexException(input.getPath(), "Corrupted block", ex);
        }
    }

    /**
     * Using {@link RandomAccessReader#readInt()} directly is faster than {@link DataInput#readInt()} which
     * calls {@link DataInput#readByte()} one by one
     */
    @Override
    public int readInt() throws IOException
    {
        try
        {
            return input.readInt();
        }
        catch (CorruptBlockException ex)
        {
            throw new CorruptIndexException(input.getPath(), "Corrupted block", ex);
        }
    }

    /**
     * Using {@link RandomAccessReader#readLong()} directly is faster than {@link DataInput#readLong()} which
     * calls {@link DataInput#readByte()} one by one
     */
    @Override
    public long readLong() throws IOException
    {
        try
        {
            return input.readLong();
        }
        catch (CorruptBlockException ex)
        {
            throw new CorruptIndexException(input.getPath(), "Corrupted block", ex);
        }
    }

    @Override
    public void close()
    {
        try
        {
            input.close();
        }
        finally
        {
            doOnClose.run();
        }
    }

    @Override
    public long getFilePointer()
    {
        return input.getFilePointer();
    }

    @Override
    public void seek(long position)
    {
        input.seek(position);
    }

    @Override
    public long length()
    {
        return input.length();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws CorruptIndexException
    {
        if (offset < 0 || length < 0 || offset + length > input.length())
        {
            throw new CorruptIndexException("Invalid slice! Offset: " + offset + ", Length: " + length + ", Input Length: " + input.length(), this);
        }

        return new IndexInputReader(input, doOnClose)
        {
            @Override
            public void seek(long position)
            {
                input.seek(position + offset);
            }

            @Override
            public long getFilePointer()
            {
                return input.getFilePointer() - offset;
            }

            @Override
            public long length()
            {
                return length;
            }
        };
    }
}
