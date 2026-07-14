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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

/**
 * This is a wrapper over a Cassandra {@link RandomAccessReader} that provides an {@link IndexInput}
 * interface for Lucene classes that need {@link IndexInput}. This is an optimisation because the
 * Lucene {@link DataInput} reads bytes one at a time whereas the {@link RandomAccessReader} is
 * optimized to read multibyte objects faster.
 */
@NotThreadSafe
public class IndexInputReader extends IndexInput
{
    public static final Runnable NO_OP_ON_CLOSE = () -> {};

    /**
     * the byte order of `input`'s native readX operations doesn't matter,
     * because we only use `readFully` and `readByte` methods. IndexInput calls these
     * (via DataInput) with methods that enforce LittleEndian-ness.
    */
    private final RandomAccessReader input;
    private final Runnable doOnClose;

    /** Absolute offset in the underlying file that this input's position 0 refers to. */
    private final long offset;

    /** Bounded length of this input, in bytes. */
    private final long length;

    private IndexInputReader(RandomAccessReader input, Runnable doOnClose, long offset, long length)
    {
        super(input.getPath());
        this.input = input;
        this.doOnClose = doOnClose;
        this.offset = offset;
        this.length = length;
    }

    public static IndexInputReader create(RandomAccessReader input)
    {
        // Top-level inputs own the underlying reader; folding its close into doOnClose lets us
        // avoid a separate ownership flag on the class.
        return new IndexInputReader(input, input::close, 0L, input.length());
    }

    public static IndexInputReader create(RandomAccessReader input, Runnable doOnClose)
    {
        Runnable close = () -> {
            try
            {
                input.close();
            }
            finally
            {
                doOnClose.run();
            }
        };
        return new IndexInputReader(input, close, 0L, input.length());
    }

    public static IndexInputReader create(FileHandle handle)
    {
        RandomAccessReader reader = handle.createReader();
        return new IndexInputReader(reader, reader::close, 0L, reader.length());
    }

    @Override
    public byte readByte() throws IOException
    {
        return input.readByte();
    }

    @Override
    public void readBytes(byte[] bytes, int off, int len) throws IOException
    {
        input.readFully(bytes, off, len);
    }

    @Override
    public void close()
    {
        doOnClose.run();
    }

    @Override
    public long getFilePointer()
    {
        return input.getFilePointer() - offset;
    }

    @Override
    public void seek(long position)
    {
        if (position > length)
            throw new IllegalArgumentException("Cannot seek to position " + position + " past length of " + length);

        input.seek(offset + position);
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length)
    {
        if (offset < 0 || length < 0 || offset + length > this.length)
            throw new IllegalArgumentException("Invalid slice: offset=" + offset + ", length=" + length + ", parent length=" + this.length + " for " + sliceDescription);

        // Slices share the underlying reader with their parent; the no-op close keeps the parent's lifecycle intact.
        IndexInputReader slice = new IndexInputReader(input, NO_OP_ON_CLOSE, this.offset + offset, length);

        // Seek to the beginning of the slice...
        slice.seek(0);

        return slice;
    }
}
