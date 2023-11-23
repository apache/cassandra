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

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;

/**
 * This is a wrapper over a Cassandra {@link RandomAccessReader} that provides an {@link IndexInput}
 * interface for Lucene classes that need {@link IndexInput}. This is an optimisation because the
 * Lucene {@link DataInput} reads bytes one at a time whereas the {@link RandomAccessReader} is
 * optimised to read multibyte objects faster.
 */
public class IndexInputReader extends IndexInput
{
    /**
     * the byte order of `input`'s native readX operations doesn't matter,
     * because we only use `readFully` and `readByte` methods. IndexInput calls these
     * (via DataInput) with methods that enforce LittleEndian-ness.
    */
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

    public static IndexInputReader create(RandomAccessReader input, Runnable doOnClose)
    {
        return new IndexInputReader(input, doOnClose);
    }

    public static IndexInputReader create(FileHandle handle)
    {
        RandomAccessReader reader = handle.createReader();
        return new IndexInputReader(reader, () -> {});
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
    public IndexInput slice(String sliceDescription, long offset, long length)
    {
        throw new UnsupportedOperationException("Slice operations are not supported");
    }
}
