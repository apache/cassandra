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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

/**
 * {@link IndexInput} adapter that exposes it as a {@link RandomAccessInput}.
 */
public class SeekingRandomAccessInput implements RandomAccessInput
{
    private final IndexInput in;

    public SeekingRandomAccessInput(IndexInput in)
    {
        this.in = in;
    }

    @Override
    public byte readByte(long pos) throws IOException
    {
        in.seek(pos);
        return in.readByte();
    }

    @Override
    public short readShort(long pos) throws IOException
    {
        in.seek(pos);
        return in.readShort();
    }

    @Override
    public int readInt(long pos) throws IOException
    {
        in.seek(pos);
        return in.readInt();
    }

    @Override
    public long readLong(long pos) throws IOException
    {
        in.seek(pos);
        return in.readLong();
    }

    @Override
    public String toString()
    {
        return "SeekingRandomAccessInput(" + in + ')';
    }
}
