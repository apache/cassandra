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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

/***
 * A wrapper around {@link ByteBuffersIndexOutput} that adds several methods that interact
 * with the underlying delegate.
 */
public class ResettableByteBuffersIndexOutput extends IndexOutput
{
    private final ByteBuffersIndexOutput bbio;
    private final ByteBuffersDataOutput delegate;

    public ResettableByteBuffersIndexOutput(String name)
    {
        //TODO CASSANDRA-18280 to investigate the initial size allocation
        this(128, name);
    }

    public ResettableByteBuffersIndexOutput(int expectedSize, String name)
    {
        super("", name);
        delegate = new ByteBuffersDataOutput(expectedSize);
        bbio = new ByteBuffersIndexOutput(delegate, "", name + "-bb");
    }

    public void copyTo(IndexOutput out) throws IOException
    {
        delegate.copyTo(out);
    }

    public int intSize() {
        return Math.toIntExact(bbio.getFilePointer());
    }

    public byte[] toArrayCopy() {
        return delegate.toArrayCopy();
    }

    public void reset()
    {
        delegate.reset();
    }

    @Override
    public String toString()
    {
        return "Resettable" + bbio.toString();
    }

    @Override
    public void close() throws IOException
    {
        bbio.close();
    }

    @Override
    public long getFilePointer()
    {
        return bbio.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException
    {
        return bbio.getChecksum();
    }

    @Override
    public void writeByte(byte b) throws IOException
    {
        bbio.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException
    {
        bbio.writeBytes(b, offset, length);
    }

    @Override
    public void writeBytes(byte[] b, int length) throws IOException
    {
        bbio.writeBytes(b, length);
    }

    @Override
    public void writeInt(int i) throws IOException
    {
        bbio.writeInt(i);
    }

    @Override
    public void writeShort(short i) throws IOException
    {
        bbio.writeShort(i);
    }

    @Override
    public void writeLong(long i) throws IOException
    {
        bbio.writeLong(i);
    }

    @Override
    public void writeString(String s) throws IOException
    {
        bbio.writeString(s);
    }

    @Override
    public void copyBytes(DataInput input, long numBytes) throws IOException
    {
        bbio.copyBytes(input, numBytes);
    }

    @Override
    public void writeMapOfStrings(Map<String, String> map) throws IOException
    {
        bbio.writeMapOfStrings(map);
    }

    @Override
    public void writeSetOfStrings(Set<String> set) throws IOException
    {
        bbio.writeSetOfStrings(set);
    }
}
