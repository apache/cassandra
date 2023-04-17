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

import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/***
 * A wrapper around {@link ByteBuffersIndexOutput} that adds several methods that interact
 * with the underlying delegate.
 */
public class ResettableByteBuffersIndexOutput extends IndexOutput
{

    private final ByteBuffersIndexOutput bbio;
    private final ByteBuffersDataOutput delegate;

    public ResettableByteBuffersIndexOutput(int expectedSize, String name)
    {
        super("", name);
        delegate = new ByteBuffersDataOutput(expectedSize);
        bbio = new ByteBuffersIndexOutput(delegate, "", name + "-bb");
    }

    public ByteBuffersDataInput toDataInput()
    {
        return delegate.toDataInput();
    }

    public IndexInput toIndexInput()
    {
        return new ByteBuffersIndexInput(toDataInput(), "");
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

    public String toString()
    {
        return "Resettable" + bbio.toString();
    }

    public void close() throws IOException
    {
        bbio.close();
    }

    public long getFilePointer()
    {
        return bbio.getFilePointer();
    }

    public long getChecksum() throws IOException
    {
        return bbio.getChecksum();
    }

    public void writeByte(byte b) throws IOException
    {
        bbio.writeByte(b);
    }

    public void writeBytes(byte[] b, int offset, int length) throws IOException
    {
        bbio.writeBytes(b, offset, length);
    }

    public void writeBytes(byte[] b, int length) throws IOException
    {
        bbio.writeBytes(b, length);
    }

    public void writeInt(int i) throws IOException
    {
        bbio.writeInt(i);
    }

    public void writeShort(short i) throws IOException
    {
        bbio.writeShort(i);
    }

    public void writeLong(long i) throws IOException
    {
        bbio.writeLong(i);
    }

    public void writeString(String s) throws IOException
    {
        bbio.writeString(s);
    }

    public void copyBytes(DataInput input, long numBytes) throws IOException
    {
        bbio.copyBytes(input, numBytes);
    }

    public void writeMapOfStrings(Map<String, String> map) throws IOException
    {
        bbio.writeMapOfStrings(map);
    }

    public void writeSetOfStrings(Set<String> set) throws IOException
    {
        bbio.writeSetOfStrings(set);
    }
}
