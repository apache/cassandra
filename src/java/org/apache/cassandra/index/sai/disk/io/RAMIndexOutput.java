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

import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;

// Use in place of RAMOutputStream (which has monitor locks)
public class RAMIndexOutput extends IndexOutput
{
    protected final GrowableByteArrayDataOutput out;

    public RAMIndexOutput(String name)
    {
        super("", name);
        out = new GrowableByteArrayDataOutput(128);
    }

    public byte[] getBytes()
    {
        return out.getBytes();
    }

    @Override
    public long getChecksum() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFilePointer()
    {
        return out.getPosition();
    }

    @Override
    public void writeByte(byte b)
    {
        out.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] bytes, int offset, int len)
    {
        out.writeBytes(bytes, offset, len);
    }

    public void writeTo(IndexOutput externalOut) throws IOException
    {
        externalOut.writeBytes(out.getBytes(), 0, out.getPosition());
    }

    public void reset()
    {
        out.reset();
    }

    @Override
    public void close() throws IOException {}
}
