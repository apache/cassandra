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

package org.apache.cassandra.db.memtable.pmem;

import java.io.DataInputStream;
import java.io.IOException;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.io.util.DataInputPlus;

public class MemoryBlockDataInputPlus implements DataInputPlus
{
    private final TransactionalMemoryBlock block;
    private int position;

    public MemoryBlockDataInputPlus(TransactionalMemoryBlock block)
    {
        this.block = block;
        this.position = 0;
    }

    public long position()
    {
        return position;
    }

    public void position(int position)
    {
        this.position = position;
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        if (b.length > 0)
        {
            block.copyToArray(position, b, 0, b.length);
            position += b.length;
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        block.copyToArray(position, b, off, len);
        position += len;
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        position += n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        byte retVal = block.getByte(position);
        position += 1;
        return retVal != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        byte retVal = block.getByte(position);
        position += Byte.BYTES;
        return retVal;
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        byte retVal = block.getByte(position);
        position += Byte.BYTES;
        return retVal;
    }

    @Override
    public short readShort() throws IOException
    {
        short retVal = block.getShort(position);
        position += Short.BYTES;
        return retVal;
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        short retVal = block.getShort(position);
        position += Short.BYTES;
        return retVal;
    }

    @Override
    public char readChar() throws IOException
    {
        char retVal = (char) (block.getByte(position));
        position += Byte.BYTES;
        return retVal;
    }

    @Override
    public int readInt() throws IOException
    {
        int retVal = block.getInt(position);
        position += Integer.BYTES;
        return retVal;
    }

    @Override
    public long readLong() throws IOException
    {
        long retVal = block.getLong(position);
        position += Long.BYTES;
        return retVal;
    }

    @Override
    public float readFloat() throws IOException
    {
        float retVal = Float.intBitsToFloat(block.getInt(position));
        position += Integer.BYTES;
        return retVal;
    }

    @Override
    public double readDouble() throws IOException
    {
        double retVal = Double.longBitsToDouble(block.getLong(position));
        position += Long.BYTES;
        return retVal;
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }
}
