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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * Extension to DataInput that provides support for reading varints
 */
public interface DataInputPlus extends DataInput
{

    default long readVInt() throws IOException
    {
        return VIntCoding.readVInt(this);
    }

    /**
     * Think hard before opting for an unsigned encoding. Is this going to bite someone because some day
     * they might need to pass in a sentinel value using negative numbers? Is the risk worth it
     * to save a few bytes?
     *
     * Signed, not a fan of unsigned values in protocols and formats
     */
    default long readUnsignedVInt() throws IOException
    {
        return VIntCoding.readUnsignedVInt(this);
    }

    public static class ForwardingDataInput implements DataInput
    {
        protected final DataInput in;

        public ForwardingDataInput(DataInput in)
        {
            this.in = in;
        }

        @Override
        public void readFully(byte[] b) throws IOException
        {
            in.readFully(b);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException
        {
            in.readFully(b, off, len);
        }

        @Override
        public int skipBytes(int n) throws IOException
        {
            return in.skipBytes(n);
        }

        @Override
        public boolean readBoolean() throws IOException
        {
            return in.readBoolean();
        }

        @Override
        public byte readByte() throws IOException
        {
            return in.readByte();
        }

        @Override
        public int readUnsignedByte() throws IOException
        {
            return in.readUnsignedByte();
        }

        @Override
        public short readShort() throws IOException
        {
            return in.readShort();
        }

        @Override
        public int readUnsignedShort() throws IOException
        {
            return in.readUnsignedShort();
        }

        @Override
        public char readChar() throws IOException
        {
            return in.readChar();
        }

        @Override
        public int readInt() throws IOException
        {
            return in.readInt();
        }

        @Override
        public long readLong() throws IOException
        {
            return in.readLong();
        }

        @Override
        public float readFloat() throws IOException
        {
            return in.readFloat();
        }

        @Override
        public double readDouble() throws IOException
        {
            return in.readDouble();
        }

        @Override
        public String readLine() throws IOException
        {
            return in.readLine();
        }

        @Override
        public String readUTF() throws IOException
        {
            return in.readUTF();
        }
    }

    public static class DataInputPlusAdapter extends ForwardingDataInput implements DataInputPlus
    {
        public DataInputPlusAdapter(DataInput in)
        {
            super(in);
        }
    }

    /**
     * Wrapper around an InputStream that provides no buffering but can decode varints
     */
    public class DataInputStreamPlus extends DataInputStream implements DataInputPlus
    {
        public DataInputStreamPlus(InputStream is)
        {
            super(is);
        }
    }
}
