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
package org.apache.cassandra.utils.vint;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.io.util.AbstractDataInput;

/**
 * Borrows idea from
 * https://developers.google.com/protocol-buffers/docs/encoding#varints
 *
 * Should be used with EncodedDataOutputStream
 */
public class EncodedDataInputStream extends AbstractDataInput implements DataInput
{
    private DataInput input;

    public EncodedDataInputStream(DataInput input)
    {
        this.input = input;
    }

    public int skipBytes(int n) throws IOException
    {
        return input.skipBytes(n);
    }

    public int read() throws IOException
    {
        return input.readByte() & 0xFF;
    }

    public void seek(long position)
    {
        throw new UnsupportedOperationException();
    }

    public long getPosition()
    {
        throw new UnsupportedOperationException();
    }

    public long getPositionLimit()
    {
        throw new UnsupportedOperationException();
    }

    protected long length()
    {
        throw new UnsupportedOperationException();
    }

    /* as all of the integer types could be decoded using VInt we can use single method vintEncode */

    public int readInt() throws IOException
    {
        return (int) vintDecode();
    }

    public long readLong() throws IOException
    {
        return vintDecode();
    }

    public int readUnsignedShort() throws IOException
    {
        return (short) vintDecode();
    }
    
    public short readShort() throws IOException
    {
        return (short) vintDecode();
    }

    private long vintDecode() throws IOException
    {
        byte firstByte = input.readByte();
        int len = vintDecodeSize(firstByte);
        if (len == 1)
            return firstByte;
        long i = 0;
        for (int idx = 0; idx < len - 1; idx++)
        {
            byte b = input.readByte();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (vintIsNegative(firstByte) ? (i ^ -1L) : i);
    }

    private int vintDecodeSize(byte value)
    {
        if (value >= -112)
        {
            return 1;
        }
        else if (value < -120)
        {
            return -119 - value;
        }
        return -111 - value;
    }

    private boolean vintIsNegative(byte value)
    {
        return value < -120 || (value >= -112 && value < 0);
    }
}
