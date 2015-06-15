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
 *
 * @deprecated Where possible use NIODataInputStream which has a more efficient implementation of buffered input
 *             for most read methods
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
        return (int) VIntCoding.readVInt(input);
    }

    public long readLong() throws IOException
    {
        return VIntCoding.readVInt(input);
    }

    public int readUnsignedShort() throws IOException
    {
        return (short) VIntCoding.readVInt(input);
    }

    public short readShort() throws IOException
    {
        return (short) VIntCoding.readVInt(input);
    }
}
