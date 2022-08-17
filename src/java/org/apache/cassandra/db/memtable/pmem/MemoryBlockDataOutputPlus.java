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

import java.io.IOException;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;

import com.intel.pmem.llpl.TransactionalMemoryBlock;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

/*Extending DataOutputStreamPlus to reuse a few methods defined in it */
public class MemoryBlockDataOutputPlus extends DataOutputStreamPlus implements DataOutputPlus
{
    private final TransactionalMemoryBlock block;
    private int position;

    public MemoryBlockDataOutputPlus(TransactionalMemoryBlock block, int initialPosition)
    {
        this.block = block;
        position = initialPosition;
    }

    public boolean hasPosition()
    {
        return true;
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
    public void write(ByteBuffer buffer) throws IOException
    {
        if (buffer.hasArray())
        {
            byte[] bufferArray = buffer.array();
            block.copyFromArray(bufferArray, buffer.arrayOffset() + buffer.position(), position, buffer.remaining());
            position += buffer.remaining();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void write(int b) throws IOException
    {
        block.setByte(position, (byte) b);
        position++;
    }

    @Override
    public void write(byte[] b) throws IOException
    {
        block.copyFromArray(b, 0, position, b.length);
        position += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        block.copyFromArray(b, off, position, len);
        position += len;
    }

    @Override
    public void writeBoolean(boolean v)
    {
        block.setByte(position, v ? (byte) 1 : (byte) 0);
        position++;
    }

    @Override
    public void writeByte(int v)
    {
        block.setByte(position, (byte) v);
        position++;
    }

    @Override
    public void writeShort(int v)
    {
        block.setShort(position, (short) v);
        position += 2;
    }

    @Override
    public void writeChar(int v)
    {
        block.setByte(position, (byte) v);
        position += Byte.BYTES;
    }

    @Override
    public void writeInt(int v)
    {
        block.setInt(position, v);
        position += Integer.BYTES;
    }

    @Override
    public void writeLong(long v)
    {
        block.setLong(position, v);
        position += Long.BYTES;
    }

    @Override
    public void writeFloat(float v)
    {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v)
    {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    public void writeBytes(String s)
    {
        for (int index = 0; index < s.length(); index++)
            writeByte(s.charAt(index));
    }

    @Override
    public void writeChars(String s)
    {
        for (int index = 0; index < s.length(); index++)
            writeChar(s.charAt(index));
    }

    /*Overriding writeUTF method to accomodate the little endianness of LLPL*/
    @Override
    public void writeUTF(String s) throws IOException
    {
        int strlen = s.length();
        if (strlen == 0)
        {
            return;
        }
        int utfCount = 0;
        int maxSize = 2;
        for (int i = 0; i < strlen; i++)
        {
            int ch = s.charAt(i);
            if ((ch > 0) & (ch <= 127))
                utfCount += 1;
            else if (ch <= 2047)
                utfCount += 2;
            else
                utfCount += maxSize = 3;
        }

        if (utfCount > 65535)
            throw new UTFDataFormatException(); //$NON-NLS-1$

        byte[] utfBytes = retrieveTemporaryBuffer(utfCount + 2);

        int bufferLength = utfBytes.length;
        if (utfCount == strlen)
        {
            utfBytes[0] = (byte) utfCount;
            utfBytes[1] = (byte) (utfCount >> 8);
            int firstIndex = 2;

            for (int offset = 0; offset < strlen; offset += bufferLength)
            {
                int runLength = Math.min(bufferLength - firstIndex, strlen - offset) + firstIndex;
                offset -= firstIndex;
                for (int i = firstIndex; i < runLength; i++)
                    utfBytes[i] = (byte) s.charAt(offset + i);
                this.write(utfBytes, 0, runLength);
                firstIndex = 0;
            }
        }
        else
        {
            int utfIndex = 2;
            int offset = 0;
            utfBytes[0] = (byte) utfCount;
            utfBytes[1] = (byte) (utfCount >> 8);

            while (strlen > 0)
            {
                int charRunLength = (utfBytes.length - utfIndex) / maxSize;
                if (charRunLength < 128 && charRunLength < strlen)
                {
                    this.write(utfBytes, 0, utfIndex);
                    utfIndex = 0;
                }
                if (charRunLength > strlen)
                    charRunLength = strlen;

                for (int i = 0; i < charRunLength; i++)
                {
                    char ch = s.charAt(offset + i);
                    if ((ch > 0) && (ch <= 127))
                    {
                        utfBytes[utfIndex++] = (byte) ch;
                    }
                    else if (ch <= 2047)
                    {
                        utfBytes[utfIndex++] = (byte) (0xc0 | (0x1f & (ch >> 6)));
                        utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & ch));
                    }
                    else
                    {
                        utfBytes[utfIndex++] = (byte) (0xe0 | (0x0f & (ch >> 12)));
                        utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & (ch >> 6)));
                        utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & ch));
                    }
                }
                offset += charRunLength;
                strlen -= charRunLength;
            }
            this.write(utfBytes, 0, utfIndex);
        }
    }

    public void writeUnsignedVInt(long i) throws IOException
    {
        int size = VIntCoding.computeUnsignedVIntSize(i);
        if (size == 1)
        {
            this.write((int)i);
            return;
        }

        this.write(VIntCoding.encodeUnsignedVInt(i, size), 0, size);
    }
}
