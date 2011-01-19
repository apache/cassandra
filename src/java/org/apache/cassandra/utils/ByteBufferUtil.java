/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.commons.lang.ArrayUtils;

/**
 * Utility methods to make ByteBuffers less painful
 * The following should illustrate the different ways byte buffers can be used 
 * 
 *        public void testArrayOffet()
 *        {
 *                
 *            byte[] b = "test_slice_array".getBytes();
 *            ByteBuffer bb = ByteBuffer.allocate(1024);
 *    
 *            assert bb.position() == 0;
 *            assert bb.limit()    == 1024;
 *            assert bb.capacity() == 1024;
 *    
 *            bb.put(b);
 *            
 *            assert bb.position()  == b.length;
 *            assert bb.remaining() == bb.limit() - bb.position();
 *            
 *            ByteBuffer bb2 = bb.slice();
 *            
 *            assert bb2.position()    == 0;
 *            
 *            //slice should begin at other buffers current position
 *            assert bb2.arrayOffset() == bb.position();
 *            
 *            //to match the position in the underlying array one needs to 
 *            //track arrayOffset
 *            assert bb2.limit()+bb2.arrayOffset() == bb.limit();
 *            
 *           
 *            assert bb2.remaining() == bb.remaining();
 *                             
 *        }
 *
 * }
 *
 */
public class ByteBufferUtil
{
    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(ArrayUtils.EMPTY_BYTE_ARRAY);

    public static int compareUnsigned(ByteBuffer o1, ByteBuffer o2)
    {
        assert o1 != null;
        assert o2 != null;

        int minLength = Math.min(o1.remaining(), o2.remaining());
        for (int x = 0, i = o1.position(), j = o2.position(); x < minLength; x++, i++, j++)
        {
            if (o1.get(i) == o2.get(j))
                continue;
            // compare non-equal bytes as unsigned
            return (o1.get(i) & 0xFF) < (o2.get(j) & 0xFF) ? -1 : 1;
        }

        return (o1.remaining() == o2.remaining()) ? 0 : ((o1.remaining() < o2.remaining()) ? -1 : 1);
    }
    
    public static int compare(byte[] o1, ByteBuffer o2)
    {
        return compareUnsigned(ByteBuffer.wrap(o1), o2);
    }

    public static int compare(ByteBuffer o1, byte[] o2)
    {
        return compareUnsigned(o1, ByteBuffer.wrap(o2));
    }

    public static String string(ByteBuffer buffer)
    {
        return string(buffer, Charset.defaultCharset());
    }

    public static String string(ByteBuffer buffer, Charset charset)
    {
        return string(buffer, buffer.position(), buffer.remaining(), charset);
    }

    public static String string(ByteBuffer buffer, int offset, int length)
    {
        return string(buffer, offset, length, Charset.defaultCharset());
    }

    public static String string(ByteBuffer buffer, int offset, int length, Charset charset)
    {
        if (buffer.hasArray())
            return new String(buffer.array(), buffer.arrayOffset() + offset, length, charset);

        byte[] buff = getArray(buffer, offset, length);
        return new String(buff, charset);
    }

    /**
     * You should almost never use this.  Instead, use the write* methods to avoid copies.
     */
    public static byte[] getArray(ByteBuffer buffer)
    {
        return getArray(buffer, buffer.position(), buffer.remaining());
    }

    public static byte[] getArray(ByteBuffer b, int start, int length)
    {
        if (b.hasArray())
            return Arrays.copyOfRange(b.array(), start + b.arrayOffset(), start + length + b.arrayOffset());

        byte[] bytes = new byte[length];

        for (int i = 0; i < length; i++)
        {
            bytes[i] = b.get(start++);
        }

        return bytes;
    }

    /**
     * ByteBuffer adoption of org.apache.commons.lang.ArrayUtils.lastIndexOf method
     *
     * @param buffer the array to traverse for looking for the object, may be <code>null</code>
     * @param valueToFind the value to find
     * @param startIndex the start index to travers backwards from
     * @return the last index of the value within the array,
     * <code>-1</code> if not found or <code>null</code> array input
     */
    public static int lastIndexOf(ByteBuffer buffer, byte valueToFind, int startIndex)
    {
        if (buffer == null)
        {
            return -1;
        }

        if (startIndex < 0)
        {
            return -1;
        }
        else if (startIndex >= buffer.limit())
        {
            startIndex = buffer.limit() - 1;
        }

        for (int i = startIndex; i >= 0; i--)
        {
            if (valueToFind == buffer.get(i))
            {
                return i;
            }
        }

        return -1;
    }

    public static ByteBuffer bytes(String s) 
    { 
        try
        {
            return ByteBuffer.wrap(s.getBytes("UTF-8"));
        }
        catch (UnsupportedEncodingException e)
        {
           throw new RuntimeException(e);
        } 
    }
    
    public static ByteBuffer clone(ByteBuffer o)
    {
        assert o != null;
        
        if (o.remaining() == 0)
            return EMPTY_BYTE_BUFFER;
          
        ByteBuffer clone = ByteBuffer.allocate(o.remaining());

        if (o.isDirect())
        {
            for (int i = o.position(); i < o.limit(); i++)
            {
                clone.put(o.get(i));
            }
            clone.flip();
        }
        else
        {
            System.arraycopy(o.array(), o.arrayOffset() + o.position(), clone.array(), 0, o.remaining());
        }

        return clone;
    }

    public static void arrayCopy(ByteBuffer buffer, int position, byte[] bytes, int offset, int length)
    {
        if (buffer.hasArray())
        {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + position, bytes, offset, length);
        }
        else
        {
            for (int i = 0; i < length; i++)
            {
                bytes[offset++] = buffer.get(position++);
            }
        }
    }

    public static void writeWithLength(ByteBuffer bytes, DataOutput out) throws IOException
    {
        out.writeInt(bytes.remaining());
        write(bytes, out); // writing data bytes to output source
    }

    public static void write(ByteBuffer buffer, DataOutput out) throws IOException
    {
        if (buffer.hasArray())
        {
            out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        else
        {
            for (int i = buffer.position(); i < buffer.limit(); i++)
            {
                out.writeByte(buffer.get(i));
            }
        }
    }

    public static void writeWithShortLength(ByteBuffer buffer, DataOutput out)
    {
        int length = buffer.remaining();
        assert 0 <= length && length <= FBUtilities.MAX_UNSIGNED_SHORT;
        try
        {
            out.writeByte((length >> 8) & 0xFF);
            out.writeByte(length & 0xFF);
            write(buffer, out); // writing data bytes to output source
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer readWithLength(DataInput in) throws IOException
    {
        int length = in.readInt();
        if (length < 0)
        {
            throw new IOException("Corrupt (negative) value length encountered");
        }

        return ByteBufferUtil.read(in, length);
    }

    /* @return An unsigned short in an integer. */
    private static int readShortLength(DataInput in) throws IOException
    {
        int length = (in.readByte() & 0xFF) << 8;
        return length | (in.readByte() & 0xFF);
    }

    /**
     * @param in data input
     * @return An unsigned short in an integer.
     * @throws IOException if an I/O error occurs.
     */
    public static ByteBuffer readWithShortLength(DataInput in) throws IOException
    {
        return ByteBufferUtil.read(in, readShortLength(in));
    }

    /**
     * @param in data input
     * @return null
     * @throws IOException if an I/O error occurs.
     */
    public static void skipShortLength(DataInput in) throws IOException
    {
        int skip = readShortLength(in);
        while (skip > 0)
        {
            int skipped = in.skipBytes(skip);
            if (skipped == 0) throw new EOFException();
            skip -= skipped;
        }
    }

    private static ByteBuffer read(DataInput in, int length) throws IOException
    {
        ByteBuffer array;

        if (in instanceof FileDataInput)
        {
            array = ((FileDataInput) in).readBytes(length);
        }
        else
        {
            byte[] buff = new byte[length];
            in.readFully(buff);
            array = ByteBuffer.wrap(buff);
        }

        return array;
    }

    /**
     * Convert a byte buffer to an integer.
     * Does not change the byte buffer position.
     *
     * @param bytes byte buffer to convert to integer
     * @return int representation of the byte buffer
     */
    public static int toInt(ByteBuffer bytes)
    {
        return bytes.getInt(bytes.position());
    }

    public static ByteBuffer bytes(int i)
    {
        return ByteBuffer.allocate(4).putInt(0, i);
    }

    public static ByteBuffer bytes(long n)
    {
        return ByteBuffer.allocate(8).putLong(0, n);
    }

    public static InputStream inputStream(ByteBuffer bytes)
    {
        final ByteBuffer copy = bytes.duplicate();

        return new InputStream()
        {
            public int read() throws IOException
            {
                if (!copy.hasRemaining())
                    return -1;

                return copy.get();
            }

            public int read(byte[] bytes, int off, int len) throws IOException
            {
                len = Math.min(len, copy.remaining());
                copy.get(bytes, off, len);

                return len;
            }
        };
    }

    public static String bytesToHex(ByteBuffer bytes)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = bytes.position(); i < bytes.limit(); i++)
        {
            int bint = bytes.get(i) & 0xff;
            if (bint <= 0xF)
                // toHexString does not 0 pad its results.
                sb.append("0");
            sb.append(Integer.toHexString(bint));
        }
        return sb.toString();
    }

    public static ByteBuffer hexToBytes(String str)
    {
        return ByteBuffer.wrap(FBUtilities.hexToBytes(str));
    }
}
