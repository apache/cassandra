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

import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

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
            return new String(buffer.array(), buffer.arrayOffset() + offset, length + buffer.arrayOffset(), charset);

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
            return FBUtilities.EMPTY_BYTE_BUFFER;
          
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
}
