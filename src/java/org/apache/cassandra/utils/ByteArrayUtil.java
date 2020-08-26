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

package org.apache.cassandra.utils;

public class ByteArrayUtil
{
    private ByteArrayUtil()
    {

    }

    /*
     * Methods for unpacking primitive values from byte arrays starting at
     * given offsets.
     */

    public static boolean getBoolean(byte[] b)
    {
        return getBoolean(b, 0);
    }

    public static boolean getBoolean(byte[] b, int off)
    {
        return b[off] != 0;
    }

    /**
     * @return signed short encoded as big endian
     */
    public static short getShort(byte[] b)
    {
        return getShort(b, 0);
    }

    /**
     * @return signed short from the given offset encoded as big endian
     */
    public static short getShort(byte[] b, int off)
    {
        return (short) (b[off    ] << 8 |
                        b[off + 1] & 255);
    }

    /**
     * @return signed int encoded as big endian
     */
    public static int getInt(byte[] b)
    {
        return getInt(b, 0);
    }

    /**
     * @return signed int from the given offset encoded as big endian
     */
    public static int getInt(byte[] b, int off)
    {
        return (b[off    ] & 255) << 24 |
               (b[off + 1] & 255) << 16 |
               (b[off + 2] & 255) <<  8 |
               (b[off + 3] & 255);
    }

    /**
     * @return signed float encoded as big endian
     */
    public static float getFloat(byte[] b)
    {
        return getFloat(b, 0);
    }

    /**
     * @return signed float from the given offset encoded as big endian
     */
    public static float getFloat(byte[] b, int off)
    {
        return Float.intBitsToFloat(getInt(b, off));
    }

    /**
     * @return signed long encoded as big endian
     */
    public static long getLong(byte[] b)
    {
        return getLong(b, 0);
    }

    /**
     * @return signed long from the given offset encoded as big endian
     */
    public static long getLong(byte[] b, int off)
    {
        return ((long) b[off    ] & 255L) << 56 |
               ((long) b[off + 1] & 255L) << 48 |
               ((long) b[off + 2] & 255L) << 40 |
               ((long) b[off + 3] & 255L) << 32 |
               ((long) b[off + 4] & 255L) << 24 |
               ((long) b[off + 5] & 255L) << 16 |
               ((long) b[off + 6] & 255L) <<  8 |
               ((long) b[off + 7] & 255L);
    }

    /**
     * @return signed double from the given offset encoded as big endian
     */
    public static double getDouble(byte[] b)
    {
        return getDouble(b, 0);
    }

    /**
     * @return signed double from the given offset encoded as big endian
     */
    public static double getDouble(byte[] b, int off)
    {
        return Double.longBitsToDouble(getLong(b, off));
    }

    /*
     * Methods for packing primitive values into byte arrays starting at given
     * offsets.
     */

    public static void putBoolean(byte[] b, int off, boolean val)
    {
        ensureCapacity(b, off, 1);
        b[off] = (byte) (val ? 1 : 0);
    }

    /**
     * Store a signed short at the given offset encoded as big endian
     */
    public static void putShort(byte[] b, int off, short val)
    {
        ensureCapacity(b, off, Short.BYTES);
        b[off + 1] = (byte) (val      );
        b[off    ] = (byte) (val >>> 8);
    }

    /**
     * Store a signed int at the given offset encoded as big endian
     */
    public static void putInt(byte[] b, int off, int val)
    {
        ensureCapacity(b, off, Integer.BYTES);
        b[off + 3] = (byte) (val       );
        b[off + 2] = (byte) (val >>>  8);
        b[off + 1] = (byte) (val >>> 16);
        b[off    ] = (byte) (val >>> 24);
    }

    /**
     * Store a signed float at the given offset encoded as big endian
     */
    public static void putFloat(byte[] b, int off, float val)
    {
        putInt(b, off,  Float.floatToIntBits(val));
    }

    /**
     * Store a signed long at the given offset encoded as big endian
     */
    public static void putLong(byte[] b, int off, long val)
    {
        ensureCapacity(b, off, Long.BYTES);
        b[off + 7] = (byte) (val       );
        b[off + 6] = (byte) (val >>>  8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off    ] = (byte) (val >>> 56);
    }

    /**
     * Store a signed double at the given offset encoded as big endian
     */
    public static void putDouble(byte[] b, int off, double val)
    {
        putLong(b, off, Double.doubleToLongBits(val));
    }

    private static void ensureCapacity(byte[] b, int off, int len)
    {
        int writable = b.length - off;
        if (writable < len)
        {
            if (writable < 0)
                throw new IndexOutOfBoundsException("Attempted to write to offset " + off + " but array length is " + b.length);
            throw new IndexOutOfBoundsException("Attempted to write " + len + " bytes to array with remaining capacity of " + writable);
        }
    }
}
