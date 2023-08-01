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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

// mostly copied from java.io.Bits
public class ByteArrayUtil
{
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    public static int compareUnsigned(byte[] o1, byte[] o2)
    {
        return FastByteOperations.compareUnsigned(o1, 0, o1.length, o2, 0, o2.length);
    }

    public static int compareUnsigned(byte[] o1, int off1, byte[] o2, int off2, int len)
    {
        return FastByteOperations.compareUnsigned(o1, off1, len, o2, off2, len);
    }

    public static byte[] bytes(byte b)
    {
        return new byte[] {b};
    }

    public static byte[] bytes(int v)
    {
        byte[] b = new byte[TypeSizes.sizeof(v)];
        putInt(b, 0, v);
        return b;
    }

    public static byte[] bytes(long v)
    {
        byte[] b = new byte[TypeSizes.sizeof(v)];
        putLong(b, 0, v);
        return b;
    }

    public static byte[] bytes(short v)
    {
        byte[] b = new byte[TypeSizes.sizeof(v)];
        putShort(b, 0, v);
        return b;
    }

    public static byte[] bytes(float v)
    {
        byte[] b = new byte[TypeSizes.sizeof(v)];
        putFloat(b, 0, v);
        return b;
    }

    public static byte[] bytes(double v)
    {
        byte[] b = new byte[TypeSizes.sizeof(v)];
        putDouble(b, 0, v);
        return b;
    }

    public static byte[] bytes(String v)
    {
        return v.getBytes();
    }

    public static byte[] bytes(String v, Charset charset)
    {
        return v.getBytes(charset);
    }

    /*
     * Methods for unpacking primitive values from byte arrays starting at
     * given offsets.
     */

    public static boolean getBoolean(byte[] b, int off) {
        return b[off] != 0;
    }

    public static char getChar(byte[] b, int off) {
        return (char) ((b[off + 1] & 0xFF) +
                       (b[off] << 8));
    }

    public static short getShort(byte[] b, int off) {
        return (short) ((b[off + 1] & 0xFF) +
                        (b[off] << 8));
    }

    public static int getUnsignedShort(byte[] b, int off) {
        return ((b[off] & 0xFF) << 8) | (b[off + 1] & 0xFF);
    }

    public static int getInt(byte[] b, int off) {
        return ((b[off + 3] & 0xFF)      ) +
               ((b[off + 2] & 0xFF) <<  8) +
               ((b[off + 1] & 0xFF) << 16) +
               ((b[off    ]       ) << 24);
    }

    public static int getInt(byte[] b) {
        return getInt(b, 0);
    }

    public static float getFloat(byte[] b, int off) {
        return Float.intBitsToFloat(getInt(b, off));
    }

    public static long getLong(byte[] b)
    {
        return getLong(b, 0);
    }

    public static long getLong(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL)      ) +
               ((b[off + 6] & 0xFFL) <<  8) +
               ((b[off + 5] & 0xFFL) << 16) +
               ((b[off + 4] & 0xFFL) << 24) +
               ((b[off + 3] & 0xFFL) << 32) +
               ((b[off + 2] & 0xFFL) << 40) +
               ((b[off + 1] & 0xFFL) << 48) +
               (((long) b[off])      << 56);
    }

    public static double getDouble(byte[] b, int off) {
        return Double.longBitsToDouble(getLong(b, off));
    }

    /*
     * Methods for packing primitive values into byte arrays starting at given
     * offsets.
     */

    public static void putBoolean(byte[] b, int off, boolean val) {
        b[off] = (byte) (val ? 1 : 0);
    }

    public static void putChar(byte[] b, int off, char val) {
        b[off + 1] = (byte) (val      );
        b[off    ] = (byte) (val >>> 8);
    }

    public static void putShort(byte[] b, int off, short val) {
        b[off + 1] = (byte) (val      );
        b[off    ] = (byte) (val >>> 8);
    }

    public static void putInt(byte[] b, int off, int val) {
        b[off + 3] = (byte) (val       );
        b[off + 2] = (byte) (val >>>  8);
        b[off + 1] = (byte) (val >>> 16);
        b[off    ] = (byte) (val >>> 24);
    }

    public static void putFloat(byte[] b, int off, float val) {
        putInt(b, off,  Float.floatToIntBits(val));
    }

    public static void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) (val       );
        b[off + 6] = (byte) (val >>>  8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off    ] = (byte) (val >>> 56);
    }

    public static void putDouble(byte[] b, int off, double val) {
        putLong(b, off, Double.doubleToLongBits(val));
    }

    public static String bytesToHex(byte[] bytes)
    {
        return Hex.bytesToHex(bytes, 0, bytes.length);
    }

    public static byte[] hexToBytes(String hex)
    {
        return Hex.hexToBytes(hex);
    }

    public static String string(byte[] bytes) throws CharacterCodingException
    {
        return ByteBufferUtil.string(ByteBuffer.wrap(bytes));
    }

    public static String string(byte[] buffer, Charset charset) throws CharacterCodingException
    {
        return new String(buffer, charset);
    }

    public static void writeWithLength(byte[] bytes, DataOutput out) throws IOException
    {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void writeWithShortLength(byte[] buffer, DataOutput out) throws IOException
    {
        int length = buffer.length;
        assert length <= FBUtilities.MAX_UNSIGNED_SHORT
         : String.format("Attempted serializing to buffer exceeded maximum of %s bytes: %s", FBUtilities.MAX_UNSIGNED_SHORT, length);
        out.writeShort(length);
        out.write(buffer);
    }

    public static void writeWithVIntLength(byte[] bytes, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(bytes.length);
        out.write(bytes);
    }

    public static int serializedSizeWithVIntLength(byte[] bytes)
    {
        return TypeSizes.sizeofUnsignedVInt(bytes.length) + bytes.length;
    }

    public static byte[] readWithLength(DataInput in) throws IOException
    {
        byte[] b = new byte[in.readInt()];
        in.readFully(b);
        return b;
    }

    public static byte[] readWithShortLength(DataInput in) throws IOException
    {
        byte[] b = new byte[in.readUnsignedShort()];
        in.readFully(b);
        return b;
    }

    public static byte[] readWithVIntLength(DataInputPlus in) throws IOException
    {
        int length = in.readUnsignedVInt32();
        if (length < 0)
            throw new IOException("Corrupt (negative) value length encountered");

        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    public static void copyBytes(byte[] src, int srcPos, byte[] dst, int dstPos, int length)
    {
        System.arraycopy(src, srcPos, dst, dstPos, length);
    }

    public static void copyBytes(byte[] src, int srcPos, ByteBuffer dst, int dstPos, int length)
    {
        FastByteOperations.copy(src, srcPos, dst, dstPos, length);
    }
}
