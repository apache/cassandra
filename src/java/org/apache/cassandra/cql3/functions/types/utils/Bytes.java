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
package org.apache.cassandra.cql3.functions.types.utils;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Simple utility methods to make working with bytes (blob) easier.
 */
public final class Bytes
{

    private Bytes()
    {
    }

    private static final byte[] charToByte = new byte[256];
    private static final char[] byteToChar = new char[16];

    static
    {
        for (char c = 0; c < charToByte.length; ++c)
        {
            if (c >= '0' && c <= '9') charToByte[c] = (byte) (c - '0');
            else if (c >= 'A' && c <= 'F') charToByte[c] = (byte) (c - 'A' + 10);
            else if (c >= 'a' && c <= 'f') charToByte[c] = (byte) (c - 'a' + 10);
            else charToByte[c] = (byte) -1;
        }

        for (int i = 0; i < 16; ++i)
        {
            byteToChar[i] = Integer.toHexString(i).charAt(0);
        }
    }

    /*
     * We use reflexion to get access to a String protected constructor
     * (if available) so we can build avoid copy when creating hex strings.
     * That's stolen from Cassandra's code.
     */
    private static final Constructor<String> stringConstructor;

    static
    {
        Constructor<String> c;
        try
        {
            c = String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            c.setAccessible(true);
        }
        catch (Exception e)
        {
            c = null;
        }
        stringConstructor = c;
    }

    private static String wrapCharArray(char[] c)
    {
        if (c == null) return null;

        String s = null;
        if (stringConstructor != null)
        {
            try
            {
                s = stringConstructor.newInstance(0, c.length, c);
            }
            catch (Exception e)
            {
                // Swallowing as we'll just use a copying constructor
            }
        }
        return s == null ? new String(c) : s;
    }

    /**
     * Converts a blob to its CQL hex string representation.
     *
     * <p>A CQL blob string representation consist of the hexadecimal representation of the blob bytes
     * prefixed by "0x".
     *
     * @param bytes the blob/bytes to convert to a string.
     * @return the CQL string representation of {@code bytes}. If {@code bytes} is {@code null}, this
     * method returns {@code null}.
     */
    public static String toHexString(ByteBuffer bytes)
    {
        if (bytes == null) return null;

        if (bytes.remaining() == 0) return "0x";

        char[] array = new char[2 * (bytes.remaining() + 1)];
        array[0] = '0';
        array[1] = 'x';
        return toRawHexString(bytes, array, 2);
    }

    /**
     * Converts a blob to its CQL hex string representation.
     *
     * <p>A CQL blob string representation consist of the hexadecimal representation of the blob bytes
     * prefixed by "0x".
     *
     * @param byteArray the blob/bytes array to convert to a string.
     * @return the CQL string representation of {@code bytes}. If {@code bytes} is {@code null}, this
     * method returns {@code null}.
     */
    public static String toHexString(byte[] byteArray)
    {
        return toHexString(ByteBuffer.wrap(byteArray));
    }

    /**
     * Parse an hex string representing a CQL blob.
     *
     * <p>The input should be a valid representation of a CQL blob, i.e. it must start by "0x"
     * followed by the hexadecimal representation of the blob bytes.
     *
     * @param str the CQL blob string representation to parse.
     * @return the bytes corresponding to {@code str}. If {@code str} is {@code null}, this method
     * returns {@code null}.
     * @throws IllegalArgumentException if {@code str} is not a valid CQL blob string.
     */
    public static ByteBuffer fromHexString(String str)
    {
        if ((str.length() & 1) == 1)
            throw new IllegalArgumentException(
            "A CQL blob string must have an even length (since one byte is always 2 hexadecimal character)");

        if (str.charAt(0) != '0' || str.charAt(1) != 'x')
            throw new IllegalArgumentException("A CQL blob string must start with \"0x\"");

        return ByteBuffer.wrap(fromRawHexString(str, 2));
    }

    /**
     * Extract the content of the provided {@code ByteBuffer} as a byte array.
     *
     * <p>This method work with any type of {@code ByteBuffer} (direct and non-direct ones), but when
     * the {@code ByteBuffer} is backed by an array, this method will try to avoid copy when possible.
     * As a consequence, changes to the returned byte array may or may not reflect into the initial
     * {@code ByteBuffer}.
     *
     * @param bytes the buffer whose content to extract.
     * @return a byte array with the content of {@code bytes}. That array may be the array backing
     * {@code bytes} if this can avoid a copy.
     */
    public static byte[] getArray(ByteBuffer bytes)
    {
        int length = bytes.remaining();

        if (bytes.hasArray())
        {
            int boff = bytes.arrayOffset() + bytes.position();
            if (boff == 0 && length == bytes.array().length) return bytes.array();
            else return Arrays.copyOfRange(bytes.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] array = new byte[length];
        bytes.duplicate().get(array);
        return array;
    }

    private static String toRawHexString(ByteBuffer bytes, char[] array, int offset)
    {
        int size = bytes.remaining();
        int bytesOffset = bytes.position();
        assert array.length >= offset + 2 * size;
        for (int i = 0; i < size; i++)
        {
            int bint = bytes.get(i + bytesOffset);
            array[offset + i * 2] = byteToChar[(bint & 0xf0) >> 4];
            array[offset + 1 + i * 2] = byteToChar[bint & 0x0f];
        }
        return wrapCharArray(array);
    }

    /**
     * Converts a CQL hex string representation into a byte array.
     *
     * <p>A CQL blob string representation consist of the hexadecimal representation of the blob
     * bytes.
     *
     * @param str       the string converted in hex representation.
     * @param strOffset he offset for starting the string conversion
     * @return the byte array which the String was representing.
     */
    private static byte[] fromRawHexString(String str, int strOffset)
    {
        byte[] bytes = new byte[(str.length() - strOffset) / 2];
        for (int i = 0; i < bytes.length; i++)
        {
            byte halfByte1 = charToByte[str.charAt(strOffset + i * 2)];
            byte halfByte2 = charToByte[str.charAt(strOffset + i * 2 + 1)];
            if (halfByte1 == -1 || halfByte2 == -1)
                throw new IllegalArgumentException("Non-hex characters in " + str);
            bytes[i] = (byte) ((halfByte1 << 4) | halfByte2);
        }
        return bytes;
    }
}
