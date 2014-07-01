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

/*
 * BE ADVISED: New imports added here might introduce new dependencies for
 * the clientutil jar.  If in doubt, run the `ant test-clientutil-jar' target
 * afterward, and ensure the tests still pass.
 */

import java.io.*;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;

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
    public static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new byte[0]);

    @Inline
    public static int compareUnsigned(ByteBuffer o1, ByteBuffer o2)
    {
        return FastByteOperations.compareUnsigned(o1, o2);
    }

    @Inline
    public static int compare(byte[] o1, ByteBuffer o2)
    {
        return FastByteOperations.compareUnsigned(o1, 0, o1.length, o2);
    }

    @Inline
    public static int compare(ByteBuffer o1, byte[] o2)
    {
        return FastByteOperations.compareUnsigned(o1, o2, 0, o2.length);
    }

    /**
     * Decode a String representation.
     * This method assumes that the encoding charset is UTF_8.
     *
     * @param buffer a byte buffer holding the string representation
     * @return the decoded string
     */
    public static String string(ByteBuffer buffer) throws CharacterCodingException
    {
        return string(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Decode a String representation.
     * This method assumes that the encoding charset is UTF_8.
     *
     * @param buffer a byte buffer holding the string representation
     * @param position the starting position in {@code buffer} to start decoding from
     * @param length the number of bytes from {@code buffer} to use
     * @return the decoded string
     */
    public static String string(ByteBuffer buffer, int position, int length) throws CharacterCodingException
    {
        return string(buffer, position, length, StandardCharsets.UTF_8);
    }

    /**
     * Decode a String representation.
     *
     * @param buffer a byte buffer holding the string representation
     * @param position the starting position in {@code buffer} to start decoding from
     * @param length the number of bytes from {@code buffer} to use
     * @param charset the String encoding charset
     * @return the decoded string
     */
    public static String string(ByteBuffer buffer, int position, int length, Charset charset) throws CharacterCodingException
    {
        ByteBuffer copy = buffer.duplicate();
        copy.position(position);
        copy.limit(copy.position() + length);
        return string(copy, charset);
    }

    /**
     * Decode a String representation.
     *
     * @param buffer a byte buffer holding the string representation
     * @param charset the String encoding charset
     * @return the decoded string
     */
    public static String string(ByteBuffer buffer, Charset charset) throws CharacterCodingException
    {
        return charset.newDecoder().decode(buffer.duplicate()).toString();
    }

    /**
     * You should almost never use this.  Instead, use the write* methods to avoid copies.
     */
    public static byte[] getArray(ByteBuffer buffer)
    {
        int length = buffer.remaining();

        if (buffer.hasArray())
        {
            int boff = buffer.arrayOffset() + buffer.position();
            return Arrays.copyOfRange(buffer.array(), boff, boff + length);
        }
        // else, DirectByteBuffer.get() is the fastest route
        byte[] bytes = new byte[length];
        buffer.duplicate().get(bytes);

        return bytes;
    }

    /**
     * ByteBuffer adaptation of org.apache.commons.lang3.ArrayUtils.lastIndexOf method
     *
     * @param buffer the array to traverse for looking for the object, may be <code>null</code>
     * @param valueToFind the value to find
     * @param startIndex the start index (i.e. BB position) to travers backwards from
     * @return the last index (i.e. BB position) of the value within the array
     * [between buffer.position() and buffer.limit()]; <code>-1</code> if not found.
     */
    public static int lastIndexOf(ByteBuffer buffer, byte valueToFind, int startIndex)
    {
        assert buffer != null;

        if (startIndex < buffer.position())
        {
            return -1;
        }
        else if (startIndex >= buffer.limit())
        {
            startIndex = buffer.limit() - 1;
        }

        for (int i = startIndex; i >= buffer.position(); i--)
        {
            if (valueToFind == buffer.get(i))
                return i;
        }

        return -1;
    }

    /**
     * Encode a String in a ByteBuffer using UTF_8.
     *
     * @param s the string to encode
     * @return the encoded string
     */
    public static ByteBuffer bytes(String s)
    {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Encode a String in a ByteBuffer using the provided charset.
     *
     * @param s the string to encode
     * @param charset the String encoding charset to use
     * @return the encoded string
     */
    public static ByteBuffer bytes(String s, Charset charset)
    {
        return ByteBuffer.wrap(s.getBytes(charset));
    }

    /**
     * @return a new copy of the data in @param buffer
     * USUALLY YOU SHOULD USE ByteBuffer.duplicate() INSTEAD, which creates a new Buffer
     * (so you can mutate its position without affecting the original) without copying the underlying array.
     */
    public static ByteBuffer clone(ByteBuffer buffer)
    {
        assert buffer != null;

        if (buffer.remaining() == 0)
            return EMPTY_BYTE_BUFFER;

        ByteBuffer clone = ByteBuffer.allocate(buffer.remaining());

        if (buffer.hasArray())
        {
            System.arraycopy(buffer.array(), buffer.arrayOffset() + buffer.position(), clone.array(), 0, buffer.remaining());
        }
        else
        {
            clone.put(buffer.duplicate());
            clone.flip();
        }

        return clone;
    }

    public static void arrayCopy(ByteBuffer src, int srcPos, byte[] dst, int dstPos, int length)
    {
        FastByteOperations.copy(src, srcPos, dst, dstPos, length);
    }

    /**
     * Transfer bytes from one ByteBuffer to another.
     * This function acts as System.arrayCopy() but for ByteBuffers.
     *
     * @param src the source ByteBuffer
     * @param srcPos starting position in the source ByteBuffer
     * @param dst the destination ByteBuffer
     * @param dstPos starting position in the destination ByteBuffer
     * @param length the number of bytes to copy
     */
    public static void arrayCopy(ByteBuffer src, int srcPos, ByteBuffer dst, int dstPos, int length)
    {
        FastByteOperations.copy(src, srcPos, dst, dstPos, length);
    }

    public static void writeWithLength(ByteBuffer bytes, DataOutputPlus out) throws IOException
    {
        out.writeInt(bytes.remaining());
        out.write(bytes);
    }

    public static void writeWithLength(byte[] bytes, DataOutput out) throws IOException
    {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void writeWithShortLength(ByteBuffer buffer, DataOutputPlus out) throws IOException
    {
        int length = buffer.remaining();
        assert 0 <= length && length <= FBUtilities.MAX_UNSIGNED_SHORT : length;
        out.writeShort(length);
        out.write(buffer);
    }

    public static void writeWithShortLength(byte[] buffer, DataOutput out) throws IOException
    {
        int length = buffer.length;
        assert 0 <= length && length <= FBUtilities.MAX_UNSIGNED_SHORT : length;
        out.writeShort(length);
        out.write(buffer);
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
    public static int readShortLength(DataInput in) throws IOException
    {
        return in.readUnsignedShort();
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
    public static ByteBuffer skipShortLength(DataInput in) throws IOException
    {
        int skip = readShortLength(in);
        FileUtils.skipBytesFully(in, skip);
        return null;
    }

    public static ByteBuffer read(DataInput in, int length) throws IOException
    {
        if (length == 0)
            return EMPTY_BYTE_BUFFER;

        if (in instanceof FileDataInput)
            return ((FileDataInput) in).readBytes(length);

        byte[] buff = new byte[length];
        in.readFully(buff);
        return ByteBuffer.wrap(buff);
    }

    public static byte[] readBytes(DataInput in, int length) throws IOException
    {
        assert length > 0 : "length is not > 0: " + length;
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
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

    public static long toLong(ByteBuffer bytes)
    {
        return bytes.getLong(bytes.position());
    }

    public static float toFloat(ByteBuffer bytes)
    {
        return bytes.getFloat(bytes.position());
    }

    public static double toDouble(ByteBuffer bytes)
    {
        return bytes.getDouble(bytes.position());
    }

    public static ByteBuffer bytes(int i)
    {
        return ByteBuffer.allocate(4).putInt(0, i);
    }

    public static ByteBuffer bytes(long n)
    {
        return ByteBuffer.allocate(8).putLong(0, n);
    }

    public static ByteBuffer bytes(float f)
    {
        return ByteBuffer.allocate(4).putFloat(0, f);
    }

    public static ByteBuffer bytes(double d)
    {
        return ByteBuffer.allocate(8).putDouble(0, d);
    }

    public static InputStream inputStream(ByteBuffer bytes)
    {
        final ByteBuffer copy = bytes.duplicate();

        return new InputStream()
        {
            public int read()
            {
                if (!copy.hasRemaining())
                    return -1;

                return copy.get() & 0xFF;
            }

            @Override
            public int read(byte[] bytes, int off, int len)
            {
                if (!copy.hasRemaining())
                    return -1;

                len = Math.min(len, copy.remaining());
                copy.get(bytes, off, len);
                return len;
            }

            @Override
            public int available()
            {
                return copy.remaining();
            }
        };
    }

    public static String bytesToHex(ByteBuffer bytes)
    {
        final int offset = bytes.position();
        final int size = bytes.remaining();
        final char[] c = new char[size * 2];
        for (int i = 0; i < size; i++)
        {
            final int bint = bytes.get(i+offset);
            c[i * 2] = Hex.byteToChar[(bint & 0xf0) >> 4];
            c[1 + i * 2] = Hex.byteToChar[bint & 0x0f];
        }
        return Hex.wrapCharArray(c);
    }

    public static ByteBuffer hexToBytes(String str)
    {
        return ByteBuffer.wrap(Hex.hexToBytes(str));
    }

    /**
     * Compare two ByteBuffer at specified offsets for length.
     * Compares the non equal bytes as unsigned.
     * @param bytes1 First byte buffer to compare.
     * @param offset1 Position to start the comparison at in the first array.
     * @param bytes2 Second byte buffer to compare.
     * @param offset2 Position to start the comparison at in the second array.
     * @param length How many bytes to compare?
     * @return -1 if byte1 is less than byte2, 1 if byte2 is less than byte1 or 0 if equal.
     */
    public static int compareSubArrays(ByteBuffer bytes1, int offset1, ByteBuffer bytes2, int offset2, int length)
    {
        if (bytes1 == null)
            return bytes2 == null ? 0 : -1;
        if (bytes2 == null) return 1;

        assert bytes1.limit() >= offset1 + length : "The first byte array isn't long enough for the specified offset and length.";
        assert bytes2.limit() >= offset2 + length : "The second byte array isn't long enough for the specified offset and length.";
        for (int i = 0; i < length; i++)
        {
            byte byte1 = bytes1.get(offset1 + i);
            byte byte2 = bytes2.get(offset2 + i);
            if (byte1 == byte2)
                continue;
            // compare non-equal bytes as unsigned
            return (byte1 & 0xFF) < (byte2 & 0xFF) ? -1 : 1;
        }
        return 0;
    }

    public static ByteBuffer bytes(InetAddress address)
    {
        return ByteBuffer.wrap(address.getAddress());
    }

    public static ByteBuffer bytes(UUID uuid)
    {
        return ByteBuffer.wrap(UUIDGen.decompose(uuid));
    }

    // Returns whether {@code prefix} is a prefix of {@code value}.
    public static boolean isPrefix(ByteBuffer prefix, ByteBuffer value)
    {
        if (prefix.remaining() > value.remaining())
            return false;

        int diff = value.remaining() - prefix.remaining();
        return prefix.equals(value.duplicate().limit(value.remaining() - diff));
    }

    /** trims size of bytebuffer to exactly number of bytes in it, to do not hold too much memory */
    public static ByteBuffer minimalBufferFor(ByteBuffer buf)
    {
        return buf.capacity() > buf.remaining() || !buf.hasArray() ? ByteBuffer.wrap(getArray(buf)) : buf;
    }

    // Doesn't change bb position
    public static int getShortLength(ByteBuffer bb, int position)
    {
        int length = (bb.get(position) & 0xFF) << 8;
        return length | (bb.get(position + 1) & 0xFF);
    }

    // changes bb position
    public static int readShortLength(ByteBuffer bb)
    {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    // changes bb position
    public static void writeShortLength(ByteBuffer bb, int length)
    {
        bb.put((byte) ((length >> 8) & 0xFF));
        bb.put((byte) (length & 0xFF));
    }

    // changes bb position
    public static ByteBuffer readBytes(ByteBuffer bb, int length)
    {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    // changes bb position
    public static ByteBuffer readBytesWithShortLength(ByteBuffer bb)
    {
        int length = readShortLength(bb);
        return readBytes(bb, length);
    }
}
