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
package org.apache.cassandra.utils.bytecomparable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.marshal.ValueAccessor;

/**
 * Contains inverse transformation utilities for {@link ByteSource}s.
 *
 * See ByteComparable.md for details about the encoding scheme.
 */
public final class ByteSourceInverse
{
    private static final int INITIAL_BUFFER_CAPACITY = 32;
    private static final int BYTE_ALL_BITS = 0xFF;
    private static final int BYTE_NO_BITS = 0x00;
    private static final int BYTE_SIGN_BIT = 1 << 7;
    private static final int SHORT_SIGN_BIT = 1 << 15;
    private static final int INT_SIGN_BIT = 1 << 31;
    private static final long LONG_SIGN_BIT = 1L << 63;

    /**
     * Consume the given number of bytes and produce a long from them, effectively treating the bytes as a big-endian
     * unsigned encoding of the number.
     */
    public static long getUnsignedFixedLengthAsLong(ByteSource byteSource, int length)
    {
        Preconditions.checkNotNull(byteSource);
        Preconditions.checkArgument(length >= 1 && length <= 8, "Between 1 and 8 bytes can be read at a time");

        long result = 0;
        for (int i = 0; i < length; ++i)
            result = (result << 8) | getAndCheckByte(byteSource, i, length);  // note: this must use the unsigned byte value

        return result;
    }

    /**
     * Produce the bytes for an encoded signed fixed-length number.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     */
    public static <V> V getSignedFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        Preconditions.checkNotNull(byteSource);
        Preconditions.checkArgument(length >= 1, "At least 1 byte should be read");

        V result = accessor.allocate(length);
        // The first byte needs to have its sign flipped
        accessor.putByte(result, 0, (byte) (getAndCheckByte(byteSource, 0, length) ^ BYTE_SIGN_BIT));
        // and the rest can be retrieved unchanged.
        for (int i = 1; i < length; ++i)
            accessor.putByte(result, i, (byte) getAndCheckByte(byteSource, i, length));
        return result;
    }

    /**
     * Produce the bytes for an encoded signed fixed-length number, also translating null to empty buffer.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     */
    public static <V> V getOptionalSignedFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        return byteSource == null ? accessor.empty() : getSignedFixedLength(accessor, byteSource, length);
    }

    /**
     * Produce the bytes for an encoded signed fixed-length floating-point number.
     * If sign bit is on, returns negated bytes. If not, clears the sign bit and passes the rest of the bytes unchanged.
     */
    public static <V> V getSignedFixedLengthFloat(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        Preconditions.checkNotNull(byteSource);
        Preconditions.checkArgument(length >= 1, "At least 1 byte should be read");

        V result = accessor.allocate(length);

        int xor;
        int first = getAndCheckByte(byteSource, 0, length);
        if (first < 0x80)
        {
            // Negative number. Invert all bits.
            xor = BYTE_ALL_BITS;
            first ^= xor;
        }
        else
        {
            // Positive number. Invert only the sign bit.
            xor = BYTE_NO_BITS;
            first ^= BYTE_SIGN_BIT;
        }
        accessor.putByte(result, 0, (byte) first);

        // xor is now applied to the rest of the bytes to flip their bits if necessary.
        for (int i = 1; i < length; ++i)
            accessor.putByte(result, i, (byte) (getAndCheckByte(byteSource, i, length) ^ xor));

        return result;
    }

    /**
     * Produce the bytes for an encoded signed fixed-length floating-point number, also translating null to an empty
     * buffer.
     * If sign bit is on, returns negated bytes. If not, clears the sign bit and passes the rest of the bytes unchanged.
     */
    public static <V> V getOptionalSignedFixedLengthFloat(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        return byteSource == null ? accessor.empty() : getSignedFixedLengthFloat(accessor, byteSource, length);
    }

    /**
     * Consume the next length bytes from the source unchanged.
     */
    public static <V> V getFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        Preconditions.checkNotNull(byteSource);
        Preconditions.checkArgument(length >= 1, "At least 1 byte should be read");

        V result = accessor.allocate(length);
        for (int i = 0; i < length; ++i)
            accessor.putByte(result, i, (byte) getAndCheckByte(byteSource, i, length));
        return result;
    }

    /**
     * Consume the next length bytes from the source unchanged, also translating null to an empty buffer.
     */
    public static <V> V getOptionalFixedLength(ValueAccessor<V> accessor, ByteSource byteSource, int length)
    {
        return byteSource == null ? accessor.empty() : getFixedLength(accessor, byteSource, length);
    }

    /**
     * Consume the next {@code int} from the current position of the given {@link ByteSource}. The source position is
     * modified accordingly (moved 4 bytes forward).
     * <p>
     * The source is not strictly required to represent just the encoding of an {@code int} value, so theoretically
     * this API could be used for reading data in 4-byte strides. Nevertheless its usage is fairly limited because:
     * <ol>
     *     <li>...it presupposes signed fixed-length encoding for the encoding of the original value</li>
     *     <li>...it decodes the data returned on each stride as an {@code int} (i.e. it inverts its leading bit)</li>
     *     <li>...it doesn't provide any meaningful guarantees (with regard to throwing) in case there are not enough
     *     bytes to read, in case a special escape value was not interpreted as such, etc.</li>
     * </ol>
     * </p>
     *
     * @param byteSource A non-null byte source, containing at least 4 bytes.
     */
    public static int getSignedInt(ByteSource byteSource)
    {
        return (int) getUnsignedFixedLengthAsLong(byteSource, 4) ^ INT_SIGN_BIT;
    }

    /**
     * Consume the next {@code long} from the current position of the given {@link ByteSource}. The source position is
     * modified accordingly (moved 8 bytes forward).
     * <p>
     * The source is not strictly required to represent just the encoding of a {@code long} value, so theoretically
     * this API could be used for reading data in 8-byte strides. Nevertheless its usage is fairly limited because:
     * <ol>
     *     <li>...it presupposes signed fixed-length encoding for the encoding of the original value</li>
     *     <li>...it decodes the data returned on each stride as a {@code long} (i.e. it inverts its leading bit)</li>
     *     <li>...it doesn't provide any meaningful guarantees (with regard to throwing) in case there are not enough
     *     bytes to read, in case a special escape value was not interpreted as such, etc.</li>
     * </ol>
     * </p>
     *
     * @param byteSource A non-null byte source, containing at least 8 bytes.
     */
    public static long getSignedLong(ByteSource byteSource)
    {
        return getUnsignedFixedLengthAsLong(byteSource, 8) ^ LONG_SIGN_BIT;
    }

    /**
     * Converts the given {@link ByteSource} to a {@code byte}.
     *
     * @param byteSource A non-null byte source, containing at least 1 byte.
     */
    public static byte getSignedByte(ByteSource byteSource)
    {
        return (byte) (getAndCheckByte(Preconditions.checkNotNull(byteSource), 0, 1) ^ BYTE_SIGN_BIT);
    }

    /**
     * Converts the given {@link ByteSource} to a {@code short}. All terms and conditions valid for
     * {@link #getSignedInt(ByteSource)} and {@link #getSignedLong(ByteSource)} translate to this as well.
     *
     * @param byteSource A non-null byte source, containing at least 2 bytes.
     *
     * @see #getSignedInt(ByteSource)
     * @see #getSignedLong(ByteSource)
     */
    public static short getSignedShort(ByteSource byteSource)
    {
        return (short) (getUnsignedFixedLengthAsLong(byteSource, 2) ^ SHORT_SIGN_BIT);
    }

    /**
     * Decode a variable-length signed integer.
     */
    public static long getVariableLengthInteger(ByteSource byteSource)
    {
        int signAndMask = getAndCheckByte(byteSource);

        long sum = 0;
        int bytes;
        // For every bit after the sign that matches the sign, read one more byte.
        for (bytes = 0; bytes < 7 && sameByteSign(signAndMask << (bytes + 1), signAndMask); ++bytes)
            sum = (sum << 8) | getAndCheckByte(byteSource);

        // The eighth length bit is stored in the second byte.
        if (bytes == 7 && sameByteSign((int) (sum >> 48), signAndMask))
            return ((sum << 8) | getAndCheckByte(byteSource)) ^ LONG_SIGN_BIT;    // 9-byte encoding, use bytes 2-9 with inverted sign
        else
        {
            sum |= (((long) signAndMask) << bytes * 8);     // add the rest of the bits
            long signMask = -0x40L << bytes * 7;            // mask of the bits that should be replaced by the sign
            long sign = (byte) (signAndMask ^ 0x80) >> 7;   // -1 if negative (0 leading bit), 0 otherwise
            return sum & ~signMask | sign & signMask;
        }
    }

    /**
     * Decode a variable-length unsigned integer, passing all bytes read through XOR with the given xorWith parameter.
     *
     * Used in BigInteger encoding to read number length, where negative numbers have their length negated
     * (i.e. xorWith = 0xFF) to ensure correct ordering.
     */
    public static long getVariableLengthUnsignedIntegerXoring(ByteSource byteSource, int xorWith)
    {
        int signAndMask = getAndCheckByte(byteSource) ^ xorWith;

        long sum = 0;
        int bytes;
        // Read an extra byte while the next most significant bit is 1.
        for (bytes = 0; bytes <= 7 && ((signAndMask << bytes) & 0x80) != 0; ++bytes)
            sum = (sum << 8) | getAndCheckByte(byteSource) ^ xorWith;

        // Strip the length bits from the leading byte.
        signAndMask &= ~(-256 >> bytes);
        return sum | (((long) signAndMask) << bytes * 8);     // Add the rest of the bits of the leading byte.
    }

    /** Returns true if the two parameters treated as bytes have the same sign. */
    private static boolean sameByteSign(int a, int b)
    {
        return ((a ^ b) & 0x80) == 0;
    }


    private static int getAndCheckByte(ByteSource byteSource)
    {
        return getAndCheckByte(byteSource, -1, -1);
    }

    private static int getAndCheckByte(ByteSource byteSource, int pos, int length)
    {
        int data = byteSource.next();
        if (data == ByteSource.END_OF_STREAM)
            throw new IllegalArgumentException(
                length > 0 ? String.format("Unexpected end of stream reached after %d bytes (expected >= %d)", pos, length)
                           : "Unexpected end of stream");
        assert data >= BYTE_NO_BITS && data <= BYTE_ALL_BITS
            : "A ByteSource must produce unsigned bytes and end in END_OF_STREAM";
        return data;
    }

    /**
     * Reads a single variable-length byte sequence (blob, string, ...) encoded according to the scheme described
     * in ByteComparable.md, decoding it back to its original, unescaped form.
     *
     * @param byteSource The source of the variable-length bytes sequence.
     * @return A byte array containing the original, unescaped bytes of the given source. Unescaped here means
     * not including any of the escape sequences of the encoding scheme used for variable-length byte sequences.
     */
    public static byte[] getUnescapedBytes(ByteSource.Peekable byteSource)
    {
        return byteSource == null ? null : readBytes(unescape(byteSource));
    }

    /**
     * As above, but converts the result to a ByteSource.
     */
    public static ByteSource unescape(ByteSource.Peekable byteSource)
    {
        return new ByteSource() {
            boolean escaped = false;

            @Override
            public int next()
            {
                if (!escaped)
                {
                    int data = byteSource.next(); // we consume this byte no matter what it is
                    if (data > ByteSource.ESCAPE)
                        return data;        // most used path leads here

                    assert data != ByteSource.END_OF_STREAM : "Invalid escaped byte sequence";
                    escaped = true;
                }

                int next = byteSource.peek();
                switch (next)
                {
                    case END_OF_STREAM:
                        // The end of a byte-comparable outside of a multi-component sequence. No matter what we have
                        // seen or peeked before, we should stop now.
                        byteSource.next();
                        return END_OF_STREAM;
                    case ESCAPED_0_DONE:
                        // The end of 1 or more consecutive 0x00 value bytes.
                        escaped = false;
                        byteSource.next();
                        return ESCAPE;
                    case ESCAPED_0_CONT:
                        // Escaped sequence continues
                        byteSource.next();
                        return ESCAPE;
                    default:
                        // An ESCAPE or ESCAPED_0_CONT won't be followed by either another ESCAPED_0_CONT, an
                        // ESCAPED_0_DONE, or an END_OF_STREAM only when the byte-comparable is part of a multi-component
                        // sequence and we have reached the end of the encoded byte-comparable. In this case, the byte
                        // we have just peeked is the separator or terminator byte between or at the end of components
                        // (which by contact must be 0x10 - 0xFE, which cannot conflict with our special bytes).
                        assert next >= ByteSource.MIN_SEPARATOR && next <= ByteSource.MAX_SEPARATOR : next;
                        // Unlike above, we don't consume this byte (the sequence decoding needs it).
                        return END_OF_STREAM;
                }
            }
        };
    }

    /**
     * Reads the bytes of the given source into a byte array. Doesn't do any transformation on the bytes, just reads
     * them until it reads an {@link ByteSource#END_OF_STREAM} byte, after which it returns an array of all the read
     * bytes, <strong>excluding the {@link ByteSource#END_OF_STREAM}</strong>.
     * <p>
     * This method sizes a tentative internal buffer array at {@code initialBufferCapacity}.  However, if
     * {@code byteSource} exceeds this size, the buffer array is recreated with doubled capacity as many times as
     * necessary.  If, after {@code byteSource} is fully exhausted, the number of bytes read from it does not exactly
     * match the current size of the tentative buffer array, then it is copied into another array sized to fit the
     * number of bytes read; otherwise, it is returned without that final copy step.
     *
     * @param byteSource The source which bytes we're interested in.
     * @param initialBufferCapacity The initial size of the internal buffer.
     * @return A byte array containing exactly all the read bytes. In case of a {@code null} source, the returned byte
     * array will be empty.
     */
    public static byte[] readBytes(ByteSource byteSource, final int initialBufferCapacity)
    {
        Preconditions.checkNotNull(byteSource);

        int readBytes = 0;
        byte[] buf = new byte[initialBufferCapacity];
        int data;
        while ((data = byteSource.next()) != ByteSource.END_OF_STREAM)
        {
            buf = ensureCapacity(buf, readBytes);
            buf[readBytes++] = (byte) data;
        }

        if (readBytes != buf.length)
        {
            buf = Arrays.copyOf(buf, readBytes);
        }
        return buf;
    }

    /**
     * Reads the bytes of the given source into a byte array. Doesn't do any transformation on the bytes, just reads
     * them until it reads an {@link ByteSource#END_OF_STREAM} byte, after which it returns an array of all the read
     * bytes, <strong>excluding the {@link ByteSource#END_OF_STREAM}</strong>.
     * <p>
     * This is equivalent to {@link #readBytes(ByteSource, int)} where the second actual parameter is
     * {@linkplain #INITIAL_BUFFER_CAPACITY} ({@value INITIAL_BUFFER_CAPACITY}).
     *
     * @param byteSource The source which bytes we're interested in.
     * @return A byte array containing exactly all the read bytes. In case of a {@code null} source, the returned byte
     * array will be empty.
     */
    public static byte[] readBytes(ByteSource byteSource)
    {
        return readBytes(byteSource, INITIAL_BUFFER_CAPACITY);
    }

    public static void copyBytes(ByteSource byteSource, byte[] bytes)
    {
        int readBytes = 0;

        int data;
        while ((data = byteSource.next()) != ByteSource.END_OF_STREAM)
        {
            if (bytes.length == readBytes)
                throw new ArrayIndexOutOfBoundsException(String.format("Number of bytes read, %d, exceeds the buffer size of %d.", readBytes + 1, bytes.length));
            bytes[readBytes++] = (byte) data;
        }
    }

    /**
     * Ensures the given buffer has capacity for taking data with the given length - if it doesn't, it returns a copy
     * of the buffer, but with double the capacity.
     */
    private static byte[] ensureCapacity(byte[] buf, int dataLengthInBytes)
    {
        if (dataLengthInBytes == buf.length)
            // We won't gain much with guarding against overflow. We'll overflow when dataLengthInBytes >= 1 << 30,
            // and if we do guard, we'll be able to extend the capacity to Integer.MAX_VALUE (which is 1 << 31 - 1).
            // Controlling the exception that will be thrown shouldn't matter that much, and  in practice, we almost
            // surely won't be reading gigabytes of ByteSource data at once.
            return Arrays.copyOf(buf, dataLengthInBytes * 2);
        else
            return buf;
    }

    /**
     * Converts the given {@link ByteSource} to a UTF-8 {@link String}.
     *
     * @param byteSource The source we're interested in.
     * @return A UTF-8 string corresponding to the given source.
     */
    public static String getString(ByteSource.Peekable byteSource)
    {
        if (byteSource == null)
            return null;

        byte[] data = getUnescapedBytes(byteSource);

        return new String(data, StandardCharsets.UTF_8);
    }

    /*
     * Multi-component sequence utilities.
     */

    /**
     * A utility for consuming components from a peekable multi-component sequence.
     * It uses the component separators, so the given sequence needs to have its last component fully consumed, in
     * order for the next consumable byte to be a separator. Identifying the end of the component that will then be
     * consumed is the responsibility of the consumer (the user of this method).
     * @param source A peekable multi-component sequence, which next byte is a component separator.
     * @return the given multi-component sequence if its next component is not null, or {@code null} if it is.
     */
    public static ByteSource.Peekable nextComponentSource(ByteSource.Peekable source)
    {
        return nextComponentSource(source, source.next());
    }

    /**
     * A utility for consuming components from a peekable multi-component sequence, very similar to
     * {@link #nextComponentSource(ByteSource.Peekable)} - the difference being that here the separator can be passed
     * in case it had to be consumed beforehand.
     */
    public static ByteSource.Peekable nextComponentSource(ByteSource.Peekable source, int separator)
    {
        return nextComponentNull(separator)
               ? null
               : source;
    }

    public static boolean nextComponentNull(int separator)
    {
        return separator == ByteSource.NEXT_COMPONENT_NULL || separator == ByteSource.NEXT_COMPONENT_EMPTY
               || separator == ByteSource.NEXT_COMPONENT_EMPTY_REVERSED;
    }
}
