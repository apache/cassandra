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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.memory.MemoryUtil;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A stream of bytes, used for byte-order-comparable representations of data, and utilities to convert various values
 * to their byte-ordered translation.
 * See ByteComparable.md for details about the encoding scheme.
 */
public interface ByteSource
{
    /** Consume the next byte, unsigned. Must be between 0 and 255, or END_OF_STREAM if there are no more bytes. */
    int next();

    /** Value returned if at the end of the stream. */
    int END_OF_STREAM = -1;

    ByteSource EMPTY = () -> END_OF_STREAM;

    /**
     * Escape value. Used, among other things, to mark the end of subcomponents (so that shorter compares before anything longer).
     * Actual zeros in input need to be escaped if this is in use (see {@link AbstractEscaper}).
     */
    int ESCAPE = 0x00;

    // Zeros are encoded as a sequence of ESCAPE, 0 or more of ESCAPED_0_CONT, ESCAPED_0_DONE so zeroed spaces only grow by 1 byte
    int ESCAPED_0_CONT = 0xFE;
    int ESCAPED_0_DONE = 0xFF;

    // All separators must be within these bounds
    int MIN_SEPARATOR = 0x10;
    int MAX_SEPARATOR = 0xEF;

    // Next component marker.
    int NEXT_COMPONENT = 0x40;
    // Marker used to present null values represented by empty buffers (e.g. by Int32Type)
    int NEXT_COMPONENT_EMPTY = 0x3F;
    int NEXT_COMPONENT_EMPTY_REVERSED = 0x41;
    // Marker for null components in tuples, maps, sets and clustering keys.
    int NEXT_COMPONENT_NULL = 0x3E;

    // Section for next component markers which is not allowed for use
    int MIN_NEXT_COMPONENT = 0x3C;
    int MAX_NEXT_COMPONENT = 0x44;

    // Default terminator byte in sequences. Smaller than NEXT_COMPONENT_NULL, but larger than LT_NEXT_COMPONENT to
    // ensure lexicographic compares go in the correct direction
    int TERMINATOR = 0x38;
    // These are special endings, for exclusive/inclusive bounds (i.e. smaller than anything with more components,
    // bigger than anything with more components)
    int LT_NEXT_COMPONENT = 0x20;
    int GT_NEXT_COMPONENT = 0x60;

    // Unsupported, for artificial bounds
    int LTLT_NEXT_COMPONENT = 0x1F; // LT_NEXT_COMPONENT - 1
    int GTGT_NEXT_COMPONENT = 0x61; // GT_NEXT_COMPONENT + 1

    // Special value for components that should be excluded from the normal min/max span. (static rows)
    int EXCLUDED = 0x18;

    /**
     * Encodes byte-accessible data as a byte-comparable source that has 0s escaped and finishes in an escaped
     * state.
     * This provides a weakly-prefix-free byte-comparable version of the content to use in sequences.
     * (See {@link AbstractEscaper} for a detailed explanation.)
     */
    static <V> ByteSource of(ValueAccessor<V> accessor, V data, Version version)
    {
        return new AccessorEscaper<>(accessor, data, version);
    }

    /**
     * Encodes a byte buffer as a byte-comparable source that has 0s escaped and finishes in an escape.
     * This provides a weakly-prefix-free byte-comparable version of the content to use in sequences.
     * (See ByteSource.BufferEscaper/Multi for explanation.)
     */
    static ByteSource of(ByteBuffer buf, Version version)
    {
        return new BufferEscaper(buf, version);
    }

    /**
     * Encodes a byte array as a byte-comparable source that has 0s escaped and finishes in an escape.
     * This provides a prefix-free byte-comparable version of the content to use in sequences.
     * (See ByteSource.BufferEscaper/Multi for explanation.)
     */
    static ByteSource of(byte[] buf, Version version)
    {
        return new ArrayEscaper(buf, version);
    }

    /**
     * Encodes a memory range as a byte-comparable source that has 0s escaped and finishes in an escape.
     * This provides a weakly-prefix-free byte-comparable version of the content to use in sequences.
     * (See ByteSource.BufferEscaper/Multi for explanation.)
     */
    static ByteSource ofMemory(long address, int length, ByteComparable.Version version)
    {
        return new MemoryEscaper(address, length, version);
    }

    /**
     * Combines a chain of sources, turning their weak-prefix-free byte-comparable representation into the combination's
     * prefix-free byte-comparable representation, with the included terminator character.
     * For correctness, the terminator must be within MIN-MAX_SEPARATOR and outside the range reserved for
     * NEXT_COMPONENT markers.
     * Typically TERMINATOR, or LT/GT_NEXT_COMPONENT if used for partially specified bounds.
     */
    static ByteSource withTerminator(int terminator, ByteSource... srcs)
    {
        assert terminator >= MIN_SEPARATOR && terminator <= MAX_SEPARATOR;
        assert terminator < MIN_NEXT_COMPONENT || terminator > MAX_NEXT_COMPONENT;
        return new Multi(srcs, terminator);
    }

    /**
     * As above, but permits any separator. The legacy format wasn't using weak prefix freedom and has some
     * non-reversible transformations.
     */
    static ByteSource withTerminatorLegacy(int terminator, ByteSource... srcs)
    {
        return new Multi(srcs, terminator);
    }

    static ByteSource withTerminatorMaybeLegacy(Version version, int legacyTerminator, ByteSource... srcs)
    {
        return version == Version.LEGACY ? withTerminatorLegacy(legacyTerminator, srcs)
                                         : withTerminator(TERMINATOR, srcs);
    }

    static ByteSource of(String s, Version version)
    {
        return new ArrayEscaper(s.getBytes(StandardCharsets.UTF_8), version);
    }

    static ByteSource of(long value)
    {
        return new Number(value ^ (1L<<63), 8);
    }

    static ByteSource of(int value)
    {
        return new Number(value ^ (1L<<31), 4);
    }

    /**
     * Produce a source for a signed fixed-length number, also translating empty to null.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     * Presumes that the length of the buffer is always either 0 or constant for the type, which permits decoding and
     * ensures the representation is prefix-free.
     */
    static <V> ByteSource optionalSignedFixedLengthNumber(ValueAccessor<V> accessor, V data)
    {
        return !accessor.isEmpty(data) ? signedFixedLengthNumber(accessor, data) : null;
    }

    /**
     * Produce a source for a signed fixed-length number.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     * Presumes that the length of the buffer is always constant for the type.
     */
    static <V> ByteSource signedFixedLengthNumber(ValueAccessor<V> accessor, V data)
    {
        return new SignedFixedLengthNumber<>(accessor, data);
    }

    /**
     * Produce a source for a signed fixed-length floating-point number, also translating empty to null.
     * If sign bit is on, returns negated bytes. If not, add the sign bit value.
     * (Sign of IEEE floats is the highest bit, the rest can be compared in magnitude by byte comparison.)
     * Presumes that the length of the buffer is always either 0 or constant for the type, which permits decoding and
     * ensures the representation is prefix-free.
     */
    static <V> ByteSource optionalSignedFixedLengthFloat(ValueAccessor<V> accessor, V data)
    {
        return !accessor.isEmpty(data) ? signedFixedLengthFloat(accessor, data) : null;
    }

    /**
     * Produce a source for a signed fixed-length floating-point number.
     * If sign bit is on, returns negated bytes. If not, add the sign bit value.
     * (Sign of IEEE floats is the highest bit, the rest can be compared in magnitude by byte comparison.)
     * Presumes that the length of the buffer is always constant for the type.
     */
    static <V> ByteSource signedFixedLengthFloat(ValueAccessor<V> accessor, V data)
    {
        return new SignedFixedLengthFloat<>(accessor, data);
    }

    /**
     * Produce a source for a signed integer, stored using variable length encoding.
     * The representation uses between 1 and 9 bytes, is prefix-free and compares
     * correctly.
     */
    static ByteSource variableLengthInteger(long value)
    {
        return new VariableLengthInteger(value);
    }

    /**
     * Returns a separator for two byte sources, i.e. something that is definitely > prevMax, and <= currMin, assuming
     * prevMax < currMin.
     * This returns the shortest prefix of currMin that is greater than prevMax.
     */
    public static ByteSource separatorPrefix(ByteSource prevMax, ByteSource currMin)
    {
        return new Separator(prevMax, currMin, true);
    }

    /**
     * Returns a separator for two byte sources, i.e. something that is definitely > prevMax, and <= currMin, assuming
     * prevMax < currMin.
     * This is a source of length 1 longer than the common prefix of the two sources, with last byte one higher than the
     * prevMax source.
     */
    public static ByteSource separatorGt(ByteSource prevMax, ByteSource currMin)
    {
        return new Separator(prevMax, currMin, false);
    }

    public static ByteSource oneByte(int i)
    {
        assert i >= 0 && i <= 0xFF : "Argument must be a valid unsigned byte.";
        return new ByteSource()
        {
            boolean consumed = false;

            @Override
            public int next()
            {
                if (consumed)
                    return END_OF_STREAM;
                consumed = true;
                return i;
            }
        };
    }

    public static ByteSource cut(ByteSource src, int cutoff)
    {
        return new ByteSource()
        {
            int pos = 0;

            @Override
            public int next()
            {
                return pos++ < cutoff ? src.next() : END_OF_STREAM;
            }
        };
    }

    /**
     * Wrap a ByteSource in a length-fixing facade.
     *
     * If the length of {@code src} is less than {@code cutoff}, then pad it on the right with {@code padding} until
     * the overall length equals {@code cutoff}.  If the length of {@code src} is greater than {@code cutoff}, then
     * truncate {@code src} to that size.  Effectively a noop if {@code src} happens to have length {@code cutoff}.
     *
     * @param src the input source to wrap
     * @param cutoff the size of the source returned
     * @param padding a padding byte (an int subject to a 0xFF mask)
     */
    public static ByteSource cutOrRightPad(ByteSource src, int cutoff, int padding)
    {
        return new ByteSource()
        {
            int pos = 0;

            @Override
            public int next()
            {
                if (pos++ >= cutoff)
                {
                    return END_OF_STREAM;
                }
                int next = src.next();
                return next == END_OF_STREAM ? padding : next;
            }
        };
    }


    /**
     * Variable-length encoding. Escapes 0s as ESCAPE + zero or more ESCAPED_0_CONT + ESCAPED_0_DONE.
     * If the source ends in 0, we use ESCAPED_0_CONT to make sure that the encoding remains smaller than that source
     * with a further 0 at the end.
     * Finishes in an escaped state (either with ESCAPE or ESCAPED_0_CONT), which in {@link Multi} is followed by
     * a component separator between 0x10 and 0xFE.
     *
     * E.g. "A\0\0B" translates to 4100FEFF4200
     *      "A\0B\0"               4100FF4200FE (+00 for {@link Version#LEGACY})
     *      "A\0"                  4100FE       (+00 for {@link Version#LEGACY})
     *      "AB"                   414200
     *
     * If in a single byte source, the bytes could be simply passed unchanged, but this would not allow us to
     * combine components. This translation preserves order, and since the encoding for 0 is higher than the separator
     * also makes sure shorter components are treated as smaller.
     *
     * The encoding is not prefix-free, since e.g. the encoding of "A" (4100) is a prefix of the encoding of "A\0"
     * (4100FE), but the byte following the prefix is guaranteed to be FE or FF, which makes the encoding weakly
     * prefix-free. Additionally, any such prefix sequence will compare smaller than the value to which it is a prefix,
     * because any permitted separator byte will be smaller than the byte following the prefix.
     */
    abstract static class AbstractEscaper implements ByteSource
    {
        private final Version version;
        private int bufpos;
        private boolean escaped;

        AbstractEscaper(int position, Version version)
        {
            this.bufpos = position;
            this.version = version;
        }

        @Override
        public final int next()
        {
            if (bufpos >= limit())
            {
                if (bufpos > limit())
                    return END_OF_STREAM;

                ++bufpos;
                if (escaped)
                {
                    escaped = false;
                    if (version == Version.LEGACY)
                        --bufpos; // place an ESCAPE at the end of sequence ending in ESCAPE
                    return ESCAPED_0_CONT;
                }
                return ESCAPE;
            }

            int index = bufpos++;
            int b = get(index) & 0xFF;
            if (!escaped)
            {
                if (b == ESCAPE)
                    escaped = true;
                return b;
            }
            else
            {
                if (b == ESCAPE)
                    return ESCAPED_0_CONT;
                --bufpos;
                escaped = false;
                return ESCAPED_0_DONE;
            }
        }

        protected abstract byte get(int index);

        protected abstract int limit();
    }

    static class AccessorEscaper<V> extends AbstractEscaper
    {
        private final V data;
        private final ValueAccessor<V> accessor;

        private AccessorEscaper(ValueAccessor<V> accessor, V data, Version version)
        {
            super(0, version);
            this.accessor = accessor;
            this.data = data;
        }

        protected int limit()
        {
            return accessor.size(data);
        }

        protected byte get(int index)
        {
            return accessor.getByte(data, index);
        }
    }

    static class BufferEscaper extends AbstractEscaper
    {
        private final ByteBuffer buf;

        private BufferEscaper(ByteBuffer buf, Version version)
        {
            super(buf.position(), version);
            this.buf = buf;
        }

        protected int limit()
        {
            return buf.limit();
        }

        protected byte get(int index)
        {
            return buf.get(index);
        }
    }

    static class ArrayEscaper extends AbstractEscaper
    {
        private final byte[] buf;

        private ArrayEscaper(byte[] buf, Version version)
        {
            super(0, version);
            this.buf = buf;
        }

        @Override
        protected byte get(int index)
        {
            return buf[index];
        }

        @Override
        protected int limit()
        {
            return buf.length;
        }
    }

    static class MemoryEscaper extends AbstractEscaper
    {
        private final long address;
        private final int length;

        MemoryEscaper(long address, int length, ByteComparable.Version version)
        {
            super(0, version);
            this.address = address;
            this.length = length;
        }

        protected byte get(int index)
        {
            return MemoryUtil.getByte(address + index);
        }

        protected int limit()
        {
            return length;
        }
    }

    /**
     * Fixed length signed number encoding. Inverts first bit (so that neg < pos), then just posts all bytes from the
     * buffer. Assumes buffer is of correct length.
     */
    static class SignedFixedLengthNumber<V> implements ByteSource
    {
        private final ValueAccessor<V> accessor;
        private final V data;
        private int bufpos;

        public SignedFixedLengthNumber(ValueAccessor<V> accessor, V data)
        {
            this.accessor = accessor;
            this.data = data;
            this.bufpos = 0;
        }

        @Override
        public int next()
        {
            if (bufpos >= accessor.size(data))
                return END_OF_STREAM;
            int v = accessor.getByte(data, bufpos) & 0xFF;
            if (bufpos == 0)
                v ^= 0x80;
            ++bufpos;
            return v;
        }
    }

    /**
     * Variable-length encoding for unsigned integers.
     * The encoding is similar to UTF-8 encoding.
     * Numbers between 0 and 127 are encoded in one byte, using 0 in the most significant bit.
     * Larger values have 1s in as many of the most significant bits as the number of additional bytes
     * in the representation, followed by a 0. This ensures that longer numbers compare larger than shorter
     * ones. Since we never use a longer representation than necessary, this implies numbers compare correctly.
     * As the number of bytes is specified in the bits of the first, no value is a prefix of another.
     */
    static class VariableLengthUnsignedInteger implements ByteSource
    {
        private final long value;
        private int pos = -1;

        public VariableLengthUnsignedInteger(long value)
        {
            this.value = value;
        }

        @Override
        public int next()
        {
            if (pos == -1)
            {
                int bitsMinusOne = 63 - (Long.numberOfLeadingZeros(value | 1)); // 0 to 63 (the | 1 is to make sure 0 maps to 0 (1 bit))
                int bytesMinusOne = bitsMinusOne / 7;
                int mask = -256 >> bytesMinusOne;   // sequence of bytesMinusOne 1s in the most-significant bits
                pos = bytesMinusOne * 8;
                return (int) ((value >>> pos) | mask) & 0xFF;
            }
            pos -= 8;
            if (pos < 0)
                return END_OF_STREAM;
            return (int) (value >>> pos) & 0xFF;
        }
    }

    /**
     * Variable-length encoding for signed integers.
     * The encoding is based on the unsigned encoding above, where the first bit stored is the inverted sign,
     * followed by as many matching bits as there are additional bytes in the encoding, followed by the two's
     * complement of the number.
     * Because of the inverted sign bit, negative numbers compare smaller than positives, and because the length
     * bits match the sign, longer positive numbers compare greater and longer negative ones compare smaller.
     *
     * Examples:
     *      0              encodes as           80
     *      1              encodes as           81
     *     -1              encodes as           7F
     *     63              encodes as           BF
     *     64              encodes as           C040
     *    -64              encodes as           40
     *    -65              encodes as           3FBF
     *   2^20-1            encodes as           EFFFFF
     *   2^20              encodes as           F0100000
     *  -2^20              encodes as           100000
     *   2^64-1            encodes as           FFFFFFFFFFFFFFFFFF
     *  -2^64              encodes as           000000000000000000
     *
     * As the number of bytes is specified in bits 2-9, no value is a prefix of another.
     */
    static class VariableLengthInteger implements ByteSource
    {
        private final long value;
        private int pos;

        public VariableLengthInteger(long value)
        {
            long negativeMask = value >> 63;    // -1 for negative, 0 for positive
            value ^= negativeMask;

            int bits = 64 - Long.numberOfLeadingZeros(value | 1); // 1 to 63 (can't be 64 because we flip negative numbers)
            int bytes = bits / 7 + 1;   // 0-6 bits 1 byte 7-13 2 bytes etc to 56-63 9 bytes
            if (bytes >= 9)
            {
                value |= 0x8000000000000000L;   // 8th bit, which doesn't fit the first byte
                pos = negativeMask < 0 ? 256 : -1; // out of 0-64 range integer such that & 0xFF is 0x00 for negative and 0xFF for positive
            }
            else
            {
                long mask = (-0x100 >> bytes) & 0xFF; // one in sign bit and as many more as there are extra bytes
                pos = bytes * 8;
                value = value | (mask << (pos - 8));
            }

            value ^= negativeMask;
            this.value = value;
        }

        @Override
        public int next()
        {
            if (pos <= 0 || pos > 64)
            {
                if (pos == 0)
                    return END_OF_STREAM;
                else
                {
                    // 8-byte value, returning first byte
                    int result = pos & 0xFF; // 0x00 for negative numbers, 0xFF for positive
                    pos = 64;
                    return result;
                }
            }
            pos -= 8;
            return (int) (value >>> pos) & 0xFF;
        }
    }

    static class Number implements ByteSource
    {
        private final long value;
        private int pos;

        public Number(long value, int length)
        {
            this.value = value;
            this.pos = length;
        }

        @Override
        public int next()
        {
            if (pos == 0)
                return END_OF_STREAM;
            return (int) ((value >> (--pos * 8)) & 0xFF);
        }
    }

    /**
     * Fixed length signed floating point number encoding. First bit is sign. If positive, add sign bit value to make
     * greater than all negatives. If not, invert all content to make negatives with bigger magnitude smaller.
     */
    static class SignedFixedLengthFloat<V> implements ByteSource
    {
        private final ValueAccessor<V> accessor;
        private final V data;
        private int bufpos;
        private boolean invert;

        public SignedFixedLengthFloat(ValueAccessor<V> accessor, V data)
        {
            this.accessor = accessor;
            this.data = data;
            this.bufpos = 0;
        }

        @Override
        public int next()
        {
            if (bufpos >= accessor.size(data))
                return END_OF_STREAM;
            int v = accessor.getByte(data, bufpos) & 0xFF;
            if (bufpos == 0)
            {
                invert = v >= 0x80;
                v |= 0x80;
            }
            if (invert)
                v = v ^ 0xFF;
            ++bufpos;
            return v;
        }
    }

    /**
     * Combination of multiple byte sources. Adds {@link NEXT_COMPONENT} before sources, or {@link NEXT_COMPONENT_NULL} if next is null.
     */
    static class Multi implements ByteSource
    {
        private final ByteSource[] srcs;
        private int srcnum = -1;
        private final int sequenceTerminator;

        Multi(ByteSource[] srcs, int sequenceTerminator)
        {
            this.srcs = srcs;
            this.sequenceTerminator = sequenceTerminator;
        }

        @Override
        public int next()
        {
            if (srcnum == srcs.length)
                return END_OF_STREAM;

            int b = END_OF_STREAM;
            if (srcnum >= 0 && srcs[srcnum] != null)
                b = srcs[srcnum].next();
            if (b > END_OF_STREAM)
                return b;

            ++srcnum;
            if (srcnum == srcs.length)
                return sequenceTerminator;
            if (srcs[srcnum] == null)
                return NEXT_COMPONENT_NULL;
            return NEXT_COMPONENT;
        }
    }

    /**
     * Construct the shortest common prefix of prevMax and currMin that separates those two byte streams.
     * If {@code useCurr == true} the last byte of the returned stream comes from {@code currMin} and is the first
     * byte which is greater than byte on the corresponding position of {@code prevMax}.
     * Otherwise, the last byte of the returned stream comes from {@code prevMax} and is incremented by one, still
     * guaranteeing that it is <= than the byte on the corresponding position of {@code currMin}.
     */
    static class Separator implements ByteSource
    {
        private final ByteSource prev;
        private final ByteSource curr;
        private boolean done = false;
        private final boolean useCurr;

        Separator(ByteSource prevMax, ByteSource currMin, boolean useCurr)
        {
            this.prev = prevMax;
            this.curr = currMin;
            this.useCurr = useCurr;
        }

        @Override
        public int next()
        {
            if (done)
                return END_OF_STREAM;
            int p = prev.next();
            int c = curr.next();
            assert p <= c : prev + " not less than " + curr;
            if (p == c)
                return c;
            done = true;
            return useCurr ? c : p + 1;
        }
    }

    static <V> ByteSource optionalFixedLength(ValueAccessor<V> accessor, V data)
    {
        return !accessor.isEmpty(data) ? fixedLength(accessor, data) : null;
    }

    /**
     * A byte source of the given bytes without any encoding.
     * The resulting source is only guaranteed to give correct comparison results and be prefix-free if the
     * underlying type has a fixed length.
     * In tests, this method is also used to generate non-escaped test cases.
     */
    public static <V> ByteSource fixedLength(ValueAccessor<V> accessor, V data)
    {
        return new ByteSource()
        {
            int pos = -1;

            @Override
            public int next()
            {
                return ++pos < accessor.size(data) ? accessor.getByte(data, pos) & 0xFF : END_OF_STREAM;
            }
        };
    }

    /**
     * A byte source of the given bytes without any encoding.
     * The resulting source is only guaranteed to give correct comparison results and be prefix-free if the
     * underlying type has a fixed length.
     * In tests, this method is also used to generate non-escaped test cases.
     */
    public static ByteSource fixedLength(ByteBuffer b)
    {
        return new ByteSource()
        {
            int pos = b.position() - 1;

            @Override
            public int next()
            {
                return ++pos < b.limit() ? b.get(pos) & 0xFF : END_OF_STREAM;
            }
        };
    }

    /**
     * A byte source of the given bytes without any encoding.
     * If used in a sequence, the resulting source is only guaranteed to give correct comparison results if the
     * underlying type has a fixed length.
     * In tests, this method is also used to generate non-escaped test cases.
     */
    public static ByteSource fixedLength(byte[] b)
    {
        return fixedLength(b, 0, b.length);
    }

    public static ByteSource fixedLength(byte[] b, int offset, int length)
    {
        checkArgument(offset >= 0 && offset <= b.length);
        checkArgument(length >= 0 && offset + length <= b.length);

        return new ByteSource()
        {
            int pos = offset - 1;

            @Override
            public int next()
            {
                return ++pos < offset + length ? b[pos] & 0xFF : END_OF_STREAM;
            }
        };
    }

    public class Peekable implements ByteSource
    {
        private static final int NONE = Integer.MIN_VALUE;

        private final ByteSource wrapped;
        private int peeked = NONE;

        public Peekable(ByteSource wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public int next()
        {
            if (peeked != NONE)
            {
                int val = peeked;
                peeked = NONE;
                return val;
            }
            else
                return wrapped.next();
        }

        public int peek()
        {
            if (peeked == NONE)
                peeked = wrapped.next();
            return peeked;
        }
    }

    public static Peekable peekable(ByteSource p)
    {
        // When given a null source, we're better off not wrapping it and just returning null. This way existing
        // code that doesn't know about ByteSource.Peekable, but handles correctly null ByteSources won't be thrown
        // off by a non-null instance that semantically should have been null.
        if (p == null)
            return null;
        return (p instanceof Peekable)
               ? (Peekable) p
               : new Peekable(p);
    }
}