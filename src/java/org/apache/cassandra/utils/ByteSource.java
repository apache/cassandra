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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.utils.ByteComparable.Version;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A stream of byte, used for byte-order-comparable representations of data.
 */
public interface ByteSource
{
    /** Get the next byte, unsigned. Must be between 0 and 255, or END_OF_STREAM if there are no more bytes. */
    int next();

    /** Value returned if at the end of the stream. */
    int END_OF_STREAM = -1;

    ByteSource EMPTY = () -> END_OF_STREAM;

    /**
     * Escape value. Used, among other things, to mark the end of subcomponents (so that shorter compares before anything longer).
     * Actual zeros in input need to be escaped if this is in use (see BufferReinterpreter).
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
    int NEXT_COMPONENT_NULL = 0x3F;
    int NEXT_COMPONENT_NULL_REVERSED = 0x41;
    // Default terminator byte in sequences. Smaller than NEXT_COMPONENT_NULL, but larger than LT_NEXT_COMPONENT to
    // ensure lexicographic compares go in the correct direction
    int TERMINATOR = 0x38;
    // These are special endings, for exclusive/inclusive bounds (i.e. smaller than anything with more components, bigger than anything with more components)
    int LT_NEXT_COMPONENT = 0x20;
    int GT_NEXT_COMPONENT = 0x60;

    /**
     * Reinterprets a byte buffer as a byte-comparable source that has 0s escaped and finishes in an escape.
     * This provides a weakly-prefix-free byte-comparable version of the content to use in sequences.
     * (See ByteSource.BufferReinterpreter/Multi for explanation.)
     */
    static ByteSource of(ByteBuffer buf, Version version)
    {
        return new BufferReinterpreter(buf, version);
    }

    /**
     * Reinterprets a byte array as a byte-comparable source that has 0s escaped and finishes in an escape.
     * This provides a prefix-free byte-comparable version of the content to use in sequences.
     * (See ByteSource.BufferReinterpreter/Multi for explanation.)
     */
    static ByteSource of(byte[] buf, Version version)
    {
        return new ReinterpreterArray(buf, version);
    }

    /**
     * Combines a chain of sources, turning their weak-prefix-free byte-comparable representation into the combination's
     * prefix-free byte-comparable representation, with the included terminator character.
     * For correctness, the terminator must be within MIN-MAX_SEPARATOR and different from NEXT_COMPONENT+/-1.
     * Typically TERMINATOR, or LT/GT_NEXT_COMPONENT if used for partially specified bounds.
     */
    static ByteSource withTerminator(int terminator, ByteSource... srcs)
    {
        return new Multi(srcs, terminator);
    }

    static ByteSource of(String s, Version version)
    {
        return new ReinterpreterArray(s.getBytes(StandardCharsets.UTF_8), version);
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
    static ByteSource optionalSignedFixedLengthNumber(ByteBuffer b)
    {
        return b.hasRemaining() ? signedFixedLengthNumber(b) : null;
    }

    /**
     * Produce a source for a signed fixed-length number.
     * The first byte has its sign bit inverted, and the rest are passed unchanged.
     * Presumes that the length of the buffer is always constant for the type.
     */
    static ByteSource signedFixedLengthNumber(ByteBuffer b)
    {
        return new SignedFixedLengthNumber(b);
    }

    /**
     * Produce a source for a signed fixed-length floating-point number, also translating empty to null.
     * If sign bit is on, returns negated bytes. If not, add the sign bit value.
     * (Sign of IEEE floats is the highest bit, the rest can be compared in magnitude by byte comparison.)
     * Presumes that the length of the buffer is always either 0 or constant for the type, which permits decoding and
     * ensures the representation is prefix-free.
     */
    static ByteSource optionalSignedFixedLengthFloat(ByteBuffer b)
    {
        return b.hasRemaining() ? signedFixedLengthFloat(b) : null;
    }

    /**
     * Produce a source for a signed fixed-length floating-point number.
     * If sign bit is on, returns negated bytes. If not, add the sign bit value.
     * (Sign of IEEE floats is the highest bit, the rest can be compared in magnitude by byte comparison.)
     * Presumes that the length of the buffer is always constant for the type.
     */
    static ByteSource signedFixedLengthFloat(ByteBuffer b)
    {
        return new SignedFixedLengthFloat(b);
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
        assert i >= 0 && i <= 0xFF;
        return new ByteSource()
        {
            boolean given = false;
            public int next()
            {
                if (given)
                    return END_OF_STREAM;
                given = true;
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
     * @return
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


    static ByteSource MAX = new ByteSource()
    {
        public int next()
        {
            return 0xFF;
        }

        public String toString()
        {
            return "MAX";
        }
    };

    /**
     * Returns a maximal ByteSource, i.e. something that compares greater to any other byte source.
     * This is an infinite sequence of 0xFF.
     *
     * Note that since the sequence is infinite, trying to calculate this item's length, copying it, trying
     * to store it in a trie, or comparing it to another max will result in an infinite loop.
     */
    public static ByteSource max()
    {
        return MAX;
    }

    /**
     * Variable-length encoding. Escapes 0s as ESCAPE + zero or more ESCAPED_0_CONT + ESCAPED_0_DONE.
     * Finishes with an escape value (to which Multi will add non-zero component separator)
     * E.g. A00B translates to 4100FEFF4200
     *      A0B0               4100FF4200FE (+00 for {@link Version#LEGACY})
     *      A0                 4100FE       (+00 for {@link Version#LEGACY})
     *
     * If in a single byte source, the bytes could be simply passed unchanged, but this would not allow us to
     * combine components. This translation preserves order, and since the encoding for 0 is higher than the separator
     * also makes sure shorter components are treated as smaller.
     *
     * The encoding is not prefix-free, since e.g. the encoding of "A" (4100) is a prefix of the encoding of "A0"
     * (4100FE), but the byte following the prefix is guaranteed to be FE or FF, which makes the encoding weakly
     * prefix-free. Additionally, any such prefix sequence will compare smaller than the value to which it is a prefix,
     * because any permitted separator byte will be smaller than the byte following the prefix.
     */
    static abstract class AbstractReinterpreter implements ByteSource
    {
        final Version version;
        int bufpos;
        boolean escaped;

        AbstractReinterpreter(int position, Version version)
        {
            this.bufpos = position;
            this.version = version;
        }

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

    static class BufferReinterpreter extends AbstractReinterpreter
    {
        final ByteBuffer buf;

        private BufferReinterpreter(ByteBuffer buf, Version version)
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

    static class ReinterpreterArray extends AbstractReinterpreter
    {
        final byte[] buf;

        private ReinterpreterArray(byte[] buf, Version version)
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

    /**
     * Fixed length signed number encoding. Inverts first bit (so that neg < pos), then just posts all bytes from the
     * buffer. Assumes buffer is of correct length.
     */
    static class SignedFixedLengthNumber implements ByteSource
    {
        ByteBuffer buf;
        int bufpos;

        public SignedFixedLengthNumber(ByteBuffer buf)
        {
            this.buf = buf;
            bufpos = buf.position();
        }

        public int next()
        {
            if (bufpos >= buf.limit())
                return END_OF_STREAM;
            int v = buf.get(bufpos) & 0xFF;
            if (bufpos == buf.position())
                v ^= 0x80;
            ++bufpos;
            return v;
        }
    }

    static class Number implements ByteSource
    {
        final long value;
        int pos;

        public Number(long value, int length)
        {
            this.value = value;
            this.pos = length;
        }

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
    static class SignedFixedLengthFloat implements ByteSource
    {
        final ByteBuffer buf;
        int bufpos;
        boolean invert;

        public SignedFixedLengthFloat(ByteBuffer buf)
        {
            this.buf = buf;
            this.bufpos = buf.position();
        }

        public int next()
        {
            if (bufpos >= buf.limit())
                return END_OF_STREAM;
            int v = buf.get(bufpos) & 0xFF;
            if (bufpos == buf.position())
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
     * Combination of multiple byte sources. Adds NEXT_COMPONENT before sources, or NEXT_COMPONENT_NULL if next is null.
     */
    static class Multi implements ByteSource
    {
        final ByteSource[] srcs;
        int srcnum = -1;
        int terminator;

        Multi(ByteSource[] srcs, int terminator)
        {
            this.srcs = srcs;
            this.terminator = terminator;
        }

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
                return terminator;
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
        final ByteSource prev;
        final ByteSource curr;
        boolean done = false;
        final boolean useCurr;

        Separator(ByteSource prevMax, ByteSource currMin, boolean useCurr)
        {
            this.prev = prevMax;
            this.curr = currMin;
            this.useCurr = useCurr;
        }

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

    static ByteSource optionalFixedLength(ByteBuffer b)
    {
        return b.hasRemaining() ? fixedLength(b) : null;
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
                return ++pos < b.limit() ? b.get(pos) & 0xFF : -1;
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

    public static ByteSource fourBit(ByteSource s)
    {
        return new ByteSource()
        {
            int pos = 0;
            int v = 0;

            @Override
            public int next()
            {
                if ((pos++ & 1) == 0)
                {
                    v = s.next();
                    if (v == END_OF_STREAM)
                        return END_OF_STREAM;
                    return (v >> 4) & 0xF;
                }
                else
                    return v & 0xF;
            }
        };
    }

    /**
     * Splits each byte into portions of bitCount bits.
     * @param s source
     * @param bitCount number of bits to issue at a time, 1-4 make sense
     */
    public static ByteSource splitBytes(ByteSource s, int bitCount)
    {
        return new ByteSource()
        {
            int pos = 8;
            int v = 0;
            int mask = (1 << bitCount) - 1;

            @Override
            public int next()
            {
                if ((pos += bitCount) >= 8)
                {
                    pos = 0;
                    v = s.next();
                    if (v == END_OF_STREAM)
                        return END_OF_STREAM;
                }
                v <<= bitCount;
                return (v >> 8) & mask;
            }
        };
    }

    /**
     * Returns the key that is immediately after src in the topology.
     * @param src
     * @return src with added 00 byte at the end
     */
    public static ByteSource nextKey(ByteSource src)
    {
        return new ByteSource()
        {
            boolean done = false;

            @Override
            public int next()
            {
                if (done)
                    return END_OF_STREAM;
                int n = src.next();
                if (n != END_OF_STREAM)
                    return n;

                done = true;
                return 0;
            }
        };
    }

    public class Peekable implements ByteSource
    {
        static final int NONE = Integer.MIN_VALUE;

        final ByteSource wrapped;
        int peeked = NONE;

        public Peekable(ByteSource wrapped)
        {
            this.wrapped = wrapped;
        }

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
