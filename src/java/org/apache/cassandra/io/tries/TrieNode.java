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
package org.apache.cassandra.io.tries;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.SizedInts;

/**
 * Trie node types and manipulation mechanisms. The main purpose of this is to allow for handling tries directly as
 * they are on disk without any serialization, and to enable the creation of such files.
 * <p>
 * The serialization methods take as argument a generic {@code SerializationNode} and provide a method {@code typeFor}
 * for choosing a suitable type to represent it, which can then be used to calculate size and write the node.
 * <p>
 * To read a file containing trie nodes, one would use {@code at} to identify the node type and then the various
 * read methods to retrieve the data. They all take a buffer (usually memory-mapped) containing the data, and a position
 * in it that identifies the node.
 * <p>
 * These node types do not specify any treatment of payloads. They are only concerned with providing 4 bits of
 * space for {@code payloadFlags}, and a way of calculating the position after the node. Users of this class by convention
 * use non-zero payloadFlags to indicate a payload exists, write it (possibly in flag-dependent format) at serialization
 * time after the node itself is written, and read it using the {@code payloadPosition} value.
 * <p>
 * To improve efficiency, multiple node types depending on the number of transitions are provided:
 * -- payload only, which has no outgoing transitions
 * -- single outgoing transition
 * -- sparse, which provides a list of transition bytes with corresponding targets
 * -- dense, where the transitions span a range of values and having the list (and the search in it) can be avoided
 * <p>
 * For each of the transition-carrying types we also have "in-page" versions where transition targets are the 4, 8 or 12
 * lowest bits of the position within the same page. To save one further byte, the single in-page versions using 4 or 12
 * bits cannot carry a payload.
 * <p>
 * This class is effectively an enumeration; abstract class permits instances to extend each other and reuse code.
 * <p>
 * See {@code org/apache/cassandra/io/sstable/format/bti/BtiFormat.md} for a description of the mechanisms of writing
 * and reading an on-disk trie.
 */
@SuppressWarnings({ "SameParameterValue" })
public abstract class TrieNode
{
    /** Value used to indicate a branch (e.g. for transition and lastTransition) does not exist. */
    public static final int NONE = -1;

    // Consumption (read) methods

    /**
     * Returns the type of node stored at this position. It can then be used to call the methods below.
     */
    public static TrieNode at(ByteBuffer src, int position)
    {
        return Types.values[(src.get(position) >> 4) & 0xF];
    }

    /**
     * Returns the 4 payload flag bits. Node types that cannot carry a payload return 0.
     */
    public int payloadFlags(ByteBuffer src, int position)
    {
        return src.get(position) & 0x0F;
    }

    /**
     * Return the position just after the node, where the payload is usually stored.
     */
    abstract public int payloadPosition(ByteBuffer src, int position);

    /**
     * Returns search index for the given byte in the node. If exact match is present, this is >= 0, otherwise as in
     * binary search.
     */
    abstract public int search(ByteBuffer src, int position, int transitionByte);       // returns as binarySearch

    /**
     * Returns the upper childIndex limit. Calling transition with values 0...transitionRange - 1 is valid.
     */
    abstract public int transitionRange(ByteBuffer src, int position);

    /**
     * Returns the byte value for this child index, or Integer.MAX_VALUE if there are no transitions with this index or
     * higher to permit listing the children without needing to call transitionRange.
     *
     * @param childIndex must be >= 0, though it is allowed to pass a value greater than {@code transitionRange - 1}
     */
    abstract public int transitionByte(ByteBuffer src, int position, int childIndex);

    /**
     * Returns the delta between the position of this node and the position of the target of the specified transition.
     * This is always a negative number. Dense nodes use 0 to specify "no transition".
     *
     * @param childIndex must be >= 0 and < {@link #transitionRange(ByteBuffer, int)} - note that this is not validated
     *                   and behaviour of this method is undefined for values outside of that range
     */
    abstract long transitionDelta(ByteBuffer src, int position, int childIndex);

    /**
     * Returns position of node to transition to for the given search index. Argument must be positive. May return NONE
     * if a transition with that index does not exist (DENSE nodes).
     * Position is the offset of the node within the ByteBuffer. positionLong is its global placement, which is the
     * base for any offset calculations.
     *
     * @param positionLong although it seems to be obvious, this argument must be "real", that is, each child must have
     *                     the calculated absolute position >= 0, otherwise the behaviour of this method is undefined
     * @param childIndex   must be >= 0 and < {@link #transitionRange(ByteBuffer, int)} - note that this is not validated
     *                     and behaviour of this method is undefined for values outside of that range
     */
    public long transition(ByteBuffer src, int position, long positionLong, int childIndex)
    {
        // note: this is not valid for dense nodes
        return positionLong + transitionDelta(src, position, childIndex);
    }

    /**
     * Returns the highest transition for this node, or NONE if none exist (PAYLOAD_ONLY nodes).
     */
    public long lastTransition(ByteBuffer src, int position, long positionLong)
    {
        return transition(src, position, positionLong, transitionRange(src, position) - 1);
    }

    /**
     * Returns a transition that is higher than the index returned by {@code search}. This may not exist (if the
     * argument was higher than the last transition byte), in which case this returns the given {@code defaultValue}.
     */
    abstract public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue);

    /**
     * Returns a transition that is lower than the index returned by {@code search}. Returns {@code defaultValue} for
     * {@code searchIndex} equals 0 or -1 as lesser transition for those indexes does not exist.
     */
    abstract public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue);

    // Construction (serialization) methods

    /**
     * Returns a node type that is suitable to store the node.
     */
    public static TrieNode typeFor(SerializationNode<?> node, long nodePosition)
    {
        int c = node.childCount();
        if (c == 0)
            return Types.PAYLOAD_ONLY;

        int bitsPerPointerIndex = 0;
        long delta = node.maxPositionDelta(nodePosition);
        assert delta < 0;
        while (!Types.singles[bitsPerPointerIndex].fits(-delta))
            ++bitsPerPointerIndex;

        if (c == 1)
        {
            if (node.payload() != null && Types.singles[bitsPerPointerIndex].bytesPerPointer == FRACTIONAL_BYTES)
                ++bitsPerPointerIndex; // next index will permit payload

            return Types.singles[bitsPerPointerIndex];
        }

        TrieNode sparse = Types.sparses[bitsPerPointerIndex];
        TrieNode dense = Types.denses[bitsPerPointerIndex];
        return (sparse.sizeofNode(node) < dense.sizeofNode(node)) ? sparse : dense;
    }

    /**
     * Returns the size needed to serialize this node.
     */
    abstract public int sizeofNode(SerializationNode<?> node);

    /**
     * Serializes the node. All transition target positions must already have been defined. {@code payloadBits} must
     * be four bits.
     */
    abstract public void serialize(DataOutputPlus out, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException;

    // Implementations

    final int bytesPerPointer;
    static final int FRACTIONAL_BYTES = 0;

    TrieNode(int ordinal, int bytesPerPointer)
    {
        this.ordinal = ordinal;
        this.bytesPerPointer = bytesPerPointer;
    }

    final int ordinal;

    static private class PayloadOnly extends TrieNode
    {
        // byte flags
        // var payload
        PayloadOnly(int ordinal)
        {
            super(ordinal, FRACTIONAL_BYTES);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 1;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            return -1;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return 0;
        }

        @Override
        public long transition(ByteBuffer src, int position, long positionLong, int childIndex)
        {
            return NONE;
        }

        @Override
        public long lastTransition(ByteBuffer src, int position, long positionLong)
        {
            return NONE;
        }

        @Override
        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            return defaultValue;
        }

        @Override
        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            return defaultValue;
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public int transitionRange(ByteBuffer src, int position)
        {
            return 0;
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            return 1;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
        }
    }

    static private class Single extends TrieNode
    {
        // byte flags
        // byte transition
        // bytesPerPointer bytes transition target
        // var payload

        Single(int ordinal, int bytesPerPointer)
        {
            super(ordinal, bytesPerPointer);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 2 + bytesPerPointer;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            int c = src.get(position + 1) & 0xFF;
            if (transitionByte == c)
                return 0;
            return transitionByte < c ? -1 : -2;
        }

        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -readBytes(src, position + 2);
        }

        @Override
        public long lastTransition(ByteBuffer src, int position, long positionLong)
        {
            return transition(src, position, positionLong, 0);
        }

        @Override
        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            return (searchIndex == -1) ? transition(src, position, positionLong, 0) : defaultValue;
        }

        @Override
        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            return searchIndex == 0 || searchIndex == -1 ? defaultValue : transition(src, position, positionLong, 0);
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return childIndex == 0 ? src.get(position + 1) & 0xFF : Integer.MAX_VALUE;
        }

        @Override
        public int transitionRange(ByteBuffer src, int position)
        {
            return 1;
        }

        public int sizeofNode(SerializationNode<?> node)
        {
            return 2 + bytesPerPointer;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            assert childCount == 1;
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));

            dest.writeByte(node.transition(0));
            writeBytes(dest, -node.serializedPositionDelta(0, nodePosition));
        }
    }

    static private class SingleNoPayload4 extends Single
    {
        // 4-bit type ordinal
        // 4-bit target delta
        // byte transition
        // no payload!
        SingleNoPayload4(int ordinal)
        {
            super(ordinal, FRACTIONAL_BYTES);
        }

        @Override
        public int payloadFlags(ByteBuffer src, int position)
        {
            return 0;
        }

        // Although we don't have a payload position, provide one for calculating the size of the node.
        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 2;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -(src.get(position) & 0xF);
        }

        @Override
        boolean fits(long delta)
        {
            return 0 <= delta && delta <= 0xF;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            assert payloadBits == 0;
            int childCount = node.childCount();
            assert childCount == 1;
            long pd = -node.serializedPositionDelta(0, nodePosition);
            assert pd > 0 && pd < 0x10;
            dest.writeByte((ordinal << 4) + (int) (pd & 0x0F));
            dest.writeByte(node.transition(0));
        }

        @Override
        public int sizeofNode(SerializationNode<?> node)
        {
            return 2;
        }
    }

    static private class SingleNoPayload12 extends Single
    {
        // 4-bit type ordinal
        // 12-bit target delta
        // byte transition
        // no payload!
        SingleNoPayload12(int ordinal)
        {
            super(ordinal, FRACTIONAL_BYTES);
        }

        @Override
        public int payloadFlags(ByteBuffer src, int position)
        {
            return 0;
        }

        // Although we don't have a payload position, provide one for calculating the size of the node.
        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 3;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            int c = src.get(position + 2) & 0xFF;
            if (transitionByte == c)
                return 0;
            return transitionByte < c ? -1 : -2;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -(src.getShort(position) & 0xFFF);
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return childIndex == 0 ? src.get(position + 2) & 0xFF : Integer.MAX_VALUE;
        }

        @Override
        boolean fits(long delta)
        {
            return 0 <= delta && delta <= 0xFFF;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            assert payloadBits == 0;
            int childCount = node.childCount();
            assert childCount == 1;
            long pd = -node.serializedPositionDelta(0, nodePosition);
            assert pd > 0 && pd < 0x1000;
            dest.writeByte((ordinal << 4) + (int) ((pd >> 8) & 0x0F));
            dest.writeByte((byte) pd);
            dest.writeByte(node.transition(0));
        }

        @Override
        public int sizeofNode(SerializationNode<?> node)
        {
            return 3;
        }
    }

    static private class Sparse extends TrieNode
    {
        // byte flags
        // byte count (<= 255)
        // count bytes transitions
        // count ints transition targets
        // var payload

        Sparse(int ordinal, int bytesPerPointer)
        {
            super(ordinal, bytesPerPointer);
        }

        @Override
        public int transitionRange(ByteBuffer src, int position)
        {
            return src.get(position + 1) & 0xFF;
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            int count = transitionRange(src, position);
            return position + 2 + (bytesPerPointer + 1) * count;
        }

        @Override
        public int search(ByteBuffer src, int position, int key)
        {
            int l = -1; // known < key
            int r = transitionRange(src, position);   // known > key
            position += 2;

            while (l + 1 < r)
            {
                int m = (l + r + 1) / 2;
                int childTransition = src.get(position + m) & 0xFF;
                int cmp = Integer.compare(key, childTransition);
                if (cmp < 0)
                    r = m;
                else if (cmp > 0)
                    l = m;
                else
                    return m;
            }

            return -r - 1;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            assert childIndex >= 0;
            int range = transitionRange(src, position);
            assert childIndex < range;
            return -readBytes(src, position + 2 + range + bytesPerPointer * childIndex);
        }

        @Override
        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            if (searchIndex < 0)
                searchIndex = -1 - searchIndex;
            else
                ++searchIndex;
            if (searchIndex >= transitionRange(src, position))
                return defaultValue;
            return transition(src, position, positionLong, searchIndex);
        }

        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            if (searchIndex == 0 || searchIndex == -1)
                return defaultValue;
            if (searchIndex < 0)
                searchIndex = -2 - searchIndex;
            else
                --searchIndex;
            return transition(src, position, positionLong, searchIndex);
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            return childIndex < transitionRange(src, position) ? src.get(position + 2 + childIndex) & 0xFF : Integer.MAX_VALUE;
        }

        @Override
        public int sizeofNode(SerializationNode<?> node)
        {
            return 2 + node.childCount() * (1 + bytesPerPointer);
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            assert childCount > 0;
            assert childCount < 256;
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            dest.writeByte(childCount);

            for (int i = 0; i < childCount; ++i)
                dest.writeByte(node.transition(i));
            for (int i = 0; i < childCount; ++i)
                writeBytes(dest, -node.serializedPositionDelta(i, nodePosition));
        }
    }

    static private class Sparse12 extends Sparse
    {
        // byte flags
        // byte count (<= 255)
        // count bytes transitions
        // count 12-bits transition targets
        // var payload
        Sparse12(int ordinal)
        {
            super(ordinal, FRACTIONAL_BYTES);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            int count = transitionRange(src, position);
            return position + 2 + (5 * count + 1) / 2;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -read12Bits(src, position + 2 + transitionRange(src, position), childIndex);
        }

        @Override
        public int sizeofNode(SerializationNode<?> node)
        {
            return 2 + (node.childCount() * 5 + 1) / 2;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            assert childCount < 256;
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            dest.writeByte(childCount);

            for (int i = 0; i < childCount; ++i)
                dest.writeByte(node.transition(i));
            int i;
            for (i = 0; i + 2 <= childCount; i += 2)
            {
                int p0 = (int) -node.serializedPositionDelta(i, nodePosition);
                int p1 = (int) -node.serializedPositionDelta(i + 1, nodePosition);
                assert p0 > 0 && p0 < (1 << 12);
                assert p1 > 0 && p1 < (1 << 12);
                dest.writeByte(p0 >> 4);
                dest.writeByte((p0 << 4) | (p1 >> 8));
                dest.writeByte(p1);
            }
            if (i < childCount)
            {
                long pd = -node.serializedPositionDelta(i, nodePosition);
                assert pd > 0 && pd < (1 << 12);
                dest.writeShort((short) (pd << 4));
            }
        }

        @Override
        boolean fits(long delta)
        {
            return 0 <= delta && delta <= 0xFFF;
        }
    }

    static private class Dense extends TrieNode
    {
        // byte flags
        // byte start
        // byte length-1
        // length ints transition targets (-1 for not present)
        // var payload

        static final int NULL_VALUE = 0;

        Dense(int ordinal, int bytesPerPointer)
        {
            super(ordinal, bytesPerPointer);
        }

        @Override
        public int transitionRange(ByteBuffer src, int position)
        {
            return 1 + (src.get(position + 2) & 0xFF);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 3 + transitionRange(src, position) * bytesPerPointer;
        }

        @Override
        public int search(ByteBuffer src, int position, int transitionByte)
        {
            int l = src.get(position + 1) & 0xFF;
            int i = transitionByte - l;
            if (i < 0)
                return -1;
            int len = transitionRange(src, position);
            if (i >= len)
                return -len - 1;
            long t = transition(src, position, 0L, i);
            return t != -1 ? i : -i - 1;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -readBytes(src, position + 3 + childIndex * bytesPerPointer);
        }

        @Override
        public long transition(ByteBuffer src, int position, long positionLong, int childIndex)
        {
            long v = transitionDelta(src, position, childIndex);
            return v != NULL_VALUE ? v + positionLong : NONE;
        }

        @Override
        public long greaterTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            if (searchIndex < 0)
                searchIndex = -1 - searchIndex;
            else
                ++searchIndex;
            int len = transitionRange(src, position);
            for (; searchIndex < len; ++searchIndex)
            {
                long t = transition(src, position, positionLong, searchIndex);
                if (t != NONE)
                    return t;
            }
            return defaultValue;
        }

        @Override
        public long lesserTransition(ByteBuffer src, int position, long positionLong, int searchIndex, long defaultValue)
        {
            if (searchIndex == 0 || searchIndex == -1)
                return defaultValue;

            if (searchIndex < 0)
                searchIndex = -2 - searchIndex;
            else
                --searchIndex;
            for (; searchIndex >= 0; --searchIndex)
            {
                long t = transition(src, position, positionLong, searchIndex);
                if (t != -1)
                    return t;
            }
            assert false : "transition must always exist at 0, and we should not be called for less of that";
            return defaultValue;
        }

        @Override
        public int transitionByte(ByteBuffer src, int position, int childIndex)
        {
            if (childIndex >= transitionRange(src, position))
                return Integer.MAX_VALUE;
            int l = src.get(position + 1) & 0xFF;
            return l + childIndex;
        }

        @Override
        public int sizeofNode(SerializationNode<?> node)
        {
            int l = node.transition(0);
            int r = node.transition(node.childCount() - 1);
            return 3 + (r - l + 1) * bytesPerPointer;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            int l = node.transition(0);
            int r = node.transition(childCount - 1);
            assert 0 <= l && l <= r && r <= 255;
            dest.writeByte(l);
            dest.writeByte(r - l);      // r is included, i.e. this is len - 1

            for (int i = 0; i < childCount; ++i)
            {
                int next = node.transition(i);
                while (l < next)
                {
                    writeBytes(dest, NULL_VALUE);
                    ++l;
                }
                writeBytes(dest, -node.serializedPositionDelta(i, nodePosition));
                ++l;
            }
        }
    }

    static private class Dense12 extends Dense
    {
        // byte flags
        // byte start
        // byte length-1
        // length 12-bits transition targets (-1 for not present)
        // var payload

        Dense12(int ordinal)
        {
            super(ordinal, FRACTIONAL_BYTES);
        }

        @Override
        public int payloadPosition(ByteBuffer src, int position)
        {
            return position + 3 + (transitionRange(src, position) * 3 + 1) / 2;
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -read12Bits(src, position + 3, childIndex);
        }

        @Override
        public int sizeofNode(SerializationNode<?> node)
        {
            int l = node.transition(0);
            int r = node.transition(node.childCount() - 1);
            return 3 + ((r - l + 1) * 3 + 1) / 2;
        }

        @Override
        public void serialize(DataOutputPlus dest, SerializationNode<?> node, int payloadBits, long nodePosition) throws IOException
        {
            int childCount = node.childCount();
            dest.writeByte((ordinal << 4) + (payloadBits & 0x0F));
            int l = node.transition(0);
            int r = node.transition(childCount - 1);
            assert 0 <= l && l <= r && r <= 255;
            dest.writeByte(l);
            dest.writeByte(r - l);      // r is included, i.e. this is len - 1

            int carry = 0;
            int start = l;
            for (int i = 0; i < childCount; ++i)
            {
                int next = node.transition(i);
                while (l < next)
                {
                    carry = write12Bits(dest, NULL_VALUE, l - start, carry);
                    ++l;
                }
                long pd = node.serializedPositionDelta(i, nodePosition);
                carry = write12Bits(dest, (int) -pd, l - start, carry);
                ++l;
            }
            if (((l - start) & 1) == 1)
                dest.writeByte(carry);
        }

        @Override
        boolean fits(long delta)
        {
            return 0 <= delta && delta <= 0xFFF;
        }
    }

    static private class LongDense extends Dense
    {
        // byte flags
        // byte start
        // byte length-1
        // length long transition targets (-1 for not present)
        // var payload
        LongDense(int ordinal)
        {
            super(ordinal, 8);
        }

        @Override
        public long transitionDelta(ByteBuffer src, int position, int childIndex)
        {
            return -src.getLong(position + 3 + childIndex * 8);
        }

        @Override
        public void writeBytes(DataOutputPlus dest, long ofs) throws IOException
        {
            dest.writeLong(ofs);
        }

        @Override
        boolean fits(long delta)
        {
            return true;
        }
    }


    static int read12Bits(ByteBuffer src, int base, int searchIndex)
    {
        int word = src.getShort(base + (3 * searchIndex) / 2);
        if ((searchIndex & 1) == 0)
            word = (word >> 4);
        return word & 0xFFF;
    }

    static int write12Bits(DataOutput dest, int value, int index, int carry) throws IOException
    {
        assert 0 <= value && value <= 0xFFF;
        if ((index & 1) == 0)
        {
            dest.writeByte(value >> 4);
            return value << 4;
        }
        else
        {
            dest.writeByte(carry | (value >> 8));
            dest.writeByte(value);
            return 0;
        }
    }

    long readBytes(ByteBuffer src, int position)
    {
        return SizedInts.readUnsigned(src, position, bytesPerPointer);
    }

    void writeBytes(DataOutputPlus dest, long ofs) throws IOException
    {
        assert fits(ofs);
        SizedInts.write(dest, ofs, bytesPerPointer);
    }

    boolean fits(long delta)
    {
        return 0 <= delta && delta < (1L << (bytesPerPointer * 8));
    }

    @Override
    public String toString()
    {
        String res = getClass().getSimpleName();
        if (bytesPerPointer >= 1)
            res += (bytesPerPointer * 8);
        return res;
    }

    static class Types
    {
        static final TrieNode PAYLOAD_ONLY = new PayloadOnly(0);
        static final TrieNode SINGLE_NOPAYLOAD_4 = new SingleNoPayload4(1);
        static final TrieNode SINGLE_8 = new Single(2, 1);
        static final TrieNode SINGLE_NOPAYLOAD_12 = new SingleNoPayload12(3);
        static final TrieNode SINGLE_16 = new Single(4, 2);
        static final TrieNode SPARSE_8 = new Sparse(5, 1);
        static final TrieNode SPARSE_12 = new Sparse12(6);
        static final TrieNode SPARSE_16 = new Sparse(7, 2);
        static final TrieNode SPARSE_24 = new Sparse(8, 3);
        static final TrieNode SPARSE_40 = new Sparse(9, 5);
        static final TrieNode DENSE_12 = new Dense12(10);
        static final TrieNode DENSE_16 = new Dense(11, 2);
        static final TrieNode DENSE_24 = new Dense(12, 3);
        static final TrieNode DENSE_32 = new Dense(13, 4);
        static final TrieNode DENSE_40 = new Dense(14, 5);
        static final TrieNode LONG_DENSE = new LongDense(15);

        // The position of each type in this list must match its ordinal value. Checked by the static block below.
        static final TrieNode[] values = new TrieNode[]{ PAYLOAD_ONLY,
                                                         SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16,
                                                         SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40,
                                                         DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40,
                                                         LONG_DENSE }; // Catch-all

        // We can't fit all types * all sizes in 4 bits, so we use a selection. When we don't have a matching instance
        // we just use something more general that can do its job.
        // The arrays below must have corresponding types for all sizes specified by the singles row.
        // Note: 12 bit sizes are important, because that size will fit any pointer within a page-packed branch.
        static final TrieNode[] singles = new TrieNode[]{ SINGLE_NOPAYLOAD_4, SINGLE_8, SINGLE_NOPAYLOAD_12, SINGLE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE };
        static final TrieNode[] sparses = new TrieNode[]{ SPARSE_8, SPARSE_8, SPARSE_12, SPARSE_16, SPARSE_24, SPARSE_40, SPARSE_40, LONG_DENSE };
        static final TrieNode[] denses = new TrieNode[]{ DENSE_12, DENSE_12, DENSE_12, DENSE_16, DENSE_24, DENSE_32, DENSE_40, LONG_DENSE };

        static
        {
            //noinspection ConstantConditions
            assert sparses.length == singles.length && denses.length == singles.length && values.length <= 16;
            for (int i = 0; i < values.length; ++i)
                assert values[i].ordinal == i;
        }
    }
}
