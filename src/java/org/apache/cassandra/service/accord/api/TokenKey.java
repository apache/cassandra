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

package org.apache.cassandra.service.accord.api;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.RangeFactory;
import accord.primitives.Ranges;
import accord.utils.Invariants;
import accord.utils.VIntCoding;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public final class TokenKey extends AccordRoutableKey implements RoutingKey, RangeFactory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TokenKey(null, null));

    @Override
    public Range asRange()
    {
        return TokenRange.create(before(), this);
    }

    // we use the first 2 bits as a prefix, and the last 6 bits as a postfix comparison
    final byte sentinel;
    final Token token;
    private TokenKey(TableId tableId, byte sentinel, Token token)
    {
        super(tableId);
        this.sentinel = sentinel;
        this.token = token;
    }

    public TokenKey(TableId tableId, Token token)
    {
        this(tableId, NORMAL_SENTINEL, token);
    }

    public TokenKey withToken(Token token)
    {
        return new TokenKey(table, sentinel, token);
    }

    @Override
    public Token token()
    {
        return token;
    }

    @Override
    public byte sentinel()
    {
        return sentinel;
    }

    public byte prefixSentinel()
    {
        return (byte) (sentinel & PREFIX_MASK);
    }

    public byte suffixSentinel()
    {
        return (byte) (sentinel & SUFFIX_MASK);
    }

    // this can be invoked to a depth of 3 from a real token
    @VisibleForTesting
    public TokenKey before()
    {
        Invariants.require(!isTokenSentinel(), "Unable to call .before() when already a token sentinel: %s", this);
        int lowestBit = Integer.lowestOneBit(sentinel);
        Invariants.require(lowestBit != 1);
        byte newSentinel = (byte)((sentinel ^ lowestBit) | (lowestBit >>> 1));
        return new TokenKey(table, newSentinel, token);
    }

    // this can be invoked to a depth of 2 from a real token
    @VisibleForTesting
    public TokenKey after()
    {
        Invariants.require(!isTokenSentinel(), "Unable to call .after() when already a token sentinel: %s", this);
        int lowestBit = Integer.lowestOneBit(sentinel);
        // we can't use 0xf as we would not be able to disambiguate with variable length byte encoding escape
        Invariants.require((lowestBit != 1) && (sentinel & 0xf) != 0xe);
        byte newSentinel = (byte)(sentinel | (lowestBit >>> 1));
        return new TokenKey(table, newSentinel, token);
    }

    @Override
    public Object suffix()
    {
        return token;
    }

    public Object printableSuffix()
    {
        Object suffix = suffix();
        if (isSentinel())
        {
            if (isTableSentinel()) suffix = isMin() ? "-Inf" : "+Inf";
            if (isTokenSentinel()) suffix = (isBefore() ? "before(" : "after(") + suffix + ')';
        }
        return suffix;
    }

    @Override
    public String toString()
    {
        return prefix() + ":" + printableSuffix();
    }

    public static TokenKey parse(String str, IPartitioner partitioner)
    {
        TableId tableId;
        {
            int split = str.indexOf(':', str.startsWith("tid:") ? 4 : 0);
            tableId = TableId.fromString(str.substring(0, split));
            str = str.substring(split + 1);
        }
        if (str.endsWith("Inf"))
        {
            return new TokenKey(tableId, str.charAt(0) == '-' ? MIN_TABLE_SENTINEL : MAX_TABLE_SENTINEL, partitioner.getMinimumToken());
        }
        if (str.endsWith(")"))
        {
            boolean isBefore = str.startsWith("before(");
            boolean isAfter = str.startsWith("after(");
            if (isBefore || isAfter)
            {
                byte sentinel = isBefore ? BEFORE_TOKEN_SENTINEL : AFTER_TOKEN_SENTINEL;
                str = str.substring(isBefore ? 7 : 6, str.length() - 1);
                return new TokenKey(tableId, sentinel, partitioner.getTokenFactory().fromString(str));
            }
        }
        return new TokenKey(tableId, partitioner.getTokenFactory().fromString(str));
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + token().getHeapSize();
    }

    public TokenKey withTable(TableId table)
    {
        return new TokenKey(table, sentinel, token);
    }

    @Override
    public RangeFactory rangeFactory()
    {
        return this;
    }

    @Override
    public Range newRange(RoutingKey start, RoutingKey end)
    {
        return TokenRange.create((TokenKey) start, (TokenKey) end);
    }

    @Override
    public Range newAntiRange(RoutingKey start, RoutingKey end)
    {
        return TokenRange.createUnsafe((TokenKey) start, (TokenKey) end);
    }

    @Override
    public RoutingKey toUnseekable()
    {
        return this;
    }

    public boolean isMin()
    {
        return (sentinel & PREFIX_MASK) == (MIN_TABLE_SENTINEL & PREFIX_MASK);
    }

    public boolean isMax()
    {
        return (sentinel & PREFIX_MASK) == (MAX_TABLE_SENTINEL & PREFIX_MASK);
    }

    public boolean isSentinel()
    {
        return sentinel != NORMAL_SENTINEL;
    }

    public boolean isTableSentinel()
    {
        return (sentinel & PREFIX_MASK) != (NORMAL_SENTINEL & PREFIX_MASK);
    }

    public boolean isTokenSentinel()
    {
        return (sentinel & SUFFIX_MASK) != (NORMAL_SENTINEL & SUFFIX_MASK);
    }

    public boolean isBefore()
    {
        return (sentinel & SUFFIX_MASK) == (BEFORE_TOKEN_SENTINEL & SUFFIX_MASK);
    }

    public boolean isAfter()
    {
        return (sentinel & SUFFIX_MASK) == (AFTER_TOKEN_SENTINEL & SUFFIX_MASK);
    }

    public static TokenKey min(TableId table, IPartitioner partitioner)
    {
        return new TokenKey(table, MIN_TABLE_SENTINEL, partitioner.getMinimumToken());
    }

    public static TokenKey max(TableId table, IPartitioner partitioner)
    {
        return new TokenKey(table, MAX_TABLE_SENTINEL, partitioner.getMinimumToken());
    }

    public static TokenKey before(TableId table, Token token)
    {
        return new TokenKey(table, BEFORE_TOKEN_SENTINEL, token);
    }

    public static final NoTableSerializer noTableSerializer = new NoTableSerializer();


    public static class NoTableSerializer implements ParameterisedUnversionedSerializer<TokenKey, TableId>
    {
        @Override
        public void serialize(TokenKey key, TableId tableId, DataOutputPlus out) throws IOException
        {
            IPartitioner partitioner = key.token.getPartitioner();
            int fixedLength = partitioner.accordFixedLength();
            if (fixedLength < 0)
            {
                int len = partitioner.accordSerializedSize(key.token);
                out.writeUnsignedVInt32(len);
            }
            serializer.serializeWithoutPrefixOrLength(key, out);
        }

        public void serialize(TokenKey key, DataOutputPlus out) throws IOException
        {
            serialize(key, key.table, out);
        }

        @Override
        public long serializedSize(TokenKey key, TableId tableId)
        {
            IPartitioner partitioner = key.token.getPartitioner();
            int tokenSize = partitioner.accordFixedLength();
            if (tokenSize >= 0)
                return 2 + tokenSize;
            tokenSize = partitioner.accordSerializedSize(key.token);
            return 2 + tokenSize + VIntCoding.sizeOfUnsignedVInt(tokenSize);
        }

        public long serializedSize(TokenKey key)
        {
            return serializedSize(key, key.table);
        }

        @Override
        public TokenKey deserialize(TableId tableId, DataInputPlus in) throws IOException
        {
            IPartitioner partitioner = getPartitioner();
            int len = partitioner.accordFixedLength();
            if (len < 0) len = in.readUnsignedVInt32();
            return serializer.deserializeWithPrefix(tableId, len + 2, in, partitioner);
        }
    }

    public static final class Serializer implements AccordSearchableKeySerializer<TokenKey>
    {
        private Serializer() {}

        // stream serialization methods - including a dynamic length for variable size tokens
        // types are byte comparable only after any length component

        @Override
        public long serializedSize(TokenKey key)
        {
            IPartitioner partitioner = key.token.getPartitioner();
            int size = 2 + key.table.serializedCompactComparableSize();
            int tokenSize = partitioner.accordFixedLength();
            if (tokenSize >= 0)
                return size + tokenSize;
            tokenSize = partitioner.accordSerializedSize(key.token);
            return size + tokenSize + VIntCoding.sizeOfUnsignedVInt(tokenSize);
        }

        @Override
        public void serialize(TokenKey key, DataOutputPlus out) throws IOException
        {
            IPartitioner partitioner = key.token.getPartitioner();
            int fixedLength = partitioner.accordFixedLength();
            if (fixedLength < 0)
            {
                int len = partitioner.accordSerializedSize(key.token);
                out.writeUnsignedVInt32(len);
            }
            key.table.serializeCompactComparable(out);
            serializeWithoutPrefixOrLength(key, out);
        }

        @Override
        public TokenKey deserialize(DataInputPlus in) throws IOException
        {
            return deserialize(in, getPartitioner());
        }

        public TokenKey deserialize(DataInputPlus in, IPartitioner partitioner) throws IOException
        {
            int len = partitioner.accordFixedLength();
            if (len < 0) len = in.readUnsignedVInt32();
            TableId tableId = deserializePrefix(in);
            return deserializeWithPrefix(tableId, len + 2, in, partitioner);
        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            skip(in, getPartitioner());
        }

        public void skip(DataInputPlus in, IPartitioner partitioner) throws IOException
        {
            int len = partitioner.accordFixedLength();
            if (len < 0) len = in.readUnsignedVInt32();
            TableId.skipCompact(in);
            in.skipBytesFully(len + 2);
        }

        // methods for encoding/decoding a single ByteBuffer value

        public ByteBuffer serialize(TokenKey key)
        {
            int size = key.table.serializedCompactComparableSize() + serializedSizeWithoutPrefix(key);
            ByteBuffer result = ByteBuffer.allocate(size);
            result.position(key.table.serializeCompactComparable(result, ByteBufferAccessor.instance, 0));
            serializeWithoutPrefixOrLength(key, result);
            result.flip();
            return result;
        }

        // WARNING: consumes buffer!
        public TokenKey deserialize(ByteBuffer buffer)
        {
            return deserialize(buffer, getPartitioner());
        }

        public TokenKey deserialize(ByteBuffer buffer, IPartitioner partitioner)
        {
            TableId tableId = TableId.deserializeCompactComparable(buffer, ByteBufferAccessor.instance, 0);
            int offset = tableId.serializedCompactComparableSize();
            return deserializeWithPrefix(tableId, buffer.remaining() - offset, buffer, ByteBufferAccessor.instance, offset, partitioner);
        }

        // WARNING: consumes buffer!
        public TokenKey deserializeAndConsume(ByteBuffer buffer, IPartitioner partitioner)
        {
            TableId tableId = TableId.deserializeCompactComparable(buffer, ByteBufferAccessor.instance, 0);
            int offset = buffer.position();
            buffer.position(offset + tableId.serializedCompactComparableSize());
            return deserializeWithPrefix(tableId, buffer.remaining(), buffer, partitioner);
        }

        // methods for encoding searchable tokens separately from tableIds

        @Override
        public int fixedKeyLengthForPrefix(Object prefix)
        {
            int size = getPartitioner().accordFixedLength();
            if (size < 0)
                return size;
            return 2 + size;
        }

        @Override
        public int serializedSizeWithoutPrefix(TokenKey key)
        {
            return 2 + key.token.getPartitioner().accordSerializedSize(key.token);
        }

        @Override
        public int serializedSizeOfPrefix(Object prefix)
        {
            return ((TableId) prefix).serializedCompactComparableSize();
        }

        @Override
        public void serializePrefix(Object prefix, DataOutputPlus out) throws IOException
        {
            ((TableId)prefix).serializeCompactComparable(out);
        }

        @Override
        public void serializeWithoutPrefixOrLength(TokenKey key, DataOutputPlus out) throws IOException
        {
            out.write(key.prefixSentinel());
            key.token.getPartitioner().accordSerialize(key.token, out);
            out.write(key.suffixSentinel());
        }

        public ByteBuffer serializeWithoutPrefixOrLength(TokenKey key)
        {
            IPartitioner partitioner = key.token.getPartitioner();
            ByteBuffer result = ByteBuffer.allocate(serializedSizeWithoutPrefix(key));
            serializeWithoutPrefixOrLength(key, result, partitioner);
            result.flip();
            return result;
        }

        public void serializeWithoutPrefixOrLength(TokenKey key, ByteBuffer out)
        {
            serializeWithoutPrefixOrLength(key, out, key.token.getPartitioner());
        }

        private static void serializeWithoutPrefixOrLength(TokenKey key, ByteBuffer out, IPartitioner partitioner)
        {
            out.put(key.prefixSentinel());
            partitioner.accordSerialize(key.token, out);
            out.put(key.suffixSentinel());
        }

        @Override
        public TableId deserializePrefix(DataInputPlus in) throws IOException
        {
            return TableId.deserializeCompactComparable(in);
        }

        @Override
        public TokenKey deserializeWithPrefix(Object tableId, int length, DataInputPlus in) throws IOException
        {
            return deserializeWithPrefix(tableId, length, in, getPartitioner());
        }

        public TokenKey deserializeWithPrefix(Object tableId, int length, DataInputPlus in, IPartitioner partitioner) throws IOException
        {
            byte sentinel = in.readByte();
            Token token = partitioner.accordDeserialize(in, length - 2);
            sentinel |= in.readByte();
            return new TokenKey((TableId) tableId, sentinel, token);
        }

        public <V> TokenKey deserializeWithPrefixAndImpliedLength(Object tableId, V src, ValueAccessor<V> accessor, int offset)
        {
            return deserializeWithPrefixAndImpliedLength(tableId, src, accessor, offset, getPartitioner());
        }

        public <V> TokenKey deserializeWithPrefixAndImpliedLength(Object tableId, V src, ValueAccessor<V> accessor, int offset, IPartitioner partitioner)
        {
            return deserializeWithPrefix(tableId, accessor.remaining(src, offset), src, accessor, offset, partitioner);
        }

        public <V> TokenKey deserializeWithPrefix(Object tableId, int length, V src, ValueAccessor<V> accessor, int offset, IPartitioner partitioner)
        {
            byte sentinel = accessor.getByte(src, offset++);
            Token token = partitioner.accordDeserialize(src, accessor, offset, length - 2);
            offset += partitioner.accordSerializedSize(token);
            sentinel |= accessor.getByte(src, offset);
            return new TokenKey((TableId) tableId, sentinel, token);
        }

        // WARNING: consumes buffer!
        public TokenKey deserializeWithPrefixAndImpliedLength(Object tableId, ByteBuffer buffer, IPartitioner partitioner)
        {
            return deserializeWithPrefix(tableId, buffer.remaining(), buffer, partitioner);
        }

        // WARNING: consumes buffer!
        public TokenKey deserializeWithPrefix(Object tableId, int length, ByteBuffer buffer, IPartitioner partitioner)
        {
            byte sentinel = buffer.get();
            Token token = partitioner.accordDeserialize(buffer, length - 2);
            sentinel |= buffer.get();
            return new TokenKey((TableId) tableId, sentinel, token);
        }

        public static final byte ESCAPE_BYTE = 0x0f;
        private static final byte[] ESCAPE_BYTES = new byte[] { ESCAPE_BYTE };
        private static final int UNESCAPE = ESCAPE_BYTE;
        private static final int UNESCAPE_MASK = 0xffff;

        public static int countEscapes(byte[] bytes)
        {
            int escapeLimit = escapeLimit(bytes);
            int i = 0;
            int count = 0;
            while ((i = nextEscape(bytes, i, escapeLimit)) >= 0)
            {
                ++count;
                ++i;
            }
            return count;
        }

        public static int serializedSize(byte[] bytes)
        {
            return 1 + bytes.length + countEscapes(bytes);
        }

        private static int escapeLimit(byte[] bytes)
        {
            return bytes.length - 1;
        }

        private static int nextEscape(byte[] bytes, int index, int escapeLimit)
        {
            while (index <= escapeLimit)
            {
                if (bytes[index] == 0 && (index == escapeLimit || (bytes[index + 1] & 0xff) <= ESCAPE_BYTE))
                    return index;
                ++index;
            }
            return -1;
        }

        public static void serializeWithEscapes(byte[] bytes, ByteBuffer out)
        {
            serializeWithEscapesInternal(bytes, out, ByteBuffer::put);
            out.put(trailingByte(bytes));
        }

        public static void serializeWithEscapes(byte[] bytes, DataOutputPlus out) throws IOException
        {
            serializeWithEscapesInternal(bytes, out, DataOutputPlus::write);
            out.writeByte(trailingByte(bytes));
        }

        interface WriteBytes<V, T extends Throwable>
        {
            void write(V out, byte[] bytes, int offset, int length) throws T;
        }

        private static byte trailingByte(byte[] bytes)
        {
            return 0;
        }

        private static <V, T extends Throwable> void serializeWithEscapesInternal(byte[] bytes, V out, WriteBytes<V, T> write) throws T
        {
            int i = 0, escapeLimit = escapeLimit(bytes);
            while (true)
            {
                int nexti = nextEscape(bytes, i, escapeLimit);
                if (nexti < 0)
                    break;
                write.write(out, bytes, i, 1 + nexti - i);
                write.write(out, ESCAPE_BYTES, 0, 1);
                i = nexti + 1;
            }
            write.write(out, bytes, i, bytes.length - i);
        }

        public static byte[] deserializeWithEscapes(ByteBuffer in, int escapedLength)
        {
            Invariants.require(escapedLength >= 1);
            --escapedLength;
            byte[] bytes = new byte[escapedLength];
            in.get(bytes, 0, Math.min(escapedLength, in.remaining()));
            byte[] result = removeEscapes(bytes);
            byte trailingEscape = in.get();
            Invariants.require(trailingEscape == trailingByte(result));
            return result;
        }

        public static <V> byte[] deserializeWithEscapes(V src, ValueAccessor<V> accessor, int offset, int escapedLength)
        {
            Invariants.require(--escapedLength >= 0);
            byte[] result = removeEscapes(accessor.toArray(src, offset, Math.min(escapedLength, accessor.remaining(src, offset))));
            Invariants.require(trailingByte(result) == accessor.getByte(src, offset + escapedLength));
            return result;
        }

        public static byte[] deserializeWithEscapes(DataInputPlus in, int escapedLength) throws IOException
        {
            byte[] result = new byte[escapedLength - 1];
            in.readFully(result);
            result = removeEscapes(result);
            byte trailingEscape = in.readByte();
            Invariants.require(trailingEscape == trailingByte(result));
            return result;
        }

        private static byte[] removeEscapes(byte[] bytes)
        {
            if (bytes.length == 0)
                return bytes;

            int count = 1;
            int escapeMatcher = bytes[0];
            for (int i = 1; i < bytes.length ; ++i)
            {
                byte next = bytes[i];
                escapeMatcher = (escapeMatcher << 8) | next;
                if ((escapeMatcher & UNESCAPE_MASK) != UNESCAPE)
                {
                    if (count != i)
                        bytes[count] = next;
                    count++;
                }
            }

            if (bytes.length != count)
                bytes = Arrays.copyOf(bytes, count);
            return bytes;
        }

    }

    public static final Serializer serializer = new Serializer();

    public static class KeyspaceSplitter implements ShardDistributor
    {
        final EvenSplit<BigInteger> subSplitter;
        public KeyspaceSplitter(EvenSplit<BigInteger> subSplitter)
        {
            this.subSplitter = subSplitter;
        }

        @Override
        public List<Ranges> split(Ranges ranges)
        {
            Map<TableId, List<Range>> byTable = new TreeMap<>();
            for (Range range : ranges)
            {
                byTable.computeIfAbsent(((AccordRoutableKey)range.start()).table, ignore -> new ArrayList<>())
                          .add(range);
            }

            List<Ranges> results = new ArrayList<>();
            for (List<Range> keyspaceRanges : byTable.values())
                results.addAll(subSplitter.split(Ranges.ofSortedAndDeoverlapped(keyspaceRanges.toArray(new Range[0]))));
            return results;
        }

        @Override
        public Range splitRange(Range range, int from, int to, int numSplits)
        {
            return subSplitter.splitRange(range, from, to, numSplits);
        }

        @Override
        public Ranges selectFirstSubRanges(Range range, Ranges subRanges, int totalSplits)
        {
            return subSplitter.selectFirstSubRanges(range, subRanges, totalSplits);
        }

        @Override
        public int numberOfSplitsPossible(Range range)
        {
            return subSplitter.numberOfSplitsPossible(range);
        }
    }
}
