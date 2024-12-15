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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.RangeFactory;
import accord.primitives.Ranges;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public abstract class AccordRoutingKey extends AccordRoutableKey implements RoutingKey, RangeFactory
{
    public enum RoutingKeyKind
    {
        TOKEN, SENTINEL, MIN_TOKEN
    }

    protected AccordRoutingKey(TableId table)
    {
        super(table);
    }

    public abstract RoutingKeyKind kindOfRoutingKey();
    public abstract long estimatedSizeOnHeap();
    public abstract AccordRoutingKey withTable(TableId table);

    @Override
    public RangeFactory rangeFactory()
    {
        return this;
    }

    @Override
    public Range newRange(RoutingKey start, RoutingKey end)
    {
        return TokenRange.create((AccordRoutingKey) start, (AccordRoutingKey) end);
    }

    @Override
    public Range newAntiRange(RoutingKey start, RoutingKey end)
    {
        return TokenRange.createUnsafe((AccordRoutingKey) start, (AccordRoutingKey) end);
    }


    public SentinelKey asSentinelKey()
    {
        return (SentinelKey) this;
    }

    public TokenKey asTokenKey()
    {
        return (TokenKey) this;
    }

    @Override
    public RoutingKey toUnseekable()
    {
        return this;
    }

    @Override
    public RoutingKey asRoutingKey()
    {
        return asTokenKey();
    }

    public static AccordRoutingKey of(Key key)
    {
        return (AccordRoutingKey) key;
    }

    // final in part because we refer to its class directly in AccordRoutableKey.compareTo
    public static final class SentinelKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SentinelKey(null, true, false));

        // Is this a min sentinel or a max sentinel
        public final boolean isMinSentinel;

        // Is this a minumum of a max or min sentinel
        // Allows conversion of a token key to a range
        public final boolean isMinMinSentinel;

        public SentinelKey(TableId table, boolean isMinSentinel, boolean isMinMinSentinel)
        {
            super(table);
            this.isMinSentinel = isMinSentinel;
            this.isMinMinSentinel = isMinMinSentinel;
        }

        @Override
        public int hashCode()
        {
            int result = table.hashCode();
            result = 31 * result + (isMinSentinel ? 1 : 0);
            result = 31 * result + (isMinMinSentinel ? 1 : 0);
            return result;
        }

        @Override
        public RoutingKeyKind kindOfRoutingKey()
        {
            return RoutingKeyKind.SENTINEL;
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE;
        }

        public AccordRoutingKey withTable(TableId table)
        {
            return new SentinelKey(table, isMinSentinel, isMinMinSentinel);
        }

        public static SentinelKey min(TableId table)
        {
            return new SentinelKey(table, true, false);
        }

        public static SentinelKey max(TableId table)
        {
            return new SentinelKey(table, false, false);
        }

        public TokenKey toTokenKeyBroken()
        {
            IPartitioner partitioner = getPartitioner();
            return new TokenKey(table, isMinSentinel ?
                                       partitioner.getMinimumToken().nextValidToken() :
                                       partitioner.getMaximumTokenForSplitting());
        }

        @Override
        public Token token()
        {
            throw new UnsupportedOperationException();
        }

        int asInt()
        {
            if (isMinSentinel)
            {
                if (isMinMinSentinel)
                    return -2;
                else
                    return -1;
            }
            else
            {
                if (isMinMinSentinel)
                    return 1;
                else
                    return 2;
            }
        }

        @Override
        public String suffix()
        {
            return isMinSentinel ? "-Inf" : "+Inf";
        }

        public static final AccordKeySerializer<SentinelKey> serializer = new AccordKeySerializer<SentinelKey>()
        {
            @Override
            public void serialize(SentinelKey key, DataOutputPlus out, int version) throws IOException
            {
                key.table.serialize(out);
                out.writeBoolean(key.isMinSentinel);
                out.writeBoolean(key.isMinMinSentinel);
            }

            @Override
            public void skip(DataInputPlus in, int version) throws IOException
            {
                in.skipBytesFully(TableId.staticSerializedSize() + 1);
            }

            @Override
            public SentinelKey deserialize(DataInputPlus in, int version) throws IOException
            {
                TableId table = TableId.deserialize(in);
                boolean isMin = in.readBoolean();
                boolean isMinMin = in.readBoolean();
                return new SentinelKey(table, isMin, isMinMin);
            }

            @Override
            public long serializedSize(SentinelKey key, int version)
            {
                return key.table().serializedSize() + TypeSizes.BOOL_SIZE + TypeSizes.BOOL_SIZE;
            }
        };

        @Override
        public Range asRange()
        {
            checkState(!isMinMinSentinel, "It might be possible to support converting a minmin sentinel to a range, but it needs to be evaluated in the context where it is failing");
            return new TokenRange(new SentinelKey(table, isMinSentinel, true), this);
        }
    }

    private static class TokenKeySerializer<T extends TokenKey> implements AccordKeySerializer<T>
    {
        private final BiFunction<TableId, Token, T> factory;

        private TokenKeySerializer(BiFunction<TableId, Token, T> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(T key, DataOutputPlus out, int version) throws IOException
        {
            key.table.serialize(out);
            Token.compactSerializer.serialize(key.token, out, version);
        }

        @Override
        public void skip(DataInputPlus in, int version) throws IOException
        {
            in.skipBytesFully(TableId.staticSerializedSize());
            // TODO (expected): should we be using the TableId partitioner here?
            Token.compactSerializer.skip(in, getPartitioner(), version);
        }

        @Override
        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            TableId table = TableId.deserialize(in).intern();
            Token token = Token.compactSerializer.deserialize(in, getPartitioner(), version);
            return factory.apply(table, token);
        }

        public T fromBytes(ByteBuffer bytes, IPartitioner partitioner)
        {
            TableId tableId = TableId.deserialize(bytes, ByteBufferAccessor.instance, 0).intern();
            bytes.position(tableId.serializedSize());
            Token token = Token.compactSerializer.deserialize(bytes, partitioner);
            return factory.apply(tableId, token);
        }

        public ByteBuffer toBytes(T tokenKey)
        {
            int size = (int) (tokenKey.table.serializedSize() + Token.compactSerializer.serializedSize(tokenKey.token));
            ByteBuffer out = ByteBuffer.allocate(size);
            int position = tokenKey.table.serialize(out, ByteBufferAccessor.instance, 0);
            out.position(position);
            Token.compactSerializer.serialize(tokenKey.token, out);
            out.flip();
            return out;
        }

        @Override
        public long serializedSize(TokenKey key, int version)
        {
            return key.table.serializedSize() + Token.compactSerializer.serializedSize(key.token(), version);
        }
    }

    // Should be final in part because we refer to its class directly in AccordRoutableKey.compareTo
    // but it's helpful for MinTokenKey to be able to extend so `asTokenKey` also works with it
    public static class TokenKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new TokenKey(null, null));

        @Override
        public Range asRange()
        {
            AccordRoutingKey before = token.isMinimum()
                                      ? new SentinelKey(table, true, false)
                                      : new MinTokenKey(table, token);

            return TokenRange.create(before, this);
        }

        final Token token;
        public TokenKey(TableId tableId, Token token)
        {
            super(tableId);
            this.token = token;
        }

        public TokenKey withToken(Token token)
        {
            return new TokenKey(table, token);
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public RoutingKeyKind kindOfRoutingKey()
        {
            return RoutingKeyKind.TOKEN;
        }

        @Override
        public String suffix()
        {
            return token.toString();
        }

        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + token().getHeapSize();
        }

        public AccordRoutingKey withTable(TableId table)
        {
            return new TokenKey(table, token);
        }

        public static final TokenKeySerializer<TokenKey> serializer = new TokenKeySerializer<>(TokenKey::new);
    }

    // Allows the creation of a Range that is begin inclusive or end exclusive for a given Token
    public static final class MinTokenKey extends TokenKey
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new MinTokenKey(null, null));

        @Override
        public Range asRange()
        {
            AccordRoutingKey before = token.isMinimum()
                                      ? new SentinelKey(table, true, false)
                                      : new TokenKey(table, token.decreaseSlightly());

            return new TokenRange(before, this);
        }

        public MinTokenKey(TableId tableId, Token token)
        {
            super(tableId, token);
        }

        public MinTokenKey withToken(Token token)
        {
            return new MinTokenKey(table, token);
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public RoutingKeyKind kindOfRoutingKey()
        {
            return RoutingKeyKind.MIN_TOKEN;
        }

        @Override
        public String suffix()
        {
            return token.toString();
        }

        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + token().getHeapSize();
        }

        public AccordRoutingKey withTable(TableId table)
        {
            return new MinTokenKey(table, token);
        }

        public static final TokenKeySerializer<MinTokenKey> serializer = new TokenKeySerializer<>(MinTokenKey::new);
    }

    public static class Serializer implements AccordKeySerializer<AccordRoutingKey>
    {
        static final RoutingKeyKind[] kinds = RoutingKeyKind.values();

        @Override
        public void serialize(AccordRoutingKey key, DataOutputPlus out, int version) throws IOException
        {
            out.write(key.kindOfRoutingKey().ordinal());
            switch (key.kindOfRoutingKey())
            {
                case TOKEN:
                    TokenKey.serializer.serialize((TokenKey) key, out, version);
                    break;
                case SENTINEL:
                    SentinelKey.serializer.serialize((SentinelKey) key, out, version);
                    break;
                case MIN_TOKEN:
                    MinTokenKey.serializer.serialize((MinTokenKey) key, out, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }

        public ByteBuffer serialize(AccordRoutingKey key)
        {
            try (DataOutputBuffer buffer = new DataOutputBuffer((int)serializedSize(key, 0)))
            {
                try
                {
                    serialize(key, buffer, 0);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
                return buffer.asNewBuffer();
            }
        }

        public AccordRoutingKey deserialize(ByteBuffer buffer)
        {
            try (DataInputBuffer in = new DataInputBuffer(buffer, true))
            {
                try
                {
                    return deserialize(in, 0);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void skip(DataInputPlus in, int version) throws IOException
        {
            RoutingKeyKind kind = kinds[in.readByte()];
            switch (kind)
            {
                case TOKEN:
                    TokenKey.serializer.skip(in, version);
                    break;
                case SENTINEL:
                    SentinelKey.serializer.skip(in, version);
                    break;
                case MIN_TOKEN:
                    MinTokenKey.serializer.skip(in, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public AccordRoutingKey deserialize(DataInputPlus in, int version) throws IOException
        {
            RoutingKeyKind kind = kinds[in.readByte()];
            switch (kind)
            {
                case TOKEN:
                    return TokenKey.serializer.deserialize(in, version);
                case SENTINEL:
                    return SentinelKey.serializer.deserialize(in, version);
                case MIN_TOKEN:
                    return MinTokenKey.serializer.deserialize(in, version);
                default:
                    throw new IllegalArgumentException();
            }
        }

        @Override
        public long serializedSize(AccordRoutingKey key, int version)
        {
            long size = TypeSizes.BYTE_SIZE; // kind ordinal
            switch (key.kindOfRoutingKey())
            {
                case TOKEN:
                    size += TokenKey.serializer.serializedSize((TokenKey) key, version);
                    break;
                case SENTINEL:
                    size += SentinelKey.serializer.serializedSize((SentinelKey) key, version);
                    break;
                case MIN_TOKEN:
                    size += MinTokenKey.serializer.serializedSize((MinTokenKey)key, version);
                    break;
                default:
                    throw new IllegalArgumentException();
            }
            return size;
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
            {
                List<Ranges> splits = subSplitter.split(Ranges.ofSortedAndDeoverlapped(keyspaceRanges.toArray(new Range[0])));

                for (int i = 0; i < splits.size(); i++)
                {
                    if (i == results.size()) results.add(Ranges.EMPTY);
                    results.set(i, results.get(i).with(splits.get(i)));
                }
            }
            return results;
        }

        @Override
        public Range splitRange(Range range, int from, int to, int numSplits)
        {
            return subSplitter.splitRange(range, from, to, numSplits);
        }

        @Override
        public int numberOfSplitsPossible(Range range)
        {
            return subSplitter.numberOfSplitsPossible(range);
        }
    }
}
