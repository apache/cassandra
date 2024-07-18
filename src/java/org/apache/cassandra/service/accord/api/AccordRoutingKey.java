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
import java.util.Objects;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.RangeFactory;
import accord.primitives.Ranges;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public abstract class AccordRoutingKey extends AccordRoutableKey implements RoutingKey
{
    public enum RoutingKeyKind
    {
        TOKEN, SENTINEL
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
        return (s, e) -> new TokenRange((AccordRoutingKey) s, (AccordRoutingKey) e);
    }

    public SentinelKey asSentinelKey()
    {
        return (SentinelKey) this;
    }

    public TokenKey asTokenKey()
    {
        return (TokenKey) this;
    }

    public static AccordRoutingKey of(Key key)
    {
        return (AccordRoutingKey) key;
    }

    // final in part because we refer to its class directly in AccordRoutableKey.compareTo
    public static final class SentinelKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SentinelKey(null, true));

        public final boolean isMin;

        public SentinelKey(TableId table, boolean isMin)
        {
            super(table);
            this.isMin = isMin;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, isMin);
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
            return new SentinelKey(table, isMin);
        }

        public static SentinelKey min(TableId table)
        {
            return new SentinelKey(table, true);
        }

        public static SentinelKey max(TableId table)
        {
            return new SentinelKey(table, false);
        }

        public TokenKey toTokenKeyBroken()
        {
            IPartitioner partitioner = getPartitioner();
            return new TokenKey(table, isMin ?
                                         partitioner.getMinimumToken().nextValidToken() :
                                         partitioner.getMaximumTokenForSplitting().decreaseSlightly());
        }

        @Override
        public Token token()
        {
            throw new UnsupportedOperationException();
        }

        int asInt()
        {
            return isMin ? -1 : 1;
        }

        @Override
        public String suffix()
        {
            return isMin ? "-Inf" : "+Inf";
        }

        public static final IVersionedSerializer<SentinelKey> serializer = new IVersionedSerializer<SentinelKey>()
        {
            @Override
            public void serialize(SentinelKey key, DataOutputPlus out, int version) throws IOException
            {
                key.table.serialize(out);
                out.writeBoolean(key.isMin);
            }

            @Override
            public SentinelKey deserialize(DataInputPlus in, int version) throws IOException
            {
                TableId table = TableId.deserialize(in);
                boolean isMin = in.readBoolean();
                return new SentinelKey(table, isMin);
            }

            @Override
            public long serializedSize(SentinelKey key, int version)
            {
                return key.table().serializedSize() + TypeSizes.BOOL_SIZE;
            }
        };

        @Override
        public Range asRange()
        {
            throw new UnsupportedOperationException();
        }
    }

    // final in part because we refer to its class directly in AccordRoutableKey.compareToe
    public static final class TokenKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE;

        @Override
        public Range asRange()
        {
            AccordRoutingKey before = token.isMinimum()
                                      ? new SentinelKey(table, true)
                                      : new TokenKey(table, token.decreaseSlightly());

            return new TokenRange(before, this);
        }

        static
        {
            EMPTY_SIZE = ObjectSizes.measure(new TokenKey(null, null));
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

        public static final Serializer serializer = new Serializer();
        public static class Serializer implements IVersionedSerializer<TokenKey>
        {
            private Serializer() {}

            @Override
            public void serialize(TokenKey key, DataOutputPlus out, int version) throws IOException
            {
                key.table.serialize(out);
                Token.compactSerializer.serialize(key.token, out, version);
            }

            @Override
            public TokenKey deserialize(DataInputPlus in, int version) throws IOException
            {
                TableId table = TableId.deserialize(in);
                Token token = Token.compactSerializer.deserialize(in, getPartitioner(), version);
                return new TokenKey(table, token);
            }

            @Override
            public long serializedSize(TokenKey key, int version)
            {
                return key.table.serializedSize() + Token.compactSerializer.serializedSize(key.token(), version);
            }
        }
    }

    public static class Serializer implements IVersionedSerializer<AccordRoutingKey>
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
        public AccordRoutingKey deserialize(DataInputPlus in, int version) throws IOException
        {
            RoutingKeyKind kind = kinds[in.readByte()];
            switch (kind)
            {
                case TOKEN:
                    return TokenKey.serializer.deserialize(in, version);
                case SENTINEL:
                    return SentinelKey.serializer.deserialize(in, version);
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
    }
}
