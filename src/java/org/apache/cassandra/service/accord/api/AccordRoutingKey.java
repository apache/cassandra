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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;

public abstract class AccordRoutingKey extends AccordRoutableKey implements RoutingKey
{
    enum RoutingKeyKind
    {
        TOKEN, SENTINEL
    }

    protected AccordRoutingKey(String keyspace)
    {
        super(keyspace);
    }

    public abstract RoutingKeyKind kindOfRoutingKey();
    public abstract long estimatedSizeOnHeap();

    public static AccordRoutingKey of(Key key)
    {
        return (AccordRoutingKey) key;
    }

    // final in part because we refer to its class directly in AccordRoutableKey.compareTo
    public static final class SentinelKey extends AccordRoutingKey
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new SentinelKey(null, true));

        private final boolean isMin;

        private SentinelKey(String keyspace, boolean isMin)
        {
            super(keyspace);
            this.isMin = isMin;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(keyspace, isMin);
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

        public static SentinelKey min(String keyspace)
        {
            return new SentinelKey(keyspace, true);
        }

        public static SentinelKey max(String keyspace)
        {
            return new SentinelKey(keyspace, false);
        }

        public TokenKey toTokenKey()
        {
            IPartitioner partitioner = getPartitioner();
            return new TokenKey(keyspace, isMin ?
                                         partitioner.getMinimumToken().increaseSlightly() :
                                         partitioner.getMaximumToken().decreaseSlightly());
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
        public String toString()
        {
            return "SentinelKey{" +
                   "keyspace=" + keyspace +
                   ", key=" + (isMin ? "min": "max") +
                   '}';
        }

        public static final IVersionedSerializer<SentinelKey> serializer = new IVersionedSerializer<SentinelKey>()
        {
            @Override
            public void serialize(SentinelKey key, DataOutputPlus out, int version) throws IOException
            {
                out.writeBoolean(key.isMin);
                out.writeUTF(key.keyspace);
            }

            @Override
            public SentinelKey deserialize(DataInputPlus in, int version) throws IOException
            {
                boolean isMin = in.readBoolean();
                String keyspace = in.readUTF();
                return new SentinelKey(keyspace, isMin);
            }

            @Override
            public long serializedSize(SentinelKey key, int version)
            {
                return TypeSizes.BOOL_SIZE + TypeSizes.sizeof(key.keyspace);
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
                                      ? new SentinelKey(keyspace, true)
                                      : new TokenKey(keyspace, token.decreaseSlightly());

            return new TokenRange(before, this);
        }

        static
        {
            Token key = getPartitioner().decorateKey(ByteBufferUtil.EMPTY_BYTE_BUFFER).getToken();
            EMPTY_SIZE = ObjectSizes.measureDeep(new TokenKey(null, key));
        }

        final Token token;
        public TokenKey(String keyspace, Token token)
        {
            super(keyspace);
            this.token = token;
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
        public String toString()
        {
            return "TokenKey{" +
                   "keyspace=" + keyspace() +
                   ", key=" + token() +
                   '}';
        }

        public long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE + token().getHeapSize();
        }

        public static final Serializer serializer = new Serializer();
        public static class Serializer implements IVersionedSerializer<TokenKey>
        {
            private Serializer() {}

            @Override
            public void serialize(TokenKey key, DataOutputPlus out, int version) throws IOException
            {
                out.writeUTF(key.keyspace);
                Token.compactSerializer.serialize(key.token, out, version);
            }

            @Override
            public TokenKey deserialize(DataInputPlus in, int version) throws IOException
            {
                String keyspace = in.readUTF();
                Token token = Token.compactSerializer.deserialize(in, getPartitioner(), version);
                return new TokenKey(keyspace, token);
            }

            @Override
            public long serializedSize(TokenKey key, int version)
            {
                return TypeSizes.sizeof(key.keyspace) + Token.compactSerializer.serializedSize(key.token(), version);
            }
        }
    }

    public static final IVersionedSerializer<AccordRoutingKey> serializer = new IVersionedSerializer<AccordRoutingKey>()
    {
        final RoutingKeyKind[] kinds = RoutingKeyKind.values();
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
    };

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
            Map<String, List<Range>> byKeyspace = new TreeMap<>();
            for (Range range : ranges)
            {
                byKeyspace.computeIfAbsent(((AccordRoutableKey)range.start()).keyspace, ignore -> new ArrayList<>())
                          .add(range);
            }

            List<Ranges> results = new ArrayList<>();
            for (List<Range> keyspaceRanges : byKeyspace.values())
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
    }
}
