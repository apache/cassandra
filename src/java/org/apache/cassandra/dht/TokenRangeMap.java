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
package org.apache.cassandra.dht;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Immutable map from token ranges to values, covering the full token ring.
 * Every point on the ring maps to exactly one value.
 *
 * Internally:
 *   bounds = [t1, t2, ..., tN]  (N inclusive upper bounds, sorted)
 *   values = [v0, v1, ..., vN]  (N+1 values)
 *
 * Semantics:
 *   (MIN, t1]  → v0
 *   (t1, t2]   → v1
 *   ...
 *   (tN, MIN]  → vN  (wraps to ring start)
 */
public class TokenRangeMap<V>
{
    private final Token[] bounds;
    private final Object[] values;

    private TokenRangeMap(Token[] bounds, Object[] values)
    {
        assert bounds.length == values.length - 1;
        this.bounds = bounds;
        this.values = values;
    }

    public static <V> TokenRangeMap<V> create(V defaultValue)
    {
        return new TokenRangeMap<>(new Token[0], new Object[]{ defaultValue });
    }

    public static <V> TokenRangeMap<V> create(Token[] bounds, V[] values)
    {
        Object[] boxed = new Object[values.length];
        System.arraycopy(values, 0, boxed, 0, values.length);
        return new TokenRangeMap<>(bounds, boxed);
    }

    @SuppressWarnings("unchecked")
    public V get(Token token)
    {
        // The minimum token is the ring's wrap sentinel: it is the inclusive upper bound
        // of the last interval (tN, MIN], so it maps to the last value rather than the first.
        if (token.isMinimum())
            return (V) values[values.length - 1];
        int idx = Arrays.binarySearch(bounds, token);
        if (idx < 0) idx = -1 - idx;
        return (V) values[idx];
    }

    public TokenRangeMap<V> set(NormalizedRanges<Token> ranges, V value)
    {
        TokenRangeMap<V> result = this;
        for (Range<Token> range : ranges)
            result = result.setNonWrapping(range.left, range.right, value);
        return result;
    }

    public TokenRangeMap<V> set(Range<Token> range, V value)
    {
        if (range.isTrulyWrapAround())
        {
            Token minToken = range.left.getPartitioner().getMinimumToken();
            TokenRangeMap<V> result = setNonWrapping(range.left, minToken, value);
            return result.setNonWrapping(minToken, range.right, value);
        }
        return setNonWrapping(range.left, range.right, value);
    }

    @SuppressWarnings("unchecked")
    public boolean allMatch(Predicate<V> predicate)
    {
        for (Object v : values)
            if (!predicate.test((V) v))
                return false;
        return true;
    }

    public boolean allEqual(V value)
    {
        return allMatch(v -> Objects.equals(v, value));
    }

    public int intervalCount()
    {
        return values.length;
    }

    @SuppressWarnings("unchecked")
    public void forEach(RangeConsumer<V> consumer)
    {
        Token minToken = minToken();
        for (int i = 0; i < values.length; i++)
        {
            Token left = i == 0 ? minToken : bounds[i - 1];
            Token right = i < bounds.length ? bounds[i] : minToken;
            consumer.accept(left, right, (V) values[i]);
        }
    }

    public Token[] bounds()
    {
        return bounds;
    }

    @SuppressWarnings("unchecked")
    public V valueAt(int idx)
    {
        return (V) values[idx];
    }

    private TokenRangeMap<V> setNonWrapping(Token left, Token right, V value)
    {
        // Find first interval overlapping with (left, right]
        int first = findInterval(left);
        if (first < bounds.length && left.equals(bounds[first]))
            first++;

        // Find last interval overlapping with (left, right]
        // MIN token means "to the end of the ring" — the last interval
        int last = right.isMinimum() ? values.length - 1 : findInterval(right);

        Builder<V> builder = new Builder<>(bounds.length + 2);

        // 1. Copy intervals before the affected region
        for (int i = 0; i < first; i++)
        {
            builder.value(values[i]);
            builder.bound(bounds[i]);
        }

        // 2. Prefix: portion of first affected interval before the range
        boolean leftAtIntervalStart = first == 0 ? left.isMinimum() : left.equals(bounds[first - 1]);
        if (!leftAtIntervalStart)
        {
            builder.value(values[first]);
            builder.bound(left);
        }

        // 3. The range itself
        builder.value(value);

        // 4. Suffix: portion of last affected interval after the range
        if (last < bounds.length)
        {
            if (!right.equals(bounds[last]))
            {
                builder.bound(right);
                builder.value(values[last]);
                builder.bound(bounds[last]);
            }
            else
            {
                builder.bound(bounds[last]);
            }
        }
        else
        {
            // Last interval has no explicit upper bound (wraps)
            if (!right.isMinimum())
            {
                builder.bound(right);
                builder.value(values[last]);
            }
        }

        // 5. Copy remaining intervals after the affected region
        for (int i = last + 1; i < values.length; i++)
        {
            builder.value(values[i]);
            if (i < bounds.length)
                builder.bound(bounds[i]);
        }

        return builder.build();
    }

    private int findInterval(Token token)
    {
        int idx = Arrays.binarySearch(bounds, token);
        if (idx < 0) idx = -1 - idx;
        return idx;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TokenRangeMap<?> that = (TokenRangeMap<?>) o;
        return Arrays.equals(bounds, that.bounds) && Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return 31 * Arrays.hashCode(bounds) + Arrays.hashCode(values);
    }

    @Override
    public String toString()
    {
        Token minToken = minToken();
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0) sb.append(", ");
            Token left = i == 0 ? minToken : bounds[i - 1];
            Token right = i < bounds.length ? bounds[i] : minToken;
            sb.append('(').append(left).append(',').append(right).append("]→").append(values[i]);
        }
        return sb.append('}').toString();
    }

    private Token minToken()
    {
        if (bounds.length > 0)
            return bounds[0].getPartitioner().getMinimumToken();
        return IPartitioner.global().getMinimumToken();
    }

    @FunctionalInterface
    public interface RangeConsumer<V>
    {
        void accept(Token left, Token right, V value);
    }

    private static class Builder<V>
    {
        private final List<Token> bounds;
        private final List<Object> values;

        Builder(int capacity)
        {
            this.bounds = new ArrayList<>(capacity);
            this.values = new ArrayList<>(capacity + 1);
        }

        void value(Object v)
        {
            if (!values.isEmpty() && Objects.equals(values.get(values.size() - 1), v))
            {
                // Same as previous value: merge by removing the separating bound
                if (!bounds.isEmpty())
                    bounds.remove(bounds.size() - 1);
                return;
            }
            values.add(v);
        }

        void bound(Token t)
        {
            bounds.add(t);
        }

        TokenRangeMap<V> build()
        {
            assert bounds.size() == values.size() - 1
                : "bounds.size=" + bounds.size() + " values.size=" + values.size();
            return new TokenRangeMap<>(bounds.toArray(new Token[0]), values.toArray());
        }
    }

    public static <V> MetadataSerializer<TokenRangeMap<V>> metadataSerializer(MetadataSerializer<V> valueSerializer)
    {
        return new MetadataSerializer<>()
        {
            @Override
            public void serialize(TokenRangeMap<V> map, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUnsignedVInt32(map.bounds.length);
                for (Token bound : map.bounds)
                    Token.metadataSerializer.serialize(bound, out, version);
                for (int i = 0; i < map.values.length; i++)
                    valueSerializer.serialize(map.valueAt(i), out, version);
            }

            @Override
            public TokenRangeMap<V> deserialize(DataInputPlus in, Version version) throws IOException
            {
                int boundsCount = in.readUnsignedVInt32();
                Token[] bounds = new Token[boundsCount];
                for (int i = 0; i < boundsCount; i++)
                    bounds[i] = Token.metadataSerializer.deserialize(in, ClusterMetadata.current().partitioner, version);

                @SuppressWarnings("unchecked")
                V[] values = (V[]) new Object[boundsCount + 1];
                for (int i = 0; i < values.length; i++)
                    values[i] = valueSerializer.deserialize(in, version);

                return TokenRangeMap.create(bounds, values);
            }

            @Override
            public long serializedSize(TokenRangeMap<V> map, Version version)
            {
                long size = TypeSizes.sizeofUnsignedVInt(map.bounds.length);
                for (Token bound : map.bounds)
                    size += Token.metadataSerializer.serializedSize(bound, version);
                for (int i = 0; i < map.values.length; i++)
                    size += valueSerializer.serializedSize(map.valueAt(i), version);
                return size;
            }
        };
    }
}
