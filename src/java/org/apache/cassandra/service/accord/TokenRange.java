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

package org.apache.cassandra.service.accord;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.utils.Invariants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.ObjectSizes;

public class TokenRange extends Range.EndInclusive
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new TokenRange(TokenKey.min(TableId.fromLong(0), Murmur3Partitioner.instance), TokenKey.max(TableId.fromLong(0), Murmur3Partitioner.instance)));

    // Don't make this public use create or createUnsafe
    protected TokenRange(TokenKey start, TokenKey end)
    {
        super(start, end);
    }

    public static TokenRange create(TokenKey start, TokenKey end)
    {
        Invariants.requireArgument(start.table().equals(end.table()),
                                 "Token ranges cannot cover more than one keyspace start:%s, end:%s",
                                 start, end);
        return new TokenRange(start, end);
    }

    public static TokenRange createUnsafe(TokenKey start, TokenKey end)
    {
        return new TokenRange(start, end);
    }

    public final TableId table()
    {
        return start().table();
    }

    @Override
    public final TokenKey start()
    {
        return (TokenKey) super.start();
    }

    @Override
    public final TokenKey end()
    {
        return  (TokenKey) super.end();
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE + start().estimatedSizeOnHeap() + end().estimatedSizeOnHeap();
    }

    public boolean isFullRange()
    {
        return start().isMin() && end().isMax();
    }

    @VisibleForTesting
    public TokenRange withTable(TableId table)
    {
        return new TokenRange(start().withTable(table), end().withTable(table));
    }

    public static TokenRange fullRange(TableId table, IPartitioner partitioner)
    {
        return new TokenRange(TokenKey.min(table, partitioner), TokenKey.max(table, partitioner));
    }

    @Override
    public TokenRange newRange(RoutingKey start, RoutingKey end)
    {
        return new TokenRange((TokenKey) start, (TokenKey) end);
    }

    /*
     * This behaves quite incorrectly with MinTokenKey because it loses the inclusivity of MinTokenKey in the conversion.
     * It's not a problem for cluster metadata and topology, but it's quite wrong for queries that convert from Bounds to
     * Range.
     */
    public org.apache.cassandra.dht.Range<Token> toKeyspaceRange()
    {
        IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
        TokenKey start = start();
        TokenKey end = end();
        Token left = start.isMin() ? partitioner.getMinimumToken() : start.token();
        Token right = end.isMax() ? partitioner.getMinimumToken() : end.token();
        return new org.apache.cassandra.dht.Range<>(left, right);
    }

    public static final Serializer serializer = new Serializer();

    public static final class Serializer implements UnversionedSerializer<TokenRange>
    {
        @Override
        public void serialize(TokenRange range, DataOutputPlus out) throws IOException
        {
            TokenKey.serializer.serialize(range.start(), out);
            TokenKey.serializer.serialize(range.end(), out);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            TokenKey.serializer.skip(in);
            TokenKey.serializer.skip(in);
        }

        @Override
        public TokenRange deserialize(DataInputPlus in) throws IOException
        {
            return TokenRange.create(TokenKey.serializer.deserialize(in),
                                     TokenKey.serializer.deserialize(in));
        }

        @Override
        public long serializedSize(TokenRange range)
        {
            return TokenKey.serializer.serializedSize(range.start())
                   + TokenKey.serializer.serializedSize(range.end());
        }
    }

    public static final UnversionedSerializer<TokenRange> noTableSerializer = new UnversionedSerializer<TokenRange>()
    {
        @Override
        public void serialize(TokenRange t, DataOutputPlus out) throws IOException
        {
            TokenKey.noTableSerializer.serialize(t.start(), out);
            TokenKey.noTableSerializer.serialize(t.end(), out);
        }

        @Override
        public TokenRange deserialize(DataInputPlus in) throws IOException
        {
            return TokenRange.create(TokenKey.noTableSerializer.deserialize(TableId.UNDEFINED, in),
                                     TokenKey.noTableSerializer.deserialize(TableId.UNDEFINED, in));
        }

        @Override
        public long serializedSize(TokenRange t)
        {
            return TokenKey.noTableSerializer.serializedSize(t.start())
                   + TokenKey.noTableSerializer.serializedSize(t.end());
        }
    };
}
