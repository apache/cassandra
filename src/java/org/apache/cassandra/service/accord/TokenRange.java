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

import com.google.common.base.Preconditions;

import accord.api.KeyRange;
import accord.topology.KeyRanges;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.TokenKey;

/*
 * TODO: the ByteOrderedPartitioner, and OrderPreservingPartitioner, do not support the split and getMaximumToken methods
 *  which accord relies on for internal range sharding, and topology range unwrapping, respectively. This may be fine
 *  for a v1, but will make testing slightly inconvenient, since they use BOP by default.
 *  We could address split in accord core by sharding on the token hash, instead of dividing ranges. And we could address
 *  getMaximumToken with min/max sentinel tokens.
 *
 * FIXME: updating accord to internally partitioning keys by hash would fix this issue
 */
public class TokenRange extends KeyRange.EndInclusive<AccordKey>
{
    public TokenRange(TableId tableId, Range<Token> range)
    {
        this(tableId, range.left, range.right);
    }

    public TokenRange(TableId tableId, Token start, Token end)
    {
        this(new TokenKey(tableId, !start.isMinimum() ? start.maxKeyBound() : start.minKeyBound()),
             new TokenKey(tableId, end.maxKeyBound()));
        Preconditions.checkArgument(start().tableId().equals(end().tableId()));
    }

    public TokenRange(TokenKey start, TokenKey end)
    {
        super(start, end);
    }

    private static TokenKey toAccordToken(AccordKey key)
    {
        if (key instanceof TokenKey)
            return (TokenKey) key;
        return new TokenKey(key.tableId(),
                                      key.partitionKey().getToken().maxKeyBound());
    }

    @Override
    public TokenRange subRange(AccordKey start, AccordKey end)
    {
        return new TokenRange(toAccordToken(start), toAccordToken(end));
    }

    @Override
    public KeyRanges split(int count)
    {
        KeyRange[] ranges = new KeyRange[count];
        TableId tableId = start().tableId();
        Token left = start().partitionKey().getToken();
        Token right = end().partitionKey().getToken();
        while (count > 1)
        {
            double ratio = 1.0f / (count - 1);
            // FIXME: IPartitioner.split isn't supported by all partitioners (specifically ByteOrderedPartitioner)
            Token midpoint = DatabaseDescriptor.getPartitioner().split(left, right, ratio);
            if (midpoint.equals(left) || midpoint.equals(right))
                return new KeyRanges(new KeyRange[]{this});
            ranges[ranges.length - count] = new TokenRange(tableId, left, midpoint);
            left = midpoint;
            count--;
        }

        ranges[ranges.length - 1] = new TokenRange(tableId, left, right);

        return new KeyRanges(ranges);
    }

    public static final IVersionedSerializer<TokenRange> serializer = new IVersionedSerializer<TokenRange>()
    {
        @Override
        public void serialize(TokenRange range, DataOutputPlus out, int version) throws IOException
        {
            TokenKey.serializer.serialize((TokenKey) range.start(), out, version);
            TokenKey.serializer.serialize((TokenKey) range.end(), out, version);
        }

        @Override
        public TokenRange deserialize(DataInputPlus in, int version) throws IOException
        {
            return new TokenRange(TokenKey.serializer.deserialize(in, version),
                                  TokenKey.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(TokenRange range, int version)
        {
            return TokenKey.serializer.serializedSize((TokenKey) range.start(), version)
                 + TokenKey.serializer.serializedSize((TokenKey) range.end(), version);
        }
    };
}
