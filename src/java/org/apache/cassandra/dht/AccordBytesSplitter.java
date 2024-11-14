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

import java.math.BigInteger;

import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;

import static accord.utils.Invariants.checkArgument;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;

public class AccordBytesSplitter extends AccordSplitter
{
    final int byteLength;

    protected AccordBytesSplitter(Ranges ranges)
    {
        int bytesLength = 0;
        for (accord.primitives.Range range : ranges)
        {
            bytesLength = Integer.max(bytesLength, byteLength(range.start()));
            bytesLength = Integer.max(bytesLength, byteLength(range.end()));
        }
        // In the single node single token case the ranges in TCM are merged to +/-inf which have no token
        // and no byte length. This isn't really a problem because byte length isn't really that important it just means
        // the shard boundaries will be arbitrary. You won't notice a problem until you go to add nodes and more tokens
        // and suddenly the splitter might use a different length and now your shards are laid out slightly differently at
        // each node which would result in a small amount of metadata moving between command stores.
        // Since BOP is already not working/supported I think it's fine to punt on this.
        if (bytesLength == 0)
        {
            checkArgument(ranges.size() <= 1);
            checkArgument(ranges.isEmpty() || ranges.get(0).start().getClass() == SentinelKey.class);
            checkArgument(ranges.isEmpty() || ranges.get(0).end().getClass() == SentinelKey.class);
            // Intentionally does not match 16 that is used by ServerTestUtils.getRandomToken to elicit breakage
            bytesLength = 8;
        }
        this.byteLength = bytesLength;
    }

    @Override
    BigInteger minimumValue()
    {
        return ZERO;
    }

    @Override
    BigInteger maximumValue()
    {
        return ONE.shiftLeft(8 * byteLength).subtract(ONE);
    }

    @Override
    BigInteger valueForToken(Token token)
    {
        byte[] bytes = ((ByteOrderedPartitioner.BytesToken) token).token;
        checkArgument(bytes.length <= byteLength);
        BigInteger value = ZERO;
        for (int i = 0 ; i < bytes.length ; ++i)
            value = value.add(BigInteger.valueOf(bytes[i] & 0xffL).shiftLeft((byteLength - 1 - i) * 8));
        return value;
    }

    @Override
    Token tokenForValue(BigInteger value)
    {
        Invariants.checkArgument(value.compareTo(ZERO) >= 0);
        byte[] bytes = new byte[byteLength];
        for (int i = 0 ; i < bytes.length ; ++i)
            bytes[i] = value.shiftRight((byteLength - 1 - i) * 8).byteValue();
        return new ByteOrderedPartitioner.BytesToken(bytes);
    }

    private static int byteLength(RoutingKey routingKey)
    {
        AccordRoutingKey accordKey = (AccordRoutingKey) routingKey;
        if (accordKey.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
            return 0;
        return byteLength(accordKey.token());
    }

    private static int byteLength(Token token)
    {
        return ((ByteOrderedPartitioner.BytesToken) token).token.length;
    }
}
