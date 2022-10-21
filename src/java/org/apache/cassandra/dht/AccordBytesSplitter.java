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
        return byteLength(((AccordRoutingKey) routingKey).token());
    }

    private static int byteLength(Token token)
    {
        return ((ByteOrderedPartitioner.BytesToken) token).token.length;
    }
}
