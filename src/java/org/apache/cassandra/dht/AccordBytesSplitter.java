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

import accord.utils.Invariants;

public abstract class AccordBytesSplitter extends AccordSplitter
{
    @Override
    BigInteger minimumValue()
    {
        return BigInteger.ZERO;
    }

    @Override
    BigInteger maximumValue(BigInteger start)
    {
        return BigInteger.ONE.shiftLeft(1 + start.bitLength());
    }

    @Override
    BigInteger valueForToken(Token token)
    {
        byte[] bytes = ((ByteOrderedPartitioner.BytesToken) token).token;
        int bitLength = bytes.length * 8;
        BigInteger value = BigInteger.ZERO;
        for (int i = 0 ; i < Math.min(8, bytes.length) ; ++i)
            value = value.add(BigInteger.valueOf(bytes[i] & 0xffL).shiftLeft(bitLength - (i + 1) * 8));
        return value;
    }

    @Override
    Token tokenForValue(BigInteger value)
    {
        // TODO (required): test
        Invariants.checkArgument(value.compareTo(BigInteger.ZERO) >= 0);
        int bitLength = value.bitLength();
        byte[] bytes = new byte[(bitLength + 7) / 8];
        for (int i = 0 ; i < bitLength ; i += 8)
            bytes[i/8] = value.shiftRight(bitLength - (i+1)*8).byteValue();
        return new ByteOrderedPartitioner.BytesToken(bytes);
    }
}
