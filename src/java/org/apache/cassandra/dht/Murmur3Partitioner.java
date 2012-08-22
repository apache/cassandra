/**
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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;

/**
 * This class generates a BigIntegerToken using a Murmur3 hash.
 */
public class Murmur3Partitioner extends AbstractPartitioner<LongToken>
{
    public static final LongToken MINIMUM = new LongToken(0L);
    public static final long MAXIMUM = Long.MAX_VALUE;

    public DecoratedKey convertFromDiskFormat(ByteBuffer key)
    {
        throw new UnsupportedOperationException();
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new DecoratedKey(getToken(key), key);
    }

    public Token midpoint(Token lToken, Token rToken)
    {
        // using BigInteger to avoid long overflow in intermediate operations
        BigInteger l = BigInteger.valueOf(((LongToken) lToken).token),
                   r = BigInteger.valueOf(((LongToken) rToken).token),
                   midpoint;

        if (l.compareTo(r) < 0)
        {
            BigInteger sum = l.add(r);
            midpoint = sum.shiftRight(1);
        }
        else // wrapping case
        {
            BigInteger max = BigInteger.valueOf(MAXIMUM);
            midpoint = max.add(r).subtract(l).shiftRight(1).add(l).mod(max);
        }

        return new LongToken(midpoint.longValue());
    }

    public LongToken getMinimumToken()
    {
        return MINIMUM;
    }

    public LongToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;

        long hash = MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0)[0];
        return new LongToken((hash < 0) ? -hash : hash);
    }

    public LongToken getRandomToken()
    {
        return new LongToken(FBUtilities.threadLocalRandom().nextLong());
    }

    public boolean preservesOrder()
    {
        return false;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        Map<Token, Float> ownerships = new HashMap<Token, Float>();
        Iterator i = sortedTokens.iterator();

        // 0-case
        if (!i.hasNext())
            throw new RuntimeException("No nodes present in the cluster. How did you call this?");
        // 1-case
        if (sortedTokens.size() == 1)
            ownerships.put((Token) i.next(), new Float(1.0));
        // n-case
        else
        {
            final float ri = MAXIMUM;                                            //  (used for addition later)
            Token start = (Token) i.next(); Long ti = ((LongToken)start).token;  // The first token and its value
            Token t; Long tim1 = ti;                                             // The last token and its value (after loop)

            while (i.hasNext())
            {
                t = (Token) i.next(); ti = ((LongToken) t).token; // The next token and its value
                float age = ((ti - tim1 + ri) % ri) / ri;         // %age = ((T(i) - T(i-1) + R) % R) / R
                ownerships.put(t, age);                           // save (T(i) -> %age)
                tim1 = ti;                                        // -> advance loop
            }

            // The start token's range extends backward to the last token, which is why both were saved above.
            float x = ((((LongToken) start).token - ti + ri) % ri) / ri;
            ownerships.put(start, x);
        }

        return ownerships;
    }

    public Token.TokenFactory<Long> getTokenFactory()
    {
        return tokenFactory;
    }

    private final Token.TokenFactory<Long> tokenFactory = new Token.TokenFactory<Long>()
    {
        public ByteBuffer toByteArray(Token<Long> longToken)
        {
            return ByteBufferUtil.bytes(longToken.token);
        }

        public Token<Long> fromByteArray(ByteBuffer bytes)
        {
            return new LongToken(bytes.getLong());
        }

        public String toString(Token<Long> longToken)
        {
            return longToken.token.toString();
        }

        public void validate(String token) throws ConfigurationException
        {
            try
            {
                Long i = Long.valueOf(token);

                if (i.compareTo(MINIMUM.token) < 0)
                    throw new ConfigurationException("Token must be >= 0");

                if (i.compareTo(MAXIMUM) > 0)
                    throw new ConfigurationException("Token must be <= " + Long.MAX_VALUE);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(e.getMessage());
            }
        }

        public Token<Long> fromString(String string)
        {
            return new LongToken(Long.valueOf(string));
        }
    };
}
