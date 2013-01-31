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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;

/**
 * This class generates a BigIntegerToken using a Murmur3 hash.
 */
public class Murmur3Partitioner extends AbstractPartitioner<LongToken>
{
    public static final LongToken MINIMUM = new LongToken(Long.MIN_VALUE);
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
            BigInteger min = BigInteger.valueOf(MINIMUM.token);
            // length of range we're bisecting is (R - min) + (max - L)
            // so we add that to L giving
            // L + ((R - min) + (max - L) / 2) = (L + R + max - min) / 2
            midpoint = (max.subtract(min).add(l).add(r)).shiftRight(1);
            if (midpoint.compareTo(max) > 0)
                midpoint = min.add(midpoint.subtract(max));
        }

        return new LongToken(midpoint.longValue());
    }

    public LongToken getMinimumToken()
    {
        return MINIMUM;
    }

    /**
     * Generate the token of a key.
     * Note that we need to ensure all generated token are strictly bigger than MINIMUM.
     * In particular we don't want MINIMUM to correspond to any key because the range (MINIMUM, X] doesn't
     * include MINIMUM but we use such range to select all data whose token is smaller than X.
     */
    public LongToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;

        long hash = MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0)[0];
        return new LongToken(normalize(hash));
    }

    public LongToken getRandomToken()
    {
        return new LongToken(normalize(FBUtilities.threadLocalRandom().nextLong()));
    }

    private long normalize(long v)
    {
        // We exclude the MINIMUM value; see getToken()
        return v == Long.MIN_VALUE ? Long.MAX_VALUE : v;
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
            final BigInteger ri = BigInteger.valueOf(MAXIMUM).subtract(BigInteger.valueOf(MINIMUM.token + 1));  //  (used for addition later)
            final BigDecimal r  = new BigDecimal(ri);
            Token start = (Token) i.next();BigInteger ti = BigInteger.valueOf(((LongToken)start).token);  // The first token and its value
            Token t; BigInteger tim1 = ti;                                                                // The last token and its value (after loop)

            while (i.hasNext())
            {
                t = (Token) i.next(); ti = BigInteger.valueOf(((LongToken) t).token); // The next token and its value
                float age = new BigDecimal(ti.subtract(tim1).add(ri).mod(ri)).divide(r, 6, BigDecimal.ROUND_HALF_EVEN).floatValue(); // %age = ((T(i) - T(i-1) + R) % R) / R
                ownerships.put(t, age);                           // save (T(i) -> %age)
                tim1 = ti;                                        // -> advance loop
            }

            // The start token's range extends backward to the last token, which is why both were saved above.
            float x = new BigDecimal(BigInteger.valueOf(((LongToken)start).token).subtract(ti).add(ri).mod(ri)).divide(r, 6, BigDecimal.ROUND_HALF_EVEN).floatValue();
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
            return new LongToken(ByteBufferUtil.toLong(bytes));
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

    public AbstractType<?> getTokenValidator()
    {
        return LongType.instance;
    }
}
