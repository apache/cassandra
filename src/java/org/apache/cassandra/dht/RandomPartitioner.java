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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

/**
 * This class generates a BigIntegerToken using MD5 hash.
 */
public class RandomPartitioner extends AbstractPartitioner
{
    public static final BigInteger ZERO = new BigInteger("0");
    public static final BigIntegerToken MINIMUM = new BigIntegerToken("-1");
    public static final BigInteger MAXIMUM = new BigInteger("2").pow(127);

    private static final int EMPTY_SIZE = (int) ObjectSizes.measureDeep(new BigIntegerToken(FBUtilities.hashToBigInteger(ByteBuffer.allocate(1))));

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public Token midpoint(Token ltoken, Token rtoken)
    {
        // the symbolic MINIMUM token should act as ZERO: the empty bit array
        BigInteger left = ltoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)ltoken).token;
        BigInteger right = rtoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)rtoken).token;
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 127);
        // discard the remainder
        return new BigIntegerToken(midpair.left);
    }

    public BigIntegerToken getMinimumToken()
    {
        return MINIMUM;
    }

    public BigIntegerToken getRandomToken()
    {
        BigInteger token = FBUtilities.hashToBigInteger(GuidGenerator.guidAsBytes());
        if ( token.signum() == -1 )
            token = token.multiply(BigInteger.valueOf(-1L));
        return new BigIntegerToken(token);
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
        public ByteBuffer toByteArray(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return ByteBuffer.wrap(bigIntegerToken.token.toByteArray());
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return new BigIntegerToken(new BigInteger(ByteBufferUtil.getArray(bytes)));
        }

        public String toString(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return bigIntegerToken.token.toString();
        }

        public void validate(String token) throws ConfigurationException
        {
            try
            {
                BigInteger i = new BigInteger(token);
                if (i.compareTo(ZERO) < 0)
                    throw new ConfigurationException("Token must be >= 0");
                if (i.compareTo(MAXIMUM) > 0)
                    throw new ConfigurationException("Token must be <= 2**127");
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(e.getMessage());
            }
        }

        public Token fromString(String string)
        {
            return new BigIntegerToken(new BigInteger(string));
        }
    };

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return false;
    }

    public BigIntegerToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;
        return new BigIntegerToken(FBUtilities.hashToBigInteger(key));
    }

    public long getHeapSizeOf(Token token)
    {
        return EMPTY_SIZE;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        Map<Token, Float> ownerships = new HashMap<Token, Float>();
        Iterator<Token> i = sortedTokens.iterator();

        // 0-case
        if (!i.hasNext()) { throw new RuntimeException("No nodes present in the cluster. Has this node finished starting up?"); }
        // 1-case
        if (sortedTokens.size() == 1) {
            ownerships.put(i.next(), new Float(1.0));
        }
        // n-case
        else {
            // NOTE: All divisions must take place in BigDecimals, and all modulo operators must take place in BigIntegers.
            final BigInteger ri = MAXIMUM;                                                  //  (used for addition later)
            final BigDecimal r  = new BigDecimal(ri);                                       // The entire range, 2**127
            Token start = i.next(); BigInteger ti = ((BigIntegerToken)start).token;  // The first token and its value
            Token t; BigInteger tim1 = ti;                                                  // The last token and its value (after loop)
            while (i.hasNext()) {
                t = i.next(); ti = ((BigIntegerToken)t).token;                                      // The next token and its value
                float x = new BigDecimal(ti.subtract(tim1).add(ri).mod(ri)).divide(r).floatValue(); // %age = ((T(i) - T(i-1) + R) % R) / R
                ownerships.put(t, x);                                                               // save (T(i) -> %age)
                tim1 = ti;                                                                          // -> advance loop
            }
            // The start token's range extends backward to the last token, which is why both were saved above.
            float x = new BigDecimal(((BigIntegerToken)start).token.subtract(ti).add(ri).mod(ri)).divide(r).floatValue();
            ownerships.put(start, x);
        }
        return ownerships;
    }

    public AbstractType<?> getTokenValidator()
    {
        return IntegerType.instance;
    }
}
