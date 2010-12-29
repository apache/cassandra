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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.Pair;

/**
 * This class generates a BigIntegerToken using MD5 hash.
 */
public class RandomPartitioner implements IPartitioner<BigIntegerToken>
{
    public static final BigInteger ZERO = new BigInteger("0");
    public static final BigIntegerToken MINIMUM = new BigIntegerToken("-1");

    private static final String DELIMITER = ":";

    public DecoratedKey<BigIntegerToken> decorateKey(String key)
    {
        return new DecoratedKey<BigIntegerToken>(getToken(key), key);
    }
    
    public DecoratedKey<BigIntegerToken> convertFromDiskFormat(String key)
    {
        int splitPoint = key.indexOf(DELIMITER);
        String first = key.substring(0, splitPoint);
        String second = key.substring(splitPoint+1);

        return new DecoratedKey<BigIntegerToken>(new BigIntegerToken(first), second);
    }

    public String convertToDiskFormat(DecoratedKey<BigIntegerToken> key)
    {
        return key.token + DELIMITER + key.key;
    }

    public BigIntegerToken midpoint(BigIntegerToken ltoken, BigIntegerToken rtoken)
    {
        // the symbolic MINIMUM token should act as ZERO: the empty bit array
        BigInteger left = ltoken.equals(MINIMUM) ? ZERO : ltoken.token;
        BigInteger right = rtoken.equals(MINIMUM) ? ZERO : rtoken.token;
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
        String guid = GuidGenerator.guid();
        BigInteger token = FBUtilities.hash(guid);
        if ( token.signum() == -1 )
            token = token.multiply(BigInteger.valueOf(-1L));
        return new BigIntegerToken(token);
    }

    private final Token.TokenFactory<BigInteger> tokenFactory = new Token.TokenFactory<BigInteger>() {
        public byte[] toByteArray(Token<BigInteger> bigIntegerToken)
        {
            return bigIntegerToken.token.toByteArray();
        }

        public Token<BigInteger> fromByteArray(byte[] bytes)
        {
            return new BigIntegerToken(new BigInteger(bytes));
        }

        public String toString(Token<BigInteger> bigIntegerToken)
        {
            return bigIntegerToken.token.toString();
        }

        public Token<BigInteger> fromString(String string)
        {
            return new BigIntegerToken(new BigInteger(string));
        }
    };

    public Token.TokenFactory<BigInteger> getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return false;
    }

    public BigIntegerToken getToken(String key)
    {
        if (key.isEmpty())
            return MINIMUM;
        return new BigIntegerToken(FBUtilities.hash(key));
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        Map<Token, Float> ownerships = new HashMap<Token, Float>();
        Iterator i = sortedTokens.iterator();

        // 0-case
        if (!i.hasNext()) { throw new RuntimeException("No nodes present in the cluster. How did you call this?"); }
        // 1-case
        if (sortedTokens.size() == 1) {
            ownerships.put((Token)i.next(), new Float(1.0));
        }
        // n-case
        else {
            // NOTE: All divisions must take place in BigDecimals, and all modulo operators must take place in BigIntegers.
            final BigInteger ri = new BigInteger("2").pow(127);                             //  (used for addition later)
            final BigDecimal r  = new BigDecimal(ri);                                       // The entire range, 2**127
            Token start = (Token)i.next(); BigInteger ti = ((BigIntegerToken)start).token;  // The first token and its value
            Token t; BigInteger tim1 = ti;                                                  // The last token and its value (after loop)
            while (i.hasNext()) {
                t = (Token)i.next(); ti = ((BigIntegerToken)t).token;                       // The next token and its value
                float x = new BigDecimal(ti.subtract(tim1)).divide(r).floatValue();         // %age = T(i) - T(i-1) / R
                ownerships.put(t, x);                                                       // save (T(i) -> %age)
                tim1 = ti;                                                                  // -> advance loop
            }
            // The start token's range extends backward to the last token, which is why both were saved
            //  above. The simple calculation for this is: T(start) - T(end) + r % r / r.
            //  (In the 1-case, this produces 0% instead of 100%.)
            ownerships.put(start, new BigDecimal(((BigIntegerToken)start).token.subtract(ti).add(ri).mod(ri)).divide(r).floatValue());
        }
        return ownerships;
    }
}
