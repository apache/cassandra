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

import static com.google.common.base.Charsets.UTF_8;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;
import org.apache.cassandra.utils.Pair;

/**
 * This class generates a BigIntegerToken using MD5 hash.
 */
public class RandomPartitioner implements IPartitioner<BigIntegerToken>
{
    public static final BigInteger TWO = new BigInteger("2");

    public static final BigIntegerToken MINIMUM = new BigIntegerToken("0");

    private static final byte DELIMITER_BYTE = ":".getBytes()[0];

    public DecoratedKey<BigIntegerToken> decorateKey(ByteBuffer key)
    {
        return new DecoratedKey<BigIntegerToken>(getToken(key), key);
    }
    
    public DecoratedKey<BigIntegerToken> convertFromDiskFormat(ByteBuffer fromdisk)
    {
        // find the delimiter position
        int splitPoint = -1;
        for (int i = fromdisk.position()+fromdisk.arrayOffset(); i < fromdisk.limit()+fromdisk.arrayOffset(); i++)
        {
            if (fromdisk.array()[i] == DELIMITER_BYTE)
            {
                splitPoint = i;
                break;
            }
        }
        assert splitPoint != -1;

        // and decode the token and key
        String token = new String(fromdisk.array(), fromdisk.position()+fromdisk.arrayOffset(), splitPoint, UTF_8);
        byte[] key = Arrays.copyOfRange(fromdisk.array(), splitPoint + 1, fromdisk.limit()+fromdisk.arrayOffset());
        return new DecoratedKey<BigIntegerToken>(new BigIntegerToken(token), ByteBuffer.wrap(key));
    }

    public Token midpoint(Token ltoken, Token rtoken)
    {
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(((BigIntegerToken)ltoken).token, ((BigIntegerToken)rtoken).token, 127);
        // discard the remainder
        return new BigIntegerToken(midpair.left);
    }

	public BigIntegerToken getMinimumToken()
    {
        return MINIMUM;
    }

    public BigIntegerToken getRandomToken()
    {
        BigInteger token = FBUtilities.md5hash(GuidGenerator.guidAsBytes());
        if ( token.signum() == -1 )
            token = token.multiply(BigInteger.valueOf(-1L));
        return new BigIntegerToken(token);
    }

    private final Token.TokenFactory<BigInteger> tokenFactory = new Token.TokenFactory<BigInteger>() {
        public ByteBuffer toByteArray(Token<BigInteger> bigIntegerToken)
        {
            return ByteBuffer.wrap(bigIntegerToken.token.toByteArray());
        }

        public Token<BigInteger> fromByteArray(ByteBuffer bytes)
        {
            byte[] b = new byte[bytes.remaining()];
            bytes.get(b);
            bytes.rewind();
            
            return new BigIntegerToken(new BigInteger(b));
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

    public BigIntegerToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;
        return new BigIntegerToken(FBUtilities.md5hash(key));
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
