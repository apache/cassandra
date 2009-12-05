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
import java.util.Comparator;
import java.util.regex.Pattern;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;

/**
 * This class generates a BigIntegerToken using MD5 hash.
 */
public class RandomPartitioner implements IPartitioner<BigIntegerToken>
{
    public static final BigInteger TWO = new BigInteger("2");
    public static final BigInteger MD5_MAX = TWO.pow(127);

    public static final BigIntegerToken MINIMUM = new BigIntegerToken("0");

    private static final String DELIMITER = ":";
    
    private static final Comparator<DecoratedKey<BigIntegerToken>> comparator =
        new Comparator<DecoratedKey<BigIntegerToken>>() {
        public int compare(DecoratedKey<BigIntegerToken> o1, DecoratedKey<BigIntegerToken> o2)
        {
            // first, compare on the bigint hash "decoration".  usually this will be enough.
            int v = o1.token.compareTo(o2.token);
            if (v != 0) {
                return v;
            }

            // if the hashes are equal, compare the strings
            return o1.key.compareTo(o2.key);
        }
    };

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

    public Comparator<DecoratedKey<BigIntegerToken>> getDecoratedKeyComparator()
    {
        return comparator;
    }

    public BigIntegerToken midpoint(BigIntegerToken ltoken, BigIntegerToken rtoken)
    {
        BigInteger left = ltoken.token;
        BigInteger right = rtoken.token;

        BigInteger midpoint;
        if (left.compareTo(right) < 0)
        {
            midpoint = left.add(right).divide(TWO);
        }
        else
        {
            // wrapping case
            BigInteger distance = MD5_MAX.add(right).subtract(left);
            BigInteger unchecked = distance.divide(TWO).add(left);
            midpoint = (unchecked.compareTo(MD5_MAX) > 0) ? unchecked.subtract(MD5_MAX) : unchecked;
        }
        return new BigIntegerToken(midpoint);
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
        return new BigIntegerToken(FBUtilities.hash(key));
    }
}
