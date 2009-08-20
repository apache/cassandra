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
import java.util.StringTokenizer;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.GuidGenerator;

/**
 * This class generates a BigIntegerToken using MD5 hash.
 */
public class RandomPartitioner implements IPartitioner
{
    private static final Comparator<String> comparator = new Comparator<String>()
    {
        public int compare(String o1, String o2)
        {
            // StringTokenizer is faster than String.split()
            StringTokenizer st1 = new StringTokenizer(o1, ":");
            StringTokenizer st2 = new StringTokenizer(o2, ":");

            // first, compare on the bigint hash "decoration".  usually this will be enough.
            BigInteger i1 = new BigInteger(st1.nextToken());
            BigInteger i2 = new BigInteger(st2.nextToken());
            int v = i1.compareTo(i2);
            if (v != 0) {
                return v;
            }

            // if the hashes are equal, compare the strings
            return st1.nextToken().compareTo(st2.nextToken());
        }
    };
    private static final Comparator<String> rcomparator = new Comparator<String>()
    {
        public int compare(String o1, String o2)
        {
            return -comparator.compare(o1, o2);
        }
    };

    public String decorateKey(String key)
    {
        return FBUtilities.hash(key).toString() + ":" + key;
    }

    public String undecorateKey(String decoratedKey)
    {
        return decoratedKey.split(":", 2)[1];
    }

    public Comparator<String> getDecoratedKeyComparator()
    {
        return comparator;
    }

    public Comparator<String> getReverseDecoratedKeyComparator()
    {
        return rcomparator;
    }

    public BigIntegerToken getDefaultToken()
    {
        String initialToken = DatabaseDescriptor.getInitialToken();
        if (initialToken != null)
            return new BigIntegerToken(new BigInteger(initialToken));

        // generate random token
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

    public Token getToken(String key)
    {
        return new BigIntegerToken(FBUtilities.hash(key));
    }
}
