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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

import org.apache.commons.lang3.ArrayUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class ByteOrderedPartitioner implements IPartitioner
{
    public static final BytesToken MINIMUM = new BytesToken(ArrayUtils.EMPTY_BYTE_ARRAY);

    public static final BigInteger BYTE_MASK = new BigInteger("255");

    private static final long EMPTY_SIZE = ObjectSizes.measure(MINIMUM);

    public static final ByteOrderedPartitioner instance = new ByteOrderedPartitioner();

    public static class BytesToken extends Token
    {
        static final long serialVersionUID = -2630749093733680626L;

        final byte[] token;

        public BytesToken(ByteBuffer token)
        {
            this(ByteBufferUtil.getArray(token));
        }

        public BytesToken(byte[] token)
        {
            this.token = token;
        }

        @Override
        public String toString()
        {
            return Hex.bytesToHex(token);
        }

        public int compareTo(Token other)
        {
            BytesToken o = (BytesToken) other;
            return FBUtilities.compareUnsigned(token, o.token, 0, 0, token.length, o.token.length);
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            return prime + Arrays.hashCode(token);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (!(obj instanceof BytesToken))
                return false;
            BytesToken other = (BytesToken) obj;

            return Arrays.equals(token, other.token);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return instance;
        }

        @Override
        public long getHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOfArray(token);
        }

        @Override
        public Object getTokenValue()
        {
            return token;
        }

        @Override
        public double size(Token next)
        {
            throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.",
                                                                  getClass().getSimpleName()));
        }

        @Override
        public Token increaseSlightly()
        {
            throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.",
                                                                  getClass().getSimpleName()));
        }
    }

    public BytesToken getToken(ByteBuffer key)
    {
        if (key.remaining() == 0)
            return MINIMUM;
        return new BytesToken(key);
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public BytesToken midpoint(Token lt, Token rt)
    {
        BytesToken ltoken = (BytesToken) lt;
        BytesToken rtoken = (BytesToken) rt;

        int sigbytes = Math.max(ltoken.token.length, rtoken.token.length);
        BigInteger left = bigForBytes(ltoken.token, sigbytes);
        BigInteger right = bigForBytes(rtoken.token, sigbytes);

        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 8*sigbytes);
        return new BytesToken(bytesForBig(midpair.left, sigbytes, midpair.right));
    }

    public Token split(Token left, Token right, double ratioToLeft)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Convert a byte array containing the most significant of 'sigbytes' bytes
     * representing a big-endian magnitude into a BigInteger.
     */
    private BigInteger bigForBytes(byte[] bytes, int sigbytes)
    {
        byte[] b;
        if (sigbytes != bytes.length)
        {
            b = new byte[sigbytes];
            System.arraycopy(bytes, 0, b, 0, bytes.length);
        } else
            b = bytes;
        return new BigInteger(1, b);
    }

    /**
     * Convert a (positive) BigInteger into a byte array representing its magnitude.
     * If remainder is true, an additional byte with the high order bit enabled
     * will be added to the end of the array
     */
    private byte[] bytesForBig(BigInteger big, int sigbytes, boolean remainder)
    {
        byte[] bytes = new byte[sigbytes + (remainder ? 1 : 0)];
        if (remainder)
        {
            // remaining bit is the most significant in the last byte
            bytes[sigbytes] |= 0x80;
        }
        // bitmask for a single byte
        for (int i = 0; i < sigbytes; i++)
        {
            int maskpos = 8 * (sigbytes - (i + 1));
            // apply bitmask and get byte value
            bytes[i] = (byte)(big.and(BYTE_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() & 0xFF);
        }
        return bytes;
    }

    public BytesToken getMinimumToken()
    {
        return MINIMUM;
    }

    public BytesToken getRandomToken()
    {
       return getRandomToken(ThreadLocalRandom.current());
    }

    public BytesToken getRandomToken(Random random)
    {
        byte[] buffer = new byte[16];
        random.nextBytes(buffer);
        return new BytesToken(buffer);
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory() 
    {
        public ByteBuffer toByteArray(Token token)
        {
            BytesToken bytesToken = (BytesToken) token;
            return ByteBuffer.wrap(bytesToken.token);
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return new BytesToken(bytes);
        }

        public String toString(Token token)
        {
            BytesToken bytesToken = (BytesToken) token;
            return Hex.bytesToHex(bytesToken.token);
        }

        public void validate(String token) throws ConfigurationException
        {
            try
            {
                if (token.length() % 2 == 1)
                    token = "0" + token;
                Hex.hexToBytes(token);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException("Token " + token + " contains non-hex digits");
            }
        }

        public Token fromString(String string)
        {
            if (string.length() % 2 == 1)
                string = "0" + string;
            return new BytesToken(Hex.hexToBytes(string));
        }
    };

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
        Map<Token, Float> allTokens = new HashMap<Token, Float>();
        List<Range<Token>> sortedRanges = new ArrayList<Range<Token>>(sortedTokens.size());

        // this initializes the counts to 0 and calcs the ranges in order.
        Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
        for (Token node : sortedTokens)
        {
            allTokens.put(node, new Float(0.0));
            sortedRanges.add(new Range<Token>(lastToken, node));
            lastToken = node;
        }

        for (String ks : Schema.instance.getKeyspaces())
        {
            for (CFMetaData cfmd : Schema.instance.getTablesAndViews(ks))
            {
                for (Range<Token> r : sortedRanges)
                {
                    // Looping over every KS:CF:Range, get the splits size and add it to the count
                    allTokens.put(r.right, allTokens.get(r.right) + StorageService.instance.getSplits(ks, cfmd.cfName, r, 1).size());
                }
            }
        }

        // Sum every count up and divide count/total for the fractional ownership.
        Float total = new Float(0.0);
        for (Float f : allTokens.values())
            total += f;
        for (Map.Entry<Token, Float> row : allTokens.entrySet())
            allTokens.put(row.getKey(), row.getValue() / total);

        return allTokens;
    }

    public AbstractType<?> getTokenValidator()
    {
        return BytesType.instance;
    }

    public AbstractType<?> partitionOrdering()
    {
        return BytesType.instance;
    }
}
