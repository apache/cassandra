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
import java.util.*;

import org.apache.cassandra.config.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.Pair;

public abstract class AbstractByteOrderedPartitioner implements IPartitioner<BytesToken>
{
    public static final BytesToken MINIMUM = new BytesToken(ArrayUtils.EMPTY_BYTE_ARRAY);

    public static final BigInteger BYTE_MASK = new BigInteger("255");

    public DecoratedKey<BytesToken> decorateKey(ByteBuffer key)
    {
        return new DecoratedKey<BytesToken>(getToken(key), key);
    }

    public DecoratedKey<BytesToken> convertFromDiskFormat(ByteBuffer key)
    {
        return new DecoratedKey<BytesToken>(getToken(key), key);
    }

    public BytesToken midpoint(Token ltoken, Token rtoken)
    {
        int ll,rl;
        ByteBuffer lb,rb;

        if(ltoken.token instanceof byte[])
        {
            ll = ((byte[])ltoken.token).length;
            lb = ByteBuffer.wrap(((byte[])ltoken.token));
        }
        else
        {
            ll = ((ByteBuffer)ltoken.token).remaining();
            lb = (ByteBuffer)ltoken.token;
        }

        if(rtoken.token instanceof byte[])
        {
            rl = ((byte[])rtoken.token).length;
            rb = ByteBuffer.wrap(((byte[])rtoken.token));
        }
        else
        {
            rl = ((ByteBuffer)rtoken.token).remaining();
            rb = (ByteBuffer)rtoken.token;
        }
        int sigbytes = Math.max(ll, rl);
        BigInteger left = bigForBytes(lb, sigbytes);
        BigInteger right = bigForBytes(rb, sigbytes);

        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 8*sigbytes);
        return new BytesToken(bytesForBig(midpair.left, sigbytes, midpair.right));
    }

    /**
     * Convert a byte array containing the most significant of 'sigbytes' bytes
     * representing a big-endian magnitude into a BigInteger.
     */
    private BigInteger bigForBytes(ByteBuffer bytes, int sigbytes)
    {
        byte[] b = new byte[sigbytes];
        Arrays.fill(b, (byte) 0); // append zeros
        ByteBufferUtil.arrayCopy(bytes, bytes.position(), b, 0, bytes.remaining());
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
        Random r = new Random();
        byte[] buffer = new byte[16];
        r.nextBytes(buffer);
        return new BytesToken(buffer);
    }

    private final Token.TokenFactory<byte[]> tokenFactory = new Token.TokenFactory<byte[]>() {
        public ByteBuffer toByteArray(Token<byte[]> bytesToken)
        {
            return ByteBuffer.wrap(bytesToken.token);
        }

        public Token<byte[]> fromByteArray(ByteBuffer bytes)
        {
            return new BytesToken(bytes);
        }

        public String toString(Token<byte[]> bytesToken)
        {
            return Hex.bytesToHex(bytesToken.token);
        }

        public void validate(String token) throws ConfigurationException
        {
            try
            {
                Hex.hexToBytes(token);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException("Token " + token + " contains non-hex digits");
            }
        }

        public Token<byte[]> fromString(String string)
        {
            return new BytesToken(Hex.hexToBytes(string));
        }
    };

    public Token.TokenFactory<byte[]> getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public abstract BytesToken getToken(ByteBuffer key);

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
        Map<Token, Float> allTokens = new HashMap<Token, Float>();
        List<Range> sortedRanges = new ArrayList<Range>();

        // this initializes the counts to 0 and calcs the ranges in order.
        Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
        for (Token node : sortedTokens)
        {
            allTokens.put(node, new Float(0.0));
            sortedRanges.add(new Range(lastToken, node));
            lastToken = node;
        }

        for (String ks : Schema.instance.getTables())
        {
            for (CFMetaData cfmd : Schema.instance.getKSMetaData(ks).cfMetaData().values())
            {
                for (Range r : sortedRanges)
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
}
