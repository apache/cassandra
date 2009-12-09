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
import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class CollatingOrderPreservingPartitioner implements IPartitioner<BytesToken>
{
    static final Collator collator = Collator.getInstance(new Locale("en", "US"));

    public static final BytesToken MINIMUM = new BytesToken(new byte[0]);
    
    public static final BigInteger BYTE_MASK = new BigInteger("255");

    /**
     * Comparators for decorated keys.
     */
    private static final Comparator<DecoratedKey<BytesToken>> comparator = new Comparator<DecoratedKey<BytesToken>>() {
        public int compare(DecoratedKey<BytesToken> o1, DecoratedKey<BytesToken> o2)
        {
            return FBUtilities.compareByteArrays(o1.token.token, o2.token.token);
        }
    };

    public DecoratedKey<BytesToken> decorateKey(String key)
    {
        return new DecoratedKey<BytesToken>(getToken(key), key);
    }
    
    public DecoratedKey<BytesToken> convertFromDiskFormat(String key)
    {
        return new DecoratedKey<BytesToken>(getToken(key), key);
    }

    public String convertToDiskFormat(DecoratedKey<BytesToken> key)
    {
        return key.key;
    }

    public Comparator<DecoratedKey<BytesToken>> getDecoratedKeyComparator()
    {
        return comparator;
    }

    public BytesToken midpoint(BytesToken ltoken, BytesToken rtoken)
    {
        int sigbytes = Math.max(ltoken.token.length, rtoken.token.length);
        BigInteger left = bigForBytes(ltoken.token, sigbytes);
        BigInteger right = bigForBytes(rtoken.token, sigbytes);

        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 8*sigbytes);
        return new BytesToken(bytesForBig(midpair.left, sigbytes, midpair.right));
    }

    /**
     * Convert a byte array containing the most significant of 'sigbytes' bytes
     * representing a big-endian magnitude into a BigInteger.
     */
    private BigInteger bigForBytes(byte[] bytes, int sigbytes)
    {
        if (bytes.length != sigbytes)
        {
            // append zeros
            bytes = Arrays.copyOf(bytes, sigbytes);
        }
        return new BigInteger(1, bytes);
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
        public byte[] toByteArray(Token<byte[]> bytesToken)
        {
            return bytesToken.token;
        }

        public Token<byte[]> fromByteArray(byte[] bytes)
        {
            return new BytesToken(bytes);
        }

        public String toString(Token<byte[]> bytesToken)
        {
            return FBUtilities.bytesToHex(bytesToken.token);
        }

        public Token<byte[]> fromString(String string)
        {
            return new BytesToken(FBUtilities.hexToBytes(string));
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

    public BytesToken getToken(String key)
    {
        return new BytesToken(collator.getCollationKey(key).toByteArray());
    }
}
