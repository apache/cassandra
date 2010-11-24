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
import java.util.Arrays;
import java.util.Random;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.ArrayUtils;

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
        System.arraycopy(bytes.array(), bytes.position()+bytes.arrayOffset(), b, 0, bytes.remaining());
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

    public abstract BytesToken getToken(ByteBuffer key);
}
