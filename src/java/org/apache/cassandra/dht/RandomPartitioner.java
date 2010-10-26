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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
}
