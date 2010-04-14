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
import java.util.Arrays;
import java.util.Comparator;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DBConstants;
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

    public DecoratedKey<BigIntegerToken> decorateKey(String key)
    {
        return new DecoratedKey<BigIntegerToken>(getToken(key), key);
    }
    
    public DecoratedKey<BigIntegerToken> convertFromDiskFormat(byte[] fromdisk)
    {
        // find the delimiter position
        int splitPoint = -1;
        for (int i = 0; i < fromdisk.length; i++)
        {
            if (fromdisk[i] == DELIMITER_BYTE)
            {
                splitPoint = i;
                break;
            }
        }
        assert splitPoint != -1;

        // and decode the token and key
        String token = new String(fromdisk, 0, splitPoint, FBUtilities.UTF8);
        byte[] key = Arrays.copyOfRange(fromdisk, splitPoint + 1, fromdisk.length);
        return new DecoratedKey<BigIntegerToken>(new BigIntegerToken(token), key);
    }

    public byte[] convertToDiskFormat(DecoratedKey<BigIntegerToken> key)
    {
        // encode token prefix and calculate final length (with delimiter)
        byte[] prefix = key.token.toString().getBytes(FBUtilities.UTF8);
        int length = prefix.length + 1 + key.key.length;
        assert length <= FBUtilities.MAX_UNSIGNED_SHORT;

        // copy into output bytes
        byte[] todisk = new byte[length];
        System.arraycopy(prefix, 0, todisk, 0, prefix.length);
        todisk[prefix.length] = DELIMITER_BYTE;
        System.arraycopy(key.key, 0, todisk, prefix.length + 1, key.key.length);
        return todisk;
    }

    public BigIntegerToken midpoint(BigIntegerToken ltoken, BigIntegerToken rtoken)
    {
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(ltoken.token, rtoken.token, 127);
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
}
