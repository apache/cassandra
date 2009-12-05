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

import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.FBUtilities;

public class CollatingOrderPreservingPartitioner implements IPartitioner<BytesToken>
{
    static final Collator collator = Collator.getInstance(new Locale("en", "US"));

    public static final BytesToken MINIMUM = new BytesToken(new byte[0]);

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

    /**
     * @return A new byte array that will compare (via compareByteArrays)
     * approximately halfway between the parameters.
     */
    private static byte[] midpoint(byte[] lbytes, byte[] rbytes)
    {
        // pad the arrays to equal length, for convenience
        int inlength;
        int comparison = FBUtilities.compareByteArrays(lbytes, rbytes);
        if (comparison < 0)
        {
            inlength = Math.max(lbytes.length, rbytes.length);
            if (lbytes.length < inlength)
                lbytes = Arrays.copyOf(lbytes, inlength);
            else if (rbytes.length < inlength)
                rbytes = Arrays.copyOf(rbytes, inlength);
        }
        else
        {
            // wrapping range must involve the minimum token
            assert Arrays.equals(MINIMUM.token, rbytes);

            inlength = Math.max(lbytes.length, 1);
            if (lbytes.length < inlength)
                lbytes = Arrays.copyOf(lbytes, inlength);
            rbytes = new byte[inlength];
            Arrays.fill(rbytes, (byte)0xFF);
        }

        // if the lsbits of the two inputs are not equal we have to extend
        // the result array to make room for a carried bit during the right shift
        int outlength = (((int)lbytes[inlength-1] & 0x01) == ((int)rbytes[inlength-1] & 0x01))
                        ? inlength
                        : inlength+1;
        byte[] result = new byte[outlength];
        boolean carrying = false;

        // perform the addition
        for (int i = inlength-1; i >= 0; i--)
        {
            // initialize the lsbit if we're carrying
            int sum = carrying ? 1 : 0;

            // remove the sign bit, and sum left and right
            sum += (lbytes[i] & 0xFF) + (rbytes[i] & 0xFF);
            
            // see if we'll need to carry
            carrying = sum > 0xFF;

            // set to the sum (truncating the msbit)
            result[i] = (byte)sum;
        }
        // the carried bit from addition will be shifted in as the msbit

        // perform the division (as a right shift)
        for (int i = 0; i < inlength; i++)
        {
            // initialize the msbit if we're carrying
            byte shifted = (byte)(carrying ? 0x80 : 0x00);

            // check the lsbit to see if we'll need to continue carrying
            carrying = (result[i] & 0x01) == 0x01;

            // OR the right shifted value into the result byte
            result[i] = (byte)(shifted | ((result[i] & 0xFF) >>> 1));
        }

        if (carrying)
            // the last byte in the result array
            result[inlength] |= 0x80;
        return result;
    }

    public BytesToken midpoint(BytesToken ltoken, BytesToken rtoken)
    {
        return new BytesToken(midpoint(ltoken.token, rtoken.token));
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
