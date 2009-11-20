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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;

public class OrderPreservingPartitioner implements IPartitioner<StringToken>
{
    public static final StringToken MINIMUM = new StringToken("");

    /**
     * Comparators for decorated keys.
     */
    private static final Comparator<DecoratedKey<StringToken>> comparator =
        new Comparator<DecoratedKey<StringToken>>() {
        public int compare(DecoratedKey<StringToken> o1, DecoratedKey<StringToken> o2)
        {
            return o1.key.compareTo(o2.key);
        }
    };

    public DecoratedKey<StringToken> decorateKey(String key)
    {
        return new DecoratedKey<StringToken>(new StringToken(key), key);
    }
    
    public DecoratedKey<StringToken> convertFromDiskFormat(String key)
    {
        return new DecoratedKey<StringToken>(new StringToken(key), key);
    }

    public String convertToDiskFormat(DecoratedKey<StringToken> key)
    {
        return key.key;
    }

    public Comparator<DecoratedKey<StringToken>> getDecoratedKeyComparator()
    {
        return comparator;
    }

    /**
     * Copies the given string into a char array, padding the end
     * with empty chars up to length.
     */
    private static char[] getChars(String str, int length)
    {
        char[] chars;
        if (str.length() < length)
        {
            chars = new char[length];
            str.getChars(0, str.length(), chars, 0);
        }
        else if (str.length() == length)
        {
            chars = str.toCharArray();
        }
        else
            throw new RuntimeException("Cannot truncate string of length " + str.length() + " to length " + length);
        return chars;
    }

    /**
     * @return A new String array that will compare
     * approximately halfway between the parameters.
     */
    private static String midpoint(String left, String right)
    {
        int inlength;
        char[] lchars;
        char[] rchars;
        int comparison = left.compareTo(right);
        if (comparison < 0)
        {
            inlength = Math.max(left.length(), right.length());
            lchars = getChars(left, inlength);
            rchars = getChars(right, inlength);
        }
        else
        {
            // wrapping range must involve the minimum token
            assert MINIMUM.token.equals(right);
            
            inlength = Math.max(left.length(), 1);
            lchars = getChars(left, inlength);
            rchars = new char[inlength];
            Arrays.fill(rchars, (char)0xFFFF);
        }


        // if the lsbits of the two inputs are not equal we have to extend
        // the result array to make room for a carried bit during the right shift
        int outlength = (((int)lchars[inlength-1] & 0x0001) == ((int)rchars[inlength-1] & 0x0001))
                        ? inlength
                        : inlength+1;
        char[] result = new char[outlength];
        boolean carrying = false;

        // perform the addition
        for (int i = inlength-1; i >= 0; i--)
        {
            // initialize the lsbit if we're carrying
            int sum = carrying ? 0x0001 : 0x0000;

            // remove the sign bit, and sum left and right
            sum += (lchars[i] & 0xFFFF) + (rchars[i] & 0xFFFF);
            
            // see if we'll need to carry
            carrying = sum > 0xFFFF;

            // set to the sum (truncating the msbit)
            result[i] = (char)sum;
        }
        // the carried bit from addition will be shifted in as the msbit

        // perform the division (as a right shift)
        for (int i = 0; i < inlength; i++)
        {
            // initialize the msbit if we're carrying
            char shifted = (char)(carrying ? 0x8000 : 0x0000);

            // check the lsbit to see if we'll need to continue carrying
            carrying = (result[i] & 0x0001) == 0x0001;

            // OR the right shifted value into the result char
            result[i] = (char)(shifted | ((result[i] & 0xFFFF) >>> 1));
        }

        if (carrying)
            // the last char in the result array
            result[inlength] |= 0x8000;
        return new String(result);
    }

    public StringToken midpoint(StringToken ltoken, StringToken rtoken)
    {
        return new StringToken(midpoint(ltoken.token, rtoken.token));
    }

    public StringToken getMinimumToken()
    {
        return MINIMUM;
    }

    public StringToken getRandomToken()
    {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random r = new Random();
        StringBuilder buffer = new StringBuilder();
        for (int j = 0; j < 16; j++) {
            buffer.append(chars.charAt(r.nextInt(chars.length())));
        }
        return new StringToken(buffer.toString());
    }

    private final Token.TokenFactory<String> tokenFactory = new Token.TokenFactory<String>() {
        public byte[] toByteArray(Token<String> stringToken)
        {
            try
            {
                return stringToken.token.getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
        }

        public Token<String> fromByteArray(byte[] bytes)
        {
            try
            {
                return new StringToken(new String(bytes, "UTF-8"));
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException(e);
            }
        }

        public String toString(Token<String> stringToken)
        {
            return stringToken.token;
        }

        public Token<String> fromString(String string)
        {
            return new StringToken(string);
        }
    };

    public Token.TokenFactory<String> getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
        return true;
    }

    public StringToken getToken(String key)
    {
        return new StringToken(key);
    }
}
