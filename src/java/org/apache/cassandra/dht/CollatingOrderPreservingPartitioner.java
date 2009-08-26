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
import java.math.BigInteger;
import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

public class CollatingOrderPreservingPartitioner implements IPartitioner<BytesToken>
{
    static final Collator collator = Collator.getInstance(new Locale("en", "US"));

    public static final BytesToken MINIMUM = new BytesToken(new byte[0]);

    /**
     * Comparators for decorated keys.
     */
    private static final Comparator<String> comparator = new Comparator<String>() {
        public int compare(String o1, String o2)
        {
            return collator.compare(o1, o2);
        }
    };
    private static final Comparator<String> reverseComparator = new Comparator<String>() {
        public int compare(String o1, String o2)
        {
            return -comparator.compare(o1, o2);
        }
    };

    public String decorateKey(String key)
    {
        return key;
    }

    public String undecorateKey(String decoratedKey)
    {
        return decoratedKey;
    }

    public Comparator<String> getDecoratedKeyComparator()
    {
        return comparator;
    }

    public Comparator<String> getReverseDecoratedKeyComparator()
    {
        return reverseComparator;
    }

    public BytesToken getMinimumToken()
    {
        return MINIMUM;
    }

    public BytesToken getDefaultToken()
    {
        String initialToken = DatabaseDescriptor.getInitialToken();
        if (initialToken != null)
            // assume that the user specified the intial Token as a String key
            return getToken(initialToken);

        // generate random token
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
