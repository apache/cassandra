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
import java.util.Comparator;
import java.util.Random;

public class OrderPreservingPartitioner implements IPartitioner
{
    private static final Comparator<String> comparator = new Comparator<String>() {
        public int compare(String o1, String o2)
        {
            return o2.compareTo(o1);
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

    public Comparator<String> getReverseDecoratedKeyComparator()
    {
        return comparator;
    }

    public StringToken getDefaultToken()
    {
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random r = new Random();
        StringBuffer buffer = new StringBuffer();
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

        public Token<String> fromString(String string)
        {
            return new StringToken(string);
        }
    };

    public Token.TokenFactory<String> getTokenFactory()
    {
        return tokenFactory;
    }

    public Token getTokenForKey(String key)
    {
        return new StringToken(key);
    }
}
