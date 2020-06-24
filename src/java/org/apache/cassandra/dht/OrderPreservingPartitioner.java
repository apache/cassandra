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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;

public class OrderPreservingPartitioner implements IPartitioner
{
    private static final String rndchars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    public static final StringToken MINIMUM = new StringToken("");

    public static final BigInteger CHAR_MASK = new BigInteger("65535");

    private static final long EMPTY_SIZE = ObjectSizes.measure(MINIMUM);

    public static final OrderPreservingPartitioner instance = new OrderPreservingPartitioner();

    public DecoratedKey decorateKey(ByteBuffer key)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-7096
        return new CachedHashDecoratedKey(getToken(key), key);
    }

    public StringToken midpoint(Token ltoken, Token rtoken)
    {
        int sigchars = Math.max(((StringToken)ltoken).token.length(), ((StringToken)rtoken).token.length());
        BigInteger left = bigForString(((StringToken)ltoken).token, sigchars);
        BigInteger right = bigForString(((StringToken)rtoken).token, sigchars);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-519
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 16*sigchars);
        return new StringToken(stringForBig(midpair.left, sigchars, midpair.right));
    }

    public Token split(Token left, Token right, double ratioToLeft)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12777
        throw new UnsupportedOperationException();
    }

    /**
     * Copies the characters of the given string into a BigInteger.
     *
     * TODO: Does not acknowledge any codepoints above 0xFFFF... problem?
     */
    private static BigInteger bigForString(String str, int sigchars)
    {
        assert str.length() <= sigchars;

        BigInteger big = BigInteger.ZERO;
        for (int i = 0; i < str.length(); i++)
        {
            int charpos = 16 * (sigchars - (i + 1));
            BigInteger charbig = BigInteger.valueOf(str.charAt(i) & 0xFFFF);
            big = big.or(charbig.shiftLeft(charpos));
        }
        return big;
    }

    /**
     * Convert a (positive) BigInteger into a String.
     * If remainder is true, an additional char with the high order bit enabled
     * will be added to the end of the String.
     */
    private String stringForBig(BigInteger big, int sigchars, boolean remainder)
    {
        char[] chars = new char[sigchars + (remainder ? 1 : 0)];
        if (remainder)
            // remaining bit is the most significant in the last char
            chars[sigchars] |= 0x8000;
        for (int i = 0; i < sigchars; i++)
        {
            int maskpos = 16 * (sigchars - (i + 1));
            // apply bitmask and get char value
            chars[i] = (char)(big.and(CHAR_MASK.shiftLeft(maskpos)).shiftRight(maskpos).intValue() & 0xFFFF);
        }
        return new String(chars);
    }

    public StringToken getMinimumToken()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-242
        return MINIMUM;
    }

    public StringToken getRandomToken()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-11844
        return getRandomToken(ThreadLocalRandom.current());
    }

    public StringToken getRandomToken(Random random)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-273
        StringBuilder buffer = new StringBuilder();
        for (int j = 0; j < 16; j++)
            buffer.append(rndchars.charAt(random.nextInt(rndchars.length())));
        return new StringToken(buffer.toString());
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory()
    {
        public ByteBuffer toByteArray(Token token)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8171
            StringToken stringToken = (StringToken) token;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2367
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-0
            return ByteBufferUtil.bytes(stringToken.token);
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2091
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-0
            try
            {
                return new StringToken(ByteBufferUtil.string(bytes));
            }
            catch (CharacterCodingException e)
            {
                throw new RuntimeException(e);
            }
        }

        public String toString(Token token)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8171
            StringToken stringToken = (StringToken) token;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-114
            return stringToken.token;
        }

        public void validate(String token) throws ConfigurationException
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2762
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-0
            if (token.contains(VersionedValue.DELIMITER_STR))
                throw new ConfigurationException("Tokens may not contain the character " + VersionedValue.DELIMITER_STR);
        }

        public Token fromString(String string)
        {
            return new StringToken(string);
        }
    };

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    public boolean preservesOrder()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-242
        return true;
    }

    public static class StringToken extends ComparableObjectToken<String>
    {
        static final long serialVersionUID = 5464084395277974963L;

        public StringToken(String token)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8244
            super(token);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return instance;
        }

        @Override
        public long getHeapSize()
        {
            return EMPTY_SIZE + ObjectSizes.sizeOf(token);
        }
    }

    public StringToken getToken(ByteBuffer key)
    {
        String skey;
        try
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2367
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-0
            skey = ByteBufferUtil.string(key);
        }
        catch (CharacterCodingException e)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5793
            skey = ByteBufferUtil.bytesToHex(key);
        }
        return new StringToken(skey);
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
        Map<Token, Float> allTokens = new HashMap<Token, Float>();
        List<Range<Token>> sortedRanges = new ArrayList<Range<Token>>(sortedTokens.size());
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-3949

        // this initializes the counts to 0 and calcs the ranges in order.
        Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
        for (Token node : sortedTokens)
        {
            allTokens.put(node, new Float(0.0));
            sortedRanges.add(new Range<Token>(lastToken, node));
            lastToken = node;
        }

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5613
        for (String ks : Schema.instance.getKeyspaces())
        {
            for (TableMetadata cfmd : Schema.instance.getTablesAndViews(ks))
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-1034
                for (Range<Token> r : sortedRanges)
                {
                    // Looping over every KS:CF:Range, get the splits size and add it to the count
                    allTokens.put(r.right, allTokens.get(r.right) + StorageService.instance.getSplits(ks, cfmd.name, r, cfmd.params.minIndexInterval).size());
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
        return UTF8Type.instance;
    }

    public AbstractType<?> partitionOrdering()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-5198
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
        return UTF8Type.instance;
    }
}
