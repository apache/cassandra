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
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.dht.KeyCollisionTest.BigIntegerToken;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class LengthPartitioner implements IPartitioner
{
    public static final BigInteger ZERO = new BigInteger("0");
    public static final BigIntegerToken MINIMUM = new BigIntegerToken("-1");

    public static LengthPartitioner instance = new LengthPartitioner();

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public BigIntegerToken midpoint(Token ltoken, Token rtoken)
    {
        // the symbolic MINIMUM token should act as ZERO: the empty bit array
        BigInteger left = ltoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)ltoken).token;
        BigInteger right = rtoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)rtoken).token;
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(left, right, 127);
        // discard the remainder
        return new BigIntegerToken(midpair.left);
    }

    public Token split(Token left, Token right, double ratioToLeft)
    {
        throw new UnsupportedOperationException();
    }

    public BigIntegerToken getMinimumToken()
    {
        return MINIMUM;
    }

    @Override
    public Token getMaximumToken()
    {
        return null;
    }

    public BigIntegerToken getRandomToken()
    {
        return getRandomToken(ThreadLocalRandom.current());
    }

    public BigIntegerToken getRandomToken(Random random)
    {
        return new BigIntegerToken(BigInteger.valueOf(random.nextInt(15)));
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory() {
        public ByteBuffer toByteArray(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return ByteBuffer.wrap(bigIntegerToken.token.toByteArray());
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return new BigIntegerToken(new BigInteger(ByteBufferUtil.getArray(bytes)));
        }

        public String toString(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return bigIntegerToken.token.toString();
        }

        public Token fromString(String string)
        {
            return new BigIntegerToken(new BigInteger(string));
        }

        public void validate(String token) {}
    };

    public Token.TokenFactory getTokenFactory()
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
        return new BigIntegerToken(BigInteger.valueOf(key.remaining()));
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        // allTokens will contain the count and be returned, sorted_ranges is shorthand for token<->token math.
        Map<Token, Float> allTokens = new HashMap<Token, Float>();
        List<Range<Token>> sortedRanges = new ArrayList<Range<Token>>();

        // this initializes the counts to 0 and calcs the ranges in order.
        Token lastToken = sortedTokens.get(sortedTokens.size() - 1);
        for (Token node : sortedTokens)
        {
            allTokens.put(node, new Float(0.0));
            sortedRanges.add(new Range<Token>(lastToken, node));
            lastToken = node;
        }

        for (String ks : Schema.instance.getKeyspaces())
        {
            for (CFMetaData cfmd : Schema.instance.getTablesAndViews(ks))
            {
                for (Range<Token> r : sortedRanges)
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

    public AbstractType<?> getTokenValidator()
    {
        return IntegerType.instance;
    }

    public AbstractType<?> partitionOrdering()
    {
        return new PartitionerDefinedOrder(this);
    }
}