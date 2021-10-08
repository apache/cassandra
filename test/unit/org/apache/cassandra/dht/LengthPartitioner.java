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

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.PartitionerDefinedOrder;
import org.apache.cassandra.schema.SchemaManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class LengthPartitioner implements IPartitioner
{
    public static final Long ZERO = 0L;
    public static final BigIntegerToken MINIMUM = new BigIntegerToken(-1L);
    public static final BigIntegerToken MAXIMUM = new BigIntegerToken(Long.MAX_VALUE);

    private final Splitter splitter = new Splitter(this)
    {
        public Token tokenForValue(BigInteger value)
        {
            return new BigIntegerToken(value.longValue());
        }

        public BigInteger valueForToken(Token token)
        {
            return BigInteger.valueOf(((BigIntegerToken)token).getTokenValue());
        }
    };

    public static LengthPartitioner instance = new LengthPartitioner();

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new BufferDecoratedKey(getToken(key), key);
    }

    public BigIntegerToken midpoint(Token ltoken, Token rtoken)
    {
        // the symbolic MINIMUM token should act as ZERO: the empty bit array
        Long left = ltoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)ltoken).token;
        Long right = rtoken.equals(MINIMUM) ? ZERO : ((BigIntegerToken)rtoken).token;
        Pair<BigInteger,Boolean> midpair = FBUtilities.midpoint(BigInteger.valueOf(left), BigInteger.valueOf(right), 127);
        // discard the remainder
        return new BigIntegerToken(midpair.left.longValue());
    }

    public Token split(Token tleft, Token tright, double ratio)
    {
        assert ratio >= 0.0 && ratio <= 1.0;
        BigIntegerToken ltoken = (BigIntegerToken) tleft;
        BigIntegerToken rtoken = (BigIntegerToken) tright;

        long left = ltoken.token;
        long right = rtoken.token;

        if (left < right)
        {
            return new BigIntegerToken((long)(((right - left) * ratio) + left));
        }
        else
        {  // wrapping case
            Long max = MAXIMUM.token;
            return new BigIntegerToken((long)(((max + right) - left) * ratio) + left);
        }
    }

    public BigIntegerToken getMinimumToken()
    {
        return MINIMUM;
    }

    @Override
    public Token getMaximumToken()
    {
        return MAXIMUM;
    }

    public BigIntegerToken getRandomToken()
    {
        return getRandomToken(ThreadLocalRandom.current());
    }

    public BigIntegerToken getRandomToken(Random random)
    {
        return new BigIntegerToken((long)random.nextInt(15));
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory()
    {
        public ByteBuffer toByteArray(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return ByteBufferUtil.bytes(bigIntegerToken.token);
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return new BigIntegerToken(ByteBufferUtil.toLong(bytes));
        }

        @Override
        public Token fromComparableBytes(ByteSource.Peekable comparableBytes, ByteComparable.Version version)
        {
            return new BigIntegerToken(ByteSourceInverse.getSignedLong(comparableBytes));
        }

        public String toString(Token token)
        {
            BigIntegerToken bigIntegerToken = (BigIntegerToken) token;
            return bigIntegerToken.token.toString();
        }

        public Token fromString(String string)
        {
            return new BigIntegerToken(Long.valueOf(string));
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
        return new BigIntegerToken((long)key.remaining());
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

        for (String ks : SchemaManager.instance.getKeyspaces())
        {
            for (TableMetadata cfmd : SchemaManager.instance.getTablesAndViews(ks))
            {
                for (Range<Token> r : sortedRanges)
                {
                    // Looping over every KS:CF:Range, get the splits size and add it to the count
                    allTokens.put(r.right, allTokens.get(r.right) + StorageService.instance.getSplits(ks, cfmd.name, r, 1).size());
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

    public Optional<Splitter> splitter()
    {
        return Optional.of(splitter);
    }

    static class BigIntegerToken extends ComparableObjectToken<Long>
    {
        private static final long serialVersionUID = 1L;

        public BigIntegerToken(Long token)
        {
            super(token);
        }

        // convenience method for testing
        public BigIntegerToken(String token) {
            this(Long.valueOf(token));
        }

        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            ByteBuffer tokenBuffer = LongType.instance.decompose(token);
            return LongType.instance.asComparableBytes(tokenBuffer, version);
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return LengthPartitioner.instance;
        }

        @Override
        public long getHeapSize()
        {
            return 0;
        }

        @Override
        public long getLongValue()
        {
            return token;
        }

        @Override
        public double size(Token next)
        {
            BigIntegerToken n = (BigIntegerToken) next;
            long v = n.token - token;  // Overflow acceptable and desired.
            double d = Math.scalb((double)v, -127); // Scale so that the full range is 1.
            return d > 0.0 ? d : (d + 1.0); // Adjust for signed long, also making sure t.size(t) == 1.
        }
    }
}
