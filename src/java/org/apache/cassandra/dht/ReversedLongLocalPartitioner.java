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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.primitives.Longs;

import org.apache.cassandra.db.CachedHashDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.memory.HeapCloner;

/**
 * Special ordered partitioner for the TCM snapshots table, sorting in reverse. Keys are longs (epoch), and tokens
 * become Long.MAX_VALUE - key. Keys are required to be >= 0.
 */
public class ReversedLongLocalPartitioner implements IPartitioner
{
    public static ReversedLongLocalPartitioner instance = new ReversedLongLocalPartitioner();
    private static final ReversedLongLocalToken MIN_TOKEN = new ReversedLongLocalToken(Long.MIN_VALUE);
    private static final long HEAP_SIZE = ObjectSizes.measure(MIN_TOKEN);

    private ReversedLongLocalPartitioner() {}

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return new CachedHashDecoratedKey(getToken(key), key); // CachedHashDecoratedKey is used for bloom filter hash calculation
    }

    public Token midpoint(Token ltoken, Token rtoken)
    {
        // the symbolic MINIMUM token should act as ZERO: the empty bit array
        BigInteger left = ltoken.equals(MIN_TOKEN) ? BigInteger.ZERO : BigInteger.valueOf(ltoken.getLongValue());
        BigInteger right = rtoken.equals(MIN_TOKEN) ? BigInteger.ZERO : BigInteger.valueOf(rtoken.getLongValue());
        Pair<BigInteger, Boolean> midpair = FBUtilities.midpoint(left, right, 63);
        // discard the remainder
        return new ReversedLongLocalToken(midpair.left.longValue());
    }

    public Token split(Token left, Token right, double ratioToLeft)
    {
        throw new UnsupportedOperationException();
    }

    public Token getMinimumToken()
    {
        return MIN_TOKEN;
    }

    public Token getToken(ByteBuffer key)
    {
        if (!key.hasRemaining())
            return MIN_TOKEN;
        long longKey = ByteBufferUtil.toLong(HeapCloner.instance.clone(key));
        assert longKey >= 0 : "ReversedLocalLongToken only supports non-negative keys, not " + longKey;
        return new ReversedLongLocalToken(Long.MAX_VALUE - longKey);
    }

    public Token getRandomToken()
    {
        throw new UnsupportedOperationException();
    }

    public Token getRandomToken(Random random)
    {
        throw new UnsupportedOperationException();
    }

    public Token.TokenFactory getTokenFactory()
    {
        return tokenFactory;
    }

    private final Token.TokenFactory tokenFactory = new Token.TokenFactory()
    {
        public Token fromComparableBytes(ByteSource.Peekable comparableBytes, ByteComparable.Version version)
        {
            long tokenData = ByteSourceInverse.getSignedLong(comparableBytes);
            return new ReversedLongLocalToken(tokenData);
        }

        public ByteBuffer toByteArray(Token token)
        {
            ReversedLongLocalToken longToken = (ReversedLongLocalToken) token;
            return ByteBufferUtil.bytes(longToken.token);
        }

        public Token fromByteArray(ByteBuffer bytes)
        {
            return new ReversedLongLocalToken(ByteBufferUtil.toLong(bytes));
        }

        public String toString(Token token)
        {
            return token.toString();
        }

        public void validate(String token)
        {
            LongType.instance.validate(LongType.instance.fromString(token));
        }

        public Token fromString(String string)
        {
            return new ReversedLongLocalToken(Long.parseLong(string));
        }
    };

    public boolean preservesOrder()
    {
        return true;
    }

    public Map<Token, Float> describeOwnership(List<Token> sortedTokens)
    {
        return Collections.singletonMap(getMinimumToken(), 1.0F);
    }

    public AbstractType<?> getTokenValidator()
    {
        return LongType.instance;
    }

    public AbstractType<?> partitionOrdering()
    {
        return LongType.instance;
    }

    private static class ReversedLongLocalToken extends Token
    {
        private final long token;

        public ReversedLongLocalToken(long token)
        {
            this.token = token;
        }

        @Override
        public IPartitioner getPartitioner()
        {
            return ReversedLongLocalPartitioner.instance;
        }

        @Override
        public long getHeapSize()
        {
            return HEAP_SIZE;
        }

        @Override
        public Object getTokenValue()
        {
            return token;
        }

        @Override
        public long getLongValue()
        {
            return token;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            return ByteSource.of(token);
        }

        @Override
        public double size(Token next)
        {
            throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.",
                                                                  getClass().getSimpleName()));
        }

        @Override
        public Token nextValidToken()
        {
            throw new UnsupportedOperationException(String.format("Token type %s does not support token allocation.",
                                                                  getClass().getSimpleName()));
        }

        @Override
        public int compareTo(Token o)
        {
            return Long.compare(token, ((ReversedLongLocalToken)o).token);
        }

        @Override
        public String toString()
        {
            return String.valueOf(token);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof ReversedLongLocalToken)) return false;
            ReversedLongLocalToken that = (ReversedLongLocalToken) o;
            return token == that.token;
        }

        @Override
        public int hashCode()
        {
            return Longs.hashCode(token);
        }
    }
}
