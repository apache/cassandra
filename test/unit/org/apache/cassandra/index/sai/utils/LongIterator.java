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
package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.LongFunction;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.Token;
import org.apache.cassandra.index.sai.memory.InMemoryToken;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LongIterator extends RangeIterator
{
    private final List<LongToken> tokens;
    private int currentIdx = 0;

    /**
     * whether LongIterator should throw exception during iteration.
     */
    private boolean shouldThrow = false;
    private final Random random = new Random();

    public LongIterator(long[] tokens)
    {
        this(tokens, t -> t);
    }

    public LongIterator(long[] tokens, LongFunction<Long> toOffset)
    {
        super(tokens.length == 0 ? null : tokens[0], tokens.length == 0 ? null: tokens[tokens.length - 1], tokens.length);

        this.tokens = new ArrayList<>(tokens.length);
        for (long token : tokens)
            this.tokens.add(new LongToken(token, toOffset.apply(token)));
    }

    public LongIterator throwsException()
    {
        this.shouldThrow = true;
        return this;
    }

    @Override
    protected Token computeNext()
    {
        // throws exception if it's last element or chosen 1 out of n
        if (shouldThrow && (currentIdx >= tokens.size() - 1 || random.nextInt(tokens.size()) == 0))
            throw new RuntimeException("injected exception");

        if (currentIdx >= tokens.size())
            return endOfData();

        return tokens.get(currentIdx++);
    }

    @Override
    protected void performSkipTo(Long nextToken)
    {
        for (int i = currentIdx == 0 ? 0 : currentIdx - 1; i < tokens.size(); i++)
        {
            LongToken token = tokens.get(i);
            if (token.get() >= nextToken)
            {
                currentIdx = i;
                break;
            }
        }
    }

    @Override
    public void close()
    {}

    public static class LongToken extends Token
    {
        public final Set<Long> offsets = new TreeSet<>();

        LongToken(long token, long offset)
        {
            super(token);
            offsets.add(offset);
        }

        @Override
        public Iterator<DecoratedKey> keys()
        {
            return new AbstractIterator<DecoratedKey>()
            {
                Iterator<Long> iterator = offsets.iterator();

                @Override
                protected DecoratedKey computeNext()
                {
                    if (!iterator.hasNext())
                        return endOfData();

                    long offset = iterator.next();
                    return new BufferDecoratedKey(new Murmur3Partitioner.LongToken(offset), ByteBufferUtil.bytes(offset));
                }
            };
        }
    }

    public static List<Long> convert(RangeIterator tokens)
    {
        List<Long> results = new ArrayList<>();
        while (tokens.hasNext())
            results.add(tokens.next().get());

        return results;
    }

    public static List<Long> convert(final long... nums)
    {
        return new ArrayList<Long>(nums.length)
        {{
            for (long n : nums)
                add(n);
        }};
    }

    static List<Long> convertOffsets(RangeIterator tokens)
    {
        List<Long> results = new ArrayList<>();
        while (tokens.hasNext())
        {
            Token token = tokens.next();
            if (token instanceof LongToken)
            {
                LongToken longToken = (LongToken) token;
                results.addAll(longToken.offsets);
            }
            else
            {
                // extract the fake key and token from LongToken#keys
                InMemoryToken inMemoryToken = (InMemoryToken) token;
                inMemoryToken.keys().forEachRemaining(key -> results.add(key.getToken().getLongValue()));
            }
        }

        return results;
    }
}
