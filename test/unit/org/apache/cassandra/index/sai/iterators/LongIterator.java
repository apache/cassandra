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
package org.apache.cassandra.index.sai.iterators;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

public class LongIterator extends KeyRangeIterator
{
    private final List<PrimaryKey> keys;
    private int currentIdx = 0;

    public LongIterator(long[] tokens)
    {
        super(tokens.length == 0 ? null : makeKey(tokens[0]), tokens.length == 0 ? null : makeKey(tokens[tokens.length - 1]), tokens.length);

        this.keys = new ArrayList<>(tokens.length);
        for (long token : tokens)
            this.keys.add(makeKey(token));
    }

    @Override
    protected PrimaryKey computeNext()
    {
        if (currentIdx >= keys.size())
            return endOfData();

        return keys.get(currentIdx++);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextToken)
    {
        while (currentIdx < keys.size() && keys.get(currentIdx).compareTo(nextToken) < 0)
            currentIdx++;
    }

    @Override
    public void close()
    {}

    /// Generates a row-aware primary key with given token assuming {@link Murmur3Partitioner} is used.
    /// The partition key is filled with inverse murmur3 hash.
    /// The clustering of the returned key is empty.
    public static PrimaryKey makeKey(long token)
    {
        Murmur3Partitioner.LongToken longToken =  new Murmur3Partitioner.LongToken(token);
        ByteBuffer keyBytes = Murmur3Partitioner.LongToken.keyForToken(longToken);
        DecoratedKey partitionKey = new BufferDecoratedKey(longToken, keyBytes);
        return SAITester.TEST_FACTORY.create(partitionKey, Clustering.EMPTY);
    }

    public static List<Long> convert(KeyRangeIterator tokens)
    {
        List<Long> results = new ArrayList<>();
        while (tokens.hasNext())
            results.add(tokens.next().token().getLongValue());

        return results;
    }

    public static List<Long> convert(final long... nums)
    {
        return new ArrayList<>(nums.length)
        {{
            for (long n : nums)
                add(n);
        }};
    }
}
