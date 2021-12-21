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
package org.apache.cassandra.index.sai.memory;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.disk.v2.RowAwarePrimaryKeyFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@Ignore
public abstract class AbstractKeyRangeIteratorTest
{
    protected PrimaryKey.Factory primaryKeyFactory;

    @Before
    public void setup()
    {
        primaryKeyFactory = new RowAwarePrimaryKeyFactory(SAITester.EMPTY_COMPARATOR);
    }

    @Test
    public void singleTokenIsReturned() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 1, 1);

        assertIterator(iterator, 1);
    }

    @Test
    public void duplicateSingleTokenIsReturned() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 1, 1, 1);

        assertIterator(iterator, 1);
    }

    @Test
    public void withoutSkipAllTokensAreReturnedInTokenOrder() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3);

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void tokensAddedOutOfOrderAreReturnedInOrder() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 3, 2, 1);

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void matchingTokensAreIgnoredAtStart() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 1, 2, 3);

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void matchingTokensAreIgnoredInMiddle() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 2, 3);

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void matchingTokensAreIgnoredAtEnd() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3, 3);

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void skipToTokenBeforeFirstTokenWillReturnAllTokens() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3);

        iterator.skipTo(primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(0)));

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void skipToFirstTokenWillReturnAllTokens() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3);

        iterator.skipTo(primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(1)));

        assertIterator(iterator, 1, 2, 3);
    }

    @Test
    public void skipToMiddleTokenWillReturnRemainingTokens() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3);

        iterator.skipTo(primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(2)));

        assertIterator(iterator, 2, 3);
    }

    @Test
    public void skipToLastTokenWillReturnLastToken() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3);

        iterator.skipTo(primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(3)));

        assertIterator(iterator, 3);
    }

    @Test
    public void skipToAfterLastTokenWillReturnNoTokens() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 2, 3);

        iterator.skipTo(primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(4)));

        assertIterator(iterator);
    }

    @Test
    public void skipToWithMatchingTokensWithReturnCorrectTokens() throws Exception
    {
        RangeIterator iterator= makeIterator(1, 3, 1, 1, 2, 2, 3, 3);

        iterator.skipTo(primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(2)));

        assertIterator(iterator, 2, 3);
    }

    private void assertIterator(RangeIterator iterator, long... tokens) throws Exception
    {
        for(long token : tokens)
        {
            assertEquals(token, iterator.next().token().getLongValue());
        }
        assertFalse(iterator.hasNext());
    }


    protected abstract RangeIterator makeIterator(long minimumTokenValue, long maximumTokenValue, long... tokens);

    protected PrimaryKey keyForToken(long token)
    {
        return primaryKeyFactory.createTokenOnly(new Murmur3Partitioner.LongToken(token));
    }
}
