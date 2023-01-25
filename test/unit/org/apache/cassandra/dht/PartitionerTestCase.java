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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.Ranges;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class PartitionerTestCase
{
    private static final double SPLIT_RATIO_MIN = 0.10;
    private static final double SPLIT_RATIO_MAX = 1 - SPLIT_RATIO_MIN;

    protected IPartitioner partitioner;

    public abstract void initPartitioner();

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Before
    public void clean()
    {
        initPartitioner();
    }

    public Token tok(byte[] key)
    {
        return partitioner.getToken(ByteBuffer.wrap(key));
    }

    public Token tok(String key)
    {
        return tok(key.getBytes());
    }

    /**
     * Recurses randomly to the given depth a few times.
     */
    public void assertMidpoint(Token left, Token right, int depth)
    {
        Random rand = new Random();
        for (int i = 0; i < 1000; i++)
        {
            assertMidpoint(left, right, rand, depth);
        }
    }

    private void assertMidpoint(Token left, Token right, Random rand, int depth)
    {
        Token mid = partitioner.midpoint(left, right);
        assert new Range<Token>(left, right).contains(mid)
                : "For " + left + "," + right + ": range did not contain mid:" + mid;
        if (depth < 1)
            return;

        if (rand.nextBoolean())
            assertMidpoint(left, mid, rand, depth-1);
        else
            assertMidpoint(mid, right, rand, depth-1);
    }

    @Test
    public void testMidpoint()
    {
        assertMidpoint(tok("a"), tok("b"), 16);
        assertMidpoint(tok("a"), tok("bbb"), 16);
    }

    @Test
    public void testMidpointMinimum()
    {
        midpointMinimumTestCase();
    }

    protected void midpointMinimumTestCase()
    {
        Token mintoken = partitioner.getMinimumToken();
        assert mintoken.compareTo(partitioner.midpoint(mintoken, mintoken)) != 0;
        assertMidpoint(mintoken, tok("a"), 16);
        assertMidpoint(mintoken, tok("aaa"), 16);
        assertMidpoint(mintoken, mintoken, 126);
        assertMidpoint(tok("a"), mintoken, 16);
    }

    @Test
    public void testMidpointWrapping()
    {
        assertMidpoint(tok("b"), tok("a"), 16);
        assertMidpoint(tok("bbb"), tok("a"), 16);
    }

    /**
     * Test split token ranges
     */
    public void assertSplit(Token left, Token right, int depth)
    {
        Random rand = new Random();
        for (int i = 0; i < 1000; i++)
        {
            assertSplit(left, right ,rand, depth);
        }
    }

    protected abstract boolean shouldStopRecursion(Token left, Token right);

    private void assertSplit(Token left, Token right, Random rand, int depth)
    {
        if (shouldStopRecursion(left, right))
        {
            System.out.println("Stop assertSplit at depth: " + depth);
            return;
        }

        double ratio = SPLIT_RATIO_MIN + (SPLIT_RATIO_MAX - SPLIT_RATIO_MIN) * rand.nextDouble();
        Token newToken = partitioner.split(left, right, ratio);

        assertEquals("For " + left + "," + right + ", new token: " + newToken,
                     ratio, left.size(newToken) / left.size(right), 0.1);

        assert new Range<Token>(left, right).contains(newToken)
            : "For " + left + "," + right + ": range did not contain new token:" + newToken;

        if (depth < 1)
            return;

        if (rand.nextBoolean())
            assertSplit(left, newToken, rand, depth-1);
        else
            assertSplit(newToken, right, rand, depth-1);
    }

    @Test
    public void testTokenFactoryBytes()
    {
        Token.TokenFactory factory = partitioner.getTokenFactory();
        assert tok("a").compareTo(factory.fromByteArray(factory.toByteArray(tok("a")))) == 0;
    }

    @Test
    public void testTokenFactoryStrings()
    {
        Token.TokenFactory factory = partitioner.getTokenFactory();
        assert tok("a").compareTo(factory.fromString(factory.toString(tok("a")))) == 0;
    }

    @Test
    public void testDescribeOwnership()
    {
        // This call initializes StorageService, needed to populate the keyspaces.
        // TODO: This points to potential problems in the initialization sequence. Should be solved by CASSANDRA-7837.
        StorageService.instance.getKeyspaces();

        try
        {
            testDescribeOwnershipWith(0);
            fail();
        }
        catch (RuntimeException e)
        {
            // success
        }
        testDescribeOwnershipWith(1);
        testDescribeOwnershipWith(2);
        testDescribeOwnershipWith(256);
    }

    private void testDescribeOwnershipWith(int numTokens)
    {
        List<Token> tokens = new ArrayList<Token>();
        while (tokens.size() < numTokens)
        {
            Token randomToken = partitioner.getRandomToken();
            if (!tokens.contains(randomToken))
                tokens.add(randomToken);
        }
        Collections.sort(tokens);
        Map<Token, Float> owns = partitioner.describeOwnership(tokens);

        float totalOwnership = 0;
        for (float ownership : owns.values())
            totalOwnership += ownership;
        assertEquals(1.0, totalOwnership, 0.001);
    }

    @Test
    public void testCompare()
    {
        if (!partitioner.preservesOrder())
            return;

        assert tok("").compareTo(tok("asdf")) < 0;
        assert tok("asdf").compareTo(tok("")) > 0;
        assert tok("").compareTo(tok("")) == 0;
        assert tok("z").compareTo(tok("a")) > 0;
        assert tok("a").compareTo(tok("z")) < 0;
        assert tok("asdf").compareTo(tok("asdf")) == 0;
        assert tok("asdz").compareTo(tok("asdf")) > 0;
    }

    @Test
    public void testCompareSplitter()
    {
        for (int i = 0 ; i < 16 ; ++i)
        {
            Token a = partitioner.getRandomToken(), b = partitioner.getRandomToken();
            while (a.equals(b))
                b = partitioner.getRandomToken();
            if (a.compareTo(b) > 0) { Token tmp = a; a = b; b = tmp; }
            testCompareSplitter(a, b);
        }

        if (!partitioner.preservesOrder())
            return;

        testCompareSplitter(tok(""), tok("asdf"));
        testCompareSplitter(tok(""), tok(""));
        testCompareSplitter(tok("a"), tok("z"));
        testCompareSplitter(tok("asdf"), tok("asdf"));
        testCompareSplitter(tok("asd"), tok("asdf"));
        testCompareSplitter(tok("asdf"), tok("asf"));
        testCompareSplitter(tok("asdf"), tok("asdz"));
    }

    @Test
    public void testSplitter()
    {
        for (int i = 0 ; i < 1024 ; ++i)
        {
            Token a = partitioner.getRandomToken(), b = partitioner.getRandomToken();
            while (a.equals(b))
                b = partitioner.getRandomToken();
            if (a.compareTo(b) > 0) { Token tmp = a; a = b; b = tmp; }
            testSplitter(a, b);
        }

        if (!partitioner.preservesOrder())
            return;

        testSplitter(tok(""), tok("asdf"));
        testSplitter(tok("a"), tok("z"));
        testSplitter(tok("asd"), tok("asdf"));
        testSplitter(tok("asdf"), tok("asdz"));
    }

    void testCompareSplitter(Token less, Token more)
    {
        Ranges ranges;
        if (less.equals(more) && less.isMinimum())
            ranges = Ranges.EMPTY;
        else if (less.equals(more))
            ranges = Ranges.of(new TokenRange(new TokenKey("", partitioner.getMinimumToken()), new TokenKey("", less)));
        else
            ranges = Ranges.of(new TokenRange(new TokenKey("", less), new TokenKey("", more)));

        AccordSplitter splitter = partitioner.accordSplitter().apply(ranges);
        BigInteger lv = splitter.valueForToken(less);
        BigInteger rv = splitter.valueForToken(more);
        Assert.assertEquals(less.equals(more) ? 0 : -1, normaliseCompare(lv.compareTo(rv)));
        Assert.assertEquals(less.equals(more) ? 0 : 1, normaliseCompare(rv.compareTo(lv)));
        checkRoundTrip(less, splitter.tokenForValue(lv));
        checkRoundTrip(more, splitter.tokenForValue(rv));
    }

    void testSplitter(Token start, Token end)
    {
        accord.primitives.Range range = new TokenRange(new TokenKey("", start), new TokenKey("", end));
        AccordSplitter splitter = partitioner.accordSplitter().apply(Ranges.of(range));
        if (!start.isMinimum())
            testSplitter(new TokenRange(new TokenKey("", partitioner.getMinimumToken()), new TokenKey("", start)));
        testSplitter(new TokenRange(new TokenKey("", start), new TokenKey("", splitter.tokenForValue(splitter.maximumValue()))));
        checkRoundTrip(start, splitter.tokenForValue(splitter.valueForToken(start)));
        checkRoundTrip(end, splitter.tokenForValue(splitter.valueForToken(end)));
    }

    void testSplitter(accord.primitives.Range range)
    {
        AccordSplitter splitter = partitioner.accordSplitter().apply(Ranges.of(range));
        BigInteger size = splitter.sizeOf(range);
        Assert.assertEquals(range, splitter.subRange(range, BigInteger.ZERO, size));
    }

    protected void checkRoundTrip(Token original, Token roundTrip)
    {
        Assert.assertEquals(original, roundTrip);
    }

    static int normaliseCompare(int compareResult)
    {
        if (compareResult < 0) return -1;
        if (compareResult > 0) return 1;
        return 0;
    }
}
