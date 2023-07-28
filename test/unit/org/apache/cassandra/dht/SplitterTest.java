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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Assertions;

import static com.google.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SplitterTest
{
    private static final Logger logger = LoggerFactory.getLogger(SplitterTest.class);

    @Test
    public void randomSplitTestNoVNodesRandomPartitioner()
    {
        randomSplitTestNoVNodes(new RandomPartitioner());
    }

    @Test
    public void randomSplitTestNoVNodesMurmur3Partitioner()
    {
        randomSplitTestNoVNodes(new Murmur3Partitioner());
    }

    @Test
    public void randomSplitTestVNodesRandomPartitioner()
    {
        randomSplitTestVNodes(new RandomPartitioner());
    }

    @Test
    public void randomSplitTestVNodesMurmur3Partitioner()
    {
        randomSplitTestVNodes(new Murmur3Partitioner());
    }

    // CASSANDRA-18013
    @Test
    public void testSplitOwnedRanges() {
        Splitter splitter = getSplitter(Murmur3Partitioner.instance);
        long lt = 0;
        long rt = 31;
        Range<Token> range = new Range<>(getWrappedToken(Murmur3Partitioner.instance, BigInteger.valueOf(lt)),
                                         getWrappedToken(Murmur3Partitioner.instance, BigInteger.valueOf(rt)));

        for (int i = 1; i <= (rt - lt); i++)
        {
            List<Token> splits = splitter.splitOwnedRanges(i, Arrays.asList(new Splitter.WeightedRange(1.0d, range)), false);
            logger.info("{} splits of {} are: {}", i, range, splits);
            Assertions.assertThat(splits).hasSize(i);
        }
    }

    @Test
    public void testWithWeight()
    {
        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        ranges.add(new Splitter.WeightedRange(1.0, t(0, 10)));
        ranges.add(new Splitter.WeightedRange(1.0, t(20, 30)));
        ranges.add(new Splitter.WeightedRange(1.0, t(40, 50)));

        List<Splitter.WeightedRange> ranges2 = new ArrayList<>(); // same total coverage, split point inside weight-1 range
        ranges2.add(new Splitter.WeightedRange(1.0, t(0, 10)));
        ranges2.add(new Splitter.WeightedRange(1.0, t(20, 30)));
        ranges2.add(new Splitter.WeightedRange(0.5, t(40, 60)));

        List<Splitter.WeightedRange> ranges3 = new ArrayList<>(); // same total coverage, split point inside weight-0.5 range
        ranges3.add(new Splitter.WeightedRange(1.0, t(0, 10)));
        ranges3.add(new Splitter.WeightedRange(0.5, t(15, 35)));
        ranges3.add(new Splitter.WeightedRange(1.0, t(40, 50)));

        IPartitioner partitioner = Murmur3Partitioner.instance;
        Splitter splitter = partitioner.splitter().get();

        assertEquals(splitter.splitOwnedRanges(2, ranges, false), splitter.splitOwnedRanges(2, ranges2, false));
        assertEquals(splitter.splitOwnedRanges(2, ranges, false), splitter.splitOwnedRanges(2, ranges3, false));
    }

    @Test
    public void testWithWeight2()
    {
        List<Splitter.WeightedRange> ranges = new ArrayList<>();
        ranges.add(new Splitter.WeightedRange(0.2, t(0, 10)));
        ranges.add(new Splitter.WeightedRange(1.0, t(20, 30)));
        ranges.add(new Splitter.WeightedRange(1.0, t(40, 50)));

        List<Splitter.WeightedRange> ranges2 = new ArrayList<>();
        ranges2.add(new Splitter.WeightedRange(1.0, t(0, 2)));
        ranges2.add(new Splitter.WeightedRange(1.0, t(20, 30)));
        ranges2.add(new Splitter.WeightedRange(1.0, t(40, 50)));
        IPartitioner partitioner = Murmur3Partitioner.instance;
        Splitter splitter = partitioner.splitter().get();

        assertEquals(splitter.splitOwnedRanges(2, ranges, false), splitter.splitOwnedRanges(2, ranges2, false));
    }

    private Range<Token> t(long left, long right)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    private static void randomSplitTestNoVNodes(IPartitioner partitioner)
    {
        Splitter splitter = getSplitter(partitioner);
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            List<Splitter.WeightedRange> localRanges = generateLocalRanges(1, r.nextInt(4) + 1, splitter, r, partitioner instanceof RandomPartitioner);
            List<Token> boundaries = splitter.splitOwnedRanges(r.nextInt(9) + 1, localRanges, false);
            assertTrue("boundaries = " + boundaries + " ranges = " + localRanges, assertRangeSizeEqual(localRanges, boundaries, partitioner, splitter, true));
        }
    }

    private static void randomSplitTestVNodes(IPartitioner partitioner)
    {
        Splitter splitter = getSplitter(partitioner);
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            // we need many tokens to be able to split evenly over the disks
            int numTokens = 172 + r.nextInt(128);
            int rf = r.nextInt(4) + 2;
            int parts = r.nextInt(5) + 1;
            List<Splitter.WeightedRange> localRanges = generateLocalRanges(numTokens, rf, splitter, r, partitioner instanceof RandomPartitioner);
            List<Token> boundaries = splitter.splitOwnedRanges(parts, localRanges, true);
            if (!assertRangeSizeEqual(localRanges, boundaries, partitioner, splitter, false))
                fail(String.format("Could not split %d tokens with rf=%d into %d parts (localRanges=%s, boundaries=%s)", numTokens, rf, parts, localRanges, boundaries));
        }
    }

    private static boolean assertRangeSizeEqual(List<Splitter.WeightedRange> localRanges, List<Token> tokens, IPartitioner partitioner, Splitter splitter, boolean splitIndividualRanges)
    {
        Token start = partitioner.getMinimumToken();
        List<BigInteger> splits = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++)
        {
            Token end = i == tokens.size() - 1 ? partitioner.getMaximumToken() : tokens.get(i);
            splits.add(sumOwnedBetween(localRanges, start, end, splitter, splitIndividualRanges));
            start = end;
        }
        // when we dont need to keep around full ranges, the difference is small between the partitions
        BigDecimal delta = splitIndividualRanges ? BigDecimal.valueOf(0.001) : BigDecimal.valueOf(0.25);
        boolean allBalanced = true;
        for (BigInteger b : splits)
        {
            for (BigInteger i : splits)
            {
                BigDecimal bdb = new BigDecimal(b);
                BigDecimal bdi = new BigDecimal(i);
                BigDecimal q = bdb.divide(bdi, 2, BigDecimal.ROUND_HALF_DOWN);
                if (q.compareTo(BigDecimal.ONE.add(delta)) > 0 || q.compareTo(BigDecimal.ONE.subtract(delta)) < 0)
                    allBalanced = false;
            }
        }
        return allBalanced;
    }

    private static BigInteger sumOwnedBetween(List<Splitter.WeightedRange> localRanges, Token start, Token end, Splitter splitter, boolean splitIndividualRanges)
    {
        BigInteger sum = BigInteger.ZERO;
        for (Splitter.WeightedRange range : localRanges)
        {
            if (splitIndividualRanges)
            {
                Set<Range<Token>> intersections = new Range<>(start, end).intersectionWith(range.range());
                for (Range<Token> intersection : intersections)
                    sum = sum.add(splitter.valueForToken(intersection.right).subtract(splitter.valueForToken(intersection.left)));
            }
            else
            {
                if (new Range<>(start, end).contains(range.left()))
                    sum = sum.add(splitter.valueForToken(range.right()).subtract(splitter.valueForToken(range.left())));
            }
        }
        return sum;
    }

    private static List<Splitter.WeightedRange> generateLocalRanges(int numTokens, int rf, Splitter splitter, Random r, boolean randomPartitioner)
    {
        int localTokens = numTokens * rf;
        List<Token> randomTokens = new ArrayList<>();

        for (int i = 0; i < localTokens * 2; i++)
        {
            Token t = splitter.tokenForValue(randomPartitioner ? new BigInteger(127, r) : BigInteger.valueOf(r.nextLong()));
            randomTokens.add(t);
        }

        Collections.sort(randomTokens);

        List<Splitter.WeightedRange> localRanges = new ArrayList<>(localTokens);
        for (int i = 0; i < randomTokens.size() - 1; i++)
        {
            assert randomTokens.get(i).compareTo(randomTokens.get(i + 1)) < 0;
            localRanges.add(new Splitter.WeightedRange(1.0, new Range<>(randomTokens.get(i), randomTokens.get(i + 1))));
            i++;
        }
        return localRanges;
    }

    @Test
    public void testSplitMurmur3Partitioner()
    {
        testSplit(new Murmur3Partitioner());
    }

    @Test
    public void testSplitRandomPartitioner()
    {
        testSplit(new RandomPartitioner());
    }

    @SuppressWarnings("unchecked")
    private static void testSplit(IPartitioner partitioner)
    {
        boolean isRandom = partitioner instanceof RandomPartitioner;
        Splitter splitter = getSplitter(partitioner);
        BigInteger min = splitter.valueForToken(partitioner.getMinimumToken());
        BigInteger max = splitter.valueForToken(partitioner.getMaximumToken());
        BigInteger first = isRandom ? RandomPartitioner.ZERO : min;
        BigInteger last = isRandom ? max.subtract(BigInteger.valueOf(1)) : max;
        BigInteger midpoint = last.add(first).divide(BigInteger.valueOf(2));

        // regular single range
        testSplit(partitioner, 1, newHashSet(Pair.create(1, 100)), newHashSet(Pair.create(1, 100)));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(1, 100)),
                  newHashSet(Pair.create(1, 50), Pair.create(50, 100)));
        testSplit(partitioner, 4,
                  newHashSet(Pair.create(1, 100)),
                  newHashSet(Pair.create(1, 25), Pair.create(25, 50), Pair.create(50, 75), Pair.create(75, 100)));
        testSplit(partitioner, 5,
                  newHashSet(Pair.create(3, 79)),
                  newHashSet(Pair.create(3, 18), Pair.create(18, 33), Pair.create(33, 48), Pair.create(48, 63),
                             Pair.create(63, 79)));
        testSplit(partitioner, 3,
                  newHashSet(Pair.create(3, 20)),
                  newHashSet(Pair.create(3, 8), Pair.create(8, 14), Pair.create(14, 20)));
        testSplit(partitioner, 4,
                  newHashSet(Pair.create(3, 20)),
                  newHashSet(Pair.create(3, 7), Pair.create(7, 11), Pair.create(11, 15), Pair.create(15, 20)));

        // single range too small to be partitioned
        testSplit(partitioner, 1, newHashSet(Pair.create(1, 2)), newHashSet(Pair.create(1, 2)));
        testSplit(partitioner, 2, newHashSet(Pair.create(1, 2)), newHashSet(Pair.create(1, 2)));
        testSplit(partitioner, 4, newHashSet(Pair.create(1, 4)), newHashSet(Pair.create(1, 4)));
        testSplit(partitioner, 8, newHashSet(Pair.create(1, 2)), newHashSet(Pair.create(1, 2)));

        // single wrapping range
        BigInteger cutpoint = isRandom ? midpoint.add(BigInteger.valueOf(7)) : min.add(BigInteger.valueOf(6));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(8, 4)),
                  newHashSet(Pair.create(8, cutpoint), Pair.create(cutpoint, 4)));

        // single range around partitioner min/max values
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(8)), min)),
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(8)), max.subtract(BigInteger.valueOf(4))),
                             Pair.create(max.subtract(BigInteger.valueOf(4)), isRandom ? first : max)));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(8)), max)),
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(8)), max.subtract(BigInteger.valueOf(4))),
                             Pair.create(max.subtract(BigInteger.valueOf(4)), max)));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(min, min.add(BigInteger.valueOf(8)))),
                  newHashSet(Pair.create(min, min.add(BigInteger.valueOf(4))),
                             Pair.create(min.add(BigInteger.valueOf(4)), min.add(BigInteger.valueOf(8)))));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(max, min.add(BigInteger.valueOf(8)))),
                  newHashSet(Pair.create(max, min.add(BigInteger.valueOf(4))),
                             Pair.create(min.add(BigInteger.valueOf(4)), min.add(BigInteger.valueOf(8)))));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(4)), min.add(BigInteger.valueOf(4)))),
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(4)), last),
                             Pair.create(last, min.add(BigInteger.valueOf(4)))));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(4)), min.add(BigInteger.valueOf(8)))),
                  newHashSet(Pair.create(max.subtract(BigInteger.valueOf(4)), min.add(BigInteger.valueOf(2))),
                             Pair.create(min.add(BigInteger.valueOf(2)), min.add(BigInteger.valueOf(8)))));

        // multiple ranges
        testSplit(partitioner, 1,
                  newHashSet(Pair.create(1, 100), Pair.create(200, 300)),
                  newHashSet(Pair.create(1, 100), Pair.create(200, 300)));
        testSplit(partitioner, 2,
                  newHashSet(Pair.create(1, 100), Pair.create(200, 300)),
                  newHashSet(Pair.create(1, 100), Pair.create(200, 300)));
        testSplit(partitioner, 4,
                  newHashSet(Pair.create(1, 100), Pair.create(200, 300)),
                  newHashSet(Pair.create(1, 50), Pair.create(50, 100), Pair.create(200, 250), Pair.create(250, 300)));
        testSplit(partitioner, 4,
                  newHashSet(Pair.create(1, 100),
                             Pair.create(200, 300),
                             Pair.create(max.subtract(BigInteger.valueOf(4)), min.add(BigInteger.valueOf(4)))),
                  newHashSet(Pair.create(1, 50),
                             Pair.create(50, 100),
                             Pair.create(200, 250),
                             Pair.create(250, 300),
                             Pair.create(last, min.add(BigInteger.valueOf(4))),
                             Pair.create(max.subtract(BigInteger.valueOf(4)), last)));
    }

    private static void testSplit(IPartitioner partitioner, int parts, Set<Pair<Object, Object>> ranges, Set<Pair<Object, Object>> expected)
    {
        Splitter splitter = getSplitter(partitioner);
        Set<Range<Token>> splittedRanges = splitter.split(ranges(partitioner, ranges), parts);
        assertEquals(ranges(partitioner, expected), splittedRanges);
    }

    private static Set<Range<Token>> ranges(IPartitioner partitioner, Set<Pair<Object, Object>> pairs)
    {
        return pairs.stream().map(pair -> range(partitioner, pair)).collect(Collectors.toSet());
    }

    private static Range<Token> range(IPartitioner partitioner, Pair<?, ?> pair)
    {
        return new Range<>(token(partitioner, pair.left), token(partitioner, pair.right));
    }

    private static Token token(IPartitioner partitioner, Object n)
    {
        return partitioner.getTokenFactory().fromString(n.toString());
    }

    @Test
    public void testTokensInRangeRandomPartitioner()
    {
        testTokensInRange(new RandomPartitioner());
    }

    @Test
    public void testTokensInRangeMurmur3Partitioner()
    {
        testTokensInRange(new Murmur3Partitioner());
    }

    private static void testTokensInRange(IPartitioner partitioner)
    {
        Splitter splitter = getSplitter(partitioner);

        // test full range
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        BigInteger fullRangeSize = splitter.valueForToken(partitioner.getMaximumToken()).subtract(splitter.valueForToken(partitioner.getMinimumToken()));
        assertEquals(fullRangeSize, splitter.tokensInRange(fullRange));
        fullRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(-10)), splitter.tokenForValue(BigInteger.valueOf(-10)));
        assertEquals(fullRangeSize, splitter.tokensInRange(fullRange));

        // test small range
        Range<Token> smallRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(-5)), splitter.tokenForValue(BigInteger.valueOf(5)));
        assertEquals(BigInteger.valueOf(10), splitter.tokensInRange(smallRange));

        // test wrap-around range
        Range<Token> wrapAround = new Range<>(splitter.tokenForValue(BigInteger.valueOf(5)), splitter.tokenForValue(BigInteger.valueOf(-5)));
        assertEquals(fullRangeSize.subtract(BigInteger.TEN), splitter.tokensInRange(wrapAround));
    }

    @Test
    public void testElapsedTokensRandomPartitioner()
    {
        testElapsedMultiRange(new RandomPartitioner());
    }

    @Test
    public void testElapsedTokensMurmur3Partitioner()
    {
        testElapsedMultiRange(new Murmur3Partitioner());
    }

    private static void testElapsedMultiRange(IPartitioner partitioner)
    {
        Splitter splitter = getSplitter(partitioner);
        // small range
        Range<Token> smallRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(-1)), splitter.tokenForValue(BigInteger.valueOf(1)));
        testElapsedTokens(partitioner, smallRange, true);

        // medium range
        Range<Token> mediumRange = new Range<>(splitter.tokenForValue(BigInteger.valueOf(0)), splitter.tokenForValue(BigInteger.valueOf(123456789)));
        testElapsedTokens(partitioner, mediumRange, true);

        // wrapped range
        BigInteger min = splitter.valueForToken(partitioner.getMinimumToken());
        BigInteger max = splitter.valueForToken(partitioner.getMaximumToken());
        Range<Token> wrappedRange = new Range<>(splitter.tokenForValue(max.subtract(BigInteger.valueOf(1350))),
                                                splitter.tokenForValue(min.add(BigInteger.valueOf(20394))));
        testElapsedTokens(partitioner, wrappedRange, true);

        // full range
        Range<Token> fullRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        testElapsedTokens(partitioner, fullRange, false);
    }

    private static void testElapsedTokens(IPartitioner partitioner, Range<Token> range, boolean partialRange)
    {
        Splitter splitter = getSplitter(partitioner);

        BigInteger left = splitter.valueForToken(range.left);
        BigInteger right = splitter.valueForToken(range.right);
        BigInteger tokensInRange = splitter.tokensInRange(range);

        // elapsedTokens(left, (left, right]) = 0
        assertEquals(BigInteger.ZERO, splitter.elapsedTokens(splitter.tokenForValue(left), range));

        // elapsedTokens(right, (left, right]) = tokensInRange((left, right])
        assertEquals(tokensInRange, splitter.elapsedTokens(splitter.tokenForValue(right), range));

        // elapsedTokens(left+1, (left, right]) = 1
        assertEquals(BigInteger.ONE, splitter.elapsedTokens(splitter.tokenForValue(left.add(BigInteger.ONE)), range));

        // elapsedTokens(right-1, (left, right]) = tokensInRange((left, right]) - 1
        assertEquals(tokensInRange.subtract(BigInteger.ONE), splitter.elapsedTokens(splitter.tokenForValue(right.subtract(BigInteger.ONE)), range));

        // elapsedTokens(midpoint, (left, right]) + tokensInRange((midpoint, right]) = tokensInRange
        Token midpoint = partitioner.midpoint(range.left, range.right);
        assertEquals(tokensInRange, splitter.elapsedTokens(midpoint, range).add(splitter.tokensInRange(new Range<>(midpoint, range.right))));

        if (partialRange)
        {
            // elapsedTokens(right + 1, (left, right]) = 0
            assertEquals(BigInteger.ZERO, splitter.elapsedTokens(splitter.tokenForValue(right.add(BigInteger.ONE)), range));
        }
    }

    @Test
    public void testPositionInRangeRandomPartitioner()
    {
        testPositionInRangeMultiRange(new RandomPartitioner());
    }

    @Test
    public void testPositionInRangeMurmur3Partitioner()
    {
        testPositionInRangeMultiRange(new Murmur3Partitioner());
    }

    private static void testPositionInRangeMultiRange(IPartitioner partitioner)
    {
        Splitter splitter = getSplitter(partitioner);

        // Test tiny range
        Token start = splitter.tokenForValue(BigInteger.ZERO);
        Token end = splitter.tokenForValue(BigInteger.valueOf(3));
        Range<Token> range = new Range<>(start, end);
        assertEquals(0.0, splitter.positionInRange(start, range), 0.01);
        assertEquals(0.33, splitter.positionInRange(splitter.tokenForValue(BigInteger.valueOf(1)), range), 0.01);
        assertEquals(0.66, splitter.positionInRange(splitter.tokenForValue(BigInteger.valueOf(2)), range), 0.01);
        assertEquals(1.0, splitter.positionInRange(end, range), 0.01);
        // Token not in range should return -1.0 for position
        Token notInRange = splitter.tokenForValue(BigInteger.valueOf(10));
        assertEquals(-1.0, splitter.positionInRange(notInRange, range), 0.0);


        // Test medium range
        start = splitter.tokenForValue(BigInteger.ZERO);
        end = splitter.tokenForValue(BigInteger.valueOf(1000));
        range = new Range<>(start, end);
        testPositionInRange(partitioner, splitter, range);

        // Test wrap-around range
        start = splitter.tokenForValue(splitter.valueForToken(partitioner.getMaximumToken()).subtract(BigInteger.valueOf(123456789)));
        end = splitter.tokenForValue(splitter.valueForToken(partitioner.getMinimumToken()).add(BigInteger.valueOf(123456789)));
        range = new Range<>(start, end);
        testPositionInRange(partitioner, splitter, range);

        // Test full range
        testPositionInRange(partitioner, splitter, new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken()));
        testPositionInRange(partitioner, splitter, new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken()));
        testPositionInRange(partitioner, splitter, new Range<>(partitioner.getMaximumToken(), partitioner.getMaximumToken()));
        testPositionInRange(partitioner, splitter, new Range<>(splitter.tokenForValue(BigInteger.ONE), splitter.tokenForValue(BigInteger.ONE)));
    }

    private static void testPositionInRange(IPartitioner partitioner, Splitter splitter, Range<Token> range)
    {
        Range<Token> actualRange = range;
        //full range case
        if (range.left.equals(range.right))
        {
            actualRange = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        }
        assertEquals(0.0, splitter.positionInRange(actualRange.left, range), 0.01);
        assertEquals(0.25, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.25), range), 0.01);
        assertEquals(0.37, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.373), range), 0.01);
        assertEquals(0.5, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.5), range), 0.01);
        assertEquals(0.75, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.75), range), 0.01);
        assertEquals(0.99, splitter.positionInRange(getTokenInPosition(partitioner, actualRange, 0.999), range), 0.01);
        assertEquals(1.0, splitter.positionInRange(actualRange.right, range), 0.01);
    }

    private static Token getTokenInPosition(IPartitioner partitioner, Range<Token> range, double position)
    {
        if (range.left.equals(range.right))
        {
            range = new Range<>(partitioner.getMinimumToken(), partitioner.getMaximumToken());
        }
        Splitter splitter = getSplitter(partitioner);
        BigInteger totalTokens = splitter.tokensInRange(range);
        BigInteger elapsedTokens = BigDecimal.valueOf(position).multiply(new BigDecimal(totalTokens)).toBigInteger();
        BigInteger tokenInPosition = splitter.valueForToken(range.left).add(elapsedTokens);
        return getWrappedToken(partitioner, tokenInPosition);
    }

    private static Token getWrappedToken(IPartitioner partitioner, BigInteger position)
    {
        Splitter splitter = getSplitter(partitioner);
        BigInteger maxTokenValue = splitter.valueForToken(partitioner.getMaximumToken());
        BigInteger minTokenValue = splitter.valueForToken(partitioner.getMinimumToken());
        if (position.compareTo(maxTokenValue) > 0)
        {
            position = minTokenValue.add(position.subtract(maxTokenValue));
        }
        return splitter.tokenForValue(position);
    }

    private static Splitter getSplitter(IPartitioner partitioner)
    {
        return partitioner.splitter().orElseThrow(() -> new AssertionError(partitioner.getClass() + " must have a splitter"));
    }
}
