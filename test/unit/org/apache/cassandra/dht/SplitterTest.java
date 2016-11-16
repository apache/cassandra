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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SplitterTest
{

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

    public void randomSplitTestNoVNodes(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            List<Range<Token>> localRanges = generateLocalRanges(1, r.nextInt(4)+1, splitter, r, partitioner instanceof RandomPartitioner);
            List<Token> boundaries = splitter.splitOwnedRanges(r.nextInt(9) + 1, localRanges, false);
            assertTrue("boundaries = "+boundaries+" ranges = "+localRanges, assertRangeSizeEqual(localRanges, boundaries, partitioner, splitter, true));
        }
    }

    public void randomSplitTestVNodes(IPartitioner partitioner)
    {
        Splitter splitter = partitioner.splitter().get();
        Random r = new Random();
        for (int i = 0; i < 10000; i++)
        {
            // we need many tokens to be able to split evenly over the disks
            int numTokens = 172 + r.nextInt(128);
            int rf = r.nextInt(4) + 2;
            int parts = r.nextInt(5)+1;
            List<Range<Token>> localRanges = generateLocalRanges(numTokens, rf, splitter, r, partitioner instanceof RandomPartitioner);
            List<Token> boundaries = splitter.splitOwnedRanges(parts, localRanges, true);
            if (!assertRangeSizeEqual(localRanges, boundaries, partitioner, splitter, false))
                fail(String.format("Could not split %d tokens with rf=%d into %d parts (localRanges=%s, boundaries=%s)", numTokens, rf, parts, localRanges, boundaries));
        }
    }

    private boolean assertRangeSizeEqual(List<Range<Token>> localRanges, List<Token> tokens, IPartitioner partitioner, Splitter splitter, boolean splitIndividualRanges)
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

    private BigInteger sumOwnedBetween(List<Range<Token>> localRanges, Token start, Token end, Splitter splitter, boolean splitIndividualRanges)
    {
        BigInteger sum = BigInteger.ZERO;
        for (Range<Token> range : localRanges)
        {
            if (splitIndividualRanges)
            {
                Set<Range<Token>> intersections = new Range<>(start, end).intersectionWith(range);
                for (Range<Token> intersection : intersections)
                    sum = sum.add(splitter.valueForToken(intersection.right).subtract(splitter.valueForToken(intersection.left)));
            }
            else
            {
                if (new Range<>(start, end).contains(range.left))
                    sum = sum.add(splitter.valueForToken(range.right).subtract(splitter.valueForToken(range.left)));
            }
        }
        return sum;
    }

    private List<Range<Token>> generateLocalRanges(int numTokens, int rf, Splitter splitter, Random r, boolean randomPartitioner)
    {
        int localTokens = numTokens * rf;
        List<Token> randomTokens = new ArrayList<>();

        for (int i = 0; i < localTokens * 2; i++)
        {
            Token t = splitter.tokenForValue(randomPartitioner ? new BigInteger(127, r) : BigInteger.valueOf(r.nextLong()));
            randomTokens.add(t);
        }

        Collections.sort(randomTokens);

        List<Range<Token>> localRanges = new ArrayList<>(localTokens);
        for (int i = 0; i < randomTokens.size() - 1; i++)
        {
            assert randomTokens.get(i).compareTo(randomTokens.get(i+1)) < 0;
            localRanges.add(new Range<>(randomTokens.get(i), randomTokens.get(i+1)));
            i++;
        }
        return localRanges;
    }
}
