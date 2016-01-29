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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Partition splitter.
 */
public abstract class Splitter
{
    private final IPartitioner partitioner;

    protected Splitter(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    protected abstract Token tokenForValue(BigInteger value);

    protected abstract BigInteger valueForToken(Token token);

    public List<Token> splitOwnedRanges(int parts, List<Range<Token>> localRanges, boolean dontSplitRanges)
    {
        if (localRanges.isEmpty() || parts == 1)
            return Collections.singletonList(partitioner.getMaximumToken());

        BigInteger totalTokens = BigInteger.ZERO;
        for (Range<Token> r : localRanges)
        {
            BigInteger right = valueForToken(token(r.right));
            totalTokens = totalTokens.add(right.subtract(valueForToken(r.left)));
        }
        BigInteger perPart = totalTokens.divide(BigInteger.valueOf(parts));
        // the range owned is so tiny we can't split it:
        if (perPart.equals(BigInteger.ZERO))
            return Collections.singletonList(partitioner.getMaximumToken());

        if (dontSplitRanges)
            return splitOwnedRangesNoPartialRanges(localRanges, perPart, parts);

        List<Token> boundaries = new ArrayList<>();
        BigInteger sum = BigInteger.ZERO;
        for (Range<Token> r : localRanges)
        {
            Token right = token(r.right);
            BigInteger currentRangeWidth = valueForToken(right).subtract(valueForToken(r.left)).abs();
            BigInteger left = valueForToken(r.left);
            while (sum.add(currentRangeWidth).compareTo(perPart) >= 0)
            {
                BigInteger withinRangeBoundary = perPart.subtract(sum);
                left = left.add(withinRangeBoundary);
                boundaries.add(tokenForValue(left));
                currentRangeWidth = currentRangeWidth.subtract(withinRangeBoundary);
                sum = BigInteger.ZERO;
            }
            sum = sum.add(currentRangeWidth);
        }
        boundaries.set(boundaries.size() - 1, partitioner.getMaximumToken());

        assert boundaries.size() == parts : boundaries.size() +"!="+parts+" "+boundaries+":"+localRanges;
        return boundaries;
    }

    private List<Token> splitOwnedRangesNoPartialRanges(List<Range<Token>> localRanges, BigInteger perPart, int parts)
    {
        List<Token> boundaries = new ArrayList<>(parts);
        BigInteger sum = BigInteger.ZERO;
        int i = 0;
        while (boundaries.size() < parts - 1)
        {
            Range<Token> r = localRanges.get(i);
            Range<Token> nextRange = localRanges.get(i + 1);
            Token right = token(r.right);
            Token nextRight = token(nextRange.right);

            BigInteger currentRangeWidth = valueForToken(right).subtract(valueForToken(r.left));
            BigInteger nextRangeWidth = valueForToken(nextRight).subtract(valueForToken(nextRange.left));
            sum = sum.add(currentRangeWidth);
            // does this or next range take us beyond the per part limit?
            if (sum.compareTo(perPart) > 0 || sum.add(nextRangeWidth).compareTo(perPart) > 0)
            {
                // Either this or the next range will take us beyond the perPart limit. Will stopping now or
                // adding the next range create the smallest difference to perPart?
                BigInteger diffCurrent = sum.subtract(perPart).abs();
                BigInteger diffNext = sum.add(nextRangeWidth).subtract(perPart).abs();
                if (diffNext.compareTo(diffCurrent) >= 0)
                {
                    sum = BigInteger.ZERO;
                    boundaries.add(right);
                }
            }
            i++;
        }
        boundaries.add(partitioner.getMaximumToken());
        return boundaries;
    }

    /**
     * We avoid calculating for wrap around ranges, instead we use the actual max token, and then, when translating
     * to PartitionPositions, we include tokens from .minKeyBound to .maxKeyBound to make sure we include all tokens.
     */
    private Token token(Token t)
    {
        return t.equals(partitioner.getMinimumToken()) ? partitioner.getMaximumToken() : t;
    }

}
