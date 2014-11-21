/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.dht;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.RandomPartitioner.BigIntegerToken;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;

import static org.apache.cassandra.Util.range;


public class RangeTest
{
    @Test
    public void testContains()
    {
        Range<Token> left = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("100"));
        assert !left.contains(new BigIntegerToken("0"));
        assert left.contains(new BigIntegerToken("10"));
        assert left.contains(new BigIntegerToken("100"));
        assert !left.contains(new BigIntegerToken("101"));
    }

    @Test
    public void testContainsWrapping()
    {
        Range<Token> range = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("0"));
        assert range.contains(new BigIntegerToken("0"));
        assert range.contains(new BigIntegerToken("10"));
        assert range.contains(new BigIntegerToken("100"));
        assert range.contains(new BigIntegerToken("101"));

        range = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("0"));
        assert range.contains(new BigIntegerToken("0"));
        assert !range.contains(new BigIntegerToken("1"));
        assert !range.contains(new BigIntegerToken("100"));
        assert range.contains(new BigIntegerToken("200"));
    }

    @Test
    public void testContainsRange()
    {
        Range<Token> one = new Range<Token>(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range<Token> two = new Range<Token>(new BigIntegerToken("2"), new BigIntegerToken("5"));
        Range<Token> thr = new Range<Token>(new BigIntegerToken("5"), new BigIntegerToken("10"));
        Range<Token> fou = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("12"));

        assert one.contains(two);
        assert one.contains(thr);
        assert !one.contains(fou);

        assert !two.contains(one);
        assert !two.contains(thr);
        assert !two.contains(fou);

        assert !thr.contains(one);
        assert !thr.contains(two);
        assert !thr.contains(fou);

        assert !fou.contains(one);
        assert !fou.contains(two);
        assert !fou.contains(thr);
    }

    @Test
    public void testContainsRangeWrapping()
    {
        Range<Token> one = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("2"));
        Range<Token> two = new Range<Token>(new BigIntegerToken("5"), new BigIntegerToken("3"));
        Range<Token> thr = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("12"));
        Range<Token> fou = new Range<Token>(new BigIntegerToken("2"), new BigIntegerToken("6"));
        Range<Token> fiv = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("0"));

        assert !one.contains(two);
        assert one.contains(thr);
        assert !one.contains(fou);

        assert two.contains(one);
        assert two.contains(thr);
        assert !two.contains(fou);

        assert !thr.contains(one);
        assert !thr.contains(two);
        assert !thr.contains(fou);

        assert !fou.contains(one);
        assert !fou.contains(two);
        assert !fou.contains(thr);

        assert fiv.contains(one);
        assert fiv.contains(two);
        assert fiv.contains(thr);
        assert fiv.contains(fou);
    }

    @Test
    public void testContainsRangeOneWrapping()
    {
        Range<Token> wrap1 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range<Token> wrap2 = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("2"));

        Range<Token> nowrap1 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("2"));
        Range<Token> nowrap2 = new Range<Token>(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range<Token> nowrap3 = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("100"));

        assert wrap1.contains(nowrap1);
        assert wrap1.contains(nowrap2);
        assert wrap1.contains(nowrap3);

        assert wrap2.contains(nowrap1);
        assert !wrap2.contains(nowrap2);
        assert wrap2.contains(nowrap3);
    }

    @Test
    public void testIntersects()
    {
        Range<Token> all = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("0")); // technically, this is a wrapping range
        Range<Token> one = new Range<Token>(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range<Token> two = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("8"));
        Range<Token> not = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("12"));

        assert all.intersects(one);
        assert all.intersects(two);

        assert one.intersects(two);
        assert two.intersects(one);

        assert !one.intersects(not);
        assert !not.intersects(one);

        assert !two.intersects(not);
        assert !not.intersects(two);
    }

    @Test
    public void testIntersectsWrapping()
    {
        Range<Token> onewrap = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("2"));
        Range<Token> onecomplement = new Range<Token>(onewrap.right, onewrap.left);
        Range<Token> onestartswith = new Range<Token>(onewrap.left, new BigIntegerToken("12"));
        Range<Token> oneendswith = new Range<Token>(new BigIntegerToken("1"), onewrap.right);
        Range<Token> twowrap = new Range<Token>(new BigIntegerToken("5"), new BigIntegerToken("3"));
        Range<Token> not = new Range<Token>(new BigIntegerToken("2"), new BigIntegerToken("6"));

        assert !onewrap.intersects(onecomplement);
        assert onewrap.intersects(onestartswith);
        assert onewrap.intersects(oneendswith);

        assert onewrap.intersects(twowrap);
        assert twowrap.intersects(onewrap);

        assert !onewrap.intersects(not);
        assert !not.intersects(onewrap);

        assert twowrap.intersects(not);
        assert not.intersects(twowrap);
    }

    @SafeVarargs
    static <T extends RingPosition<T>> void assertIntersection(Range<T> one, Range<T> two, Range<T> ... ranges)
    {
        Set<Range<T>> correct = Range.rangeSet(ranges);
        Set<Range<T>> result1 = one.intersectionWith(two);
        assert result1.equals(correct) : String.format("%s != %s",
                                                       StringUtils.join(result1, ","),
                                                       StringUtils.join(correct, ","));
        Set<Range<T>> result2 = two.intersectionWith(one);
        assert result2.equals(correct) : String.format("%s != %s",
                                                       StringUtils.join(result2, ","),
                                                       StringUtils.join(correct, ","));
    }

    private void assertNoIntersection(Range<Token> wraps1, Range<Token> nowrap3)
    {
        assertIntersection(wraps1, nowrap3);
    }

    @Test
    public void testIntersectionWithAll()
    {
        Range<Token> all0 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range<Token> all10 = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("10"));
        Range<Token> all100 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("100"));
        Range<Token> all1000 = new Range<Token>(new BigIntegerToken("1000"), new BigIntegerToken("1000"));
        Range<Token> wraps = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("10"));

        assertIntersection(all0, wraps, wraps);
        assertIntersection(all10, wraps, wraps);
        assertIntersection(all100, wraps, wraps);
        assertIntersection(all1000, wraps, wraps);
    }

    @Test
    public void testIntersectionContains()
    {
        Range<Token> wraps1 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("10"));
        Range<Token> wraps2 = new Range<Token>(new BigIntegerToken("90"), new BigIntegerToken("20"));
        Range<Token> wraps3 = new Range<Token>(new BigIntegerToken("90"), new BigIntegerToken("0"));
        Range<Token> nowrap1 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("110"));
        Range<Token> nowrap2 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("10"));
        Range<Token> nowrap3 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("9"));

        assertIntersection(wraps1, wraps2, wraps1);
        assertIntersection(wraps3, wraps2, wraps3);

        assertIntersection(wraps1, nowrap1, nowrap1);
        assertIntersection(wraps1, nowrap2, nowrap2);
        assertIntersection(nowrap2, nowrap3, nowrap3);

        assertIntersection(wraps1, wraps1, wraps1);
        assertIntersection(nowrap1, nowrap1, nowrap1);
        assertIntersection(nowrap2, nowrap2, nowrap2);
        assertIntersection(wraps3, wraps3, wraps3);
    }

    @Test
    public void testNoIntersection()
    {
        Range<Token> wraps1 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("10"));
        Range<Token> wraps2 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("0"));
        Range<Token> nowrap1 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("100"));
        Range<Token> nowrap2 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("200"));
        Range<Token> nowrap3 = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("100"));

        assertNoIntersection(wraps1, nowrap3);
        assertNoIntersection(wraps2, nowrap1);
        assertNoIntersection(nowrap1, nowrap2);
    }

    @Test
    public void testIntersectionOneWraps()
    {
        Range<Token> wraps1 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("10"));
        Range<Token> wraps2 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("0"));
        Range<Token> nowrap1 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("200"));
        Range<Token> nowrap2 = new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("100"));

        assertIntersection(wraps1,
                           nowrap1,
                           new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("10")),
                           new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("200")));
        assertIntersection(wraps2,
                           nowrap1,
                           new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("200")));
        assertIntersection(wraps1,
                           nowrap2,
                           new Range<Token>(new BigIntegerToken("0"), new BigIntegerToken("10")));
    }

    @Test
    public void testIntersectionTwoWraps()
    {
        Range<Token> wraps1 = new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("20"));
        Range<Token> wraps2 = new Range<Token>(new BigIntegerToken("120"), new BigIntegerToken("90"));
        Range<Token> wraps3 = new Range<Token>(new BigIntegerToken("120"), new BigIntegerToken("110"));
        Range<Token> wraps4 = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("0"));
        Range<Token> wraps5 = new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("1"));
        Range<Token> wraps6 = new Range<Token>(new BigIntegerToken("30"), new BigIntegerToken("10"));

        assertIntersection(wraps1,
                           wraps2,
                           new Range<Token>(new BigIntegerToken("120"), new BigIntegerToken("20")));
        assertIntersection(wraps1,
                           wraps3,
                           new Range<Token>(new BigIntegerToken("120"), new BigIntegerToken("20")),
                           new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("110")));
        assertIntersection(wraps1,
                           wraps4,
                           new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("20")),
                           new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("0")));
        assertIntersection(wraps1,
                           wraps5,
                           new Range<Token>(new BigIntegerToken("10"), new BigIntegerToken("20")),
                           new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("1")));
        assertIntersection(wraps1,
                           wraps6,
                           new Range<Token>(new BigIntegerToken("100"), new BigIntegerToken("10")));
    }

    @Test
    public void testByteTokensCompare()
    {
        Token t1 = new BytesToken(ByteBuffer.wrap(new byte[] { 1,2,3 }));
        Token t2 = new BytesToken(ByteBuffer.wrap(new byte[] { 1,2,3 }));
        Token t3 = new BytesToken(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));

        assert t1.compareTo(t2) == 0;
        assert t1.compareTo(t3) < 0;
        assert t3.compareTo(t1) > 0;
        assert t1.compareTo(t1) == 0;

        Token t4 = new BytesToken(new byte[] { 1,2,3 });
        Token t5 = new BytesToken(new byte[] { 4,5,6,7 });

        assert t4.compareTo(t5) < 0;
        assert t5.compareTo(t4) > 0;
        assert t1.compareTo(t4) == 0;
    }

    private Range<Token> makeRange(String token1, String token2)
    {
        return new Range<Token>(new BigIntegerToken(token1), new BigIntegerToken(token2));
    }

    private Set<Range<Token>> makeRanges(String[][] tokenPairs)
    {
        Set<Range<Token>> ranges = new HashSet<Range<Token>>();
        for (int i = 0; i < tokenPairs.length; ++i)
            ranges.add(makeRange(tokenPairs[i][0], tokenPairs[i][1]));
        return ranges;
    }

    private void checkDifference(Range<Token> oldRange, String[][] newTokens, String[][] expected)
    {
        Set<Range<Token>> ranges = makeRanges(newTokens);
        for (Range<Token> newRange : ranges)
        {
            Set<Range<Token>> diff = oldRange.differenceToFetch(newRange);
            assert diff.equals(makeRanges(expected)) : "\n" +
                                                       "Old range: " + oldRange.toString() + "\n" +
                                                       "New range: " + newRange.toString() + "\n" +
                                                       "Difference: (result) " + diff.toString() + " != " + makeRanges(expected) + " (expected)";
        }
    }

    @Test
    public void testDifferenceToFetchNoWrap()
    {
        Range<Token> oldRange = makeRange("10", "40");

        // New range is entirely contained
        String[][] newTokens1 = { { "20", "30" }, { "10", "20" }, { "10", "40" }, { "20", "40" } };
        String[][] expected1 = { };
        checkDifference(oldRange, newTokens1, expected1);

        // Right half of the new range is needed
        String[][] newTokens2 = { { "10", "50" }, { "20", "50" }, { "40", "50" } };
        String[][] expected2 = { { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        // Left half of the new range is needed
        String[][] newTokens3 = { { "0", "10" }, { "0", "20" }, { "0", "40" } };
        String[][] expected3 = { { "0", "10" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Parts on both ends of the new range are needed
        String[][] newTokens4 = { { "0", "50" } };
        String[][] expected4 = { { "0", "10" }, { "40", "50" } };
        checkDifference(oldRange, newTokens4, expected4);
    }

    @Test
    public void testDifferenceToFetchBothWrap()
    {
        Range<Token> oldRange = makeRange("1010", "40");

        // New range is entirely contained
        String[][] newTokens1 = { { "1020", "30" }, { "1010", "20" }, { "1010", "40" }, { "1020", "40" } };
        String[][] expected1 = { };
        checkDifference(oldRange, newTokens1, expected1);

        // Right half of the new range is needed
        String[][] newTokens2 = { { "1010", "50" }, { "1020", "50" }, { "1040", "50" } };
        String[][] expected2 = { { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        // Left half of the new range is needed
        String[][] newTokens3 = { { "1000", "10" }, { "1000", "20" }, { "1000", "40" } };
        String[][] expected3 = { { "1000", "1010" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Parts on both ends of the new range are needed
        String[][] newTokens4 = { { "1000", "50" } };
        String[][] expected4 = { { "1000", "1010" }, { "40", "50" } };
        checkDifference(oldRange, newTokens4, expected4);
    }

    @Test
    public void testDifferenceToFetchOldWraps()
    {
        Range<Token> oldRange = makeRange("1010", "40");

        // New range is entirely contained
        String[][] newTokens1 = { { "0", "30" }, { "0", "40" }, { "10", "40" } };
        String[][] expected1 = { };
        checkDifference(oldRange, newTokens1, expected1);

        // Right half of the new range is needed
        String[][] newTokens2 = { { "0", "50" }, { "10", "50" }, { "40", "50" } };
        String[][] expected2 = { { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        // Whole range is needed
        String[][] newTokens3 = { { "50", "90" } };
        String[][] expected3 = { { "50", "90" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Both ends of the new range overlaps the old range
        String[][] newTokens4 = { { "10", "1010" }, { "40", "1010" }, { "10", "1030" }, { "40", "1030" } };
        String[][] expected4 = { { "40", "1010" } };
        checkDifference(oldRange, newTokens4, expected4);

        // Only RHS of the new range overlaps the old range
        String[][] newTokens5 = { { "60", "1010" }, { "60", "1030" } };
        String[][] expected5 = { { "60", "1010" } };
        checkDifference(oldRange, newTokens5, expected5);
    }

    @Test
    public void testDifferenceToFetchNewWraps()
    {
        Range<Token> oldRange = makeRange("0", "40");

        // Only the LHS of the new range is needed
        String[][] newTokens1 = { { "1010", "0" }, { "1010", "10" }, { "1010", "40" } };
        String[][] expected1 = { { "1010", "0" } };
        checkDifference(oldRange, newTokens1, expected1);

        // Both ends of the new range are needed
        String[][] newTokens2 = { { "1010", "50" } };
        String[][] expected2 = { { "1010", "0" }, { "40", "50" } };
        checkDifference(oldRange, newTokens2, expected2);

        oldRange = makeRange("20", "40");

        // Whole new range is needed
        String[][] newTokens3 = { { "1010", "0" } };
        String[][] expected3 = { { "1010", "0" } };
        checkDifference(oldRange, newTokens3, expected3);

        // Whole new range is needed (matching endpoints)
        String[][] newTokens4 = { { "1010", "20" } };
        String[][] expected4 = { { "1010", "20" } };
        checkDifference(oldRange, newTokens4, expected4);

        // Only RHS of new range is needed
        String[][] newTokens5 = { { "30", "0" }, { "40", "0" } };
        String[][] expected5 = { { "40", "0" } };
        checkDifference(oldRange, newTokens5, expected5);

        // Only RHS of new range is needed (matching endpoints)
        String[][] newTokens6 = { { "30", "20" }, { "40", "20" } };
        String[][] expected6 = { { "40", "20" } };
        checkDifference(oldRange, newTokens6, expected6);
    }

    private <T extends RingPosition<T>> void assertNormalize(List<Range<T>> input, List<Range<T>> expected)
    {
        List<Range<T>> result = Range.normalize(input);
        assert result.equals(expected) : "Expecting " + expected + " but got " + result;
    }

    @Test
    public void testNormalizeNoop()
    {
        List<Range<RowPosition>> l;

        l = asList(range("1", "3"), range("4", "5"));
        assertNormalize(l, l);
    }

    @Test
    public void testNormalizeSimpleOverlap()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("1", "4"), range("3", "5"));
        expected = asList(range("1", "5"));
        assertNormalize(input, expected);

        input = asList(range("1", "4"), range("1", "4"));
        expected = asList(range("1", "4"));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeSort()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("4", "5"), range("1", "3"));
        expected = asList(range("1", "3"), range("4", "5"));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeUnwrap()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("9", "2"));
        expected = asList(range("", "2"), range("9", ""));
        assertNormalize(input, expected);
    }

    @Test
    public void testNormalizeComplex()
    {
        List<Range<RowPosition>> input, expected;

        input = asList(range("8", "2"), range("7", "9"), range("4", "5"));
        expected = asList(range("", "2"), range("4", "5"), range("7", ""));
        assertNormalize(input, expected);

        input = asList(range("5", "9"), range("2", "5"));
        expected = asList(range("2", "9"));
        assertNormalize(input, expected);

        input = asList(range ("", "1"), range("9", "2"), range("4", "5"), range("", ""));
        expected = asList(range("", ""));
        assertNormalize(input, expected);

        input = asList(range ("", "1"), range("1", "4"), range("4", "5"), range("5", ""));
        expected = asList(range("", ""));
        assertNormalize(input, expected);
    }
}
