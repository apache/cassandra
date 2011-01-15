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
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import org.junit.Test;

public class RangeTest
{
    @Test
    public void testContains()
    {
        Range left = new Range(new BigIntegerToken("0"), new BigIntegerToken("100"));
        assert !left.contains(new BigIntegerToken("0"));
        assert left.contains(new BigIntegerToken("10"));
        assert left.contains(new BigIntegerToken("100"));
        assert !left.contains(new BigIntegerToken("101"));
    }

    @Test
    public void testContainsWrapping()
    {
        Range range = new Range(new BigIntegerToken("0"), new BigIntegerToken("0"));
        assert range.contains(new BigIntegerToken("0"));
        assert range.contains(new BigIntegerToken("10"));
        assert range.contains(new BigIntegerToken("100"));
        assert range.contains(new BigIntegerToken("101"));

        range = new Range(new BigIntegerToken("100"), new BigIntegerToken("0"));
        assert range.contains(new BigIntegerToken("0"));
        assert !range.contains(new BigIntegerToken("1"));
        assert !range.contains(new BigIntegerToken("100"));
        assert range.contains(new BigIntegerToken("200"));
    }

    @Test
    public void testContainsRange()
    {
        Range one = new Range(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range two = new Range(new BigIntegerToken("2"), new BigIntegerToken("5"));
        Range thr = new Range(new BigIntegerToken("5"), new BigIntegerToken("10"));
        Range fou = new Range(new BigIntegerToken("10"), new BigIntegerToken("12"));

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
        Range one = new Range(new BigIntegerToken("10"), new BigIntegerToken("2"));
        Range two = new Range(new BigIntegerToken("5"), new BigIntegerToken("3"));
        Range thr = new Range(new BigIntegerToken("10"), new BigIntegerToken("12"));
        Range fou = new Range(new BigIntegerToken("2"), new BigIntegerToken("6"));
        Range fiv = new Range(new BigIntegerToken("0"), new BigIntegerToken("0"));

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
        Range wrap1 = new Range(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range wrap2 = new Range(new BigIntegerToken("10"), new BigIntegerToken("2"));

        Range nowrap1 = new Range(new BigIntegerToken("0"), new BigIntegerToken("2"));
        Range nowrap2 = new Range(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range nowrap3 = new Range(new BigIntegerToken("10"), new BigIntegerToken("100"));

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
        Range all = new Range(new BigIntegerToken("0"), new BigIntegerToken("0")); // technically, this is a wrapping range
        Range one = new Range(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range two = new Range(new BigIntegerToken("0"), new BigIntegerToken("8"));
        Range not = new Range(new BigIntegerToken("10"), new BigIntegerToken("12"));

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
        Range onewrap = new Range(new BigIntegerToken("10"), new BigIntegerToken("2"));
        Range onecomplement = new Range(onewrap.right, onewrap.left);
        Range onestartswith = new Range(onewrap.left, new BigIntegerToken("12"));
        Range oneendswith = new Range(new BigIntegerToken("1"), onewrap.right);
        Range twowrap = new Range(new BigIntegerToken("5"), new BigIntegerToken("3"));
        Range not = new Range(new BigIntegerToken("2"), new BigIntegerToken("6"));

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

    static void assertIntersection(Range one, Range two, Range ... ranges)
    {
        Set<Range> correct = Range.rangeSet(ranges);
        Set<Range> result1 = one.intersectionWith(two);
        assert result1.equals(correct) : String.format("%s != %s",
                                                       StringUtils.join(result1, ","),
                                                       StringUtils.join(correct, ","));
        Set<Range> result2 = two.intersectionWith(one);
        assert result2.equals(correct) : String.format("%s != %s",
                                                       StringUtils.join(result2, ","),
                                                       StringUtils.join(correct, ","));
    }

    private void assertNoIntersection(Range wraps1, Range nowrap3)
    {
        assertIntersection(wraps1, nowrap3);
    }

    @Test
    public void testIntersectionWithAll()
    {
        Range all0 = new Range(new BigIntegerToken("0"), new BigIntegerToken("0"));
        Range all10 = new Range(new BigIntegerToken("10"), new BigIntegerToken("10"));
        Range all100 = new Range(new BigIntegerToken("100"), new BigIntegerToken("100"));
        Range all1000 = new Range(new BigIntegerToken("1000"), new BigIntegerToken("1000"));
        Range wraps = new Range(new BigIntegerToken("100"), new BigIntegerToken("10"));

        assertIntersection(all0, wraps, wraps);
        assertIntersection(all10, wraps, wraps);
        assertIntersection(all100, wraps, wraps);
        assertIntersection(all1000, wraps, wraps);
    }

    @Test
    public void testIntersectionContains()
    {
        Range wraps1 = new Range(new BigIntegerToken("100"), new BigIntegerToken("10"));
        Range wraps2 = new Range(new BigIntegerToken("90"), new BigIntegerToken("20"));
        Range wraps3 = new Range(new BigIntegerToken("90"), new BigIntegerToken("0"));
        Range nowrap1 = new Range(new BigIntegerToken("100"), new BigIntegerToken("110"));
        Range nowrap2 = new Range(new BigIntegerToken("0"), new BigIntegerToken("10"));
        Range nowrap3 = new Range(new BigIntegerToken("0"), new BigIntegerToken("9"));

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
        Range wraps1 = new Range(new BigIntegerToken("100"), new BigIntegerToken("10"));
        Range wraps2 = new Range(new BigIntegerToken("100"), new BigIntegerToken("0"));
        Range nowrap1 = new Range(new BigIntegerToken("0"), new BigIntegerToken("100"));
        Range nowrap2 = new Range(new BigIntegerToken("100"), new BigIntegerToken("200"));
        Range nowrap3 = new Range(new BigIntegerToken("10"), new BigIntegerToken("100"));

        assertNoIntersection(wraps1, nowrap3);
        assertNoIntersection(wraps2, nowrap1);
        assertNoIntersection(nowrap1, nowrap2);
    }

    @Test
    public void testIntersectionOneWraps()
    {
        Range wraps1 = new Range(new BigIntegerToken("100"), new BigIntegerToken("10"));
        Range wraps2 = new Range(new BigIntegerToken("100"), new BigIntegerToken("0"));
        Range nowrap1 = new Range(new BigIntegerToken("0"), new BigIntegerToken("200"));
        Range nowrap2 = new Range(new BigIntegerToken("0"), new BigIntegerToken("100"));

        assertIntersection(wraps1,
                           nowrap1,
                           new Range(new BigIntegerToken("0"), new BigIntegerToken("10")),
                           new Range(new BigIntegerToken("100"), new BigIntegerToken("200")));
        assertIntersection(wraps2,
                           nowrap1,
                           new Range(new BigIntegerToken("100"), new BigIntegerToken("200")));
        assertIntersection(wraps1,
                           nowrap2,
                           new Range(new BigIntegerToken("0"), new BigIntegerToken("10")));
    }

    @Test
    public void testIntersectionTwoWraps()
    {
        Range wraps1 = new Range(new BigIntegerToken("100"), new BigIntegerToken("20"));
        Range wraps2 = new Range(new BigIntegerToken("120"), new BigIntegerToken("90"));
        Range wraps3 = new Range(new BigIntegerToken("120"), new BigIntegerToken("110"));
        Range wraps4 = new Range(new BigIntegerToken("10"), new BigIntegerToken("0"));
        Range wraps5 = new Range(new BigIntegerToken("10"), new BigIntegerToken("1"));
        Range wraps6 = new Range(new BigIntegerToken("30"), new BigIntegerToken("10"));

        assertIntersection(wraps1,
                           wraps2,
                           new Range(new BigIntegerToken("120"), new BigIntegerToken("20")));
        assertIntersection(wraps1,
                           wraps3,
                           new Range(new BigIntegerToken("120"), new BigIntegerToken("20")),
                           new Range(new BigIntegerToken("100"), new BigIntegerToken("110")));
        assertIntersection(wraps1,
                           wraps4,
                           new Range(new BigIntegerToken("10"), new BigIntegerToken("20")),
                           new Range(new BigIntegerToken("100"), new BigIntegerToken("0")));
        assertIntersection(wraps1,
                           wraps5,
                           new Range(new BigIntegerToken("10"), new BigIntegerToken("20")),
                           new Range(new BigIntegerToken("100"), new BigIntegerToken("1")));
        assertIntersection(wraps1,
                           wraps6,
                           new Range(new BigIntegerToken("100"), new BigIntegerToken("10")));
    }

    @Test
    public void testByteTokensCompare()
    {
        Token t1 = new BytesToken(ByteBuffer.wrap(new byte[] { 1,2,3 }));
        Token t2 = new BytesToken(ByteBuffer.wrap(new byte[] { 1,2,3 }));
        Token t3 = new BytesToken(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));

        assert Range.compare(t1, t2) == 0;
        assert Range.compare(t1, t3) == -1;
        assert Range.compare(t3, t1) == 1;
        assert Range.compare(t1, t1) == 0;

        Token t4 = new BytesToken(new byte[] { 1,2,3 });
        Token t5 = new BytesToken(new byte[] { 4,5,6,7 });

        assert Range.compare(t4, t5) == -1;
        assert Range.compare(t5, t4) == 1;
        assert Range.compare(t1, t4) == 0;
    }
}
