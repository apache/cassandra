package org.apache.cassandra.dht;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class RangeIntersectionTest
{
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
}
