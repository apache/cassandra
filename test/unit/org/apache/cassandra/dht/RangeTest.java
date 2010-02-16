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

import java.util.Arrays;
import java.util.List;

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
}
