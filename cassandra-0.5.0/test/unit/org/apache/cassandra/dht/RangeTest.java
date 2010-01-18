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

import org.junit.Test;

public class RangeTest {
    @Test
    public void testContains() {
        Range left = new Range(new BigIntegerToken("0"), new BigIntegerToken("100"));
        assert !left.contains(new BigIntegerToken("0"));
        assert left.contains(new BigIntegerToken("10"));
        assert left.contains(new BigIntegerToken("100"));
        assert !left.contains(new BigIntegerToken("101"));
    }

    @Test
    public void testContainsWrapping() {
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
    public void testContainsRange() {
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
    public void testContainsRangeWrapping() {
        Range one = new Range(new BigIntegerToken("10"), new BigIntegerToken("2"));
        Range two = new Range(new BigIntegerToken("5"), new BigIntegerToken("3"));
        Range thr = new Range(new BigIntegerToken("10"), new BigIntegerToken("12"));
        Range fou = new Range(new BigIntegerToken("2"), new BigIntegerToken("6"));

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
    }

    @Test
    public void testIntersects() {
        Range one = new Range(new BigIntegerToken("2"), new BigIntegerToken("10"));
        Range two = new Range(new BigIntegerToken("0"), new BigIntegerToken("8"));
        Range not = new Range(new BigIntegerToken("10"), new BigIntegerToken("12"));

        assert one.intersects(two);
        assert two.intersects(one);

        assert !one.intersects(not);
        assert !not.intersects(one);

        assert !two.intersects(not);
        assert !not.intersects(two);
    }

    @Test
    public void testIntersectsWrapping() {
        Range onewrap = new Range(new BigIntegerToken("10"), new BigIntegerToken("2"));
        Range twowrap = new Range(new BigIntegerToken("5"), new BigIntegerToken("3"));
        Range not = new Range(new BigIntegerToken("2"), new BigIntegerToken("6"));

        assert onewrap.intersects(twowrap);
        assert twowrap.intersects(onewrap);

        assert !onewrap.intersects(not);
        assert !not.intersects(onewrap);

        assert twowrap.intersects(not);
        assert not.intersects(twowrap);
    }
}
