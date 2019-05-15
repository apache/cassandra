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

package org.apache.cassandra.db.rows;

import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.LivenessInfo;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;

public class EncodingStatsTest
{
    @Test
    public void testCollectWithNoStats()
    {
        EncodingStats none = EncodingStats.merge(ImmutableList.of(
            EncodingStats.NO_STATS,
            EncodingStats.NO_STATS,
            EncodingStats.NO_STATS
        ), Function.identity());
        Assert.assertEquals(none, EncodingStats.NO_STATS);
    }

    @Test
    public void testCollectWithNoStatsWithEmpty()
    {
        EncodingStats none = EncodingStats.merge(ImmutableList.of(
            EncodingStats.NO_STATS,
            EncodingStats.NO_STATS,
            new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 0)
        ), Function.identity());
        Assert.assertEquals(none, EncodingStats.NO_STATS);
    }

    @Test
    public void testCollectWithNoStatsWithTimestamp()
    {
        EncodingStats single = new EncodingStats(1, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
            EncodingStats.NO_STATS,
            EncodingStats.NO_STATS,
            single,
            EncodingStats.NO_STATS
        ), Function.identity());
        Assert.assertEquals(single, result);
    }

    @Test
    public void testCollectWithNoStatsWithExpires()
    {
        EncodingStats single = new EncodingStats(LivenessInfo.NO_TIMESTAMP, 1, 0);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
        EncodingStats.NO_STATS,
        single,
        EncodingStats.NO_STATS
        ), Function.identity());
        Assert.assertEquals(single, result);
    }

    @Test
    public void testCollectWithNoStatsWithTTL()
    {
        EncodingStats single = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 1);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
            EncodingStats.NO_STATS,
            single,
            EncodingStats.NO_STATS
        ), Function.identity());
        Assert.assertEquals(single, result);
    }

    @Test
    public void testCollectOneEach()
    {
        EncodingStats tsp = new EncodingStats(1, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats exp = new EncodingStats(LivenessInfo.NO_TIMESTAMP, 1, 0);
        EncodingStats ttl = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME, 1);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
            tsp,
            exp,
            ttl
        ), Function.identity());
        Assert.assertEquals(new EncodingStats(1, 1, 1), result);
    }

    @Test
    public void testTimestamp()
    {
        EncodingStats one = new EncodingStats(1, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats two = new EncodingStats(2, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats thr = new EncodingStats(3, LivenessInfo.NO_EXPIRATION_TIME, 0);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
            one,
            two,
            thr
        ), Function.identity());
        Assert.assertEquals(one, result);
    }

    @Test
    public void testExpires()
    {
        EncodingStats one = new EncodingStats(LivenessInfo.NO_TIMESTAMP,1, 0);
        EncodingStats two = new EncodingStats(LivenessInfo.NO_TIMESTAMP,2, 0);
        EncodingStats thr = new EncodingStats(LivenessInfo.NO_TIMESTAMP,3, 0);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
            one,
            two,
            thr
        ), Function.identity());
        Assert.assertEquals(one, result);
    }

    @Test
    public void testTTL()
    {
        EncodingStats one = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME,1);
        EncodingStats two = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME,2);
        EncodingStats thr = new EncodingStats(LivenessInfo.NO_TIMESTAMP, LivenessInfo.NO_EXPIRATION_TIME,3);
        EncodingStats result = EncodingStats.merge(ImmutableList.of(
            thr,
            one,
            two
        ), Function.identity());
        Assert.assertEquals(one, result);
    }

    @Test
    public void testEncodingStatsCollectWithNone()
    {
        qt().forAll(longs().between(Long.MIN_VALUE+1, Long.MAX_VALUE),
                    integers().between(0, Integer.MAX_VALUE-1),
                    integers().allPositive())
            .asWithPrecursor(EncodingStats::new)
            .check((timestamp, expires, ttl, stats) ->
                   {
                       EncodingStats result = EncodingStats.merge(ImmutableList.of(
                           EncodingStats.NO_STATS,
                           stats,
                           EncodingStats.NO_STATS
                       ), Function.identity());
                       return result.minTTL == ttl
                              && result.minLocalDeletionTime == expires
                              && result.minTimestamp == timestamp;
                   });
    }

}
