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

package org.apache.cassandra.metrics;

import org.junit.Assert;
import org.junit.Test;

import org.agrona.concurrent.NoOpLock;

public class HitRateTest
{
    static long M = 1024;
    @Test
    public void testRollingCounters()
    {
        ShardedHitRate.HitRateShard shard = new ShardedHitRate.HitRateShard(new NoOpLock());
        ShardedHitRate.Snapshot snapshot = new ShardedHitRate.Snapshot();

        long now = M/2;
        shard.markHitExclusive(now);
        shard.markHitExclusive(now);
        shard.markHitExclusive(now += M);
        shard.markHitExclusive(now);
        shard.markHitExclusive(now += M);
        shard.markHitExclusive(now);
        shard.markHitExclusive(now += M);
        shard.markHitExclusive(now);
        shard.markHitExclusive(now += M);
        shard.markHitExclusive(now);
        shard.updateSnapshot(snapshot, now);
        Assert.assertEquals(10, snapshot.totalHits);
        Assert.assertEquals(3, snapshot.requests(1));
        Assert.assertEquals(9, snapshot.requests(4));
        Assert.assertEquals(10, snapshot.requests(5));
        Assert.assertEquals(10, snapshot.requests(6));
        snapshot.reset();
        shard.updateSnapshot(snapshot, now + M/2);
        Assert.assertEquals(10, snapshot.totalHits);
        Assert.assertEquals(2, snapshot.requests(1));
        Assert.assertEquals(8, snapshot.requests(4));
        Assert.assertEquals(10, snapshot.requests(5));
        Assert.assertEquals(10, snapshot.requests(6));
    }
}
