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

package org.apache.cassandra.service.accord;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import accord.local.Node;
import accord.utils.RandomTestRunner;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.service.accord.api.AccordAgent;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccordAgentTest
{
    @Test
    public void testNonClashingStartTimes()
    {
        RandomTestRunner.test().check(rnd -> {
            SortedArrayList<Node.Id> nodes; {
                Node.Id[] ids = new Node.Id[rnd.nextInt(4, 16)];
                for (int i = 0 ; i < ids.length ; ++i)
                    ids[i] = new Node.Id(i);
                nodes = new SortedArrayList<>(ids);
            }

            long[] startTimes = new long[nodes.size()];
            // each node is given an exclusive slice of this length, so the window is timeSlice * rf and consecutive
            // nodes are spaced exactly one slice apart
            long timeSlice = SECONDS.toMicros(1);
            long window = timeSlice * nodes.size();
            for (int i = 0 ; i < 10000 ; ++i)
            {
                long startTime = rnd.nextLong(1, TimeUnit.DAYS.toMicros(100L));
                for (int j = 0 ; j < startTimes.length ; ++j)
                {
                    long nonClashingStartTime = AccordAgent.nonClashingStartTime(startTime, nodes, nodes.get(j), timeSlice, rnd);
                    assertTrue(nonClashingStartTime >= startTime);
                    assertTrue(nonClashingStartTime < startTime + window);
                    startTimes[j] = nonClashingStartTime;
                }

                Arrays.sort(startTimes);
                for (int j = 1 ; j < startTimes.length ; ++j)
                    assertEquals(timeSlice, startTimes[j] - startTimes[j - 1]);
            }
        });
    }

    /**
     * The index overload's contract: callers derive replicaIndex from the same list they take replicaCount from
     * (ShardDurability passes shard.nodes.find(id) with shard.rf(), and Topology.forNode only yields shards that
     * contain the node, so the index is always in range). An out of range index would put the result in the past and
     * the caller's Math.max(1, start - now) would then collapse its backoff to 1us, so it is rejected rather than
     * normalised.
     */
    @Test
    public void testNonClashingStartTimeRejectsOutOfRangeReplica()
    {
        long timeSlice = SECONDS.toMicros(1);
        RandomTestRunner.test().check(rnd -> {
            int replicaCount = rnd.nextInt(1, 16);
            long window = timeSlice * replicaCount;
            for (int i = 0 ; i < 1000 ; ++i)
            {
                long startTime = rnd.nextLong(1, TimeUnit.DAYS.toMicros(100L));
                int replicaIndex = rnd.nextInt(0, replicaCount);
                long nonClashingStartTime = AccordAgent.nonClashingStartTime(startTime, replicaIndex, replicaCount, timeSlice);
                assertTrue("start time " + nonClashingStartTime + " precedes " + startTime + " for index " + replicaIndex,
                           nonClashingStartTime >= startTime);
                assertTrue(nonClashingStartTime < startTime + window);
            }

            long startTime = rnd.nextLong(1, TimeUnit.DAYS.toMicros(100L));
            // SortedList.find returns a negative insertion point for a non-member, and a caller must not pass one on
            assertThatThrownBy(() -> AccordAgent.nonClashingStartTime(startTime, -1, replicaCount, timeSlice))
                .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> AccordAgent.nonClashingStartTime(startTime, replicaCount, replicaCount, timeSlice))
                .isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> AccordAgent.nonClashingStartTime(startTime, 0, 0, timeSlice))
                .isInstanceOf(IllegalStateException.class);
        });
    }
}
