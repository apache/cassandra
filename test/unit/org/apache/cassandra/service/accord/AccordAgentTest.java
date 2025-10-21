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
            long oneSecond = SECONDS.toMicros(1);
            long targetDelta = oneSecond / nodes.size();
            for (int i = 0 ; i < 10000 ; ++i)
            {
                long startTime = rnd.nextLong(1, TimeUnit.DAYS.toMicros(100L));
                for (int j = 0 ; j < startTimes.length ; ++j)
                {
                    long nonClashingStartTime = AccordAgent.nonClashingStartTime(startTime, nodes, nodes.get(j), oneSecond, rnd);
                    assertTrue(nonClashingStartTime >= startTime);
                    startTimes[j] = nonClashingStartTime;
                }

                Arrays.sort(startTimes);
                for (int j = 1 ; j < startTimes.length ; ++j)
                {
                    long actualDelta = startTimes[j] - startTimes[j - 1];
                    assertTrue(Math.abs(targetDelta - actualDelta) <= startTimes.length);
                }
            }
        });
    }

}
