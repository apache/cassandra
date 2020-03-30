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

package org.apache.cassandra.service.paxos;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.junit.Test;

import static org.apache.cassandra.service.paxos.PaxosPropose.*;

public class PaxosProposeTest
{
    static class V
    {
        static final AtomicLongFieldUpdater<V> updater = AtomicLongFieldUpdater.newUpdater(V.class, "v");
        volatile long v;
        public boolean valid()
        {
            return v == 0;
        }
    }
    @Test
    public void testShouldSignal()
    {
        int[] signalledAtK = new int[12];
        V[] v = new V[] { new V(), new V(), new V(), new V(), new V(), new V(), new V(), new V(), new V(), new V(), new V(), new V() };
        boolean[] signalled = new boolean[12];
        for (int total = 2 ; total < 16 ; ++total)
        {
            for (int required = (total/2) + 1 ; required < total ; ++required)
            {
                for (int i = 0 ; i < total ; ++i)
                {
                    for (int j = 0 ; j < total - i ; ++j)
                    {
                        Arrays.fill(signalled, false);
                        Arrays.fill(signalledAtK, Integer.MAX_VALUE);
                        for (int x = 0 ; x < v.length ; ++x)
                            v[x].v = 0;

                        for (int k = 0 ; k <= total - (i + j) ; ++k)
                        {
                            signalled[0] = v[0].valid() && shouldSignal(responses(i, j, k), required, total, true, V.updater, v[0]);
                            signalled[1] = v[1].valid() && shouldSignal(responses(j, i, k), required, total, true, V.updater, v[1]);
                            signalled[2] = v[2].valid() && shouldSignal(responses(j, k, i), required, total, true, V.updater, v[2]);
                            signalled[3] = v[3].valid() && shouldSignal(responses(k, i, j), required, total, true, V.updater, v[3]);
                            signalled[4] = v[4].valid() && shouldSignal(responses(i, k, j), required, total, true, V.updater, v[4]);
                            signalled[5] = v[5].valid() && shouldSignal(responses(k, j, i), required, total, true, V.updater, v[5]);
                            signalled[6] = v[6].valid() && shouldSignal(responses(i, j, k), required, total, false, V.updater, v[6]);
                            signalled[7] = v[7].valid() && shouldSignal(responses(j, i, k), required, total, false, V.updater, v[7]);
                            signalled[8] = v[8].valid() && shouldSignal(responses(j, k, i), required, total, false, V.updater, v[8]);
                            signalled[9] = v[9].valid() && shouldSignal(responses(k, i, j), required, total, false, V.updater, v[9]);
                            signalled[10] = v[10].valid() && shouldSignal(responses(i, k, j), required, total, false, V.updater, v[10]);
                            signalled[11] = v[11].valid() && shouldSignal(responses(k, j, i), required, total, false, V.updater, v[11]);
                            for (int x = 0 ; x < 12 ; ++x)
                            {
                                if (signalled[x] && signalledAtK[x] < k)
                                    throw new IllegalStateException(String.format("(%d,%d,%d): (%d,%d,%d,%d)", total, required, x, i, j, k, signalledAtK[x]));
                                else if (signalled[x])
                                    signalledAtK[x] = k;
                            }
                        }

                        for (int x = 0 ; x < 12 ; ++x)
                        {
                            if (signalledAtK[x] == Integer.MAX_VALUE)
                                throw new IllegalStateException(String.format("(%d,%d,%d): (%d, %d)", total, required, x, i, j));
                        }
                    }
                }
            }
        }
    }

    private static long responses(int i, int j, int k)
    {
        return i * ACCEPT_INCREMENT + j * REFUSAL_INCREMENT + k * FAILURE_INCREMENT;
    }

}
