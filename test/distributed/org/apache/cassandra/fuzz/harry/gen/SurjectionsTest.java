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

package org.apache.cassandra.fuzz.harry.gen;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Surjections;
import org.apache.cassandra.harry.gen.rng.PcgRSUFast;

public class SurjectionsTest
{
    private static int RUNS = 1000000;

    @Test
    public void weightedTest()
    {
        int[] weights = new int[] {50, 40, 10};

        Surjections.Surjection<String> gen = Surjections.weighted(Surjections.weights(weights),
                                                                  "a", "b", "c");

        Map<String, Integer> frequencies = new HashMap<>();
        EntropySource rng = new PcgRSUFast(System.currentTimeMillis(), 0);

        for (int i = 0; i < RUNS; i++)
        {
            String s = gen.inflate(rng.next());
            frequencies.compute(s, (s1, i1) -> {
                if (i1 == null)
                    return 1;
                else
                    return i1 + 1;
            });
        }

        Assert.assertEquals(frequencies.get("a") / 10000, weights[0], 1);
        Assert.assertEquals(frequencies.get("b") / 10000, weights[1], 1);
        Assert.assertEquals(frequencies.get("c") / 10000, weights[2], 1);
    }


}
