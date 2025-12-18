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

package org.apache.cassandra.service;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.junit.Test;

import accord.utils.Gen;
import accord.utils.RandomTestRunner;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.RetrySpec;
import org.apache.cassandra.repair.SharedContext;
import org.assertj.core.api.Assertions;

public class RetryStrategyTest
{
    @Test
    public void fuzzParser()
    {
        Gen<IntFunction<String>> expressionGen = random -> i -> {
            switch (i)
            {
                case 0: return String.format("p%d * %d", random.nextInt(100), random.nextInt(0, Integer.MAX_VALUE));
                case 1: return String.format("p%d * %d * attempts", random.nextInt(100), random.nextInt(0, Integer.MAX_VALUE));
                case 2: return String.format("p%d * attempts", random.nextInt(100));
                case 3: return String.format("p%d * %d ^ attempts", random.nextInt(100), random.nextInt(0, Integer.MAX_VALUE));
                case 4: return String.format("%dms", random.nextInt(0, Integer.MAX_VALUE));
                default:
                    throw new IllegalArgumentException(Integer.toString(i));
            }
        };

        RandomTestRunner.test().check(rs -> {
            IntFunction<String> expression = expressionGen.next(rs);
            for (int i = 0; i < 10_000; i++)
            {
                RetryStrategy.parse(String.format("%dms <= %s", rs.nextInt(0, Integer.MAX_VALUE), expression.apply(rs.nextInt(5))),
                                    new TestLatencySourceFactory());
                RetryStrategy.parse(String.format("%dms <= %s <= %dms",
                                                  rs.nextInt(0, Integer.MAX_VALUE),
                                                  expression.apply(rs.nextInt(4)),
                                                  rs.nextInt(0, Integer.MAX_VALUE)),
                                    new TestLatencySourceFactory());
                RetryStrategy.parse(String.format("%dms <= %s ... %s <= %dms",
                                                  rs.nextInt(0, Integer.MAX_VALUE),
                                                  expression.apply(rs.nextInt(4)),
                                                  expression.apply(rs.nextInt(4)),
                                                  rs.nextInt(0, Integer.MAX_VALUE)),
                                    new TestLatencySourceFactory());
            }
        });
    }

    @Test
    public void testSeededWaitRandomizer()
    {
        RetrySpec spec = new RetrySpec(new RetrySpec.MaxAttempt(10),
                                       new DurationSpec.LongMillisecondsBound("200ms"),
                                       new DurationSpec.LongMillisecondsBound("1000ms"));
        long wait1 = RetrySpec.toStrategy(sharedContext(100), spec).computeWait(1, TimeUnit.MILLISECONDS);
        long wait2 = RetrySpec.toStrategy(sharedContext(100), spec).computeWait(1, TimeUnit.MILLISECONDS);
        long wait3 = RetrySpec.toStrategy(sharedContext(200), spec).computeWait(1, TimeUnit.MILLISECONDS);
        Assertions.assertThat(wait1).isEqualTo(wait2);
        Assertions.assertThat(wait1).isNotEqualTo(wait3);
    }

    private static SharedContext sharedContext(long seed)
    {
        return new SharedContext.ForwardingSharedContext(SharedContext.Global.instance)
        {
            private final Random seededRandom = new Random(seed);

            @Override
            public Supplier<Random> random()
            {
                return () -> seededRandom;
            }
        };
    }

    private static class TestLatencySourceFactory implements TimeoutStrategy.LatencySourceFactory
    {

        @Override
        public TimeoutStrategy.LatencySource source(String params)
        {
            return percentile -> 10;
        }
    }
}