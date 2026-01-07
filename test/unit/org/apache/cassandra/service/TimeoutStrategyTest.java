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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TimeoutStrategyTest {

    @Test
    public void testParseLatencyModifierExponential() {
        long expectedBaseLatencyMicros = TimeUnit.MILLISECONDS.toMicros(30);
        String spec = "30ms * 2^attempts";
        TimeoutStrategy.Wait w = TimeoutStrategy.parseWait(spec, TimeoutStrategy.LatencySourceFactory.none());

        // Attempt 1: baseLatency * 2^(1-1) = baseLatency * 1
        Assertions.assertThat(w.getMicros(1))
                .isEqualTo(expectedBaseLatencyMicros);

        // Attempt 2: baseLatency * 2^(2-1) = baseLatency * 2
        Assertions.assertThat(w.getMicros(2))
                .isEqualTo(expectedBaseLatencyMicros * 2);

        // Attempt 3: baseLatency * 2^(3-1) = baseLatency * 4
        Assertions.assertThat(w.getMicros(3))
                .isEqualTo(expectedBaseLatencyMicros * 4);

        // Edge case to check for 0 or negative attempts: max(0, -1) = 0
        Assertions.assertThat(w.getMicros(0))
                .isEqualTo(expectedBaseLatencyMicros);

        Assertions.assertThat(w.getMicros(Integer.MIN_VALUE))
                .isEqualTo(expectedBaseLatencyMicros);
    }

    @Test
    public void testParseLatencyModifierFractionalBaseExponential() {
        long expectedBaseLatencyMicros = TimeUnit.MILLISECONDS.toMicros(30);
        String spec = "30ms * 1.5^attempts";
        TimeoutStrategy.Wait w = TimeoutStrategy.parseWait(spec, TimeoutStrategy.LatencySourceFactory.none());

        // Attempt 1: baseLatency * 1.5^(1-1) = baseLatency * 1
        Assertions.assertThat(w.getMicros(1))
                .isEqualTo((int) expectedBaseLatencyMicros);

        // Attempt 2: baseLatency * 1.5^(2-1) = baseLatency * 1.5
        Assertions.assertThat(w.getMicros(2))
                .isEqualTo((int) (expectedBaseLatencyMicros * 1.5));

        // Attempt 3: baseLatency * 1.5^(3-1) = baseLatency * 2.25
        Assertions.assertThat(w.getMicros(3))
                .isEqualTo((int) (expectedBaseLatencyMicros * 2.25));
    }
}
