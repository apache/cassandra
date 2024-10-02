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

package org.apache.cassandra.test.microbench;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;
import org.apache.cassandra.utils.MonotonicClock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(4)
@Fork(value = 3)
@State(Scope.Benchmark)
public class DecayingEstimatedHistogramBench
{
    @Param({ "100000", "500000", "1000000" })
    private long landmarkResetIntervalNs;

    DecayingEstimatedHistogramReservoir reservoir;

    @Setup
    public void setup()
    {
        reservoir = new DecayingEstimatedHistogramReservoir(DEFAULT_ZERO_CONSIDERATION,
                                                            DEFAULT_BUCKET_COUNT,
                                                            DEFAULT_STRIPE_COUNT,
                                                            MonotonicClock.Global.approxTime,
                                                            landmarkResetIntervalNs);
    }

    @State(Scope.Thread)
    public static class HistogramUpdateState
    {
        int update;

        @Setup(Level.Invocation)
        public void setup() throws Throwable
        {
            update = ThreadLocalRandom.current().nextInt(10, 1000);
        }
    }

    @Benchmark
    public void update(HistogramUpdateState state)
    {
        reservoir.update(state.update);
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                              .include(DecayingEstimatedHistogramBench.class.getSimpleName())
                              .build();
        new Runner(options).run();
    }
}
