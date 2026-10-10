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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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

import org.apache.cassandra.concurrent.CassandraThread;
import org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir;

import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_BUCKET_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_STRIPE_COUNT;
import static org.apache.cassandra.metrics.DecayingEstimatedHistogramReservoir.DEFAULT_ZERO_CONSIDERATION;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(4)
@Fork(value = 3, jvmArgsAppend = { "-Djmh.executor=CUSTOM",
                                   "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor" })
@State(Scope.Benchmark)
public class DecayingEstimatedHistogramBench
{
    /** Updates replayed per invocation; large enough that the JMH cost per update is negligible. */
    private static final int BATCH = 1024;


    /**
     * Upper bound of the updated values. Values are spread over the log-linear buckets between 1 and this bound,
     * so it controls how many distinct buckets a thread touches
     */
    @Param({ "10", "500", "100000" })
    private int maxValue;

    /**
     * How many histograms a thread rotates through. One buffer serves all of a thread's histograms, so together
     * with {@link #maxValue} this sets how many distinct (histogram, bucket) pairs a flush has to aggregate, and
     * therefore how much of the batch it can coalesce.
     */
    @Param({ "1", "16" })
    private int histogramCount;

    private DecayingEstimatedHistogramReservoir[] reservoirs;

    @Setup(Level.Trial)
    public void setup()
    {
        reservoirs = new DecayingEstimatedHistogramReservoir[histogramCount];
        for (int i = 0; i < histogramCount; i++)
            reservoirs[i] = new DecayingEstimatedHistogramReservoir(DEFAULT_ZERO_CONSIDERATION, DEFAULT_BUCKET_COUNT, DEFAULT_STRIPE_COUNT);
    }

    @State(Scope.Thread)
    public static class Values
    {
        long[] values;

        @Setup(Level.Trial)
        public void setup(DecayingEstimatedHistogramBench bench)
        {
            if (!(Thread.currentThread() instanceof CassandraThread))
                throw new IllegalStateException("benchmark must run on a CassandraThread, got "
                                                + Thread.currentThread().getClass().getName());

            values = new long[BATCH];
            for (int i = 0; i < values.length; i++)
                values[i] = ThreadLocalRandom.current().nextInt(1, bench.maxValue);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BATCH)
    public void update(Values state)
    {
        long[] values = state.values;
        DecayingEstimatedHistogramReservoir[] reservoirs = this.reservoirs;
        int count = reservoirs.length;
        int reservoir = 0;
        for (int i = 0; i < values.length; i++)
        {
            reservoirs[reservoir].update(values[i]);
            if (++reservoir == count)
                reservoir = 0;
        }
    }

    public static void main(String[] args) throws RunnerException
    {
        Options options = new OptionsBuilder()
                          .include(DecayingEstimatedHistogramBench.class.getSimpleName())
                          .build();
        new Runner(options).run();
    }
}
