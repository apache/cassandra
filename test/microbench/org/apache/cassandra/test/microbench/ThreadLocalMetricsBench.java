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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

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

import org.apache.cassandra.metrics.Counter;
import org.apache.cassandra.metrics.ThreadLocalCounter;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2,
      jvmArgsAppend = { "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4)
@State(Scope.Benchmark)
public class ThreadLocalMetricsBench
{
    @Param({"LongAdder", "ThreadLocalCounter"})
    private String type;

    @Param({"true", "false"})
    private boolean polluteCpuCaches;

    @Param({"50", "100"})
    private int metricsCount;

    private Counter[] counters;


    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        counters = new Counter[metricsCount];
        for (int i = 0; i < metricsCount; i++)
        {
            Counter counter;
            switch (type)
            {
                case "LongAdder":
                    counter = new LongAdderCounter();
                    break;
                case "ThreadLocalCounter":
                    counter = new ThreadLocalCounter();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            counters[i] = counter;
        }
    }

    private final AtomicLongArray anotherMemory = new AtomicLongArray(256 * 1024);

    @Setup(Level.Invocation)
    public void polluteCpuCaches()
    {
        if (polluteCpuCaches)
            for (int i = 0; i < anotherMemory.length(); i++)
                anotherMemory.incrementAndGet(i);
    }

    @Benchmark
    public void increment()
    {
        for (Counter counter : counters)
            counter.inc();
    }

    public static class LongAdderCounter implements Counter
    {
        private final LongAdder counter = new LongAdder();

        @Override
        public void inc()
        {
            counter.increment();
        }

        @Override
        public void inc(long n)
        {
            counter.add(n);
        }

        @Override
        public void dec()
        {
            counter.decrement();
        }

        @Override
        public void dec(long n)
        {
            counter.add(-n);
        }

        @Override
        public long getCount()
        {
            return counter.sum();
        }
    }
}
