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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

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

import org.apache.cassandra.metrics.Meter;
import org.apache.cassandra.metrics.ThreadLocalMeter;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 4, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2,
jvmArgsAppend = { "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4)
@State(Scope.Benchmark)
public class MetersBench
{
    @Param({ "ThreadLocal", "Dropwizard"})
    private String type;

    @Param({"10"})
    private int metricsCount;

    @Param({"true"})
    private boolean polluteCpuCaches;

    private List<Meter> meters;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        meters = new ArrayList<>(metricsCount);
        for (int i = 0; i < metricsCount; i++)
        {
            Meter meter;
            switch (type)
            {
                case "ThreadLocal":
                    meter = new ThreadLocalMeter();
                    break;
                case "Dropwizard":
                    meter = new DropwizardMeter();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            meters.add(meter);
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
    public void mark() {
        for (Meter meter : meters)
            meter.mark();
    }

    private static class DropwizardMeter implements Meter
    {
        private final com.codahale.metrics.Meter meter = new com.codahale.metrics.Meter();

        @Override
        public void mark()
        {
            meter.mark();
        }

        @Override
        public void mark(long n)
        {
            meter.mark(n);
        }

        @Override
        public long getCount()
        {
            return meter.getCount();
        }

        @Override
        public double getFifteenMinuteRate()
        {
            return meter.getFifteenMinuteRate();
        }

        @Override
        public double getFiveMinuteRate()
        {
            return meter.getFiveMinuteRate();
        }

        @Override
        public double getMeanRate()
        {
            return meter.getMeanRate();
        }

        @Override
        public double getOneMinuteRate()
        {
            return meter.getOneMinuteRate();
        }
    }
}
