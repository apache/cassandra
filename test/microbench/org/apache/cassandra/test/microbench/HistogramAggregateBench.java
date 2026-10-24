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

import org.apache.cassandra.metrics.HistogramUpdateBuffers;

/**
 * Measures {@link HistogramUpdateBuffers#aggregate} cost in isolation.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 6, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@Fork(3)
@State(Scope.Thread)
public class HistogramAggregateBench
{
    /** How many reservoirs the batch is spread over; this is what the id directory has to group. */
    @Param({ "1", "16", "64" })
    private int reservoirs;

    /** Distinct buckets each reservoir touches; with the above it sets how many distinct entries a batch holds. */
    @Param({ "8", "40" })
    private int buckets;

    private HistogramUpdateBuffers.Scratch scratch;
    private int count;

    @Setup(Level.Trial)
    public void setup()
    {
        scratch = new HistogramUpdateBuffers.Scratch();
        count = HistogramUpdateBuffers.CAPACITY;

        // round-robin over reservoirs, cycling buckets, which is what a thread updating several histograms in a
        // loop actually leaves in the ring. Every fork aggregates the identical batch.
        for (int i = 0; i < count; i++)
        {
            int id = i % reservoirs;
            int bucket = (i / reservoirs) % buckets;
            scratch.entries[i] = HistogramUpdateBuffers.encode(id, bucket, 0);
        }
    }

    @Benchmark
    public int aggregate()
    {
        return HistogramUpdateBuffers.aggregate(scratch, count);
    }
}
