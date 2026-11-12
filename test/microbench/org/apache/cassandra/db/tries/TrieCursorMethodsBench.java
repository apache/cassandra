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

package org.apache.cassandra.db.tries;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class TrieCursorMethodsBench
{
    long[] prepared = new long[4 << 10];
    long[] preparedForSkip = new long[4 << 10];

    int index = 0;
    int mask = prepared.length - 1;

    public TrieCursorMethodsBench()
    {
        Random r = ThreadLocalRandom.current();
        for (int i = 0; i < prepared.length; ++i)
        {
            prepared[i] = Cursor.encode(r.nextInt(10000), r.nextInt(100), Direction.fromBoolean(r.nextBoolean()));
            preparedForSkip[i] = Cursor.positionForSkippingBranch(prepared[i]);
        }
    }


    @Benchmark
    public void testIncomingTransition(final Blackhole bh)
    {
        bh.consume(Cursor.incomingTransition(prepared[index++ & mask]));
    }

    @Benchmark
    public void testDepth(final Blackhole bh)
    {
        bh.consume(Cursor.depth(prepared[index++ & mask]));
    }

    @Benchmark
    public void testNoop(final Blackhole bh)
    {
        bh.consume(prepared[index++ & mask]);
    }

    @Benchmark
    public void testDirection(final Blackhole bh)
    {
        bh.consume(Cursor.direction(prepared[index++ & mask]));
    }

    @Benchmark
    public void testIsExhausted(final Blackhole bh)
    {
        bh.consume(Cursor.isExhausted(prepared[index++ & mask]));
    }

    @Benchmark
    public void testExhaustedPosition(final Blackhole bh)
    {
        bh.consume(Cursor.exhaustedPosition(prepared[index++ & mask]));
    }

    @Benchmark
    public void testAscended(final Blackhole bh)
    {
        int i = index++ & mask;
        bh.consume(Cursor.ascended(prepared[i], preparedForSkip[i]));
    }

    @Benchmark
    public void testCompare(final Blackhole bh)
    {
        int i = index++ & mask;
        bh.consume(Cursor.compare(prepared[i], preparedForSkip[i]));
    }
}
