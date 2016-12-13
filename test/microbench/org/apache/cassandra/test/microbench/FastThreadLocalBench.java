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

import io.netty.util.concurrent.FastThreadLocal;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = {"-Xmx512M", "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4) // make sure this matches the number of _physical_cores_
@State(Scope.Benchmark)
public class FastThreadLocalBench
{
    @Param({"2", "4", "8", "12"})
    private int variables = 2;

    static final int max = 20;
    static final ThreadLocal[] threadLocals = new ThreadLocal[max];
    static final FastThreadLocal[] fastThreadLocals = new FastThreadLocal[max];
    static
    {
        for (int i = 0; i < max; i++)
        {
            threadLocals[i] = ThreadLocal.withInitial(Object::new);
            fastThreadLocals[i] = new FastThreadLocal() {
                protected Object initialValue() throws Exception
                {
                    return new Object();
                }
            };
        }
    }

    @State(Scope.Thread)
    public static class FastThreadLocalBenchState
    {
        public int index;
    }

    @Benchmark
    public void baseline(FastThreadLocalBenchState state, Blackhole bh)
    {
        if (variables != 2)
            throw new IllegalArgumentException("skipped");

        bh.consume("foo");
    }

    @Benchmark
    public void threadLocal(FastThreadLocalBenchState state, Blackhole bh)
    {
        bh.consume(threadLocals[state.index % max].get());
    }

    @Benchmark
    public void fastThreadLocal(FastThreadLocalBenchState state, Blackhole bh)
    {
        bh.consume(fastThreadLocals[state.index % max].get());
    }
}
