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
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.cassandra.metrics.ThreadLocalMetrics;

import io.netty.util.concurrent.FastThreadLocal;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 30, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2,
      jvmArgsAppend = { "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4)
@State(Scope.Benchmark)
public class CassandraThreadLocalBench
{
    private static final FastThreadLocal<ThreadLocalMetrics> threadLocalMetricsCurrent = new FastThreadLocal<>()
    {

        @Override
        protected ThreadLocalMetrics initialValue()
        {
            return ThreadLocalMetrics.create();
        }
    };

    @Benchmark
    public void netty()
    {
        threadLocalMetricsCurrent.get();
    }

    @Benchmark
    public void cassandra()
    {
        ThreadLocalMetrics.get();
    }

}
