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

import org.apache.cassandra.metrics.CounterMetric;
import org.apache.cassandra.metrics.LazySetArrayThreadLocalMetrics;
import org.apache.cassandra.metrics.LongAdderCounter;
import org.apache.cassandra.metrics.PiggybackArrayThreadLocalMetrics;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2,
      jvmArgsAppend = { "-Djmh.executor=CUSTOM", "-Djmh.executor.class=org.apache.cassandra.test.microbench.FastThreadExecutor"})
@Threads(4)
@State(Scope.Benchmark)
public class ThreadLocalMetricsBench
{
    @Param({"LongAdder", "LazySetArray", "PiggybackArray"})
    private String type;

    private CounterMetric counterMetric;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        switch (type)
        {
            case "LongAdder"       : counterMetric = new LongAdderCounter(); break;
            case "LazySetArray"    : counterMetric = LazySetArrayThreadLocalMetrics.createCounter(); break;
            case "PiggybackArray"  : counterMetric = PiggybackArrayThreadLocalMetrics.createCounter(); break;
        }
    }

    @Benchmark
    public void increment() {
        counterMetric.inc();
    }
}
