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

package org.apache.cassandra.test.microbench.instance;


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

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class ReadSmallPartitionsBench extends ReadBenchBase
{
    String readStatement()
    {
        return "SELECT * from "+table+" where userid=?";
    }

    @Override
    public Object[] writeArguments(long i)
    {
        return new Object[] { i, i, i };
    }

    @Benchmark
    public Object readRandomInside() throws Throwable
    {
        return performRead(readStatement(), () -> new Object[] { (long) rand.nextInt(count) });
    }

    @Benchmark
    public Object readRandomWOutside() throws Throwable
    {
        return performRead(readStatement(), () -> new Object[] { (long) rand.nextInt(count + count / 6) });
    }

    @Benchmark
    public Object readFixed() throws Throwable
    {
        return performRead(readStatement(), () -> new Object[] { 1234567890123L % count });
    }

    @Benchmark
    public Object readOutside() throws Throwable
    {
        return performRead(readStatement(), () -> new Object[] { count + 1234567L });
    }
}
