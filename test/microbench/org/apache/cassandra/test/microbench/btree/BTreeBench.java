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

package org.apache.cassandra.test.microbench.btree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;

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
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class BTreeBench extends Megamorphism
{
    final AtomicInteger uniqueThreadInitialisation = new AtomicInteger();

    Integer[] data;
    // three iterables to simulate megamorphic callsites for iterable
    List<Integer> dataAsIterable1;
    List<Integer> dataAsIterable2;
    List<Integer> dataAsIterable3;

//     produces sizes between [n/2..n+n/2]
    @Param({"1", "4", "16", "64", "256", "1024", "16384"})
    int dataSize;

    @Setup(Level.Trial)
    public void setup()
    {
        setup(dataSize);
    }

    void setup(int size)
    {
        data = new Integer[size + size/2];
        for (int i = 0 ; i < data.length; i++)
            data[i] = i;
        dataAsIterable1 = Arrays.asList(data);
        dataAsIterable2 = ImmutableList.copyOf(data);
        dataAsIterable3 = new ArrayList<>(dataSize);
        dataAsIterable3.addAll(dataAsIterable1);
    }

    @State(Scope.Thread)
    public static class BuildSizeState extends IntVisitor
    {
        @Setup(Level.Trial)
        public void setup(BTreeBench bench)
        {
            super.setup(bench.dataSize);
        }
    }
}
