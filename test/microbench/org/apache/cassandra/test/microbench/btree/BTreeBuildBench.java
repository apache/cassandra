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

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
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

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 6, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 8, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class BTreeBuildBench extends BTreeBench
{
    @Benchmark
    public Object[] buildWithIterableStaticBuildMethod(BuildSizeState state)
    {
        List<Integer> list;
        int size = state.next();
        switch (state.i() % 3)
        {
            case 0: list = dataAsIterable1; break;
            case 1: list = dataAsIterable2; break;
            case 2: list = dataAsIterable3; break;
            default: throw new IllegalStateException();
        }
        return BTree.build(BulkIterator.of(list.iterator()), size, UpdateFunction.noOp());
    }

    @Benchmark
    public Object[] buildWithMegamorphicBulkStaticBuildNoop(BuildSizeState state)
    {
        return buildWithMegamorphicBulkStaticBuild(state, UpdateFunction.noOp());
    }

    public Object[] buildWithMegamorphicBulkStaticBuild(BuildSizeState state, UpdateFunction<Integer, Integer> updateF)
    {
        BulkIterator<Integer> iter;
        int size = state.next();
        switch (state.i() % 3)
        {
            case 0: iter = BulkIterator.of(data); break;
            case 1: iter = FromArrayCopy.of(data); break;
            case 2: iter = FromArrayCopy2.of(data); break;
            default: throw new IllegalStateException();
        }
        Object[] result = BTree.build(iter, size, updateF);
        iter.close();
        return result;
    }

    @Benchmark
    public Object[] buildWithBulkStaticBuild(BuildSizeState state)
    {
        int size = state.next();
        try (BulkIterator<Integer> iter = BulkIterator.of(data))
        {
            return BTree.build(iter, size, UpdateFunction.noOp());
        }
    }

    @Benchmark
    public Object[] buildWithBuilderAuto(BuildSizeState state)
    {
        int size = state.next();
        BTree.Builder<Integer> builder = BTree.builder(Comparator.naturalOrder());
        for (int i = 0 ; i < size ; ++i)
            builder.add(data[i]);
        return builder.build();
    }

    @Benchmark
    public Object[] buildWithBuilderManual(BuildSizeState state)
    {
        int size = state.next();
        BTree.Builder<Integer> builder = BTree.builder(Comparator.naturalOrder());
        builder.auto(false);
        for (int i = 0 ; i < size ; ++i)
            builder.add(data[i]);
        return builder.build();
    }

    @Benchmark
    public Object[] buildWithFastBuilder(BuildSizeState state)
    {
        int size = state.next();
        try (BTree.FastBuilder<Integer> builder = BTree.fastBuilder())
        {
            for (int i = 0 ; i < size ; ++i)
                builder.add(data[i]);
            return builder.build();
        }
    }
}