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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
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
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class BTreeBuildBench
{
    private List<Integer> data;

    @Param({"1", "2", "5", "10", "20", "40", "100", "1000", "10000", "100000"})
    int dataSize;

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    @Setup(Level.Trial)
    public void setup()
    {
        data = new ArrayList<>(dataSize);
        for (int i = 0 ; i < dataSize; i++)
            data.add(i);
    }

    private int buildTree(List<Integer> data)
    {
        Object[] btree = BTree.build(data, UpdateFunction.noOp());
        // access the btree to avoid java optimized out this code
        return BTree.size(btree);
    }

    private int treeBuilderAddAll(List<Integer> data)
    {
        BTree.Builder<Integer> builder = BTree.builder(Comparator.naturalOrder());
        Object[] btree = builder.addAll(data).build();
        return BTree.size(btree);
    }

    @Benchmark
    public int treeBuilderRecycleAdd()
    {
        BTree.Builder<Integer> builder = BTree.builder(Comparator.naturalOrder());
        builder.auto(false);
        for (Integer v : data)
            builder.add(v);
        Object[] btree = builder.build();
        return BTree.size(btree);
    }

    @Benchmark
    public int buildTreeTest()
    {
        return buildTree(data);
    }
}
