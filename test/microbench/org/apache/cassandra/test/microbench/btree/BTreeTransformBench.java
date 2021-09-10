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

import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.cassandra.utils.BulkIterator;
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
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
// TODO: parameterise build method for input to transform
public class BTreeTransformBench extends BTreeBench
{
    public enum Distribution { CONTIGUOUS, RANDOM }

    Integer[] data2;

    @Param({"false"})
    boolean uniquePerTrial;

    @Param({"0", "0.0001", "0.001", "0.01", "0.0625", "0.125", "0.25", "0.5", "1"})
    float ratio;

    @Param({"CONTIGUOUS", "RANDOM"})
    Distribution distribution;

    @Setup(Level.Trial)
    public void setup()
    {
        setup(2 * dataSize);
        data2 = data.clone();
        for (int i = 0 ; i < data2.length ; ++i)
            data2[i] = Integer.valueOf(data2[i]);
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        final Random random = new Random(0); // initialised to a seed below

        final BitSet bitSet = new BitSet();

        Integer[] data, data2;
        boolean uniquePerTrial;
        float ratio;
        Distribution distribution;

        // unique trials
        // instead of doing per-invocation, we do per-iteration, as perfasm measures trial setup costs
        Object[][] updates;
        BuildSizeState buildSizeState = new BuildSizeState();

        // current trial
        Object[] update;

        @Setup(Level.Trial)
        public void doTrialSetup(BTreeTransformBench bench, BuildSizeState invocationBuildSizeState)
        {
            this.random.setSeed(bench.uniqueThreadInitialisation.incrementAndGet());
            this.data = bench.data;
            this.data2 = bench.data2;
            this.uniquePerTrial = bench.uniquePerTrial;
            this.ratio = bench.ratio;
            this.distribution = bench.distribution;
            if (!uniquePerTrial)
            {
                buildSizeState.setup(bench);
                buildSizeState.randomise(random);
                int numberOfUniqueTrials = (int) Math.min(4096, Runtime.getRuntime().maxMemory() / (4 * 8 * bench.dataSize));
                updates = new Object[numberOfUniqueTrials][];
                for (int i = 0; i < numberOfUniqueTrials; ++i)
                    updates[i] = createTree(buildSizeState);
            }
            invocationBuildSizeState.randomise(random);
        }

        @Setup(Level.Invocation)
        public void doInvocationSetup(BuildSizeState buildSizeState)
        {
            if (!uniquePerTrial)
            {
                update = updates[buildSizeState.i() % updates.length];
                buildSizeState.next();
            }
            else
            {
                this.update = createTree(buildSizeState);
            }
            int size = BTree.size(update);
            int setBits = (int) Math.ceil(size * (ratio > 0.5f ? 1 - ratio : ratio));
            switch (distribution)
            {
                case CONTIGUOUS: setContiguousBits(setBits, size); break;
                case RANDOM: setRandomBits(setBits, size); break;
            }
        }

        private Object[] createTree(BuildSizeState buildSizeState)
        {
            int buildSize = buildSizeState.next();
            try (BulkIterator.FromArray<Integer> iter = BulkIterator.of(data))
            {
                return BTree.build(iter, buildSize, UpdateFunction.noOp());
            }
        }

        private void setRandomBits(int count, int range)
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            bitSet.clear();
            while (count > 0)
            {
                int next = random.nextInt(range);
                if (bitSet.get(next))
                    continue;
                bitSet.set(next);
                --count;
            }
        }

        private void setContiguousBits(int count, int range)
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            bitSet.clear();
            int start = count >= range ? 0 : random.nextInt(range - count);
            bitSet.set(start, start + count);
        }

        Function<Integer, Integer> apply(Function<Integer, Integer> replace)
        {
            return ratio > 0.5f ? i -> bitSet.get(i) ? i : replace.apply(i)
                                : i -> bitSet.get(i) ? replace.apply(i) : i;

        }
    }

    @Benchmark
    public Object[] transformReplace(ThreadState state)
    {
        return BTree.transform(state.update, state.apply(i -> data2[i]));
    }

    @Benchmark
    public Object[] transformAndFilterReplace(ThreadState state)
    {
        return BTree.transformAndFilter(state.update, state.apply(i -> data2[i]));
    }

    @Benchmark
    public Object[] transformAndFilterRemove(ThreadState state)
    {
        return BTree.transformAndFilter(state.update, state.apply(i -> null));
    }
}