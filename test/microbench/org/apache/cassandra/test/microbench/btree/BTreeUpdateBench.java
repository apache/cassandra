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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.cassandra.utils.Pair;
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
@Warmup(iterations = 10, time = 1)
@Measurement(iterations = 10, time = 2)
@Fork(value = 4)
@Threads(4)
@State(Scope.Benchmark)
// TODO: parameterise build method for inputs to update
public class BTreeUpdateBench extends BTreeBench
{
    public enum Distribution { RANDOM, CONTIGUOUS }

    Supplier<Comparator<? super Integer>> comparator = Comparator::naturalOrder;

    Integer[] data2;

    @Param({"1", "4", "16", "64", "256", "1024", "16384"})
    int insertSize;

    @Param({"0", "0.5", "1"})
    float overlap;

    @Param({"RANDOM", "CONTIGUOUS"})
    Distribution distribution;

    @Param({"false", "true"})
    boolean keepOld;

    @Param({"SIMPLE", "SIMPLE_MEGAMORPH", "UNSIMPLE"})
    UpdateF updateF;

    @Param({"false"})
    boolean uniquePerTrial;

    @Setup(Level.Trial)
    public void setup()
    {
        setup(2 * (dataSize + insertSize));
        data2 = data.clone();
        for (int i = 0 ; i < data2.length ; ++i)
            data2[i] = Integer.valueOf(data2[i]);
    }

    @State(Scope.Thread)
    public static class InsertSizeState extends IntVisitor
    {
        @Setup(Level.Trial)
        public void setup(BTreeUpdateBench bench)
        {
            super.setup(bench.insertSize);
        }
    }

    @State(Scope.Thread)
    public static class ThreadState
    {
        final Random random = new Random(0); // initialised to a seed below

        Comparator<? super Integer> comparator;
        Integer[] data, data2;
        int[] randomOverlaps, randomBuild;
        Distribution distribution;
        float overlap;
        boolean uniquePerTrial;
        IntFunction<UpdateFunction<Integer, Integer>> updateFGetter;

        BTree.Builder<Integer> buildBuilder = BTree.<Integer>builder(Comparator.naturalOrder()).auto(false);
        BTree.Builder<Integer> insertBuilder = BTree.<Integer>builder(Comparator.naturalOrder()).auto(false);

        // unique trials
        // instead of doing per-invocation, we do per-iteration, as perfasm measures trial setup costs
        Object[][] updates, inserts;

        // current trial
        Object[] update, insert;
        UpdateFunction<Integer, Integer> updateF;

        @Setup(Level.Trial)
        public void doTrialSetup(BTreeUpdateBench bench, BuildSizeState invocationBuildSizeState, InsertSizeState invocationInsertSizeState)
        {
            random.setSeed(bench.uniqueThreadInitialisation.incrementAndGet());
            this.comparator = bench.comparator.get();
            this.data = bench.data;
            this.data2 = bench.data2;
            this.randomOverlaps = IntStream.range(0, data.length).toArray();
            this.randomBuild = randomOverlaps.clone();
            this.overlap = bench.overlap;
            this.distribution = bench.distribution;
            this.updateFGetter = updateFGetter(bench.keepOld, bench.updateF);
            this.uniquePerTrial = bench.uniquePerTrial;
            if (!uniquePerTrial)
            {
                BuildSizeState buildSizeState = new BuildSizeState();
                InsertSizeState insertSizeState = new InsertSizeState();
                buildSizeState.setup(bench);
                insertSizeState.setup(bench);
                buildSizeState.randomise(random);
                insertSizeState.randomise(random);
                int numberOfUniqueTrials = (int) Math.min(2048, Runtime.getRuntime().maxMemory() / (4 * 8 * (bench.insertSize + bench.dataSize)));
                updates = new Object[numberOfUniqueTrials][];
                inserts = new Object[numberOfUniqueTrials][];
                for (int i = 0; i < numberOfUniqueTrials; ++i)
                {
                    Pair<Object[], Object[]> updateAndInsert = createUpdateAndInsert(buildSizeState, insertSizeState);
                    updates[i] = updateAndInsert.left;
                    inserts[i] = updateAndInsert.right;
                }
            }
            invocationBuildSizeState.randomise(random);
            invocationInsertSizeState.randomise(random);
        }

        @Setup(Level.Invocation)
        public void doInvocationSetup(BuildSizeState buildSizeState, InsertSizeState insertSizeState)
        {
            if (!uniquePerTrial)
            {
                update = updates[buildSizeState.i() % updates.length];
                insert = inserts[buildSizeState.i() % inserts.length];
                buildSizeState.next();
            }
            else
            {
                Pair<Object[], Object[]> updateAndInsert = createUpdateAndInsert(buildSizeState, insertSizeState);
                this.update = updateAndInsert.left;
                this.insert = updateAndInsert.right;
            }
            updateF = updateFGetter.apply(buildSizeState.i());
        }

        /**
         * Create an iteration to the benchmark's spec, i.e. two trees with the specified size and overlap
         */
        private Pair<Object[], Object[]> createUpdateAndInsert(BuildSizeState buildSizeState, InsertSizeState insertSizeState)
        {
            int buildSize = buildSizeState.next();
            int insertSize = insertSizeState.next();
            int overlapSize = (int) (Math.min(buildSize, insertSize) * overlap);
            assert overlapSize <= buildSize && overlapSize <= insertSize;

            BTree.Builder<Integer> build = buildBuilder;
            BTree.Builder<Integer> insert = insertBuilder;
            build.reuse();
            insert.reuse();

            switch (distribution)
            {
                case RANDOM:
                {
                    updateOverlap(overlapSize);
                    buildRandom(build, data, buildSize, overlapSize);
                    buildRandom(insert, data2, insertSize, overlapSize);
                    break;
                }
                case CONTIGUOUS:
                {
                    switch (buildSizeState.i() % 4)
                    {
                        case 0:
                        {
                            // left-hand insert overlap
                            int i = 0;
                            for (int j = 0, mj = insertSize-overlapSize ; j < mj ; ++j)
                                insert.add(data2[i++]);
                            for (int j = 0 ; j < overlapSize ; ++j)
                            {
                                build.add(data[i]);
                                insert.add(data2[i++]);
                            }
                            for (int j = 0, mj = buildSize-overlapSize ; j < mj ; ++j)
                                build.add(data[i++]);
                            break;
                        }
                        case 1:
                        {
                            // right-hand insert overlap
                            int i = 0;
                            for (int j = 0, mj = buildSize-overlapSize ; j < mj ; ++j)
                                build.add(data[i++]);
                            for (int j = 0 ; j < overlapSize ; ++j)
                            {
                                build.add(data[i]);
                                insert.add(data2[i++]);
                            }
                            for (int j = 0, mj = insertSize-overlapSize ; j < mj ; ++j)
                                insert.add(data2[i++]);
                            break;
                        }
                        case 2:
                        {
                            // straddle insert overlap
                            int i = 0;
                            for (int j = 0, mj = (insertSize-overlapSize)/2 ; j < mj ; ++j)
                                insert.add(data2[i++]);
                            for (int j = 0, mj = (buildSize-overlapSize)/2 ; j < mj ; ++j)
                                build.add(data[i++]);
                            for (int j = 0 ; j < overlapSize ; ++j)
                            {
                                build.add(data[i]);
                                insert.add(data2[i++]);
                            }
                            for (int j = 0, mj = buildSize - (overlapSize + (buildSize-overlapSize)/2) ; j < mj ; ++j)
                                build.add(data[i++]);
                            for (int j = 0, mj = insertSize - (overlapSize + (insertSize-overlapSize)/2); j < mj ; ++j)
                                insert.add(data2[i++]);
                            break;
                        }
                        case 3:
                        {
                            // straddle update overlap
                            int i = 0;
                            for (int j = 0, mj = (buildSize-overlapSize)/2 ; j < mj ; ++j)
                                build.add(data[i++]);
                            for (int j = 0, mj = (insertSize-overlapSize)/2 ; j < mj ; ++j)
                                insert.add(data2[i++]);
                            for (int j = 0 ; j < overlapSize ; ++j)
                            {
                                build.add(data[i]);
                                insert.add(data2[i++]);
                            }
                            for (int j = 0, mj = insertSize - (overlapSize + (insertSize-overlapSize)/2); j < mj ; ++j)
                                insert.add(data2[i++]);
                            for (int j = 0, mj = buildSize - (overlapSize + (buildSize-overlapSize)/2) ; j < mj ; ++j)
                                build.add(data[i++]);
                        }
                    }
                    break;
                }
            }

            return Pair.create(build.build(), insert.build());
        }

        /**
         * Randomise the elements of {@code data} with occur in the first {@code size} elements, then sort them
         */
        private void shuffleAndSort(int[] data, int size)
        {
            for (int i = 0 ; i < size ; ++i)
            {
                int swap = random.nextInt(data.length);
                int tmp = data[swap];
                data[swap] = data[i];
                data[i] = tmp;
            }
            Arrays.sort(data, 0, size);
        }

        /**
         * Randomise the elements of {@link #randomOverlaps} with occur in the first {@code size} elements, then sort them
         */
        private void updateOverlap(int size)
        {
            shuffleAndSort(randomOverlaps, size);
        }

        private void buildRandom(BTree.Builder<Integer> build, Integer[] data, int buildSize, int overlapSize)
        {
            shuffleAndSort(randomBuild, buildSize + overlapSize);
            // linear merge
            int i = 0, c = 0, j = 0;
            for ( ; c < buildSize && j < overlapSize ; ++c)
            {
                if (randomBuild[i] < randomOverlaps[j]) build.add(data[randomBuild[i++]]);
                else if (randomBuild[i] > randomOverlaps[j]) build.add(data[randomOverlaps[j++]]);
                else { build.add(data[randomBuild[i++]]); j++; }
            }
            while (c++ < buildSize)
                build.add(data[randomBuild[i++]]);
        }
    }

    @Benchmark
    public Object[] benchUpdate(ThreadState state)
    {
        return BTree.update(state.update, state.insert, state.comparator, state.updateF);
    }

}
