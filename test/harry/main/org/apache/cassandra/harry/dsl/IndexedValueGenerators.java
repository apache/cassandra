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

package org.apache.cassandra.harry.dsl;

import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.harry.gen.IndexGenerators;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;

public abstract class IndexedValueGenerators extends ValueGenerators<Object[], Object[]>
{
    private final Generator<Long> pkIdxGen;

    public IndexedValueGenerators(HistoryBuilder.IndexedBijection<Object[]> pkGen)
    {
        super(pkGen);
        this.pkIdxGen = Generators.int64(0, pkGen().population());
    }

    public IndexedPartitionValues forPdIdx(long pdIdx)
    {
        return forPd(pkGen().descriptorAt(pdIdx));
    }

    @Override
    public HistoryBuilder.IndexedBijection<Object[]> pkGen()
    {
        return (HistoryBuilder.IndexedBijection<Object[]>) super.pkGen();
    }

    @Override
    public abstract IndexedPartitionValues forPd(long pd);

    public Generator<Long> pkIdxGen()
    {
        return pkIdxGen;
    }

    /**
     * Indexed value generators, for which values that can be generated inside each partition are shared
     */
    public static class Shared extends IndexedValueGenerators
    {
        private final IndexedPartitionValues partitionValues;

        public Shared(HistoryBuilder.IndexedBijection<Object[]> pkGen,
                      HistoryBuilder.IndexedBijection<Object[]> ckGen,
                      List<HistoryBuilder.IndexedBijection<Object>> regularColumnGens,
                      List<HistoryBuilder.IndexedBijection<Object>> staticColumnGens,
                      List<Comparator<Object>> ckComparators,
                      List<Comparator<Object>> regularComparators,
                      List<Comparator<Object>> staticComparators)
        {
            super(pkGen);
            this.partitionValues = IndexedPartitionValues.uniform(ckGen,
                                                                  regularColumnGens,
                                                                  staticColumnGens,
                                                                  ckComparators,
                                                                  regularComparators,
                                                                  staticComparators);
        }

        @Override
        public IndexedPartitionValues forPd(long pd)
        {
            return partitionValues;
        }
    }

    public static class IndexedPartitionValues extends PartitionValues<Object[]>
    {
        public final Generator<Integer> ckIdxGen;
        public final Generator<Integer>[] regularIdxGens;
        public final Generator<Integer>[] staticIdxGens;

        /**
         * Index partition value wrapper with random value pickers
         */
        public static IndexedPartitionValues uniform(Bijections.Bijection<Object[]> ckGen,
                                                     List<? extends Bijections.Bijection<?>> regularColumnGens,
                                                     List<? extends Bijections.Bijection<?>> staticColumnGens,
                                                     List<Comparator<Object>> ckComparators,
                                                     List<Comparator<Object>> regularComparators,
                                                     List<Comparator<Object>> staticComparators)
        {
            return new IndexedPartitionValues(ckGen,

                                              regularColumnGens,
                                              staticColumnGens,
                                              ckComparators,
                                              regularComparators,
                                              staticComparators,

                                              IndexGenerators.uniform(ckGen),
                                              IndexGenerators.uniform(regularColumnGens),
                                              IndexGenerators.uniform(regularColumnGens));
        }

        public IndexedPartitionValues(Bijections.Bijection<Object[]> ckGen,

                                      List<? extends Bijections.Bijection<?>> regularColumnGens,
                                      List<? extends Bijections.Bijection<?>> staticColumnGens,
                                      List<Comparator<Object>> ckComparators,
                                      List<Comparator<Object>> regularComparators,
                                      List<Comparator<Object>> staticComparators,

                                      Generator<Integer> ckIdxGen,
                                      Generator<Integer>[] regularIdxGens,
                                      Generator<Integer>[] staticIdxGens)
        {
            super(ckGen, ArrayAccessor.instance, regularColumnGens, staticColumnGens, ckComparators, regularComparators, staticComparators);

            this.ckIdxGen = ckIdxGen;
            this.regularIdxGens = regularIdxGens;
            this.staticIdxGens = staticIdxGens;
        }

        @Override
        public HistoryBuilder.IndexedBijection<Object[]> ckGen()
        {
            return (HistoryBuilder.IndexedBijection<Object[]>) super.ckGen();
        }

        @Override
        public HistoryBuilder.IndexedBijection<Object> regularColumnGen(int idx)
        {
            return (HistoryBuilder.IndexedBijection<Object>) super.regularColumnGen(idx);
        }

        @Override
        public HistoryBuilder.IndexedBijection<Object> staticColumnGen(int idx)
        {
            return (HistoryBuilder.IndexedBijection<Object>) super.staticColumnGen(idx);
        }

        public Generator<Integer> ckIdxGen()
        {
            return ckIdxGen;
        }
        public Generator<Integer>[] regularIdxGens()
        {
            return regularIdxGens;
        }

        public Generator<Integer>[] staticIdxGens()
        {
            return staticIdxGens;
        }
    }
}
