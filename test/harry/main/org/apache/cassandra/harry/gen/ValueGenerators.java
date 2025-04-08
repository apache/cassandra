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

package org.apache.cassandra.harry.gen;

import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.harry.gen.Bijections.Bijection;

public abstract class ValueGenerators<PartitionKey, ClusteringKey>
{
    protected final Bijection<PartitionKey> pkGen;

    public ValueGenerators(Bijection<PartitionKey> pkGen)
    {
        this.pkGen = pkGen;
    }

    public Bijection<PartitionKey> pkGen()
    {
        return pkGen;
    }

    public abstract PartitionValues<ClusteringKey> forPd(long pd);

    public static class PartitionValues<CK>
    {
        protected final Bijection<CK> ckGen;

        protected final Accessor<CK> ckAccessor;

        protected final List<? extends Bijection<? extends Object>> regularColumnGens;
        protected final List<? extends Bijection<? extends Object>> staticColumnGens;

        protected final List<Comparator<Object>> ckComparators;
        protected final List<Comparator<Object>> regularComparators;
        protected final List<Comparator<Object>> staticComparators;

        public PartitionValues(Bijection<CK> ckGen,
                               Accessor<CK> ckAccessor,

                               List<? extends Bijection<? extends Object>> regularColumnGens,
                               List<? extends Bijection<? extends Object>> staticColumnGens,

                               List<Comparator<Object>> ckComparators,
                               List<Comparator<Object>> regularComparators,
                               List<Comparator<Object>> staticComparators)
        {

            this.ckGen = ckGen;
            this.ckAccessor = ckAccessor;
            this.regularColumnGens = regularColumnGens;
            this.staticColumnGens = staticColumnGens;
            this.ckComparators = ckComparators;
            this.regularComparators = regularComparators;
            this.staticComparators = staticComparators;
        }

        public Bijection<CK> ckGen()
        {
            return ckGen;
        }

        public Bijection regularColumnGen(int idx)
        {
            return regularColumnGens.get(idx);
        }

        public Bijection staticColumnGen(int idx)
        {
            return staticColumnGens.get(idx);
        }

        public int ckColumnCount()
        {
            return ckComparators.size();
        }

        public int regularColumnCount()
        {
            return regularColumnGens.size();
        }

        public int staticColumnCount()
        {
            return staticColumnGens.size();
        }

        public Comparator<Object> ckComparator(int idx)
        {
            return ckComparators.get(idx);
        }

        public Comparator<Object> regularComparator(int idx)
        {
            return regularComparators.get(idx);
        }

        public Comparator<Object> staticComparator(int idx)
        {
            return staticComparators.get(idx);
        }

        public Accessor<CK> ckAccessor()
        {
            return ckAccessor;
        }

        public int ckPopulation()
        {
            return Math.toIntExact(ckGen.population());
        }

        public int regularPopulation(int i)
        {
            return Math.toIntExact(regularColumnGens.get(i).population());
        }

        public int staticPopulation(int i)
        {
            return Math.toIntExact(staticColumnGens.get(i).population());
        }
    }

    public interface Accessor<T>
    {
        Object access(int field, T value);
    }

    public enum ArrayAccessor implements Accessor<Object[]>
    {
        instance;

        @Override
        public Object access(int field, Object[] value)
        {
            return value[field];
        }
    }
}