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

public class ValueGenerators<PartitionKey, ClusteringKey>
{
    protected final Bijection<PartitionKey> pkGen;
    protected final Bijection<ClusteringKey> ckGen;

    protected final Accessor<ClusteringKey> ckAccessor;

    protected final List<? extends Bijection<? extends Object>> regularColumnGens;
    protected final List<? extends Bijection<? extends Object>> staticColumnGens;

    protected final List<Comparator<Object>> pkComparators;
    protected final List<Comparator<Object>> ckComparators;
    protected final List<Comparator<Object>> regularComparators;
    protected final List<Comparator<Object>> staticComparators;

    public ValueGenerators(Bijection<PartitionKey> pkGen,
                           Bijection<ClusteringKey> ckGen,
                           Accessor<ClusteringKey> ckAccessor,

                           List<? extends Bijection<? extends Object>> regularColumnGens,
                           List<? extends Bijection<? extends Object>> staticColumnGens,

                           List<Comparator<Object>> pkComparators,
                           List<Comparator<Object>> ckComparators,
                           List<Comparator<Object>> regularComparators,
                           List<Comparator<Object>> staticComparators)
    {
        this.pkGen = pkGen;
        this.ckGen = ckGen;
        this.ckAccessor = ckAccessor;
        this.regularColumnGens = regularColumnGens;
        this.staticColumnGens = staticColumnGens;
        this.pkComparators = pkComparators;
        this.ckComparators = ckComparators;
        this.regularComparators = regularComparators;
        this.staticComparators = staticComparators;
    }

    public Bijection<PartitionKey> pkGen()
    {
        return pkGen;
    }

    public Bijection<ClusteringKey> ckGen()
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

    public Comparator<Object> pkComparator(int idx)
    {
        return pkComparators.get(idx);
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

    public Accessor<ClusteringKey> ckAccessor()
    {
        return ckAccessor;
    }

    public int pkPopulation()
    {
        return pkGen.population();
    }

    public int ckPopulation()
    {
        return ckGen.population();
    }

    public int regularPopulation(int i)
    {
        return regularColumnGens.get(i).population();
    }

    public int staticPopulation(int i)
    {
        return staticColumnGens.get(i).population();
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