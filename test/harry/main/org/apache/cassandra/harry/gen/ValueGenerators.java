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

public class ValueGenerators
{
    protected final Bijections.Bijection<Object[]> pkGen;
    protected final Bijections.Bijection<Object[]> ckGen;

    protected final List<Bijections.Bijection<Object>> regularColumnGens;
    protected final List<Bijections.Bijection<Object>> staticColumnGens;

    protected final List<Comparator<Object>> pkComparators;
    protected final List<Comparator<Object>> ckComparators;
    protected final List<Comparator<Object>> regularComparators;
    protected final List<Comparator<Object>> staticComparators;

    public ValueGenerators(Bijections.Bijection<Object[]> pkGen,
                           Bijections.Bijection<Object[]> ckGen,
                           List<Bijections.Bijection<Object>> regularColumnGens,
                           List<Bijections.Bijection<Object>> staticColumnGens,

                           List<Comparator<Object>> pkComparators,
                           List<Comparator<Object>> ckComparators,
                           List<Comparator<Object>> regularComparators,
                           List<Comparator<Object>> staticComparators)
    {
        this.pkGen = pkGen;
        this.ckGen = ckGen;
        this.regularColumnGens = regularColumnGens;
        this.staticColumnGens = staticColumnGens;
        this.pkComparators = pkComparators;
        this.ckComparators = ckComparators;
        this.regularComparators = regularComparators;
        this.staticComparators = staticComparators;
    }

    public Bijections.Bijection<Object[]> pkGen()
    {
        return pkGen;
    }

    public Bijections.Bijection<Object[]> ckGen()
    {
        return ckGen;
    }

    public Bijections.Bijection<Object> regularColumnGen(int idx)
    {
        return regularColumnGens.get(idx);
    }

    public Bijections.Bijection<Object> staticColumnGen(int idx)
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
}