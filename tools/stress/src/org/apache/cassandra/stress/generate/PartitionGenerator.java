package org.apache.cassandra.stress.generate;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterables;

import org.apache.cassandra.stress.generate.values.Generator;

public class PartitionGenerator
{

    public final double maxRowCount;
    final List<Generator> partitionKey;
    final List<Generator> clusteringComponents;
    final List<Generator> valueComponents;
    final int[] clusteringChildAverages;

    private final Map<String, Integer> indexMap;

    final List<Partition> recyclable = new ArrayList<>();
    int partitionsInUse = 0;

    public void reset()
    {
        partitionsInUse = 0;
    }

    public PartitionGenerator(List<Generator> partitionKey, List<Generator> clusteringComponents, List<Generator> valueComponents)
    {
        this.partitionKey = partitionKey;
        this.clusteringComponents = clusteringComponents;
        this.valueComponents = valueComponents;
        this.clusteringChildAverages = new int[clusteringComponents.size()];
        for (int i = clusteringChildAverages.length - 1 ; i >= 0 ; i--)
            clusteringChildAverages[i] = (int) (i < (clusteringChildAverages.length - 1) ? clusteringComponents.get(i + 1).clusteringDistribution.average() * clusteringChildAverages[i + 1] : 1);
        double maxRowCount = 1d;
        for (Generator component : clusteringComponents)
            maxRowCount *= component.clusteringDistribution.maxValue();
        this.maxRowCount = maxRowCount;
        this.indexMap = new HashMap<>();
        int i = 0;
        for (Generator generator : partitionKey)
            indexMap.put(generator.name, --i);
        i = 0;
        for (Generator generator : Iterables.concat(clusteringComponents, valueComponents))
            indexMap.put(generator.name, i++);
    }

    public int indexOf(String name)
    {
        Integer i = indexMap.get(name);
        if (i == null)
            throw new NoSuchElementException();
        return i;
    }

    public Partition generate(long seed)
    {
        if (recyclable.size() <= partitionsInUse || recyclable.get(partitionsInUse) == null)
            recyclable.add(new Partition(this));

        Partition partition = recyclable.get(partitionsInUse++);
        partition.setSeed(seed);
        return partition;
    }

    public ByteBuffer convert(int c, Object v)
    {
        if (c < 0)
            return partitionKey.get(-1-c).type.decompose(v);
        if (c < clusteringComponents.size())
            return clusteringComponents.get(c).type.decompose(v);
        return valueComponents.get(c - clusteringComponents.size()).type.decompose(v);
    }
}
