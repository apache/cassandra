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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.collect.Iterables;

import org.apache.cassandra.stress.generate.values.Generator;

public class PartitionGenerator
{

    public static enum Order
    {
        ARBITRARY, SHUFFLED, SORTED
    }

    public final double maxRowCount;
    public final double minRowCount;
    final List<Generator> partitionKey;
    final List<Generator> clusteringComponents;
    final List<Generator> valueComponents;
    final int[] clusteringDescendantAverages;
    final int[] clusteringComponentAverages;

    private final Map<String, Integer> indexMap;
    final Order order;

    public PartitionGenerator(List<Generator> partitionKey, List<Generator> clusteringComponents, List<Generator> valueComponents, Order order)
    {
        this.partitionKey = partitionKey;
        this.clusteringComponents = clusteringComponents;
        this.valueComponents = valueComponents;
        this.order = order;
        this.clusteringDescendantAverages = new int[clusteringComponents.size()];
        this.clusteringComponentAverages = new int[clusteringComponents.size()];
        for (int i = 0 ; i < clusteringComponentAverages.length ; i++)
            clusteringComponentAverages[i] = (int) clusteringComponents.get(i).clusteringDistribution.average();
        for (int i = clusteringDescendantAverages.length - 1 ; i >= 0 ; i--)
            clusteringDescendantAverages[i] = (int) (i < (clusteringDescendantAverages.length - 1) ? clusteringComponentAverages[i + 1] * clusteringDescendantAverages[i + 1] : 1);
        double maxRowCount = 1d;
        double minRowCount = 1d;
        for (Generator component : clusteringComponents)
        {
            maxRowCount *= component.clusteringDistribution.maxValue();
            minRowCount *= component.clusteringDistribution.minValue();
        }
        this.maxRowCount = maxRowCount;
        this.minRowCount = minRowCount;
        this.indexMap = new HashMap<>();
        int i = 0;
        for (Generator generator : partitionKey)
            indexMap.put(generator.name, --i);
        i = 0;
        for (Generator generator : Iterables.concat(clusteringComponents, valueComponents))
            indexMap.put(generator.name, i++);
    }

    public boolean permitNulls(int index)
    {
        return !(index < 0 || index < clusteringComponents.size());
    }

    public int indexOf(String name)
    {
        Integer i = indexMap.get(name);
        if (i == null)
            throw new NoSuchElementException();
        return i;
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
