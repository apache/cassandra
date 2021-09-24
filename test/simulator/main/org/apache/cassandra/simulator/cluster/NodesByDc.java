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

package org.apache.cassandra.simulator.cluster;

import java.util.Arrays;
import java.util.function.IntConsumer;

import org.apache.cassandra.simulator.RandomSource;

@SuppressWarnings("ManualArrayCopy")
class NodesByDc
{
    final NodeLookup lookup;
    final int[][] dcs;
    final int[] dcSizes;
    int size;

    public NodesByDc(NodeLookup lookup, int[] dcSizes)
    {
        this.lookup = lookup;
        this.dcs = new int[dcSizes.length][];
        for (int i = 0; i < dcs.length; ++i)
        {
            dcs[i] = new int[dcSizes[i]];
            Arrays.fill(dcs[i], Integer.MAX_VALUE);
        }
        this.dcSizes = new int[dcSizes.length];
    }

    boolean contains(int node)
    {
        int[] dc = dcs[lookup.dcOf(node)];
        return Arrays.binarySearch(dc, node) >= 0;
    }

    void add(int node)
    {
        int dcIndex = lookup.dcOf(node);
        int dcSize = dcSizes[dcIndex];
        int[] dc = dcs[dcIndex];
        int insertPos = -1 - Arrays.binarySearch(dc, node);
        assert insertPos >= 0;
        for (int i = dcSize; i > insertPos; --i)
            dc[i] = dc[i - 1];
        dc[insertPos] = node;
        ++size;
        ++dcSizes[dcIndex];
    }

    int removeRandom(RandomSource random, int dcIndex)
    {
        return removeIndex(dcIndex, random.uniform(0, dcSizes[dcIndex]));
    }

    private int removeIndex(int dcIndex, int nodeIndex)
    {
        int dcSize = dcSizes[dcIndex];
        int[] dc = dcs[dcIndex];
        int node = dc[nodeIndex];
        for (int i = nodeIndex + 1; i < dcSize; ++i)
            dc[i - 1] = dc[i];
        dc[dcSize - 1] = Integer.MAX_VALUE;
        --size;
        --dcSizes[dcIndex];
        return node;
    }

    int removeRandom(RandomSource random)
    {
        int index = random.uniform(0, size);
        for (int dcIndex = 0; dcIndex < dcSizes.length; ++dcIndex)
        {
            if (dcSizes[dcIndex] > index)
                return removeIndex(dcIndex, index);
            index -= dcSizes[dcIndex];
        }
        throw new IllegalStateException();
    }

    int selectRandom(RandomSource random, int dcIndex)
    {
        int i = random.uniform(0, dcSizes[dcIndex]);
        return dcs[dcIndex][i];
    }

    int selectRandom(RandomSource random)
    {
        int index = random.uniform(0, size);
        for (int dcIndex = 0; dcIndex < dcSizes.length; ++dcIndex)
        {
            if (dcSizes[dcIndex] > index)
                return dcs[dcIndex][index];
            index -= dcSizes[dcIndex];
        }
        throw new IllegalStateException();
    }

    void remove(int node)
    {
        int dcIndex = lookup.dcOf(node);
        int[] dc = dcs[dcIndex];
        removeIndex(dcIndex, Arrays.binarySearch(dc, node));
    }

    void forEach(IntConsumer consumer)
    {
        for (int dc = 0; dc < dcs.length; ++dc)
            forEach(consumer, dc);
    }

    void forEach(IntConsumer consumer, int dc)
    {
        for (int i : dcs[dc])
        {
            if (i == Integer.MAX_VALUE)
                break;
            consumer.accept(dcs[dc][i]);
        }
    }

    int[] toArray()
    {
        int[] result = new int[size];
        int count = 0;
        for (int dcIndex = 0; dcIndex < dcs.length; ++dcIndex)
        {
            int dcSize = dcSizes[dcIndex];
            System.arraycopy(dcs[dcIndex], 0, result, count, dcSize);
            count += dcSize;
        }
        return result;
    }

    int[] toArray(int dcIndex)
    {
        int size = dcSizes[dcIndex];
        int[] result = new int[size];
        System.arraycopy(dcs[dcIndex], 0, result, 0, size);
        return result;
    }

    boolean isEmpty()
    {
        return size == 0;
    }

    int size()
    {
        return size;
    }

    int size(int dc)
    {
        return dcSizes[dc];
    }
}
