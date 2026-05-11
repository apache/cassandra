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

package org.apache.cassandra.index.sai.disk.v1.vector;

import java.util.function.IntConsumer;

import io.github.jbellis.jvector.graph.NeighborQueue;
import io.github.jbellis.jvector.graph.NeighborSimilarity;

/**
 * A specialized data structure that stores segment row id to ordinal pairs efficiently. Implemented as an array of int
 * pairs that avoids boxing.
 */
public class SegmentRowIdOrdinalPairs
{
    private final int capacity;
    private int size;
    private final int[] array;

    /**
     * Create a new SegmentRowIdOrdinalPairs with the given capacity.
     * @param capacity the capacity
     */
    public SegmentRowIdOrdinalPairs(int capacity)
    {
        assert capacity < Integer.MAX_VALUE / 2 : "capacity is too large " + capacity;
        this.capacity = capacity;
        this.size = 0;
        this.array = new int[capacity * 2];
    }

    /**
     * Add a pair to the array.
     * @param segmentRowId the first value
     * @param ordinal the second value
     */
    public void add(int segmentRowId, int ordinal)
    {
        if (size == capacity)
            throw new ArrayIndexOutOfBoundsException(size);
        array[size * 2] = segmentRowId;
        array[size * 2 + 1] = ordinal;
        size++;
    }

    /**
     * Get the row id at the given index.
     * @param index the index
     * @return the row id
     */
    public int getSegmentRowId(int index)
    {
        if ( index < 0 || index >= size)
            throw new ArrayIndexOutOfBoundsException(index);
        return array[index * 2];
    }

    /**
     * Get the ordinal at the given index.
     * @param index the index
     * @return the ordinal
     */
    public int getOrdinal(int index)
    {
        if ( index < 0 || index >= size)
            throw new ArrayIndexOutOfBoundsException(index);
        return array[index * 2 + 1];
    }

    /**
     * The number of pairs in the array.
     * @return the number of pairs in the array
     */
    public int size()
    {
        return size;
    }

    /**
     * Create an iterator over the segment row id and scored ordinal pairs in the array.
     * @param scoreFunction the score function to use to compute the next score based on the ordinal
     * @return a {@link NeighborQueue}
     */
    public NeighborQueue mapToSegmentRowIdScoreHeap(NeighborSimilarity.ScoreFunction scoreFunction)
    {
        // TODO this could be improved using Floyd's algorithm in a later jvector version
        NeighborQueue queue = new NeighborQueue(size(), true);
        for (int i = 0; i < size; i++)
            queue.add(array[i * 2], scoreFunction.similarityTo(array[i * 2 + 1])); // rowid, score
        return queue;
    }

    /**
     * Create an iterator over the index and scored ordinal pairs in the array.
     * @param scoreFunction the score function to use to compute the next score based on the ordinal
     */
    public NeighborQueue mapToIndexScoreIterator(NeighborSimilarity.ScoreFunction scoreFunction)
    {
        // TODO this could be improved using Floyd's algorithm in a later jvector version
        NeighborQueue queue = new NeighborQueue(size(), true);
        for (int i = 0; i < size; i++)
            queue.add(i, scoreFunction.similarityTo(array[i * 2 + 1])); // index, score
        return queue;
    }

    /**
     * Calls the consumer for each right value in each pair of the array.
     * @param consumer the consumer to call for each right value
     */
    public void forEachOrdinal(IntConsumer consumer)
    {
        for (int i = 0; i < size; i++)
            consumer.accept(array[i * 2 + 1]);
    }
}
