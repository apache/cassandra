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
package org.apache.cassandra.db.lifecycle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class SSTableIntervalTree extends IntervalTree<PartitionPosition, SSTableReader, Interval<PartitionPosition, SSTableReader>>
{
    private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

    public static final int MAX_INTERVALS_ADDED_BEFORE_REBUILD = 100;

    SSTableIntervalTree(Collection<Interval<PartitionPosition, SSTableReader>> intervals)
    {
        super(intervals);
    }

    SSTableIntervalTree(int count, IntervalNode head, int cnt)
    {
        super(count, head, cnt);
    }

    public static SSTableIntervalTree empty()
    {
        return EMPTY;
    }

    public static SSTableIntervalTree build(Iterable<SSTableReader> sstables)
    {
        return new SSTableIntervalTree(buildIntervals(sstables));
    }

    /**
     * Creates a new SSTableIntervalTree where SSTableReaders within the replacementMap are updated from the map's
     * key to its value. The new SSTableIntervalTree shares some {@code IntervalNode} instances with
     * the original tree. Only the nodes along the paths to the replaced SSTableReaders are recreated, minimizing
     * the extent of changes to the tree structure.
     *
     * Assumption: all SSTableReader keys of replacementMap are present within the current SSTableIntervalTree.
     *
     * @param replacementMap Map of SSTableReader(s) (toRemove, toAdd) that need to be replaced within the tree
     * @return A new SSTableIntervalTree, partially sharing structure with the original tree, but with the specified
     *         SSTableReaders replaced.
     */
    public SSTableIntervalTree copyAndReplaceSSTables(Map<SSTableReader, SSTableReader> replacementMap)
    {
        Map<Interval<PartitionPosition, SSTableReader>, Interval<PartitionPosition, SSTableReader>> replacementIntervalsMap = new HashMap<>();
        for (Map.Entry<SSTableReader, SSTableReader> entry : replacementMap.entrySet())
        {
            replacementIntervalsMap.put(Interval.create(entry.getKey().first, entry.getKey().last, entry.getKey()),
                                        Interval.create(entry.getValue().first, entry.getValue().last, entry.getValue()));
        }
        return new SSTableIntervalTree(intervalCount(), copyAndReplace(replacementIntervalsMap), updateCount());
    }

    /**
     * Creates a new SSTableIntervalTree with provided SSTableReaders added into the tree
     * The new SSTableIntervalTree shares some {@code IntervalNode} instances with the original tree.
     * Only the nodes along the paths to the node where SSTableReader is added are recreated, minimizing
     * the extent of changes to the tree structure.
     *
     * Note: this will fall back to rebuild method after certain updates w/o rebuild, as the addInterval method
     *       doesn't guarentee balanced interval tree. After any rebuild the tree will be built from scratch
     *       and the tree will be balanced.
     *
     * @param allSSTables List of all SSTableReader(s) of the new tree
     * @param toAdd List of SSTableReader(s) to be added within the tree
     * @return A new SSTableIntervalTree, partially sharing structure with the original tree, but with the
     *         SSTableReaders added into the tree (midpoints remain the same).
     */
    public SSTableIntervalTree copyAndAddSSTables(Iterable<SSTableReader> allSSTables, Iterable<SSTableReader> toAdd)
    {
        List<Interval<PartitionPosition, SSTableReader>> intervalsToAdd = buildIntervals(toAdd);
        if (updateCount() + intervalsToAdd.size() >= MAX_INTERVALS_ADDED_BEFORE_REBUILD)
            return new SSTableIntervalTree(buildIntervals(allSSTables));
        else
            return new SSTableIntervalTree(intervalCount() + intervalsToAdd.size(),
                                           copyAndAddIntervals(intervalsToAdd),
                                           updateCount() + intervalsToAdd.size());
    }

    public static List<Interval<PartitionPosition, SSTableReader>> buildIntervals(Iterable<SSTableReader> sstables)
    {
        List<Interval<PartitionPosition, SSTableReader>> intervals = new ArrayList<>(Iterables.size(sstables));
        for (SSTableReader sstable : sstables)
            intervals.add(Interval.<PartitionPosition, SSTableReader>create(sstable.first, sstable.last, sstable));
        return intervals;
    }
}
