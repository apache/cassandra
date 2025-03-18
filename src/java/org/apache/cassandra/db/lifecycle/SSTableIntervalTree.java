/*
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
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkArgument;

public class SSTableIntervalTree extends IntervalTree<PartitionPosition, SSTableReader, Interval<PartitionPosition, SSTableReader>>
{
    private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

    SSTableIntervalTree(Collection<Interval<PartitionPosition, SSTableReader>> intervals)
    {
        super(intervals);
    }

    SSTableIntervalTree(IntervalNode head, int modCount, Interval<PartitionPosition, SSTableReader>[] minOrder, Interval<PartitionPosition, SSTableReader>[] maxOrder)
    {
        super(head, modCount, minOrder, maxOrder);
    }

    private SSTableIntervalTree(Interval<PartitionPosition, SSTableReader>[] minOrder, Interval<PartitionPosition, SSTableReader>[] maxOrder)
    {
        super(minOrder, maxOrder);
    }

    @Override
    protected SSTableIntervalTree create(Interval<PartitionPosition, SSTableReader>[] minOrder, Interval<PartitionPosition, SSTableReader>[] maxOrder)
    {
        return new SSTableIntervalTree(minOrder, maxOrder);
    }

    @Override
    protected SSTableIntervalTree create(IntervalNode head, int modCount, Interval<PartitionPosition, SSTableReader>[] minOrder, Interval<PartitionPosition, SSTableReader>[] maxOrder)
    {
        return new SSTableIntervalTree(head, modCount, minOrder, maxOrder);
    }

    @Override
    protected SSTableIntervalTree create(Collection<Interval<PartitionPosition, SSTableReader>> intervals)
    {
        return new SSTableIntervalTree(intervals);
    }

    @Override
    public SSTableIntervalTree replace(List<Pair<Interval<PartitionPosition, SSTableReader>, Interval<PartitionPosition, SSTableReader>>> replacements)
    {
        checkArgument(!replacements.isEmpty(), "Shouldn't call replace with no replacements");
        return (SSTableIntervalTree) super.replace(replacements);
    }

    public static SSTableIntervalTree empty()
    {
        return EMPTY;
    }

    public static SSTableIntervalTree buildSSTableIntervalTree(Collection<SSTableReader> sstables)
    {
        if (sstables.isEmpty())
            return EMPTY;
        return new SSTableIntervalTree(buildIntervals(sstables));
    }

    public static <S extends CompactionSSTable> List<Interval<PartitionPosition, S>> buildIntervals(Collection<S> sstables)
    {
        List<Interval<PartitionPosition, S>> intervals = new ArrayList<>(sstables.size());
        for (S sstable : sstables)
            intervals.add(Interval.create(sstable.getFirst(), sstable.getLast(), sstable));
        return intervals;
    }

    public static Interval<PartitionPosition, SSTableReader>[] buildIntervalsArray(Collection<SSTableReader> sstables)
    {
        if (sstables == null || sstables.isEmpty())
            return IntervalTree.EMPTY_ARRAY;
        Interval<PartitionPosition, SSTableReader>[] intervals = new Interval[sstables.size()];
        int i = 0;
        for (SSTableReader sstable : sstables)
            intervals[i++] = sstable.getInterval();
        return intervals;
    }

    public static SSTableIntervalTree update(SSTableIntervalTree tree, Collection<SSTableReader> removals, Collection<SSTableReader> additions)
    {
        return (SSTableIntervalTree) tree.update(buildIntervalsArray(removals), buildIntervalsArray(additions));
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
    public static SSTableIntervalTree replace(SSTableIntervalTree tree, Map<SSTableReader, SSTableReader> replacementMap)
    {
        checkArgument(!replacementMap.isEmpty(), "Replacement map shouldn't be empty for SSTableIntervalTree.replace");
        List<Pair<Interval<PartitionPosition, SSTableReader>, Interval<PartitionPosition, SSTableReader>>> replacementIntervalsMap = new ArrayList<>();
        for (Map.Entry<SSTableReader, SSTableReader> entry : replacementMap.entrySet())
        {
            SSTableReader originalSSTable = entry.getKey();
            SSTableReader replacementSSTable = entry.getValue();
            Interval<PartitionPosition, SSTableReader> originalInterval = originalSSTable.getInterval();
            Interval<PartitionPosition, SSTableReader> replacementInterval = replacementSSTable.getInterval();
            replacementIntervalsMap.add(Pair.create(originalInterval, replacementInterval));
        }
        return tree.replace(replacementIntervalsMap);
    }

    public static SSTableIntervalTree addSSTables(SSTableIntervalTree tree, Collection<SSTableReader> additions)
    {
        return (SSTableIntervalTree) tree.add(buildIntervalsArray(additions));
    }
}