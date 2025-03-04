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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class SSTableIntervalTree extends IntervalTree<PartitionPosition, SSTableReader, Interval<PartitionPosition, SSTableReader>>
{
    private static final SSTableIntervalTree EMPTY = new SSTableIntervalTree(null);

    SSTableIntervalTree(Collection<Interval<PartitionPosition, SSTableReader>> intervals)
    {
        super(intervals);
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

    public static List<Interval<PartitionPosition, SSTableReader>> buildIntervals(Collection<SSTableReader> sstables)
    {
        if (sstables == null || sstables.isEmpty())
            return Collections.emptyList();
        return Arrays.asList(buildIntervalsArray(sstables));
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
}
