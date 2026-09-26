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

package org.apache.cassandra.db.compaction.differential;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Structural guards a corpus shape must pass before its differential comparison is trusted. They inspect
 * only the column family store and sstable state, so any differential engine (compaction, read, or flush)
 * calls them statically with no inheritance.
 */
public final class DifferentialShapeGuards
{
    private DifferentialShapeGuards()
    {
    }

    /**
     * Asserts the live sstables actually reconcile across each other, so a shape can never silently
     * degrade into a no-op merge. Two conditions must both hold, because range overlap alone is not
     * enough: two sstables can overlap in key range yet share no partition key, in which case the merge
     * is a per-partition concatenation that reconciles nothing.
     * <ol>
     *   <li>at least two live sstables have overlapping key ranges, and</li>
     *   <li>at least one partition key appears in two of them.</li>
     * </ol>
     */
    public static void assertLiveSSTablesOverlap(ColumnFamilyStore cfs, DifferentialSchema schema)
    {
        List<SSTableReader> live = new ArrayList<>(cfs.getLiveSSTables());
        if (live.size() < 2)
            throw new AssertionError("shape '" + schema.name() + "' must leave at least two sstables to " +
                                     "merge; got " + live.size());

        if (!someRangesOverlap(live))
            throw new AssertionError("shape '" + schema.name() + "' left " + live.size() + " sstables but none " +
                                     "have overlapping key ranges: the merge would be a no-op concatenation, " +
                                     "not a real reconciliation");

        if (!someKeyAppearsInTwoSSTables(live))
            throw new AssertionError("shape '" + schema.name() + "' left overlapping sstable ranges but no " +
                                     "partition key appears in two sstables: the merge reconciles nothing " +
                                     "across sstables, so the comparison is trivial");
    }

    /** True when any pair of live sstables has overlapping key ranges. */
    private static boolean someRangesOverlap(List<SSTableReader> live)
    {
        for (int i = 0; i < live.size(); i++)
            for (int j = i + 1; j < live.size(); j++)
                if (rangesOverlap(live.get(i), live.get(j)))
                    return true;
        return false;
    }

    /** Two sstables overlap when neither's key range ends before the other's begins. */
    private static boolean rangesOverlap(SSTableReader a, SSTableReader b)
    {
        return a.getFirst().compareTo(b.getLast()) <= 0 && b.getFirst().compareTo(a.getLast()) <= 0;
    }

    /** True as soon as one partition key read from an sstable was already seen in an earlier one. */
    private static boolean someKeyAppearsInTwoSSTables(List<SSTableReader> live)
    {
        Set<DecoratedKey> seen = new HashSet<>();
        for (SSTableReader sstable : live)
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    // an sstable holds one partition per key, so a repeat can only be a cross-sstable one
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        if (!seen.add(partition.partitionKey()))
                            return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Asserts at least one partition of some live sstable carries a promoted row index of more than one
     * block, proving the shape actually exercises the block-navigation path rather than silently shrinking
     * to a single-block partition.
     */
    public static void assertSomePartitionSpansMultipleBlocks(ColumnFamilyStore cfs, DifferentialSchema schema)
    {
        for (SSTableReader sstable : cfs.getLiveSSTables())
        {
            try (ISSTableScanner scanner = sstable.getScanner())
            {
                while (scanner.hasNext())
                {
                    DecoratedKey key;
                    try (UnfilteredRowIterator partition = scanner.next())
                    {
                        key = partition.partitionKey();
                    }
                    AbstractRowIndexEntry entry = sstable.getRowIndexEntry(key, SSTableReader.Operator.EQ);
                    if (entry != null && entry.blockCount() > 1)
                        return;
                }
            }
        }
        throw new AssertionError("shape '" + schema.name() + "' declares it spans multiple index blocks, " +
                                 "but no partition of the flushed sstables carries a promoted row index of " +
                                 "more than one block: the block-navigation path is not being exercised");
    }
}
