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

package org.apache.cassandra.io.sstable.compaction;

import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.ColumnMetadataVersionComparator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Merger;
import org.apache.cassandra.utils.Reducer;

/**
 * Cursor merger, which employs the Merger object to combine multiple cursors into a single stream.
 *
 * Most of the complexity of this class is in applying hierarchical deletions, of which there are four kinds:
 *  - partition-level deletion
 *  - range tombstones
 *  - row-level deletion
 *  - complex column deletion
 * In addition to the values for each level (which we must report to the consumer), we also track combined
 *  - activeRangeDeletion (newest of partition-level and active range tombstone)
 *  - mergedRowDeletion (newest of active and row-level deletion)
 *  - mergedCellDeletion (newest of merged row and complex column deletion)
 *  and use the mergedCellDeletion to remove no-longer active cells.
 */
public class SSTableCursorMerger extends Reducer<SSTableCursor, SSTableCursor> implements SSTableCursor
{
    private final Merger<SSTableCursor, SSTableCursor, SSTableCursor> merger;
    private final MergeListener mergeListener;

    private Type currentType;
    private DecoratedKey currentPartitionKey;
    private DeletionTime partitionLevelDeletion;
    private ClusteringPrefix<?> currentClusteringKey;
    private DeletionTime rowLevelDeletion;
    private LivenessInfo currentLivenessInfo;
    private DeletionTime activeRangeDeletion; // uses partitionLevelDeletion as base instead of LIVE (which makes the logic a little simpler)
    private DeletionTime mergedRowDeletion; // the deletion that applies to this row, merge(activeRangeDeletion, rowLevelDeletion)
    private DeletionTime mergedCellDeletion; // the deletion that applies to this cell, merge(mergedRowDeletion, complexColumnDeletion)

    private ColumnMetadata columnMetadata;
    private DeletionTime complexColumnDeletion;

    private Cell<?> currentCell;
    private int currentIndex;
    private int numMergedVersions = 0;

    public SSTableCursorMerger(List<SSTableCursor> cursors, TableMetadata metadata)
    {
        this(cursors, metadata, NO_MERGE_LISTENER);
    }

    public SSTableCursorMerger(List<SSTableCursor> cursors, TableMetadata metadata, MergeListener mergeListener)
    {
        assert !cursors.isEmpty();
        this.mergeListener = mergeListener;
        this.merger = new Merger<>(cursors,
                                   x -> {
                                       x.advance();
                                       return x;
                                   },
                                   SSTableCursor::close,
                                   mergeComparator(metadata),
                                   this);
        this.currentType = Type.UNINITIALIZED;
    }

    public Type type()
    {
        return currentType;
    }

    public DecoratedKey partitionKey()
    {
        return currentPartitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public ClusteringPrefix clusteringKey()
    {
        return currentClusteringKey;
    }

    public LivenessInfo clusteringKeyLivenessInfo()
    {
        return currentLivenessInfo;
    }

    public DeletionTime rowLevelDeletion()
    {
        // Note: this is used for both range tombstone markers and rows. For the former we default to the
        // partition-level deletion if no (newer) range tombstone is in effect, but we must report this case
        // as LIVE to any consumer.
        return rowLevelDeletion == partitionLevelDeletion ? DeletionTime.LIVE : rowLevelDeletion;
    }

    public DeletionTime activeRangeDeletion()
    {
        return activeRangeDeletion == partitionLevelDeletion ? DeletionTime.LIVE : activeRangeDeletion;
    }

    public DeletionTime complexColumnDeletion()
    {
        return complexColumnDeletion;
    }

    public ColumnMetadata column()
    {
        return columnMetadata;
    }

    public Cell cell()
    {
        return currentCell;
    }

    public long bytesProcessed()
    {
        long bytesProcessed = 0;
        for (SSTableCursor cursor : merger.allSources())
            bytesProcessed += cursor.bytesProcessed();
        return bytesProcessed;
    }

    public long bytesTotal()
    {
        long bytesTotal = 0;
        for (SSTableCursor cursor : merger.allSources())
            bytesTotal += cursor.bytesTotal();
        return bytesTotal;
    }

    public void close()
    {
        merger.close(); // this will also close the inputs via the supplied onClose consumer
    }

    public Type advance()
    {
        if (currentType == Type.RANGE_TOMBSTONE)
            activeRangeDeletion = rowLevelDeletion;

        // We don't need to use hasNext because the streams finish on Level.EXHAUSTED.
        // If the reducer returns null, get next entry.
        while (merger.next() == null) {}

        return currentType;
    }

    public void onKeyChange()
    {
        currentIndex = -1;
        numMergedVersions = 0;
    }

    public void reduce(int idx, SSTableCursor current)
    {
        ++numMergedVersions;
        if (currentIndex == -1)
        {
            currentIndex = idx;
            currentType = current.type();
            switch (currentType)
            {
                case COMPLEX_COLUMN:
                    columnMetadata = current.column();
                    complexColumnDeletion = current.complexColumnDeletion();
                    return;
                case SIMPLE_COLUMN:
                    mergedCellDeletion = mergedRowDeletion;
                case COMPLEX_COLUMN_CELL:
                    Cell cell = current.cell();
                    if (!mergedCellDeletion.deletes(cell))
                        currentCell = cell;
                    else
                        currentCell = null;
                    return;
                case ROW:
                    currentClusteringKey = current.clusteringKey();
                    currentLivenessInfo = current.clusteringKeyLivenessInfo();
                    rowLevelDeletion = current.rowLevelDeletion();
                    return;
                case RANGE_TOMBSTONE:
                    currentClusteringKey = current.clusteringKey();
                    rowLevelDeletion = current.rowLevelDeletion();
                    return;
                case PARTITION:
                    currentPartitionKey = current.partitionKey();
                    partitionLevelDeletion = current.partitionLevelDeletion();
                    return;
                case EXHAUSTED:
                default:
                    return;
            }
        }
        else
        {
            switch (currentType)
            {
                case COMPLEX_COLUMN:
                    if ((ColumnMetadataVersionComparator.INSTANCE.compare(columnMetadata, current.column()) < 0))
                        columnMetadata = current.column();
                    if (current.complexColumnDeletion().supersedes(complexColumnDeletion))
                        complexColumnDeletion = current.complexColumnDeletion();
                    return;
                case SIMPLE_COLUMN:
                case COMPLEX_COLUMN_CELL:
                    Cell cell = current.cell();
                    if (!mergedCellDeletion.deletes(cell))
                        currentCell = currentCell != null
                                      ? Cells.reconcile(currentCell, cell)
                                      : cell;
                    return;
                case ROW:
                    currentLivenessInfo = LivenessInfo.merge(currentLivenessInfo, current.clusteringKeyLivenessInfo());
                    rowLevelDeletion = DeletionTime.merge(rowLevelDeletion, current.rowLevelDeletion());
                    return;
                case RANGE_TOMBSTONE:
                    rowLevelDeletion = DeletionTime.merge(rowLevelDeletion, current.rowLevelDeletion());
                    return;
                case PARTITION:
                    partitionLevelDeletion = DeletionTime.merge(partitionLevelDeletion, current.partitionLevelDeletion());
                    return;
                case EXHAUSTED:
                default:
                    return;
            }
        }
    }

    public SSTableCursor getReduced()
    {
        mergeListener.onItem(this, numMergedVersions);

        switch (currentType)
        {
            case COMPLEX_COLUMN_CELL:
                if (currentCell == null)
                    return null;
                break;
            case COMPLEX_COLUMN:
                if (complexColumnDeletion.supersedes(mergedRowDeletion))
                    mergedCellDeletion = complexColumnDeletion;
                else
                {
                    complexColumnDeletion = DeletionTime.LIVE;
                    mergedCellDeletion = mergedRowDeletion;
                }
                break;
            case SIMPLE_COLUMN:
                if (currentCell == null)
                    return null;
                columnMetadata = currentCell.column();
                break;
            case ROW:
                if (rowLevelDeletion.supersedes(activeRangeDeletion))
                    mergedRowDeletion = rowLevelDeletion;
                else
                {
                    rowLevelDeletion = DeletionTime.LIVE;
                    mergedRowDeletion = activeRangeDeletion;
                }

                if (mergedRowDeletion.deletes(currentLivenessInfo))
                    currentLivenessInfo = LivenessInfo.EMPTY;
                break;
            case RANGE_TOMBSTONE:
                if (!rowLevelDeletion.supersedes(activeRangeDeletion))
                {
                    // The new deletion is older than some of the active. We need to check if this is the end of the
                    // deletion that is currently active, or something else (some previous start or end that got itself
                    // deleted). To do this, check all active deletions for the sources that did not take part in this
                    // tombstone -- if something newer is still active, we should be using that deletion time instead.

                    // For example, consider the merge of deletions over 1-6 with time 3, and 3-9 with time 2:
                    //       at 1 we have active=LIVE row=3 other=LIVE and switch from LIVE to 3, i.e. return row=3
                    //       at 3 we have active=3 row=2 other=3 and switch from 3 to 3, i.e. issue nothing
                    //       at 6 we have active=3 row=LIVE other=2 and switch from 3 to 2, i.e. return row=2
                    //       at 9 we have active=2 row=LIVE other=LIVE and switch from 2 to LIVE, i.e. return row=LIVE
                    // (where active stands for activeRangeDeletion, row - rowLevelDeletion and other - otherActive)

                    if (activeRangeDeletion.equals(rowLevelDeletion))
                        return null;    // nothing to report, old and new are the same
                    DeletionTime otherActive = gatherDeletions(partitionLevelDeletion, merger.allGreaterValues());
                    if (!rowLevelDeletion.supersedes(otherActive))
                    {
                        if (activeRangeDeletion.equals(otherActive))
                            return null;   // this deletion is fully covered by other sources, nothing has changed
                        else
                            rowLevelDeletion = otherActive; // a newer deletion was closed, use the still valid from others
                    }
                } // otherwise this is introducing a deletion that beats all and should be reported

                currentClusteringKey = adjustClusteringKeyForMarker(currentClusteringKey,
                                                                    activeRangeDeletion != partitionLevelDeletion,
                                                                    rowLevelDeletion != partitionLevelDeletion);
                break;
            case PARTITION:
                activeRangeDeletion = partitionLevelDeletion;
                break;
        }

        return this;
    }

    private DeletionTime gatherDeletions(DeletionTime initialValue, Iterable<SSTableCursor> sources)
    {
        DeletionTime collected = initialValue;
        for (SSTableCursor cursor : sources)
        {
            if (cursor.type() == Type.ROW || cursor.type() == Type.RANGE_TOMBSTONE)
                collected = DeletionTime.merge(collected, cursor.activeRangeDeletion());
        }
        return collected;
    }

    /**
     * Adjust the clustering key for the type of range tombstone marker needed. For the different marker types we have
     * equal but separate clustering kinds. As a the new marker may be the result of combining multiple different ones,
     * the type of marker we have to issue is not guaranteed to be the type of marker we got as input (for example,
     * if one deletion ends at 2 exclusive but another starts at 2 inclusive, these two markers have equal clustering
     * keys, but their merge is not the same as either, but a boundary of the exclusive-end-inclusive-start type).
     */
    private static ClusteringPrefix adjustClusteringKeyForMarker(ClusteringPrefix clusteringKey, boolean activeBefore, boolean activeAfter)
    {
        if (activeBefore & activeAfter)
        {
            // We may need to upgrade from a pair of bounds to a boundary.
            switch (clusteringKey.kind())
            {
                case EXCL_END_INCL_START_BOUNDARY:
                case INCL_END_EXCL_START_BOUNDARY:
                    return clusteringKey;  // already a boundary, good
                case INCL_START_BOUND:
                case EXCL_END_BOUND:
                    return ClusteringBoundary.create(ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY,
                                                     clusteringKey);
                case EXCL_START_BOUND:
                case INCL_END_BOUND:
                    return ClusteringBoundary.create(ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY,
                                                     clusteringKey);
                default:
                    throw new AssertionError();
            }
        }
        else if (activeBefore)
        {
            // Partition-level deletion can cause one side of a boundary to be dropped.
            // Note that because we can have many deletions clashing on the same position, we may have even picked up
            // the clustering key from an overwritten open marker.
            switch (clusteringKey.kind())
            {
                case EXCL_END_BOUND:
                case INCL_END_BOUND:
                    return clusteringKey;  // already a close bound, good
                case EXCL_END_INCL_START_BOUNDARY:
                case INCL_START_BOUND:
                    return ClusteringBound.create(ClusteringPrefix.Kind.EXCL_END_BOUND,
                                                  clusteringKey);
                case INCL_END_EXCL_START_BOUNDARY:
                case EXCL_START_BOUND:
                    return ClusteringBound.create(ClusteringPrefix.Kind.INCL_END_BOUND,
                                                  clusteringKey);
                default:
                    throw new AssertionError();
            }
        }
        else if (activeAfter)
        {
            switch (clusteringKey.kind())
            {
                case EXCL_START_BOUND:
                case INCL_START_BOUND:
                    return clusteringKey;  // already an open bound, good
                case EXCL_END_INCL_START_BOUNDARY:
                case EXCL_END_BOUND:
                    return ClusteringBound.create(ClusteringPrefix.Kind.INCL_START_BOUND,
                                                  clusteringKey);
                case INCL_END_EXCL_START_BOUNDARY:
                case INCL_END_BOUND:
                    return ClusteringBound.create(ClusteringPrefix.Kind.EXCL_START_BOUND,
                                                  clusteringKey);
                default:
                    throw new AssertionError();
            }
        }
        else
            throw new AssertionError();
    }

    public interface MergeListener
    {
        void onItem(SSTableCursor cursor, int numVersions);
    }

    static MergeListener NO_MERGE_LISTENER = (cursor, numVersions) -> {};

    public static Comparator<SSTableCursor> mergeComparator(TableMetadata metadata)
    {
        ClusteringComparator clusteringComparator = metadata.comparator;
        return (a, b) ->
        {
            // Since we are advancing the sources together, a difference in levels means that either:
            // - we compared partition/clustering/column keys before, they were different, and we did not advance one of
            //   the sources into the partition/row's content
            // - one of the sources exhausted the partition/row/column's content and is now producing the next
            // In either case the other source is still producing content for a partition/row/column that should be
            // exhausted before we have to look at that key again.
            if (a.type().level != b.type().level)
                return Integer.compare(a.type().level, b.type().level);

            // If the sources are at the same level, we are guaranteed by the order and comparison above that all
            // keys above this level match and thus we only need to compare the current.
            switch (a.type())
            {
                case COMPLEX_COLUMN_CELL:
                    return a.cell().column().cellPathComparator().compare(a.cell().path(), b.cell().path());
                case SIMPLE_COLUMN:
                case COMPLEX_COLUMN:
                    return a.column().compareTo(b.column());
                case ROW:
                case RANGE_TOMBSTONE:
                    return clusteringComparator.compare(a.clusteringKey(), b.clusteringKey());
                case PARTITION:
                    return a.partitionKey().compareTo(b.partitionKey());
                case EXHAUSTED:
                default:
                    return 0;
            }
        };
    }
}
