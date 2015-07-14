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
package org.apache.cassandra.db.rows;

import java.util.Comparator;
import java.util.Iterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;

/**
 * An iterator that merges a source of rows with the range tombstone and partition level deletion of a give partition.
 * <p>
 * This is used by our {@code Partition} implementations to produce a {@code UnfilteredRowIterator} by merging the rows
 * and deletion infos that are kept separate. This has also 2 additional role:
 *   1) this make sure the row returned only includes the columns selected for the resulting iterator.
 *   2) this (optionally) remove any data that can be shadowed (see commet on 'removeShadowedData' below for more details)
 */
public class RowAndDeletionMergeIterator extends AbstractUnfilteredRowIterator
{
    // For some of our Partition implementation, we can't guarantee that the deletion information (partition level
    // deletion and range tombstones) don't shadow data in the rows. If that is the case, this class also take
    // cares of skipping such shadowed data (since it is the contract of an UnfilteredRowIterator that it doesn't
    // shadow its own data). Sometimes however, we know this can't happen, in which case we can skip that step.
    private final boolean removeShadowedData;
    private final Comparator<Clusterable> comparator;
    private final ColumnFilter selection;

    private final Iterator<Row> rows;
    private Row nextRow;

    private final Iterator<RangeTombstone> ranges;
    private RangeTombstone nextRange;

    // The currently open tombstone. Note that unless this is null, there is no point in checking nextRange.
    private RangeTombstone openRange;

    public RowAndDeletionMergeIterator(CFMetaData metadata,
                                       DecoratedKey partitionKey,
                                       DeletionTime partitionLevelDeletion,
                                       ColumnFilter selection,
                                       Row staticRow,
                                       boolean isReversed,
                                       EncodingStats stats,
                                       Iterator<Row> rows,
                                       Iterator<RangeTombstone> ranges,
                                       boolean removeShadowedData)
    {
        super(metadata, partitionKey, partitionLevelDeletion, selection.fetchedColumns(), staticRow, isReversed, stats);
        this.comparator = isReversed ? metadata.comparator.reversed() : metadata.comparator;
        this.selection = selection;
        this.removeShadowedData = removeShadowedData;
        this.rows = rows;
        this.ranges = ranges;
    }

    protected Unfiltered computeNext()
    {
        while (true)
        {
            updateNextRow();
            if (nextRow == null)
            {
                if (openRange != null)
                    return closeOpenedRange();

                updateNextRange();
                return nextRange == null ? endOfData() : openRange();
            }

            // We have a next row

            if (openRange == null)
            {
                // We have no currently open tombstone range. So check if we have a next range and if it sorts before this row.
                // If it does, the opening of that range should go first. Otherwise, the row goes first.
                updateNextRange();
                if (nextRange != null && comparator.compare(openBound(nextRange), nextRow.clustering()) < 0)
                    return openRange();

                Row row = consumeNextRow();
                // it's possible for the row to be fully shadowed by the current range tombstone
                if (row != null)
                    return row;
            }
            else
            {
                // We have both a next row and a currently opened tombstone. Check which goes first between the range closing and the row.
                if (comparator.compare(closeBound(openRange), nextRow.clustering()) < 0)
                    return closeOpenedRange();

                Row row = consumeNextRow();
                if (row != null)
                    return row;
            }
        }
    }

    private void updateNextRow()
    {
        if (nextRow == null && rows.hasNext())
            nextRow = rows.next();
    }

    private void updateNextRange()
    {
        while (nextRange == null && ranges.hasNext())
        {
            nextRange = ranges.next();
            if (removeShadowedData && partitionLevelDeletion().supersedes(nextRange.deletionTime()))
                nextRange = null;
        }
    }

    private Row consumeNextRow()
    {
        Row row = nextRow;
        nextRow = null;
        if (!removeShadowedData)
            return row.filter(selection, metadata());

        DeletionTime activeDeletion = openRange == null ? partitionLevelDeletion() : openRange.deletionTime();
        return row.filter(selection, activeDeletion, false, metadata());
    }

    private RangeTombstone consumeNextRange()
    {
        RangeTombstone range = nextRange;
        nextRange = null;
        return range;
    }

    private RangeTombstone consumeOpenRange()
    {
        RangeTombstone range = openRange;
        openRange = null;
        return range;
    }

    private Slice.Bound openBound(RangeTombstone range)
    {
        return range.deletedSlice().open(isReverseOrder());
    }

    private Slice.Bound closeBound(RangeTombstone range)
    {
        return range.deletedSlice().close(isReverseOrder());
    }

    private RangeTombstoneMarker closeOpenedRange()
    {
        // Check if that close if actually a boundary between markers
        updateNextRange();
        RangeTombstoneMarker marker;
        if (nextRange != null && comparator.compare(closeBound(openRange), openBound(nextRange)) == 0)
        {
            marker = RangeTombstoneBoundaryMarker.makeBoundary(isReverseOrder(), closeBound(openRange), openBound(nextRange), openRange.deletionTime(), nextRange.deletionTime());
            openRange = consumeNextRange();
        }
        else
        {
            RangeTombstone toClose = consumeOpenRange();
            marker = new RangeTombstoneBoundMarker(closeBound(toClose), toClose.deletionTime());
        }
        return marker;
    }

    private RangeTombstoneMarker openRange()
    {
        assert openRange == null && nextRange != null;
        openRange = consumeNextRange();
        return new RangeTombstoneBoundMarker(openBound(openRange), openRange.deletionTime());
    }

}
