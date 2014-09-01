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

import org.apache.cassandra.db.*;

public class FilteringRowIterator extends WrappingUnfilteredRowIterator
{
    private final FilteringRow filter;
    private Unfiltered next;

    public FilteringRowIterator(UnfilteredRowIterator toFilter)
    {
        super(toFilter);
        this.filter = makeRowFilter();
    }

    // Subclasses that want to filter withing row should overwrite this. Note that since FilteringRow
    // is a reusable object, this method won't be called for every filtered row and the same filter will
    // be used for every regular rows. However, this still can be called twice if we have a static row
    // to filter, because we don't want to use the same object for them as this makes for weird behavior
    // if calls to staticRow() are interleaved with hasNext().
    protected FilteringRow makeRowFilter()
    {
        return null;
    }

    protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        return true;
    }

    // Allows to modify the range tombstone returned. This is called *after* includeRangeTombstoneMarker has been called.
    protected RangeTombstoneMarker filterRangeTombstoneMarker(RangeTombstoneMarker marker, boolean reversed)
    {
        return marker;
    }

    protected boolean includeRow(Row row)
    {
        return true;
    }

    protected boolean includePartitionDeletion(DeletionTime dt)
    {
        return true;
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        DeletionTime dt = wrapped.partitionLevelDeletion();
        return includePartitionDeletion(dt) ? dt : DeletionTime.LIVE;
    }

    @Override
    public Row staticRow()
    {
        Row row = super.staticRow();
        if (row == Rows.EMPTY_STATIC_ROW)
            return row;

        FilteringRow filter = makeRowFilter();
        if (filter != null)
            row = filter.setTo(row);

        return !row.isEmpty() && includeRow(row) ? row : Rows.EMPTY_STATIC_ROW;
    }

    @Override
    public boolean hasNext()
    {
        if (next != null)
            return true;

        while (super.hasNext())
        {
            Unfiltered unfiltered = super.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
            {
                Row row = filter == null ? (Row) unfiltered : filter.setTo((Row) unfiltered);
                if (!row.isEmpty() && includeRow(row))
                {
                    next = row;
                    return true;
                }
            }
            else
            {
                RangeTombstoneMarker marker = (RangeTombstoneMarker) unfiltered;
                if (includeRangeTombstoneMarker(marker))
                {
                    next = filterRangeTombstoneMarker(marker, isReverseOrder());
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Unfiltered next()
    {
        if (next == null)
            hasNext();

        Unfiltered toReturn = next;
        next = null;
        return toReturn;
    }
}
