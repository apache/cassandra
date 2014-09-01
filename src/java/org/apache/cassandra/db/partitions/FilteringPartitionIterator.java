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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

/**
 * Abstract class to make it easier to write iterators that filter some
 * parts of another iterator (used for purging tombstones and removing dropped columns).
 */
public abstract class FilteringPartitionIterator extends WrappingUnfilteredPartitionIterator
{
    private UnfilteredRowIterator next;

    protected FilteringPartitionIterator(UnfilteredPartitionIterator iter)
    {
        super(iter);
    }

    // The filter to use for filtering row contents. Is null by default to mean no particular filtering
    // but can be overriden by subclasses. Please see FilteringAtomIterator for details on how this is used.
    protected FilteringRow makeRowFilter()
    {
        return null;
    }

    // Whether or not we should bother filtering the provided rows iterator. This
    // exists mainly for preformance
    protected boolean shouldFilter(UnfilteredRowIterator iterator)
    {
        return true;
    }

    protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        return true;
    }

    protected boolean includePartitionDeletion(DeletionTime dt)
    {
        return true;
    }

    // Allows to modify the range tombstone returned. This is called *after* includeRangeTombstoneMarker has been called.
    protected RangeTombstoneMarker filterRangeTombstoneMarker(RangeTombstoneMarker marker, boolean reversed)
    {
        return marker;
    }

    // Called when a particular partition is skipped due to being empty post filtering
    protected void onEmpty(DecoratedKey key)
    {
    }

    public boolean hasNext()
    {
        while (next == null && super.hasNext())
        {
            UnfilteredRowIterator iterator = super.next();
            if (shouldFilter(iterator))
            {
                next = new FilteringIterator(iterator);
                if (!isForThrift() && next.isEmpty())
                {
                    onEmpty(iterator.partitionKey());
                    iterator.close();
                    next = null;
                }
            }
            else
            {
                next = iterator;
            }
        }
        return next != null;
    }

    public UnfilteredRowIterator next()
    {
        UnfilteredRowIterator toReturn = next;
        next = null;
        return toReturn;
    }

    @Override
    public void close()
    {
        try
        {
            super.close();
        }
        finally
        {
            if (next != null)
                next.close();
        }
    }

    private class FilteringIterator extends FilteringRowIterator
    {
        private FilteringIterator(UnfilteredRowIterator iterator)
        {
            super(iterator);
        }

        @Override
        protected FilteringRow makeRowFilter()
        {
            return FilteringPartitionIterator.this.makeRowFilter();
        }

        @Override
        protected boolean includeRangeTombstoneMarker(RangeTombstoneMarker marker)
        {
            return FilteringPartitionIterator.this.includeRangeTombstoneMarker(marker);
        }

        @Override
        protected RangeTombstoneMarker filterRangeTombstoneMarker(RangeTombstoneMarker marker, boolean reversed)
        {
            return FilteringPartitionIterator.this.filterRangeTombstoneMarker(marker, reversed);
        }

        @Override
        protected boolean includePartitionDeletion(DeletionTime dt)
        {
            return FilteringPartitionIterator.this.includePartitionDeletion(dt);
        }
    }
}
