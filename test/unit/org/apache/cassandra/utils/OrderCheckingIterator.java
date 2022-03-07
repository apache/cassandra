/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Wrapping iterator that checks source for problems with iteration order and tombstone bounds.
 */
public final class OrderCheckingIterator extends AbstractIterator<Unfiltered> implements UnfilteredRowIterator
{
    /**
     * The decorated iterator.
     */
    private final UnfilteredRowIterator iterator;

    private final ClusteringComparator comparator;

    private Unfiltered previous;

    private RangeTombstoneMarker openMarker;

    public OrderCheckingIterator(UnfilteredRowIterator iterator)
    {
        this.iterator = iterator;
        this.comparator = iterator.metadata().comparator;
    }

    public TableMetadata metadata()
    {
        return iterator.metadata();
    }

    public boolean isReverseOrder()
    {
        return iterator.isReverseOrder();
    }

    public RegularAndStaticColumns columns()
    {
        return iterator.columns();
    }

    public DecoratedKey partitionKey()
    {
        return iterator.partitionKey();
    }

    public Row staticRow()
    {
        return iterator.staticRow();
    }

    @Override
    public boolean isEmpty()
    {
        return iterator.isEmpty();
    }

    public void close()
    {
        iterator.close();
    }

    public DeletionTime partitionLevelDeletion()
    {
        return iterator.partitionLevelDeletion();
    }

    public EncodingStats stats()
    {
        return iterator.stats();
    }

    protected Unfiltered computeNext()
    {
        if (!iterator.hasNext())
        {
            // Check that we are not left with an unclosed tombstone.
            if (openMarker != null)
                throw new AssertionError(String.format("Found orphaned open tombstone marker (with no " +
                                                       "closing marker) at clustering %s of partition %s",
                                                       openMarker.clustering().toString(metadata()),
                                                       metadata().partitionKeyType.getString(partitionKey().getKey())));
            return endOfData();
        }

        boolean reversed = isReverseOrder();
        Unfiltered next = iterator.next();

        // Check data comes in the right order.
        if (previous != null && comparator.compare(next, previous) < 0)
        {
            // We may have to return one more close marker in this call, but we're done no matter what after that.
            throw new AssertionError(String.format("Found out of order data, clustering %s after %s in partition %s",
                                                   next.clustering().toString(metadata()),
                                                   previous.clustering().toString(metadata()),
                                                   metadata().partitionKeyType.getString(partitionKey().getKey())));
        }

        // Validate invariants of range tombstones markers, namely that:
        // - we don't have a close without a previous open.
        // - both deletionTime match.
        // - we don't have ineffective tombstones, i.e.
        //   -- boundaries with equal deletions on both sides
        //   -- tombstones with DeletionTime.LIVE
        if (next.isRangeTombstoneMarker())
        {
            Logger.getLogger(getClass()).info("tombstone: " + next.toString(metadata()));
            RangeTombstoneMarker marker = (RangeTombstoneMarker) next;

            if (marker.isOpen(reversed) && marker.openDeletionTime(reversed).isLive() ||
                marker.isClose(reversed) && marker.closeDeletionTime(reversed).isLive())
            {
                throw new AssertionError(String.format("Found an ineffective tombstone bound (with live " +
                                                       "deletion time) at clustering %s of partition %s",
                                                       marker.clustering().toString(metadata()),
                                                       metadata().partitionKeyType.getString(partitionKey().getKey())));
            }

            if (marker.isBoundary() && marker.openDeletionTime(reversed).equals(marker.closeDeletionTime(reversed)))
            {
                throw new AssertionError(String.format("Found an ineffective tombstone boundary (with equal close " +
                                                       "and open deletion times) at clustering %s of partition %s",
                                                       marker.clustering().toString(metadata()),
                                                       metadata().partitionKeyType.getString(partitionKey().getKey())));
            }

            // Marker can be a boundary, and if so, handling his close first is easier.
            if (marker.isClose(reversed))
            {
                if (openMarker == null)
                    throw new AssertionError(String.format("Found orphaned close tombstone marker (with no prior " +
                                                           "opening marker) at clustering %s of partition %s",
                                                           marker.clustering().toString(metadata()),
                                                           metadata().partitionKeyType.getString(partitionKey().getKey())));
                if (!openMarker.openDeletionTime(reversed).equals(marker.closeDeletionTime(reversed)))
                    throw new AssertionError(String.format("Mismatched open and close tombstone markers in partition %s: " +
                                                           "open marker at clustering %s had deletion info %s, " +
                                                           "but close marker at clustering %s has deletion info %s",
                                                           metadata().partitionKeyType.getString(partitionKey().getKey()),
                                                           openMarker.clustering().toString(metadata()),
                                                           openMarker.openDeletionTime(reversed),
                                                           marker.clustering().toString(metadata()),
                                                           marker.closeDeletionTime(reversed)));
                openMarker = null;
            }
            if (marker.isOpen(reversed))
            {
                // Same as above, we check invariants, namely that we should not have a current open marker (it
                // should have been closed before any open marker.
                if (openMarker != null)
                    throw new AssertionError(String.format("Found non-closed open tombstone marker at clustering %s " +
                                                           "of partition %s: a new marker is opened at clustering %s " +
                                                           "without having seen a prior close.",
                                                           openMarker.clustering().toString(metadata()),
                                                           metadata().partitionKeyType.getString(partitionKey().getKey()),
                                                           marker.clustering().toString(metadata())));
                openMarker = marker;
            }
        }
        previous = next;
        return next;
    }
}
