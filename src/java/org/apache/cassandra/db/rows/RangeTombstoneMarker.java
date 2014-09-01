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

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * A marker for a range tombstone bound.
 * <p>
 * There is 2 types of markers: bounds (see {@link RangeTombstoneBound}) and boundaries (see {@link RangeTombstoneBoundary}).
 */
public interface RangeTombstoneMarker extends Unfiltered
{
    @Override
    public RangeTombstone.Bound clustering();

    public boolean isBoundary();

    public void copyTo(RangeTombstoneMarker.Writer writer);

    public boolean isOpen(boolean reversed);
    public boolean isClose(boolean reversed);
    public DeletionTime openDeletionTime(boolean reversed);
    public DeletionTime closeDeletionTime(boolean reversed);

    public interface Writer extends Slice.Bound.Writer
    {
        public void writeBoundDeletion(DeletionTime deletion);
        public void writeBoundaryDeletion(DeletionTime endDeletion, DeletionTime startDeletion);
        public void endOfMarker();
    }

    public static class Builder implements Writer
    {
        private final ByteBuffer[] values;
        private int size;

        private RangeTombstone.Bound.Kind kind;
        private DeletionTime firstDeletion;
        private DeletionTime secondDeletion;

        public Builder(int maxClusteringSize)
        {
            this.values = new ByteBuffer[maxClusteringSize];
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            values[size++] = value;
        }

        public void writeBoundKind(RangeTombstone.Bound.Kind kind)
        {
            this.kind = kind;
        }

        public void writeBoundDeletion(DeletionTime deletion)
        {
            firstDeletion = deletion;
        }

        public void writeBoundaryDeletion(DeletionTime endDeletion, DeletionTime startDeletion)
        {
            firstDeletion = endDeletion;
            secondDeletion = startDeletion;
        }

        public void endOfMarker()
        {
        }

        public RangeTombstoneMarker build()
        {
            assert kind != null : "Nothing has been written";
            if (kind.isBoundary())
                return new RangeTombstoneBoundaryMarker(new RangeTombstone.Bound(kind, Arrays.copyOfRange(values, 0, size)), firstDeletion, secondDeletion);
            else
                return new RangeTombstoneBoundMarker(new RangeTombstone.Bound(kind, Arrays.copyOfRange(values, 0, size)), firstDeletion);
        }

        public Builder reset()
        {
            Arrays.fill(values, null);
            size = 0;
            kind = null;
            return this;
        }
    }

    /**
     * Utility class to help merging range tombstone markers coming from multiple inputs (UnfilteredRowIterators).
     * <p>
     * The assumption that each individual input must validate and that we must preserve in the output is that every
     * open marker has a corresponding close marker with the exact same deletion info, and that there is no other range
     * tombstone marker between those open and close marker (of course, they could be rows in between). In other word,
     * for any {@code UnfilteredRowIterator}, you only ever have to remenber the last open marker (if any) to have the
     * full picture of what is deleted by range tombstones at any given point of iterating that iterator.
     * <p>
     * Note that this class can merge both forward and reverse iterators. To deal with reverse, we just reverse how we
     * deal with open and close markers (in forward order, we'll get open-close, open-close, ..., while in reverse we'll
     * get close-open, close-open, ...).
     */
    public static class Merger
    {
        // Boundaries sorts like the bound that have their equivalent "inclusive" part and that's the main action we
        // care about as far as merging goes. So MergedKind just group those as the same case, and tell us whether
        // we're dealing with an open or a close (based on whether we're dealing with reversed iterators or not).
        // Really this enum is just a convenience for merging.
        private enum MergedKind
        {
            INCL_OPEN, EXCL_CLOSE, EXCL_OPEN, INCL_CLOSE;

            public static MergedKind forBound(RangeTombstone.Bound bound, boolean reversed)
            {
                switch (bound.kind())
                {
                    case INCL_START_BOUND:
                    case EXCL_END_INCL_START_BOUNDARY:
                        return reversed ? INCL_CLOSE : INCL_OPEN;
                    case EXCL_END_BOUND:
                        return reversed ? EXCL_OPEN : EXCL_CLOSE;
                    case EXCL_START_BOUND:
                        return reversed ? EXCL_CLOSE : EXCL_OPEN;
                    case INCL_END_EXCL_START_BOUNDARY:
                    case INCL_END_BOUND:
                        return reversed ? INCL_OPEN : INCL_CLOSE;
                }
                throw new AssertionError();
            }
        }

        private final CFMetaData metadata;
        private final UnfilteredRowIterators.MergeListener listener;
        private final DeletionTime partitionDeletion;
        private final boolean reversed;

        private RangeTombstone.Bound bound;
        private final RangeTombstoneMarker[] markers;

        // For each iterator, what is the currently open marker deletion time (or null if there is no open marker on that iterator)
        private final DeletionTime[] openMarkers;
        // The index in openMarkers of the "biggest" marker, the one with the biggest deletion time. Is < 0 iff there is no open
        // marker on any iterator.
        private int biggestOpenMarker = -1;

        public Merger(CFMetaData metadata, int size, DeletionTime partitionDeletion, boolean reversed, UnfilteredRowIterators.MergeListener listener)
        {
            this.metadata = metadata;
            this.listener = listener;
            this.partitionDeletion = partitionDeletion;
            this.reversed = reversed;

            this.markers = new RangeTombstoneMarker[size];
            this.openMarkers = new DeletionTime[size];
        }

        public void clear()
        {
            Arrays.fill(markers, null);
        }

        public void add(int i, RangeTombstoneMarker marker)
        {
            bound = marker.clustering();
            markers[i] = marker;
        }

        public RangeTombstoneMarker merge()
        {
            /*
             * Merging of range tombstones works this way:
             *   1) We remember what is the currently open marker in the merged stream
             *   2) We update our internal states of what range is opened on the input streams based on the new markers to merge
             *   3) We compute what should be the state in the merge stream after 2)
             *   4) We return what marker should be issued on the merged stream based on the difference between the state from 1) and 3)
             */

            DeletionTime previousDeletionTimeInMerged = currentOpenDeletionTimeInMerged();

            updateOpenMarkers();

            DeletionTime newDeletionTimeInMerged = currentOpenDeletionTimeInMerged();
            if (previousDeletionTimeInMerged.equals(newDeletionTimeInMerged))
                return null;

            ByteBuffer[] values = bound.getRawValues();

            RangeTombstoneMarker merged;
            switch (MergedKind.forBound(bound, reversed))
            {
                case INCL_OPEN:
                    merged = previousDeletionTimeInMerged.isLive()
                           ? RangeTombstoneBoundMarker.inclusiveOpen(reversed, values, newDeletionTimeInMerged)
                           : RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(reversed, values, previousDeletionTimeInMerged, newDeletionTimeInMerged);
                    break;
                case EXCL_CLOSE:
                    merged = newDeletionTimeInMerged.isLive()
                           ? RangeTombstoneBoundMarker.exclusiveClose(reversed, values, previousDeletionTimeInMerged)
                           : RangeTombstoneBoundaryMarker.exclusiveCloseInclusiveOpen(reversed, values, previousDeletionTimeInMerged, newDeletionTimeInMerged);
                    break;
                case EXCL_OPEN:
                    merged = previousDeletionTimeInMerged.isLive()
                           ? RangeTombstoneBoundMarker.exclusiveOpen(reversed, values, newDeletionTimeInMerged)
                           : RangeTombstoneBoundaryMarker.inclusiveCloseExclusiveOpen(reversed, values, previousDeletionTimeInMerged, newDeletionTimeInMerged);
                    break;
                case INCL_CLOSE:
                    merged = newDeletionTimeInMerged.isLive()
                           ? RangeTombstoneBoundMarker.inclusiveClose(reversed, values, previousDeletionTimeInMerged)
                           : RangeTombstoneBoundaryMarker.inclusiveCloseExclusiveOpen(reversed, values, previousDeletionTimeInMerged, newDeletionTimeInMerged);
                    break;
                default:
                    throw new AssertionError();
            }

            if (listener != null)
                listener.onMergedRangeTombstoneMarkers(merged, markers);

            return merged;
        }

        private DeletionTime currentOpenDeletionTimeInMerged()
        {
            if (biggestOpenMarker < 0)
                return DeletionTime.LIVE;

            DeletionTime biggestDeletionTime = openMarkers[biggestOpenMarker];
            // it's only open in the merged iterator if it's not shadowed by the partition level deletion
            return partitionDeletion.supersedes(biggestDeletionTime) ? DeletionTime.LIVE : biggestDeletionTime.takeAlias();
        }

        private void updateOpenMarkers()
        {
            for (int i = 0; i < markers.length; i++)
            {
                RangeTombstoneMarker marker = markers[i];
                if (marker == null)
                    continue;

                // Note that we can have boundaries that are both open and close, but in that case all we care about
                // is what it the open deletion after the marker, so we favor the opening part in this case.
                if (marker.isOpen(reversed))
                    openMarkers[i] = marker.openDeletionTime(reversed).takeAlias();
                else
                    openMarkers[i] = null;
            }

            // Recompute what is now the biggest open marker
            biggestOpenMarker = -1;
            for (int i = 0; i < openMarkers.length; i++)
            {
                if (openMarkers[i] != null && (biggestOpenMarker < 0 || openMarkers[i].supersedes(openMarkers[biggestOpenMarker])))
                    biggestOpenMarker = i;
            }
        }

        public DeletionTime activeDeletion()
        {
            DeletionTime openMarker = currentOpenDeletionTimeInMerged();
            // We only have an open marker in the merged stream if it's not shadowed by the partition deletion (which can be LIVE itself), so
            // if have an open marker, we know it's the "active" deletion for the merged stream.
            return openMarker.isLive() ? partitionDeletion : openMarker;
        }
    }
}
