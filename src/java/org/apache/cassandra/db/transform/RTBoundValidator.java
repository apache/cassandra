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
package org.apache.cassandra.db.transform;

import java.util.Objects;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

/**
 * A validating transformation that sanity-checks the sequence of RT bounds and boundaries in every partition.
 *
 * What we validate, specifically:
 * - that open markers are only followed by close markers
 * - that open markers and close markers have equal deletion times
 * - optionally, that the iterator closes its last RT marker
 */
public final class RTBoundValidator extends Transformation<UnfilteredRowIterator>
{
    public enum Stage { MEMTABLE, SSTABLE, MERGED, PURGED, PROCESSED }

    private final Stage stage;
    private final boolean enforceIsClosed;

    private RTBoundValidator(Stage stage, boolean enforceIsClosed)
    {
        this.stage = stage;
        this.enforceIsClosed = enforceIsClosed;
    }

    public static UnfilteredPartitionIterator validate(UnfilteredPartitionIterator partitions, Stage stage, boolean enforceIsClosed)
    {
        return Transformation.apply(partitions, new RTBoundValidator(stage, enforceIsClosed));
    }

    public static UnfilteredRowIterator validate(UnfilteredRowIterator partition, Stage stage, boolean enforceIsClosed)
    {
        return Transformation.apply(partition, new RowsTransformation(stage, partition, enforceIsClosed));
    }

    @Override
    public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        return Transformation.apply(partition, new RowsTransformation(stage, partition, enforceIsClosed));
    }

    private final static class RowsTransformation extends Transformation
    {
        private final Stage stage;
        private final boolean enforceIsClosed;
        private final UnfilteredRowIterator partition;

        private DeletionTime openMarkerDeletionTime;

        private RowsTransformation(Stage stage, UnfilteredRowIterator partition, boolean enforceIsClosed)
        {
            this.stage = stage;
            this.partition = partition;
            this.enforceIsClosed = enforceIsClosed;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            if (null == openMarkerDeletionTime)
            {
                 // there is no open RT in the stream - we are expecting a *_START_BOUND
                if (marker.isClose(partition.isReverseOrder()))
                    throw ise("unexpected end bound or boundary " + marker.toString(partition.metadata()));
            }
            else
            {
                // there is an open RT in the stream - we are expecting a *_BOUNDARY or an *_END_BOUND
                if (!marker.isClose(partition.isReverseOrder()))
                    throw ise("start bound followed by another start bound " + marker.toString(partition.metadata()));

                // deletion times of open/close markers must match
                DeletionTime deletionTime = marker.closeDeletionTime(partition.isReverseOrder());
                if (!deletionTime.equals(openMarkerDeletionTime))
                    throw ise("open marker and close marker have different deletion times, close=" + deletionTime);

                openMarkerDeletionTime = null;
            }

            if (marker.isOpen(partition.isReverseOrder()))
                openMarkerDeletionTime = marker.openDeletionTime(partition.isReverseOrder());

            return marker;
        }

        @Override
        public void onPartitionClose()
        {
            if (enforceIsClosed && null != openMarkerDeletionTime)
                throw ise("expected all RTs to be closed, but the last one is open");
        }

        private IllegalStateException ise(String why)
        {
            throw new IllegalStateException(message(why));
        }

        private String message(String why)
        {
            return String.format("%s UnfilteredRowIterator for %s (key: %s omdt: [%s]) has an illegal RT bounds sequence: %s",
                                 stage,
                                 partition.metadata(),
                                 partition.metadata().partitionKeyType.getString(partition.partitionKey().getKey()),
                                 Objects.toString(openMarkerDeletionTime, "not present"),
                                 why);
        }
    }
}
