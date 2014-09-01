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
import java.security.MessageDigest;
import java.util.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;

/**
 * A range tombstone marker that represents a boundary between 2 range tombstones (i.e. it closes one range and open another).
 */
public class RangeTombstoneBoundaryMarker extends AbstractRangeTombstoneMarker
{
    private final DeletionTime endDeletion;
    private final DeletionTime startDeletion;

    public RangeTombstoneBoundaryMarker(RangeTombstone.Bound bound, DeletionTime endDeletion, DeletionTime startDeletion)
    {
        super(bound);
        assert bound.kind().isBoundary();
        this.endDeletion = endDeletion;
        this.startDeletion = startDeletion;
    }

    public static RangeTombstoneBoundaryMarker exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime closeDeletion, DeletionTime openDeletion)
    {
        RangeTombstone.Bound bound = RangeTombstone.Bound.exclusiveCloseInclusiveOpen(reversed, boundValues);
        DeletionTime endDeletion = reversed ? openDeletion : closeDeletion;
        DeletionTime startDeletion = reversed ? closeDeletion : openDeletion;
        return new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion);
    }

    public static RangeTombstoneBoundaryMarker inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime closeDeletion, DeletionTime openDeletion)
    {
        RangeTombstone.Bound bound = RangeTombstone.Bound.inclusiveCloseExclusiveOpen(reversed, boundValues);
        DeletionTime endDeletion = reversed ? openDeletion : closeDeletion;
        DeletionTime startDeletion = reversed ? closeDeletion : openDeletion;
        return new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion);
    }

    public boolean isBoundary()
    {
        return true;
    }

    /**
     * The deletion time for the range tombstone this boundary ends (in clustering order).
     */
    public DeletionTime endDeletionTime()
    {
        return endDeletion;
    }

    /**
     * The deletion time for the range tombstone this boundary starts (in clustering order).
     */
    public DeletionTime startDeletionTime()
    {
        return startDeletion;
    }

    public DeletionTime closeDeletionTime(boolean reversed)
    {
        return reversed ? startDeletion : endDeletion;
    }

    public DeletionTime openDeletionTime(boolean reversed)
    {
        return reversed ? endDeletion : startDeletion;
    }

    public boolean isOpen(boolean reversed)
    {
        // A boundary always open one side
        return true;
    }

    public boolean isClose(boolean reversed)
    {
        // A boundary always close one side
        return true;
    }

    public static boolean isBoundary(ClusteringComparator comparator, Slice.Bound close, Slice.Bound open)
    {
        if (!comparator.isOnSameClustering(close, open))
            return false;

        // If both bound are exclusive, then it's not a boundary, otherwise it is one.
        // Note that most code should never call this with 2 inclusive bound: this would mean we had
        // 2 RTs that were overlapping and RangeTombstoneList don't create that. However, old
        // code was generating that so supporting this case helps dealing with backward compatibility.
        return close.isInclusive() || open.isInclusive();
    }

    // Please note that isBoundary *must* have been called (and returned true) before this is called.
    public static RangeTombstoneBoundaryMarker makeBoundary(boolean reversed, Slice.Bound close, Slice.Bound open, DeletionTime closeDeletion, DeletionTime openDeletion)
    {
        boolean isExclusiveClose = close.isExclusive() || (close.isInclusive() && open.isInclusive() && openDeletion.supersedes(closeDeletion));
        return isExclusiveClose
             ? exclusiveCloseInclusiveOpen(reversed, close.getRawValues(), closeDeletion, openDeletion)
             : inclusiveCloseExclusiveOpen(reversed, close.getRawValues(), closeDeletion, openDeletion);
    }

    public RangeTombstoneBoundMarker createCorrespondingCloseBound(boolean reversed)
    {
        return new RangeTombstoneBoundMarker(bound.withNewKind(bound.kind().closeBoundOfBoundary(reversed)), endDeletion);
    }

    public RangeTombstoneBoundMarker createCorrespondingOpenBound(boolean reversed)
    {
        return new RangeTombstoneBoundMarker(bound.withNewKind(bound.kind().openBoundOfBoundary(reversed)), startDeletion);
    }

    public void copyTo(RangeTombstoneMarker.Writer writer)
    {
        copyBoundTo(writer);
        writer.writeBoundaryDeletion(endDeletion, startDeletion);
        writer.endOfMarker();
    }

    public void digest(MessageDigest digest)
    {
        bound.digest(digest);
        endDeletion.digest(digest);
        startDeletion.digest(digest);
    }

    public String toString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Marker ");
        sb.append(bound.toString(metadata));
        sb.append("@").append(endDeletion.markedForDeleteAt()).append("-").append(startDeletion.markedForDeleteAt());
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof RangeTombstoneBoundaryMarker))
            return false;

        RangeTombstoneBoundaryMarker that = (RangeTombstoneBoundaryMarker)other;
        return this.bound.equals(that.bound)
            && this.endDeletion.equals(that.endDeletion)
            && this.startDeletion.equals(that.startDeletion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bound, endDeletion, startDeletion);
    }
}
