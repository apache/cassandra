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
import java.util.Objects;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;

import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;


/**
 * A range tombstone marker that represents a boundary between 2 range tombstones (i.e. it closes one range and open another).
 */
public class RangeTombstoneBoundaryMarker extends AbstractRangeTombstoneMarker<ClusteringBoundary<?>>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneBoundaryMarker(new ArrayClusteringBoundary(ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY, new byte[][] { new byte[0]}), null, null));

    private final DeletionTime endDeletion;
    private final DeletionTime startDeletion;

    public RangeTombstoneBoundaryMarker(ClusteringBoundary<?> bound, DeletionTime endDeletion, DeletionTime startDeletion)
    {
        super(bound);
        assert bound.isBoundary();
        this.endDeletion = endDeletion;
        this.startDeletion = startDeletion;
    }

    public static <V> RangeTombstoneBoundaryMarker exclusiveCloseInclusiveOpen(boolean reversed,
                                                                               V[] boundValues,
                                                                               ValueAccessor<V> accessor,
                                                                               DeletionTime closeDeletion,
                                                                               DeletionTime openDeletion)
    {
        ClusteringBoundary<V> bound = accessor.factory().exclusiveCloseInclusiveOpen(reversed, boundValues);
        DeletionTime endDeletion = reversed ? openDeletion : closeDeletion;
        DeletionTime startDeletion = reversed ? closeDeletion : openDeletion;
        return new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion);
    }

    public static <V> RangeTombstoneBoundaryMarker exclusiveCloseInclusiveOpen(boolean reversed,
                                                                               ClusteringPrefix<V> from,
                                                                               DeletionTime closeDeletion,
                                                                               DeletionTime openDeletion)
    {
        return exclusiveCloseInclusiveOpen(reversed, from.getRawValues(), from.accessor(), closeDeletion, openDeletion);
    }

    public static <V> RangeTombstoneBoundaryMarker inclusiveCloseExclusiveOpen(boolean reversed,
                                                                               V[] boundValues,
                                                                               ValueAccessor<V> accessor,
                                                                               DeletionTime closeDeletion,
                                                                               DeletionTime openDeletion)
    {
        ClusteringBoundary<V> bound = accessor.factory().inclusiveCloseExclusiveOpen(reversed, boundValues);
        DeletionTime endDeletion = reversed ? openDeletion : closeDeletion;
        DeletionTime startDeletion = reversed ? closeDeletion : openDeletion;
        return new RangeTombstoneBoundaryMarker(bound, endDeletion, startDeletion);
    }


    public static <V> RangeTombstoneBoundaryMarker inclusiveCloseExclusiveOpen(boolean reversed,
                                                                               ClusteringPrefix<V> from,
                                                                               DeletionTime closeDeletion,
                                                                               DeletionTime openDeletion)
    {
        return inclusiveCloseExclusiveOpen(reversed, from.getRawValues(), from.accessor(), closeDeletion, openDeletion);
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

    public boolean openIsInclusive(boolean reversed)
    {
        return (bound.kind() == ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY) ^ reversed;
    }

    public ClusteringBound<?> openBound(boolean reversed)
    {
        return bound.openBound(reversed);
    }

    public ClusteringBound<?> closeBound(boolean reversed)
    {
        return bound.closeBound(reversed);
    }

    public boolean closeIsInclusive(boolean reversed)
    {
        return (bound.kind() == ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY) ^ reversed;
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

    public boolean hasInvalidDeletions()
    {
        return !startDeletion.validate() || !endDeletion.validate();
    }

    @Override
    public RangeTombstoneBoundaryMarker clone(ByteBufferCloner cloner)
    {
        return new RangeTombstoneBoundaryMarker((ClusteringBoundary<ByteBuffer>) clustering().clone(cloner), endDeletion, startDeletion);
    }

    public RangeTombstoneBoundaryMarker withNewOpeningDeletionTime(boolean reversed, DeletionTime newDeletionTime)
    {
        return new RangeTombstoneBoundaryMarker(clustering(),
                                                reversed ? newDeletionTime : endDeletion,
                                                reversed ? startDeletion : newDeletionTime);
    }

    public static <V1, V2> RangeTombstoneBoundaryMarker makeBoundary(boolean reversed,
                                                                     ClusteringBound<V1> close,
                                                                     ClusteringBound<V2> open,
                                                                     DeletionTime closeDeletion,
                                                                     DeletionTime openDeletion)
    {
        assert ClusteringPrefix.Kind.compare(close.kind(), open.kind()) == 0 : "Both bound don't form a boundary";
        boolean isExclusiveClose = close.isExclusive() || (close.isInclusive() && open.isInclusive() && openDeletion.supersedes(closeDeletion));
        return isExclusiveClose
             ? exclusiveCloseInclusiveOpen(reversed, close.getRawValues(), close.accessor(), closeDeletion, openDeletion)
             : inclusiveCloseExclusiveOpen(reversed, close.getRawValues(), close.accessor(), closeDeletion, openDeletion);
    }

    public RangeTombstoneBoundMarker createCorrespondingCloseMarker(boolean reversed)
    {
        return new RangeTombstoneBoundMarker(closeBound(reversed), closeDeletionTime(reversed));
    }

    public RangeTombstoneBoundMarker createCorrespondingOpenMarker(boolean reversed)
    {
        return new RangeTombstoneBoundMarker(openBound(reversed), openDeletionTime(reversed));
    }

    public void digest(Digest digest)
    {
        bound.digest(digest);
        endDeletion.digest(digest);
        startDeletion.digest(digest);
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + startDeletion.unsharedHeapSize() + endDeletion.unsharedHeapSize();
    }

    public String toString(TableMetadata metadata)
    {
        return String.format("Marker %s@%d/%d-%d/%d",
                             bound.toString(metadata),
                             endDeletion.markedForDeleteAt(), endDeletion.localDeletionTime(),
                             startDeletion.markedForDeleteAt(), startDeletion.localDeletionTime());
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
