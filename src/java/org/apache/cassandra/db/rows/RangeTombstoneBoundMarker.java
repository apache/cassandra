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

import java.util.Objects;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * A range tombstone marker that indicates the bound of a range tombstone (start or end).
 */
public class RangeTombstoneBoundMarker extends AbstractRangeTombstoneMarker<ClusteringBound<?>>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RangeTombstoneBoundMarker(new ArrayClusteringBound(ClusteringPrefix.Kind.INCL_START_BOUND, AbstractArrayClusteringPrefix.EMPTY_VALUES_ARRAY), null));

    private final DeletionTime deletion;

    public RangeTombstoneBoundMarker(ClusteringBound<?> bound, DeletionTime deletion)
    {
        super(bound);
        this.deletion = deletion;
    }

    public static <V> RangeTombstoneBoundMarker inclusiveOpen(boolean reversed, V[] boundValues, ValueAccessor<V> accessor,  DeletionTime deletion)
    {
        ClusteringBound<V> bound = accessor.factory().inclusiveOpen(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static <V> RangeTombstoneBoundMarker inclusiveOpen(boolean reversed, ClusteringPrefix<V> from,  DeletionTime deletion)
    {
        return inclusiveOpen(reversed, from.getRawValues(), from.accessor(), deletion);
    }

    public static <V> RangeTombstoneBoundMarker exclusiveOpen(boolean reversed, V[] boundValues, ValueAccessor<V> accessor, DeletionTime deletion)
    {
        ClusteringBound<V> bound = accessor.factory().exclusiveOpen(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static <V> RangeTombstoneBoundMarker exclusiveOpen(boolean reversed, ClusteringPrefix<V> from, DeletionTime deletion)
    {
        return exclusiveOpen(reversed, from.getRawValues(), from.accessor(), deletion);
    }

    public static <V> RangeTombstoneBoundMarker inclusiveClose(boolean reversed, V[] boundValues, ValueAccessor<V> accessor, DeletionTime deletion)
    {
        ClusteringBound<V> bound = accessor.factory().inclusiveClose(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static <V> RangeTombstoneBoundMarker inclusiveClose(boolean reversed, ClusteringPrefix<V> from, DeletionTime deletion)
    {
        return inclusiveClose(reversed, from.getRawValues(), from.accessor(), deletion);
    }

    public static <V> RangeTombstoneBoundMarker exclusiveClose(boolean reversed, V[] boundValues, ValueAccessor<V> accessor, DeletionTime deletion)
    {
        ClusteringBound<V> bound = accessor.factory().exclusiveClose(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static <V> RangeTombstoneBoundMarker exclusiveClose(boolean reversed, ClusteringPrefix<V> from, DeletionTime deletion)
    {
        return exclusiveClose(reversed, from.getRawValues(), from.accessor(), deletion);
    }

    public boolean isBoundary()
    {
        return false;
    }

    public boolean hasInvalidDeletions()
    {
        return !deletionTime().validate();
    }

    /**
     * The deletion time for the range tombstone this is a bound of.
     */
    public DeletionTime deletionTime()
    {
        return deletion;
    }

    public DeletionTime openDeletionTime(boolean reversed)
    {
        if (!isOpen(reversed))
            throw new IllegalStateException();
        return deletion;
    }

    public DeletionTime closeDeletionTime(boolean reversed)
    {
        if (isOpen(reversed))
            throw new IllegalStateException();
        return deletion;
    }

    public boolean openIsInclusive(boolean reversed)
    {
        if (!isOpen(reversed))
            throw new IllegalStateException();
        return bound.isInclusive();
    }

    public boolean closeIsInclusive(boolean reversed)
    {
        if (isOpen(reversed))
            throw new IllegalStateException();
        return bound.isInclusive();
    }

    public ClusteringBound<?> openBound(boolean reversed)
    {
        return isOpen(reversed) ? clustering() : null;
    }

    public ClusteringBound<?> closeBound(boolean reversed)
    {
        return isClose(reversed) ? clustering() : null;
    }

    @Override
    public RangeTombstoneBoundMarker clone(ByteBufferCloner cloner)
    {
        return new RangeTombstoneBoundMarker(clustering().clone(cloner), deletion);
    }

    public RangeTombstoneBoundMarker withNewOpeningDeletionTime(boolean reversed, DeletionTime newDeletionTime)
    {
        if (!isOpen(reversed))
            throw new IllegalStateException();

        return new RangeTombstoneBoundMarker(clustering(), newDeletionTime);
    }

    public void digest(Digest digest)
    {
        bound.digest(digest);
        deletion.digest(digest);
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + deletion.unsharedHeapSize();
    }

    public String toString(TableMetadata metadata)
    {
        return String.format("Marker %s@%d/%d", bound.toString(metadata), deletion.markedForDeleteAt(), deletion.localDeletionTime());
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof RangeTombstoneBoundMarker))
            return false;

        RangeTombstoneBoundMarker that = (RangeTombstoneBoundMarker)other;
        return this.bound.equals(that.bound)
            && this.deletion.equals(that.deletion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bound, deletion);
    }
}

