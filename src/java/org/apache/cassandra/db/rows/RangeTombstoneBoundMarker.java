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
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * A range tombstone marker that indicates the bound of a range tombstone (start or end).
 */
public class RangeTombstoneBoundMarker extends AbstractRangeTombstoneMarker<ClusteringBound>
{
    private final DeletionTime deletion;

    public RangeTombstoneBoundMarker(ClusteringBound bound, DeletionTime deletion)
    {
        super(bound);
        this.deletion = deletion;
    }

    public static RangeTombstoneBoundMarker inclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        ClusteringBound bound = ClusteringBound.inclusiveOpen(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static RangeTombstoneBoundMarker exclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        ClusteringBound bound = ClusteringBound.exclusiveOpen(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static RangeTombstoneBoundMarker inclusiveClose(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        ClusteringBound bound = ClusteringBound.inclusiveClose(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static RangeTombstoneBoundMarker exclusiveClose(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        ClusteringBound bound = ClusteringBound.exclusiveClose(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public boolean isBoundary()
    {
        return false;
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

    public ClusteringBound openBound(boolean reversed)
    {
        return isOpen(reversed) ? clustering() : null;
    }

    public ClusteringBound closeBound(boolean reversed)
    {
        return isClose(reversed) ? clustering() : null;
    }

    public RangeTombstoneBoundMarker copy(AbstractAllocator allocator)
    {
        return new RangeTombstoneBoundMarker(clustering().copy(allocator), deletion);
    }

    public RangeTombstoneBoundMarker withNewOpeningDeletionTime(boolean reversed, DeletionTime newDeletionTime)
    {
        if (!isOpen(reversed))
            throw new IllegalStateException();

        return new RangeTombstoneBoundMarker(clustering(), newDeletionTime);
    }

    public void digest(MessageDigest digest)
    {
        bound.digest(digest);
        deletion.digest(digest);
    }

    @Override
    public void digest(MessageDigest digest, Set<ByteBuffer> columnsToExclude)
    {
        digest(digest);
    }

    public String toString(CFMetaData metadata)
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

