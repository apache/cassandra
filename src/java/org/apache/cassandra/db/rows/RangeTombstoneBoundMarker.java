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
 * A range tombstone marker that indicates the bound of a range tombstone (start or end).
 */
public class RangeTombstoneBoundMarker extends AbstractRangeTombstoneMarker
{
    private final DeletionTime deletion;

    public RangeTombstoneBoundMarker(RangeTombstone.Bound bound, DeletionTime deletion)
    {
        super(bound);
        assert bound.kind().isBound();
        this.deletion = deletion;
    }

    public RangeTombstoneBoundMarker(Slice.Bound bound, DeletionTime deletion)
    {
        this(new RangeTombstone.Bound(bound.kind(), bound.getRawValues()), deletion);
    }

    public static RangeTombstoneBoundMarker inclusiveStart(ClusteringPrefix clustering, DeletionTime deletion)
    {
        return new RangeTombstoneBoundMarker(new RangeTombstone.Bound(RangeTombstone.Bound.Kind.INCL_START_BOUND, clustering.getRawValues()), deletion);
    }

    public static RangeTombstoneBoundMarker inclusiveEnd(ClusteringPrefix clustering, DeletionTime deletion)
    {
        return new RangeTombstoneBoundMarker(new RangeTombstone.Bound(RangeTombstone.Bound.Kind.INCL_END_BOUND, clustering.getRawValues()), deletion);
    }

    public static RangeTombstoneBoundMarker inclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        RangeTombstone.Bound bound = RangeTombstone.Bound.inclusiveOpen(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static RangeTombstoneBoundMarker exclusiveOpen(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        RangeTombstone.Bound bound = RangeTombstone.Bound.exclusiveOpen(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static RangeTombstoneBoundMarker inclusiveClose(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        RangeTombstone.Bound bound = RangeTombstone.Bound.inclusiveClose(reversed, boundValues);
        return new RangeTombstoneBoundMarker(bound, deletion);
    }

    public static RangeTombstoneBoundMarker exclusiveClose(boolean reversed, ByteBuffer[] boundValues, DeletionTime deletion)
    {
        RangeTombstone.Bound bound = RangeTombstone.Bound.exclusiveClose(reversed, boundValues);
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

    public boolean isOpen(boolean reversed)
    {
        return bound.kind().isOpen(reversed);
    }

    public boolean isClose(boolean reversed)
    {
        return bound.kind().isClose(reversed);
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

    public void copyTo(RangeTombstoneMarker.Writer writer)
    {
        copyBoundTo(writer);
        writer.writeBoundDeletion(deletion);
        writer.endOfMarker();
    }

    public void digest(MessageDigest digest)
    {
        bound.digest(digest);
        deletion.digest(digest);
    }

    public String toString(CFMetaData metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Marker ");
        sb.append(bound.toString(metadata));
        sb.append("@").append(deletion.markedForDeleteAt());
        return sb.toString();
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

