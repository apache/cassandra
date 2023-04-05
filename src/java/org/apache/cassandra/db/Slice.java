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
package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A slice represents the selection of a range of rows.
 * <p>
 * A slice has a start and an end bound that are both (potentially full) clustering prefixes.
 * A slice selects every rows whose clustering is bigger than the slice start prefix but smaller
 * than the end prefix. Both start and end can be either inclusive or exclusive.
 */
public class Slice
{
    public static final Serializer serializer = new Serializer();

    /** The slice selecting all rows (of a given partition) */
    public static final Slice ALL = new Slice(BufferClusteringBound.BOTTOM, BufferClusteringBound.TOP)
    {
        @Override
        public boolean includes(ClusteringComparator comparator, ClusteringPrefix<?> clustering)
        {
            return true;
        }

        @Override
        public boolean intersects(ClusteringComparator comparator, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
        {
            return true;
        }

        @Override
        public String toString(ClusteringComparator comparator)
        {
            return "ALL";
        }
    };

    private final ClusteringBound<?> start;
    private final ClusteringBound<?> end;

    private Slice(ClusteringBound<?> start, ClusteringBound<?> end)
    {
        assert start.isStart() && end.isEnd();
        this.start = start;
        this.end = end;
    }

    public static Slice make(ClusteringBound<?> start, ClusteringBound<?> end)
    {
        assert start != null && end != null;

        if (start.isBottom() && end.isTop())
            return ALL;

        return new Slice(start, end);
    }

    public static Slice make(ClusteringComparator comparator, Object... values)
    {
        CBuilder builder = CBuilder.create(comparator);
        for (Object val : values)
        {
            if (val instanceof ByteBuffer)
                builder.add((ByteBuffer) val);
            else
                builder.add(val);
        }
        return new Slice(builder.buildBound(true, true), builder.buildBound(false, true));
    }

    public static Slice make(Clustering<?> clustering)
    {
        // This doesn't give us what we want with the clustering prefix
        assert clustering != Clustering.STATIC_CLUSTERING;
        return new Slice(ClusteringBound.inclusiveStartOf(clustering), ClusteringBound.inclusiveEndOf(clustering));
    }

    public static Slice make(Clustering<?> start, Clustering<?> end)
    {
        // This doesn't give us what we want with the clustering prefix
        assert start != Clustering.STATIC_CLUSTERING && end != Clustering.STATIC_CLUSTERING;

        return new Slice(ClusteringBound.inclusiveStartOf(start), ClusteringBound.inclusiveEndOf(end));
    }

    public ClusteringBound<?> start()
    {
        return start;
    }

    public ClusteringBound<?> end()
    {
        return end;
    }

    public ClusteringBound<?> open(boolean reversed)
    {
        return reversed ? end : start;
    }

    public ClusteringBound<?> close(boolean reversed)
    {
        return reversed ? start : end;
    }

    /**
     * Return whether the slice is empty.
     *
     * @param comparator the comparator to compare the bounds.
     * @return whether the slice formed is empty or not.
     */
    public boolean isEmpty(ClusteringComparator comparator)
    {
        return isEmpty(comparator, start(), end());
    }

    /**
     * Return whether the slice formed by the two provided bound is empty or not.
     *
     * @param comparator the comparator to compare the bounds.
     * @param start the start for the slice to consider. This must be a start bound.
     * @param end the end for the slice to consider. This must be an end bound.
     * @return whether the slice formed by {@code start} and {@code end} is
     * empty or not.
     */
    public static boolean isEmpty(ClusteringComparator comparator, ClusteringBound<?> start, ClusteringBound<?> end)
    {
        assert start.isStart() && end.isEnd();

        int cmp = comparator.compare(start, end);

        if (cmp < 0)
            return false;
        else if (cmp > 0)
            return true;
        else
            return start.isExclusive() || end.isExclusive();
    }

    /**
     * Returns whether a given clustering or bound is included in this slice.
     *
     * @param comparator the comparator for the table this is a slice of.
     * @param bound the bound to test inclusion of.
     *
     * @return whether {@code bound} is within the bounds of this slice.
     */
    public boolean includes(ClusteringComparator comparator, ClusteringPrefix<?> bound)
    {
        return comparator.compare(start, bound) <= 0 && comparator.compare(bound, end) <= 0;
    }

    /**
     * Returns a slice for continuing paging from the last returned clustering prefix.
     *
     * @param comparator the comparator for the table this is a filter for.
     * @param lastReturned the last clustering that was returned for the query we are paging for. The
     * resulting slices will be such that only results coming stricly after {@code lastReturned} are returned
     * (where coming after means "greater than" if {@code !reversed} and "lesser than" otherwise).
     * @param inclusive whether or not we want to include the {@code lastReturned} in the newly returned page of results.
     * @param reversed whether the query we're paging for is reversed or not.
     *
     * @return a new slice that selects results coming after {@code lastReturned}, or {@code null} if paging
     * the resulting slice selects nothing (i.e. if it originally selects nothing coming after {@code lastReturned}).
     */
    public Slice forPaging(ClusteringComparator comparator, Clustering<?> lastReturned, boolean inclusive, boolean reversed)
    {
        if (lastReturned == null)
            return this;

        if (reversed)
        {
            int cmp = comparator.compare(lastReturned, start);
            assert cmp != 0;
            // start is > than lastReturned; got nothing to return no more
            if (cmp < 0)
                return null;

            cmp = comparator.compare(end, lastReturned);
            assert cmp != 0;
            if (cmp < 0)
                return this;

            Slice slice = new Slice(start, inclusive ? ClusteringBound.inclusiveEndOf(lastReturned) : ClusteringBound.exclusiveEndOf(lastReturned));
            if (slice.isEmpty(comparator))
                return null;
            return slice;
        }
        else
        {
            int cmp = comparator.compare(end, lastReturned);
            assert cmp != 0;
            if (cmp < 0)
                return null;

            cmp = comparator.compare(lastReturned, start);
            assert cmp != 0;
            if (cmp < 0)
                return this;

            Slice slice = new Slice(inclusive ? ClusteringBound.inclusiveStartOf(lastReturned) : ClusteringBound.exclusiveStartOf(lastReturned), end);
            if (slice.isEmpty(comparator))
                return null;
            return slice;
        }
    }

    /**
     * Given the per-clustering column minimum and maximum value a sstable contains, whether or not this slice potentially
     * intersects that sstable or not.
     *
     * @param comparator the comparator for the table this is a slice of.
     * @param minClusteringValues the smallest values for each clustering column that a sstable contains.
     * @param maxClusteringValues the biggest values for each clustering column that a sstable contains.
     *
     * @return whether the slice might intersects with the sstable having {@code minClusteringValues} and
     * {@code maxClusteringValues}.
     */
    public boolean intersects(ClusteringComparator comparator, List<ByteBuffer> minClusteringValues, List<ByteBuffer> maxClusteringValues)
    {
        // If this slice starts after max clustering or ends before min clustering, it can't intersect
        return start.compareTo(comparator, maxClusteringValues) <= 0 && end.compareTo(comparator, minClusteringValues) >= 0;
    }

    public String toString(ClusteringComparator comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(start.isInclusive() ? "[" : "(");
        for (int i = 0; i < start.size(); i++)
        {
            if (i > 0)
                sb.append(':');
            sb.append(start.stringAt(i, comparator));
        }
        sb.append(", ");
        for (int i = 0; i < end.size(); i++)
        {
            if (i > 0)
                sb.append(':');
            sb.append(end.stringAt(i, comparator));
        }
        sb.append(end.isInclusive() ? "]" : ")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof Slice))
            return false;

        Slice that = (Slice)other;
        return this.start().equals(that.start())
            && this.end().equals(that.end());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start(), end());
    }

    public static class Serializer
    {
        public void serialize(Slice slice, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            ClusteringBound.serializer.serialize(slice.start, out, version, types);
            ClusteringBound.serializer.serialize(slice.end, out, version, types);
        }

        public long serializedSize(Slice slice, int version, List<AbstractType<?>> types)
        {
            return ClusteringBound.serializer.serializedSize(slice.start, version, types)
                 + ClusteringBound.serializer.serializedSize(slice.end, version, types);
        }

        public Slice deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
        {
            ClusteringBound<byte[]> start = (ClusteringBound<byte[]>) ClusteringBound.serializer.deserialize(in, version, types);
            ClusteringBound<byte[]> end = (ClusteringBound<byte[]>) ClusteringBound.serializer.deserialize(in, version, types);
            return new Slice(start, end);
        }
    }
}
