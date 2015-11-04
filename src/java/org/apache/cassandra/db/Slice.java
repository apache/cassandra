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

import org.apache.cassandra.config.CFMetaData;
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
    public static final Slice ALL = new Slice(Bound.BOTTOM, Bound.TOP)
    {
        @Override
        public boolean selects(ClusteringComparator comparator, Clustering clustering)
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

    private final Bound start;
    private final Bound end;

    private Slice(Bound start, Bound end)
    {
        assert start.isStart() && end.isEnd();
        this.start = start;
        this.end = end;
    }

    public static Slice make(Bound start, Bound end)
    {
        if (start == Bound.BOTTOM && end == Bound.TOP)
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

    public static Slice make(Clustering clustering)
    {
        // This doesn't give us what we want with the clustering prefix
        assert clustering != Clustering.STATIC_CLUSTERING;
        ByteBuffer[] values = extractValues(clustering);
        return new Slice(Bound.inclusiveStartOf(values), Bound.inclusiveEndOf(values));
    }

    public static Slice make(Clustering start, Clustering end)
    {
        // This doesn't give us what we want with the clustering prefix
        assert start != Clustering.STATIC_CLUSTERING && end != Clustering.STATIC_CLUSTERING;

        ByteBuffer[] startValues = extractValues(start);
        ByteBuffer[] endValues = extractValues(end);

        return new Slice(Bound.inclusiveStartOf(startValues), Bound.inclusiveEndOf(endValues));
    }

    private static ByteBuffer[] extractValues(ClusteringPrefix clustering)
    {
        ByteBuffer[] values = new ByteBuffer[clustering.size()];
        for (int i = 0; i < clustering.size(); i++)
            values[i] = clustering.get(i);
        return values;
    }

    public Bound start()
    {
        return start;
    }

    public Bound end()
    {
        return end;
    }

    public Bound open(boolean reversed)
    {
        return reversed ? end : start;
    }

    public Bound close(boolean reversed)
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
    public static boolean isEmpty(ClusteringComparator comparator, Slice.Bound start, Slice.Bound end)
    {
        assert start.isStart() && end.isEnd();
        return comparator.compare(end, start) < 0;
    }

    /**
     * Returns whether a given clustering is selected by this slice.
     *
     * @param comparator the comparator for the table this is a slice of.
     * @param clustering the clustering to test inclusion of.
     *
     * @return whether {@code clustering} is selected by this slice.
     */
    public boolean selects(ClusteringComparator comparator, Clustering clustering)
    {
        return comparator.compare(start, clustering) <= 0 && comparator.compare(clustering, end) <= 0;
    }

    /**
     * Returns whether a given bound is included in this slice.
     *
     * @param comparator the comparator for the table this is a slice of.
     * @param bound the bound to test inclusion of.
     *
     * @return whether {@code bound} is within the bounds of this slice.
     */
    public boolean includes(ClusteringComparator comparator, Bound bound)
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
    public Slice forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive, boolean reversed)
    {
        if (lastReturned == null)
            return this;

        if (reversed)
        {
            int cmp = comparator.compare(lastReturned, start);
            if (cmp < 0 || (!inclusive && cmp == 0))
                return null;

            cmp = comparator.compare(end, lastReturned);
            if (cmp < 0 || (inclusive && cmp == 0))
                return this;

            ByteBuffer[] values = extractValues(lastReturned);
            return new Slice(start, inclusive ? Bound.inclusiveEndOf(values) : Bound.exclusiveEndOf(values));
        }
        else
        {
            int cmp = comparator.compare(end, lastReturned);
            if (cmp < 0 || (!inclusive && cmp == 0))
                return null;

            cmp = comparator.compare(lastReturned, start);
            if (cmp < 0 || (inclusive && cmp == 0))
                return this;

            ByteBuffer[] values = extractValues(lastReturned);
            return new Slice(inclusive ? Bound.inclusiveStartOf(values) : Bound.exclusiveStartOf(values), end);
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
        // If this slice start after max or end before min, it can't intersect
        if (start.compareTo(comparator, maxClusteringValues) > 0 || end.compareTo(comparator, minClusteringValues) < 0)
            return false;

        // We could safely return true here, but there's a minor optimization: if the first component
        // of the slice is restricted to a single value (typically the slice is [4:5, 4:7]), we can
        // check that the second component falls within the min/max for that component (and repeat for
        // all components).
        for (int j = 0; j < minClusteringValues.size() && j < maxClusteringValues.size(); j++)
        {
            ByteBuffer s = j < start.size() ? start.get(j) : null;
            ByteBuffer f = j < end.size() ? end.get(j) : null;

            // we already know the first component falls within its min/max range (otherwise we wouldn't get here)
            if (j > 0 && (j < end.size() && comparator.compareComponent(j, f, minClusteringValues.get(j)) < 0 ||
                        j < start.size() && comparator.compareComponent(j, s, maxClusteringValues.get(j)) > 0))
                return false;

            // if this component isn't equal in the start and finish, we don't need to check any more
            if (j >= start.size() || j >= end.size() || comparator.compareComponent(j, s, f) != 0)
                break;
        }
        return true;
    }

    public String toString(CFMetaData metadata)
    {
        return toString(metadata.comparator);
    }

    public String toString(ClusteringComparator comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(start.isInclusive() ? "[" : "(");
        for (int i = 0; i < start.size(); i++)
        {
            if (i > 0)
                sb.append(':');
            sb.append(comparator.subtype(i).getString(start.get(i)));
        }
        sb.append(", ");
        for (int i = 0; i < end.size(); i++)
        {
            if (i > 0)
                sb.append(':');
            sb.append(comparator.subtype(i).getString(end.get(i)));
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
            Bound.serializer.serialize(slice.start, out, version, types);
            Bound.serializer.serialize(slice.end, out, version, types);
        }

        public long serializedSize(Slice slice, int version, List<AbstractType<?>> types)
        {
            return Bound.serializer.serializedSize(slice.start, version, types)
                 + Bound.serializer.serializedSize(slice.end, version, types);
        }

        public Slice deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
        {
            Bound start = Bound.serializer.deserialize(in, version, types);
            Bound end = Bound.serializer.deserialize(in, version, types);
            return new Slice(start, end);
        }
    }

    /**
     * The bound of a slice.
     * <p>
     * This can be either a start or an end bound, and this can be either inclusive or exclusive.
     */
    public static class Bound extends AbstractBufferClusteringPrefix
    {
        public static final Serializer serializer = new Serializer();

        /**
         * The smallest and biggest bound. Note that as range tomstone bounds are (special case) of slice bounds,
         * we want the BOTTOM and TOP to be the same object, but we alias them here because it's cleaner when dealing
         * with slices to refer to Slice.Bound.BOTTOM and Slice.Bound.TOP.
         */
        public static final Bound BOTTOM = RangeTombstone.Bound.BOTTOM;
        public static final Bound TOP = RangeTombstone.Bound.TOP;

        protected Bound(Kind kind, ByteBuffer[] values)
        {
            super(kind, values);
        }

        public static Bound create(Kind kind, ByteBuffer[] values)
        {
            assert !kind.isBoundary();
            return new Bound(kind, values);
        }

        public static Kind boundKind(boolean isStart, boolean isInclusive)
        {
            return isStart
                 ? (isInclusive ? Kind.INCL_START_BOUND : Kind.EXCL_START_BOUND)
                 : (isInclusive ? Kind.INCL_END_BOUND : Kind.EXCL_END_BOUND);
        }

        public static Bound inclusiveStartOf(ByteBuffer... values)
        {
            return create(Kind.INCL_START_BOUND, values);
        }

        public static Bound inclusiveEndOf(ByteBuffer... values)
        {
            return create(Kind.INCL_END_BOUND, values);
        }

        public static Bound exclusiveStartOf(ByteBuffer... values)
        {
            return create(Kind.EXCL_START_BOUND, values);
        }

        public static Bound exclusiveEndOf(ByteBuffer... values)
        {
            return create(Kind.EXCL_END_BOUND, values);
        }

        public static Bound inclusiveStartOf(ClusteringPrefix prefix)
        {
            ByteBuffer[] values = new ByteBuffer[prefix.size()];
            for (int i = 0; i < prefix.size(); i++)
                values[i] = prefix.get(i);
            return inclusiveStartOf(values);
        }

        public static Bound exclusiveStartOf(ClusteringPrefix prefix)
        {
            ByteBuffer[] values = new ByteBuffer[prefix.size()];
            for (int i = 0; i < prefix.size(); i++)
                values[i] = prefix.get(i);
            return exclusiveStartOf(values);
        }

        public static Bound inclusiveEndOf(ClusteringPrefix prefix)
        {
            ByteBuffer[] values = new ByteBuffer[prefix.size()];
            for (int i = 0; i < prefix.size(); i++)
                values[i] = prefix.get(i);
            return inclusiveEndOf(values);
        }

        public static Bound create(ClusteringComparator comparator, boolean isStart, boolean isInclusive, Object... values)
        {
            CBuilder builder = CBuilder.create(comparator);
            for (Object val : values)
            {
                if (val instanceof ByteBuffer)
                    builder.add((ByteBuffer) val);
                else
                    builder.add(val);
            }
            return builder.buildBound(isStart, isInclusive);
        }

        public Bound withNewKind(Kind kind)
        {
            assert !kind.isBoundary();
            return new Bound(kind, values);
        }

        public boolean isStart()
        {
            return kind().isStart();
        }

        public boolean isEnd()
        {
            return !isStart();
        }

        public boolean isInclusive()
        {
            return kind == Kind.INCL_START_BOUND || kind == Kind.INCL_END_BOUND;
        }

        public boolean isExclusive()
        {
            return kind == Kind.EXCL_START_BOUND || kind == Kind.EXCL_END_BOUND;
        }

        /**
         * Returns the inverse of the current bound.
         * <p>
         * This invert both start into end (and vice-versa) and inclusive into exclusive (and vice-versa).
         *
         * @return the invert of this bound. For instance, if this bound is an exlusive start, this return
         * an inclusive end with the same values.
         */
        public Slice.Bound invert()
        {
            return withNewKind(kind().invert());
        }

        // For use by intersects, it's called with the sstable bound opposite to the slice bound
        // (so if the slice bound is a start, it's call with the max sstable bound)
        private int compareTo(ClusteringComparator comparator, List<ByteBuffer> sstableBound)
        {
            for (int i = 0; i < sstableBound.size(); i++)
            {
                // Say the slice bound is a start. It means we're in the case where the max
                // sstable bound is say (1:5) while the slice start is (1). So the start
                // does start before the sstable end bound (and intersect it). It's the exact
                // inverse with a end slice bound.
                if (i >= size())
                    return isStart() ? -1 : 1;

                int cmp = comparator.compareComponent(i, get(i), sstableBound.get(i));
                if (cmp != 0)
                    return cmp;
            }

            // Say the slice bound is a start. I means we're in the case where the max
            // sstable bound is say (1), while the slice start is (1:5). This again means
            // that the slice start before the end bound.
            if (size() > sstableBound.size())
                return isStart() ? -1 : 1;

            // The slice bound is equal to the sstable bound. Results depends on whether the slice is inclusive or not
            return isInclusive() ? 0 : (isStart() ? 1 : -1);
        }

        public String toString(CFMetaData metadata)
        {
            return toString(metadata.comparator);
        }

        public String toString(ClusteringComparator comparator)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(kind()).append('(');
            for (int i = 0; i < size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(comparator.subtype(i).getString(get(i)));
            }
            return sb.append(')').toString();
        }

        /**
         * Serializer for slice bounds.
         * <p>
         * Contrarily to {@code Clustering}, a slice bound can be a true prefix of the full clustering, so we actually record
         * its size.
         */
        public static class Serializer
        {
            public void serialize(Slice.Bound bound, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
            {
                out.writeByte(bound.kind().ordinal());
                out.writeShort(bound.size());
                ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, version, types);
            }

            public long serializedSize(Slice.Bound bound, int version, List<AbstractType<?>> types)
            {
                return 1 // kind ordinal
                     + TypeSizes.sizeof((short)bound.size())
                     + ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(bound, version, types);
            }

            public Slice.Bound deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
            {
                Kind kind = Kind.values()[in.readByte()];
                return deserializeValues(in, kind, version, types);
            }

            public Slice.Bound deserializeValues(DataInputPlus in, Kind kind, int version, List<AbstractType<?>> types) throws IOException
            {
                int size = in.readUnsignedShort();
                if (size == 0)
                    return kind.isStart() ? BOTTOM : TOP;

                ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types);
                return Slice.Bound.create(kind, values);
            }
        }
    }
}
