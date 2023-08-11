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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.MarshalException;

import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.utils.bytecomparable.ByteSource.EXCLUDED;
import static org.apache.cassandra.utils.bytecomparable.ByteSource.NEXT_COMPONENT;
import static org.apache.cassandra.utils.bytecomparable.ByteSource.NEXT_COMPONENT_EMPTY;
import static org.apache.cassandra.utils.bytecomparable.ByteSource.NEXT_COMPONENT_EMPTY_REVERSED;
import static org.apache.cassandra.utils.bytecomparable.ByteSource.NEXT_COMPONENT_NULL;
import static org.apache.cassandra.utils.bytecomparable.ByteSource.TERMINATOR;

/**
 * A comparator of clustering prefixes (or more generally of {@link Clusterable}}.
 * <p>
 * This is essentially just a composite comparator that the clustering values of the provided
 * clustering prefixes in lexicographical order, with each component being compared based on
 * the type of the clustering column this is a value of.
 */
public class ClusteringComparator implements Comparator<Clusterable>
{
    private final List<AbstractType<?>> clusteringTypes;

    private final Comparator<IndexInfo> indexComparator;
    private final Comparator<IndexInfo> indexReverseComparator;
    private final Comparator<Clusterable> reverseComparator;

    private final Comparator<Row> rowComparator = (r1, r2) -> compare((ClusteringPrefix<?>) r1.clustering(),
                                                                      (ClusteringPrefix<?>) r2.clustering());

    public ClusteringComparator(AbstractType<?>... clusteringTypes)
    {
        this(ImmutableList.copyOf(clusteringTypes));
    }

    public ClusteringComparator(Iterable<AbstractType<?>> clusteringTypes)
    {
        // copy the list to ensure despatch is monomorphic
        this.clusteringTypes = ImmutableList.copyOf(clusteringTypes);

        this.indexComparator = (o1, o2) -> ClusteringComparator.this.compare((ClusteringPrefix<?>) o1.lastName,
                                                                             (ClusteringPrefix<?>) o2.lastName);
        this.indexReverseComparator = (o1, o2) -> ClusteringComparator.this.compare((ClusteringPrefix<?>) o1.firstName,
                                                                                    (ClusteringPrefix<?>) o2.firstName);
        this.reverseComparator = (c1, c2) -> ClusteringComparator.this.compare(c2, c1);
        for (AbstractType<?> type : clusteringTypes)
            type.checkComparable(); // this should already be enforced by TableMetadata.Builder.addColumn, but we check again for other constructors
    }

    /**
     * The number of clustering columns for the table this is the comparator of.
     */
    public int size()
    {
        return clusteringTypes.size();
    }

    /**
     * The "subtypes" of this clustering comparator, that is the types of the clustering
     * columns for the table this is a comparator of.
     */
    public List<AbstractType<?>> subtypes()
    {
        return clusteringTypes;
    }

    /**
     * Returns the type of the ith clustering column of the table.
     */
    public AbstractType<?> subtype(int i)
    {
        return clusteringTypes.get(i);
    }

    /**
     * Creates a row clustering based on the clustering values.
     * <p>
     * Every argument can either be a {@code ByteBuffer}, in which case it is used as-is, or a object
     * corresponding to the type of the corresponding clustering column, in which case it will be
     * converted to a byte buffer using the column type.
     *
     * @param values the values to use for the created clustering. There should be exactly {@code size()}
     * values which must be either byte buffers or of the type the column expect.
     *
     * @return the newly created clustering.
     */
    public Clustering<?> make(Object... values)
    {
        if (values.length != size())
            throw new IllegalArgumentException(String.format("Invalid number of components, expecting %d but got %d", size(), values.length));

        CBuilder builder = CBuilder.create(this);
        for (Object val : values)
        {
            if (val instanceof ByteBuffer)
                builder.add((ByteBuffer) val);
            else
                builder.add(val);
        }
        return builder.build();
    }

    public int compare(Clusterable c1, Clusterable c2)
    {
        return compare((ClusteringPrefix<?>) c1.clustering(), (ClusteringPrefix<?>) c2.clustering());
    }

    public <V1, V2> int compare(ClusteringPrefix<V1> c1, ClusteringPrefix<V2> c2)
    {
        int s1 = c1.size();
        int s2 = c2.size();
        int minSize = Math.min(s1, s2);

        for (int i = 0; i < minSize; i++)
        {
            int cmp = compareComponent(i, c1.get(i), c1.accessor(), c2.get(i), c2.accessor());
            if (cmp != 0)
                return cmp;
        }

        if (s1 == s2)
            return ClusteringPrefix.Kind.compare(c1.kind(), c2.kind());

        return s1 < s2 ? c1.kind().comparedToClustering : -c2.kind().comparedToClustering;
    }

    public <V1, V2> int compare(Clustering<V1> c1, Clustering<V2> c2)
    {
        return compare(c1, c2, size());
    }

    /**
     * Compares the specified part of the specified clusterings.
     *
     * @param c1 the first clustering
     * @param c2 the second clustering
     * @param size the number of components to compare
     * @return a negative integer, zero, or a positive integer as the first argument is less than,
     * equal to, or greater than the second.
     */
    public <V1, V2> int compare(Clustering<V1> c1, Clustering<V2> c2, int size)
    {
        for (int i = 0; i < size; i++)
        {
            int cmp = compareComponent(i, c1.get(i), c1.accessor(), c2.get(i), c2.accessor());
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }

    public <V1, V2> int compareComponent(int i, V1 v1, ValueAccessor<V1> accessor1, V2 v2, ValueAccessor<V2> accessor2)
    {
        if (v1 == null)
            return v2 == null ? 0 : -1;
        if (v2 == null)
            return 1;

        return clusteringTypes.get(i).compare(v1, accessor1, v2, accessor2);
    }

    public <V1, V2> int compareComponent(int i, ClusteringPrefix<V1> v1, ClusteringPrefix<V2> v2)
    {
        return compareComponent(i, v1.get(i), v1.accessor(), v2.get(i), v2.accessor());
    }

    /**
     * Returns whether this clustering comparator is compatible with the provided one,
     * that is if the provided one can be safely replaced by this new one.
     *
     * @param previous the previous comparator that we want to replace and test
     * compatibility with.
     *
     * @return whether {@code previous} can be safely replaced by this comparator.
     */
    public boolean isCompatibleWith(ClusteringComparator previous)
    {
        if (this == previous)
            return true;

        // Extending with new components is fine, shrinking is not
        if (size() < previous.size())
            return false;

        for (int i = 0; i < previous.size(); i++)
        {
            AbstractType<?> tprev = previous.subtype(i);
            AbstractType<?> tnew = subtype(i);
            if (!tnew.isCompatibleWith(tprev))
                return false;
        }
        return true;
    }

    /**
     * Validates the provided prefix for corrupted data.
     *
     * @param clustering the clustering prefix to validate.
     *
     * @throws MarshalException if {@code clustering} contains some invalid data.
     */
    public <T> void validate(ClusteringPrefix<T> clustering)
    {
        ValueAccessor<T> accessor = clustering.accessor();
        for (int i = 0; i < clustering.size(); i++)
        {
            T value = clustering.get(i);
            if (value != null)
                subtype(i).validate(value, accessor);
        }
    }

    /**
     * Produce a prefix-free byte-comparable representation of the given value, i.e. such a sequence of bytes that any
     * pair x, y of valid values of this type
     *   compare(x, y) == compareLexicographicallyUnsigned(asByteComparable(x), asByteComparable(y))
     * and
     *   asByteComparable(x) is not a prefix of asByteComparable(y)
     */
    public <V> ByteComparable asByteComparable(ClusteringPrefix<V> clustering)
    {
        return new ByteComparableClustering<>(clustering);
    }

    /**
     * A prefix-free byte-comparable representation for a clustering or prefix.
     *
     * Adds a NEXT_COMPONENT byte before each component (allowing inclusive/exclusive bounds over incomplete prefixes
     * of that length) and finishes with a suitable byte for the clustering kind. Also deals with null entries.
     *
     * Since all types' encodings are weakly prefix-free, this is guaranteed to be prefix-free as long as the
     * bound/ClusteringPrefix terminators are different from the separator byte. It is okay for the terminator for
     * Clustering to be the same as the separator, as all Clusterings must be completely specified.
     *
     * See also {@link AbstractType#asComparableBytes}.
     *
     * Some examples:
     *    "A", 0005, Clustering     -> 40 4100 40 0005 40
     *    "B", 0006, InclusiveEnd   -> 40 4200 40 0006 60
     *    "A", ExclusiveStart       -> 40 4100 60
     *    "", null, Clustering      -> 40 00 3F 40
     *    "", 0000, Clustering      -> 40 00 40 0000 40
     *    BOTTOM                    -> 20
     */
    private class ByteComparableClustering<V> implements ByteComparable
    {
        private final ClusteringPrefix<V> src;

        ByteComparableClustering(ClusteringPrefix<V> src)
        {
            this.src = src;
        }

        @Override
        public ByteSource asComparableBytes(Version version)
        {
            return new ByteSource()
            {
                private ByteSource current = null;
                private int srcnum = -1;

                @Override
                public int next()
                {
                    if (current != null)
                    {
                        int b = current.next();
                        if (b > END_OF_STREAM)
                            return b;
                        current = null;
                    }

                    int sz = src.size();
                    if (srcnum == sz)
                        return END_OF_STREAM;

                    ++srcnum;
                    if (srcnum == sz)
                        return src.kind().asByteComparableValue(version);

                    final V nextComponent = src.get(srcnum);
                    // We can have a null as the clustering component (this is a relic of COMPACT STORAGE, but also
                    // can appear in indexed partitions with no rows but static content),
                    if (nextComponent == null)
                    {
                        if (version != Version.LEGACY)
                            return NEXT_COMPONENT_NULL; // always sorts before non-nulls, including for reversed types
                        else
                        {
                            // legacy version did not permit nulls in clustering keys and treated these as null values
                            return subtype(srcnum).isReversed() ? NEXT_COMPONENT_EMPTY_REVERSED : NEXT_COMPONENT_EMPTY;
                        }
                    }

                    current = subtype(srcnum).asComparableBytes(src.accessor(), nextComponent, version);
                    // and also null values for some types (e.g. int, varint but not text) that are encoded as empty
                    // buffers.
                    if (current == null)
                        return subtype(srcnum).isReversed() ? NEXT_COMPONENT_EMPTY_REVERSED : NEXT_COMPONENT_EMPTY;

                    return NEXT_COMPONENT;
                }
            };
        }

        public String toString()
        {
            return src.clusteringString(subtypes());
        }
    }

    /**
     * Produces a clustering from the given byte-comparable value. The method will throw an exception if the value
     * does not correctly encode a clustering of this type, including if it encodes a position before or after a
     * clustering (i.e. a bound/boundary).
     *
     * @param accessor Accessor to use to construct components.
     * @param comparable The clustering encoded as a byte-comparable sequence.
     */
    public <V> Clustering<V> clusteringFromByteComparable(ValueAccessor<V> accessor, ByteComparable comparable)
    {
        ByteComparable.Version version = ByteComparable.Version.OSS50;
        ByteSource.Peekable orderedBytes = ByteSource.peekable(comparable.asComparableBytes(version));
        if (orderedBytes == null)
            return null;

        // First check for special cases (partition key only, static clustering) that can do without buffers.
        int sep = orderedBytes.next();
        switch (sep)
        {
        case TERMINATOR:
            assert size() == 0 : "Terminator should be after " + size() + " components, got 0";
            return accessor.factory().clustering();
        case EXCLUDED:
            return accessor.factory().staticClustering();
        default:
            // continue with processing
        }

        int cc = 0;
        V[] components = accessor.createArray(size());

        while (true)
        {
            switch (sep)
            {
            case NEXT_COMPONENT_NULL:
                components[cc] = null;
                break;
            case NEXT_COMPONENT_EMPTY:
            case NEXT_COMPONENT_EMPTY_REVERSED:
                components[cc] = subtype(cc).fromComparableBytes(accessor, null, version);
                break;
            case NEXT_COMPONENT:
                // Decode the next component, consuming bytes from orderedBytes.
                components[cc] = subtype(cc).fromComparableBytes(accessor, orderedBytes, version);
                break;
            case TERMINATOR:
                assert cc == size() : "Terminator should be after " + size() + " components, got " + cc;
                return accessor.factory().clustering(components);
            case EXCLUDED:
                throw new AssertionError("Unexpected static terminator after the first component");
            default:
                throw new AssertionError("Unexpected separator " + Integer.toHexString(sep) + " in Clustering encoding");
            }
            ++cc;
            sep = orderedBytes.next();
        }
    }

    /**
     * Produces a clustering bound from the given byte-comparable value. The method will throw an exception if the value
     * does not correctly encode a bound position of this type, including if it encodes an exact clustering.
     *
     * Note that the encoded clustering position cannot specify the type of bound (i.e. start/end/boundary) because to
     * correctly compare clustering positions the encoding must be the same for the different types (e.g. the position
     * for a exclusive end and an inclusive start is the same, before the exact clustering). The type must be supplied
     * separately (in the bound... vs boundary... call and isEnd argument).
     *
     * @param accessor Accessor to use to construct components.
     * @param comparable The clustering position encoded as a byte-comparable sequence.
     * @param isEnd true if the bound marks the end of a range, false is it marks the start.
     */
    public <V> ClusteringBound<V> boundFromByteComparable(ValueAccessor<V> accessor,
                                                          ByteComparable comparable,
                                                          boolean isEnd)
    {
        ByteComparable.Version version = ByteComparable.Version.OSS50;
        ByteSource.Peekable orderedBytes = ByteSource.peekable(comparable.asComparableBytes(version));

        int sep = orderedBytes.next();
        int cc = 0;
        V[] components = accessor.createArray(size());

        while (true)
        {
            switch (sep)
            {
            case NEXT_COMPONENT_NULL:
                components[cc] = null;
                break;
            case NEXT_COMPONENT_EMPTY:
            case NEXT_COMPONENT_EMPTY_REVERSED:
                components[cc] = subtype(cc).fromComparableBytes(accessor, null, version);
                break;
            case NEXT_COMPONENT:
                // Decode the next component, consuming bytes from orderedBytes.
                components[cc] = subtype(cc).fromComparableBytes(accessor, orderedBytes, version);
                break;
            case ByteSource.LT_NEXT_COMPONENT:
                return accessor.factory().bound(isEnd ? ClusteringPrefix.Kind.EXCL_END_BOUND
                                                      : ClusteringPrefix.Kind.INCL_START_BOUND,
                                                Arrays.copyOf(components, cc));
            case ByteSource.GT_NEXT_COMPONENT:
                return accessor.factory().bound(isEnd ? ClusteringPrefix.Kind.INCL_END_BOUND
                                                      : ClusteringPrefix.Kind.EXCL_START_BOUND,
                                                Arrays.copyOf(components, cc));

            case ByteSource.LTLT_NEXT_COMPONENT:
            case ByteSource.GTGT_NEXT_COMPONENT:
                throw new AssertionError("Unexpected sstable lower/upper bound - byte comparable representation of artificial sstable bounds is not supported");

            default:
                throw new AssertionError("Unexpected separator " + Integer.toHexString(sep) + " in ClusteringBound encoding");
            }
            ++cc;
            sep = orderedBytes.next();
        }
    }

    /**
     * Produces a clustering boundary from the given byte-comparable value. The method will throw an exception if the
     * value does not correctly encode a bound position of this type, including if it encodes an exact clustering.
     *
     * Note that the encoded clustering position cannot specify the type of bound (i.e. start/end/boundary) because to
     * correctly compare clustering positions the encoding must be the same for the different types (e.g. the position
     * for a exclusive end and an inclusive start is the same, before the exact clustering). The type must be supplied
     * separately (in the bound... vs boundary... call and isEnd argument).
     *
     * @param accessor Accessor to use to construct components.
     * @param comparable The clustering position encoded as a byte-comparable sequence.
     */
    public <V> ClusteringBoundary<V> boundaryFromByteComparable(ValueAccessor<V> accessor, ByteComparable comparable)
    {
        ByteComparable.Version version = ByteComparable.Version.OSS50;
        ByteSource.Peekable orderedBytes = ByteSource.peekable(comparable.asComparableBytes(version));

        int sep = orderedBytes.next();
        int cc = 0;
        V[] components = accessor.createArray(size());

        while (true)
        {
            switch (sep)
            {
            case NEXT_COMPONENT_NULL:
                components[cc] = null;
                break;
            case NEXT_COMPONENT_EMPTY:
            case NEXT_COMPONENT_EMPTY_REVERSED:
                components[cc] = subtype(cc).fromComparableBytes(accessor, null, version);
                break;
            case NEXT_COMPONENT:
                // Decode the next component, consuming bytes from orderedBytes.
                components[cc] = subtype(cc).fromComparableBytes(accessor, orderedBytes, version);
                break;
            case ByteSource.LT_NEXT_COMPONENT:
                return accessor.factory().boundary(ClusteringPrefix.Kind.EXCL_END_INCL_START_BOUNDARY,
                                                   Arrays.copyOf(components, cc));
            case ByteSource.GT_NEXT_COMPONENT:
                return accessor.factory().boundary(ClusteringPrefix.Kind.INCL_END_EXCL_START_BOUNDARY,
                                                   Arrays.copyOf(components, cc));
            default:
                throw new AssertionError("Unexpected separator " + Integer.toHexString(sep) + " in ClusteringBoundary encoding");
            }
            ++cc;
            sep = orderedBytes.next();
        }
    }

    /**
     * A comparator for rows.
     *
     * A {@code Row} is a {@code Clusterable} so {@code ClusteringComparator} can be used
     * to compare rows directly, but when we know we deal with rows (and not {@code Clusterable} in
     * general), this is a little faster because by knowing we compare {@code Clustering} objects,
     * we know that 1) they all have the same size and 2) they all have the same kind.
     */
    public Comparator<Row> rowComparator()
    {
        return rowComparator;
    }

    public Comparator<IndexInfo> indexComparator(boolean reversed)
    {
        return reversed ? indexReverseComparator : indexComparator;
    }

    public Comparator<Clusterable> reversed()
    {
        return reverseComparator;
    }

    @Override
    public String toString()
    {
        return String.format("comparator(%s)", Joiner.on(", ").join(clusteringTypes));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ClusteringComparator))
            return false;

        ClusteringComparator that = (ClusteringComparator)o;
        return this.clusteringTypes.equals(that.clusteringTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(clusteringTypes);
    }
}