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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;

import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UniqueComparator;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * Builder that allows to build multiple {@link Clustering}/{@link ClusteringBound} at the same time.
 * Builds a set of clusterings incrementally, by computing cartesian products of
 * sets of values present in each statement restriction. The typical use of this builder is as follows:
 * <ol>
 * <li>Call {@link MultiClusteringBuilder#extend(ClusteringElements, List)} or {@link MultiClusteringBuilder#extend(List, List)}
 * method once per each restriction. Slice restrictons, if they exist, must be added last.
 * <li>Finally, call {@link MultiClusteringBuilder#build()} or {@link MultiClusteringBuilder#buildBound(boolean)} method
 * to obtain the set of clusterings / clustering bounds.</li>
 * </ol>
 * <p>
 * Important: When dealing with slices, you likely want the number of start and end bounds to match.
 * If some columns are restricted from one side only, you can use the special {@link ClusteringElements#BOTTOM} or
 * {@link ClusteringElements#TOP} values to generate a proper clustering bound for the "unbounded"
 * side of the restriction.
 * </p>
 * <h1>Example</h1>
 * <p>
 *
 * Imagine we have a CQL query with multiple restrictions joined by AND:
 * <pre>
 * SELECT * FROM tab
 * WHERE a IN (a1, a2)
 *   AND b IN (b1, b2, b3)
 *   AND c > c1
 * </pre>
 * <p>
 * We need to generate a list of clustering bounds that will be used to fetch proper contiguous chunks of the partition.
 *
 * <p>
 * The builder initial state is a single empty clustering, denoted by the {@code ROOT} constant,
 * which is a natural zero element of cartesian set multiplication. This significantly simplifies the logic.
 * <pre>
 * point: ()
 * </pre>
 *
 * After adding the IN restriction on column {@code a} we get 2 points:
 * <pre>
 * point: (a1)
 * point: (a2)
 * </pre>
 *
 * Next when we add the IN restrction on column {@code b}, we get a cartesian product of all values
 * of {@code a} with all values of {@code b}:
 * <pre>
 * point: (a1, b1)
 * point: (a1, b2)
 * point: (a1, b3)
 * point: (a2, b1)
 * point: (a2, b2)
 * point: (a2, b3)
 * </pre>
 *
 * Finally, we add the slice of column {@code c} by specifying the lower and upper bound
 * (we use {@code TOP} for the upper bound), and we get the final set of clustering bounds:
 * <pre>
 * excl start: (a1, b1, c1)
 * incl end:   (a1, b1)
 * excl start: (a1, b2, c1)
 * incl end:   (a1, b2)
 * excl start: (a1, b3, c1)
 * incl end:   (a1, b3)
 * excl start: (a2, b1, c1)
 * incl end:   (a2, b1)
 * excl start: (a2, b2, c1)
 * incl end:   (a2, b2)
 * excl start: (a2, b3, c1)
 * incl end:   (a2, b3)
 * </pre>
 */
public class MultiClusteringBuilder
{
    /**
     * Represents a building block of a clustering.
     * Either a point or a bound.
     * Can consist of multiple column values.
     *
     * <p>
     * For bounds, it additionally stores the inclusiveness of a bound and whether it is start or end, so that
     * it is possible to mix bounds of different inclusiveness.
     */
    public static class ClusteringElements
    {
        public enum Kind
        {
            POINT, INCL_START, EXCL_START, INCL_END, EXCL_END
        }

        public static ClusteringElements BOTTOM = new ClusteringElements(Collections.emptyList(), Kind.INCL_START);
        public static ClusteringElements TOP = new ClusteringElements(Collections.emptyList(), Kind.INCL_END);
        public static ClusteringElements ROOT = new ClusteringElements(Collections.emptyList(), Kind.POINT);

        final List<ByteBuffer> values;
        final Kind kind;


        private ClusteringElements(List<ByteBuffer> values, Kind kind)
        {
            this.values = values;
            this.kind = kind;
        }

        public static ClusteringElements point(ByteBuffer value)
        {
            return point(Collections.singletonList(value));
        }

        public static ClusteringElements point(List<ByteBuffer> values)
        {
            return new ClusteringElements(values, Kind.POINT);
        }

        public static ClusteringElements bound(ByteBuffer value, Bound bound, boolean inclusive)
        {
            return bound(Collections.singletonList(value), bound, inclusive);
        }

        public static ClusteringElements bound(List<ByteBuffer> values, Bound bound, boolean inclusive)
        {
            Kind kind = bound.isStart()
                    ? (inclusive ? Kind.INCL_START : Kind.EXCL_START)
                    : (inclusive ? Kind.INCL_END : Kind.EXCL_END);
            return new ClusteringElements(values, kind);
        }

        public  boolean isBound()
        {
            return kind != Kind.POINT;
        }

        public boolean isStart()
        {
            return kind == ClusteringElements.Kind.EXCL_START ||
                   kind == ClusteringElements.Kind.INCL_START;
        }

        public boolean isInclusive()
        {
            return kind == Kind.INCL_START ||
                   kind == Kind.INCL_END ||
                   kind == Kind.POINT;
        }

        public String toString()
        {
            return "Element{" +
                   "kind=" + kind +
                   ", value=" + values +
                   '}';
        }
    }

    /**
     * The table comparator.
     */
    private final ClusteringComparator comparator;

    /**
     * Columns corresponding to the already added elements.
     */
    private final List<ColumnMetadata> columns = new ArrayList<>();

    /**
     * The elements of the clusterings.
     */
    private List<ClusteringElements> clusterings = Collections.singletonList(ClusteringElements.ROOT);


    /**
     * <code>true</code> if the clusterings have been build, <code>false</code> otherwise.
     */
    private boolean built;

    /**
     * <code>true</code> if the clusterings contains some <code>null</code> elements.
     */
    private boolean containsNull;

    /**
     * <code>true</code> if the composites contains some <code>unset</code> elements.
     */
    private boolean containsUnset;

    /**
     * <code>true</code> if the composites contains some slice bound elements.
     */
    private boolean containsSliceBound;


    private MultiClusteringBuilder(ClusteringComparator comparator)
    {
        this.comparator = comparator;
    }

    /**
     * Creates a new empty {@code MultiCBuilder}.
     */
    public static MultiClusteringBuilder create(ClusteringComparator comparator)
    {
        return new MultiClusteringBuilder(comparator);
    }


    protected void checkUpdateable()
    {
        if (!hasRemaining() || built)
            throw new IllegalStateException("This builder cannot be updated anymore");
        if (containsSliceBound)
            throw new IllegalStateException("Cannot extend clustering that contains a slice bound");
    }

    /**
     * Returns the number of elements that can be added to the clusterings.
     *
     * @return the number of elements that can be added to the clusterings.
     */
    public int remainingCount()
    {
        return comparator.size() - columns.size();
    }


    /**
     * Checks if the clusterings contains null elements.
     *
     * @return <code>true</code> if the clusterings contains <code>null</code> elements, <code>false</code> otherwise.
     */
    public boolean containsNull()
    {
        return containsNull;
    }

    /**
     * Checks if the clusterings contains unset elements.
     *
     * @return <code>true</code> if the clusterings contains <code>unset</code> elements, <code>false</code> otherwise.
     */
    public boolean containsUnset()
    {
        return containsUnset;
    }

    /**
     * Returns the current number of results when {@link #build()} is called
     *
     * @return the current number of build results
     */
    public int buildSize()
    {
        return clusterings.size();
    }

    /**
     * Returns <code>true</code> if the current number of build results is zero.
     */
    public boolean buildIsEmpty()
    {
        return clusterings.isEmpty();
    }

    /**
     * Checks if some elements can still be added to the clusterings.
     *
     * @return <code>true</code> if it is possible to add more elements to the clusterings, <code>false</code> otherwise.
     */
    public boolean hasRemaining()
    {
        return remainingCount() > 0;
    }


    /**
     * Extends each clustering with the given element(s).
     *
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add D will result in the
     * clusterings A-B-D and A-C-D.
     * </p>
     *
     * @param suffix the element to add
     * @param suffixColumns column definitions in the element; must match the subsequent comparator subtypes
     * @return this <code>CompositeBuilder</code>
     */
    public final MultiClusteringBuilder extend(ClusteringElements suffix, List<ColumnMetadata> suffixColumns)
    {
        return extend(Collections.singletonList(suffix), suffixColumns);
    }

    /**
     * Adds individually each of the specified elements to the end of all the existing clusterings.
     * The number of result clusterings is the product of the number of current clusterings and the number
     * of elements added.
     *
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add D and E will result in the 4
     * clusterings: A-B-D, A-B-E, A-C-D and A-C-E.
     * </p>
     *
     * <p>
     * Added elements can be composites as well.
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [[D, E], [F, G]] will result in
     * 4 composites: A-B-D-E, A-B-F-G, A-C-D-E and A-C-F-G.
     * </p>
     *
     * @param suffixes the elements to add
     * @param suffixColumns column definitions in each element; must match the subsequent comparator subtypes
     * @return this <code>CompositeBuilder</code>
     */
    public MultiClusteringBuilder extend(List<ClusteringElements> suffixes, List<ColumnMetadata> suffixColumns)
    {
        checkUpdateable();

        for (int i = 0; i < suffixColumns.size(); i++)
        {
            AbstractType<?> expectedType = comparator.subtype(columns.size() + i);
            AbstractType<?> actualType = suffixColumns.get(i).type;
            if (!actualType.equals(expectedType))
            {
                throw new IllegalStateException(
                        String.format("Unexpected column type %s != %s.", actualType, expectedType));
            }
        }

        for (ClusteringElements suffix: suffixes)
        {
            if (suffix.kind != ClusteringElements.Kind.POINT)
                containsSliceBound = true;
            if (suffix.values.contains(null))
                containsNull = true;
            // Cannot use `value.contains(UNSET_BYTE_BUFFER)`
            // because UNSET_BYTE_BUFFER.equals(EMPTY_BYTE_BUFFER) but UNSET_BYTE_BUFFER != EMPTY_BYTE_BUFFER
            if (suffix.values.stream().anyMatch(b -> b == ByteBufferUtil.UNSET_BYTE_BUFFER))
                containsUnset = true;
        }

        this.clusterings = columns.isEmpty() ? suffixes : cartesianProduct(clusterings, suffixes);
        this.columns.addAll(suffixColumns);

        assert columns.size() <= comparator.size();
        return this;
    }

    private static ArrayList<ClusteringElements> cartesianProduct(List<ClusteringElements> prefixes, List<ClusteringElements> suffixes)
    {
        ArrayList<ClusteringElements> newElements = new ArrayList<>(prefixes.size() * suffixes.size());
        for (ClusteringElements prefix: prefixes)
        {
            for (ClusteringElements suffix: suffixes)
            {
                List<ByteBuffer> newValue = new ArrayList<>(prefix.values.size() + suffix.values.size());
                newValue.addAll(prefix.values);
                newValue.addAll(suffix.values);
                newElements.add(new ClusteringElements(newValue, suffix.kind));
            }
        }
        assert newElements.size() == prefixes.size() * suffixes.size();
        return newElements;
    }

    /**
     * Builds the <code>clusterings</code>.
     * This cannot be used if slice restrictions were added.
     */
    public NavigableSet<Clustering<?>> build()
    {
        built = true;

        ClusteringBuilder builder = ClusteringBuilder.create(comparator);
        BTreeSet.Builder<Clustering<?>> set = BTreeSet.builder(builder.comparator());
        for (ClusteringElements element: clusterings)
        {
            assert element.kind == ClusteringElements.Kind.POINT : String.format("Not a point: %s", element);
            set.add(builder.buildWith(element.values));
        }
        return set.build();
    }

    /**
     * Builds the <code>ClusteringBound</code>s for slice restrictions.
     * The number of start bounds equals the number of end bounds.
     *
     * @param isStart if true, start bounds are returned, otherwise end bounds are returned
     */
    public NavigableSet<ClusteringBound<?>> buildBound(boolean isStart)
    {
        built = true;
        ClusteringBuilder builder = ClusteringBuilder.create(comparator);

        // Use UniqueComparator to allow duplicates.
        // We deal with start bounds and end bounds separately, so it is a bad idea to lose duplicates,
        // as this would cause the number of start bounds differ from the number of end bounds, if accidentally
        // two bounds on one end collide but their corresponding bounds on the other end do not.
        BTreeSet.Builder<org.apache.cassandra.db.ClusteringBound<?>> set = BTreeSet.builder(new UniqueComparator<>(comparator));
        for (ClusteringElements element: clusterings)
        {
            if (element.isBound() && element.isStart() != isStart)
                continue;

            org.apache.cassandra.db.ClusteringBound<?> bound = element.values.isEmpty()
                ? builder.buildBound(isStart, element.isInclusive())
                : builder.buildBoundWith(element.values, isStart, element.isInclusive());

            set.add(bound);
        }
        return set.build();
    }
}

