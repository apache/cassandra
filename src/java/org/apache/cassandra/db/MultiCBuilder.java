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
 * Builder that allow to build multiple Clustering/ClusteringBound at the same time.
 */
public class MultiCBuilder
{
    /**
     * Represents a building block of a clustering.
     * Either a point (single value) or a bound.
     * Knowing the kind of elements greatly simplifies the process of building clustering bounds.
     */
    public static class Element
    {
        public enum Kind
        {
            POINT, INCL_START, EXCL_START, INCL_END, EXCL_END
        }

        public static Element BOTTOM = new Element(Collections.emptyList(), Kind.INCL_START);
        public static Element TOP = new Element(Collections.emptyList(), Kind.INCL_END);
        public static Element ROOT = new Element(Collections.emptyList(), Kind.POINT);

        final List<ByteBuffer> value;
        final Kind kind;


        private Element(List<ByteBuffer> values, Kind kind)
        {
            this.value = values;
            this.kind = kind;
        }

        public static Element point(ByteBuffer value)
        {
            return point(Collections.singletonList(value));
        }

        public static Element point(List<ByteBuffer> value)
        {
            return new Element(value, Kind.POINT);
        }

        public static Element bound(ByteBuffer value, Bound bound, boolean inclusive)
        {
            return bound(Collections.singletonList(value), bound, inclusive);
        }

        public static Element bound(List<ByteBuffer> value, Bound bound, boolean inclusive)
        {
            Kind kind = bound.isStart()
                    ? (inclusive ? Kind.INCL_START : Kind.EXCL_START)
                    : (inclusive ? Kind.INCL_END : Kind.EXCL_END);
            return new Element(value, kind);
        }

        public  boolean isBound()
        {
            return kind != Kind.POINT;
        }

        public boolean isStart()
        {
            return kind == Element.Kind.EXCL_START ||
                   kind == Element.Kind.INCL_START;
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
                   ", value=" + value +
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
    private List<Element> elements = Collections.singletonList(Element.ROOT);


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


    private MultiCBuilder(ClusteringComparator comparator)
    {
        this.comparator = comparator;
    }

    /**
     * Creates a new empty {@code MultiCBuilder}.
     */
    public static MultiCBuilder create(ClusteringComparator comparator)
    {
        return new MultiCBuilder(comparator);
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
        return elements.size();
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
     * Extends each clustering with the given element.
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
    public final MultiCBuilder extend(Element suffix, List<ColumnMetadata> suffixColumns)
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
    public MultiCBuilder extend(List<Element> suffixes, List<ColumnMetadata> suffixColumns)
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

        for (Element suffix: suffixes)
        {
            if (suffix.kind != Element.Kind.POINT)
                containsSliceBound = true;
            if (suffix.value.contains(null))
                containsNull = true;
            // Cannot use `value.contains(UNSET_BYTE_BUFFER)`
            // because UNSET_BYTE_BUFFER.equals(EMPTY_BYTE_BUFFER) but UNSET_BYTE_BUFFER != EMPTY_BYTE_BUFFER
            if (suffix.value.stream().anyMatch(b -> b == ByteBufferUtil.UNSET_BYTE_BUFFER))
                containsUnset = true;
        }

        this.elements = columns.isEmpty() ? suffixes : cartesianProduct(elements, suffixes);
        this.columns.addAll(suffixColumns);

        assert columns.size() <= comparator.size();
        return this;
    }

    private static ArrayList<Element> cartesianProduct(List<Element> prefixes, List<Element> suffixes)
    {
        ArrayList<Element> newElements = new ArrayList<>(prefixes.size() * suffixes.size());
        for (Element prefix: prefixes)
        {
            for (Element suffix: suffixes)
            {
                List<ByteBuffer> newValue = new ArrayList<>(prefix.value.size() + suffix.value.size());
                newValue.addAll(prefix.value);
                newValue.addAll(suffix.value);
                newElements.add(new Element(newValue, suffix.kind));
            }
        }
        assert newElements.size() == prefixes.size() * suffixes.size();
        return newElements;
    }

    /**
     * Builds the <code>clusterings</code>.
     */
    public NavigableSet<Clustering<?>> build()
    {
        built = true;

        CBuilder builder = CBuilder.create(comparator);
        BTreeSet.Builder<Clustering<?>> set = BTreeSet.builder(builder.comparator());
        for (Element element: elements)
        {
            assert element.kind == Element.Kind.POINT : String.format("Not a point: %s", element);
            set.add(builder.buildWith(element.value));
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
        CBuilder builder = CBuilder.create(comparator);

        // Use UniqueComparator to allow duplicates.
        // We deal with start bounds and end bounds separately, so it is a bad idea to lose duplicates,
        // as this would cause the number of start bounds differ from the number of end bounds, if accidentally
        // two bounds on one end collide but their corresponding bounds on the other end do not.
        BTreeSet.Builder<ClusteringBound<?>> set = BTreeSet.builder(new UniqueComparator<>(comparator));
        for (Element element: elements)
        {
            if (element.isBound() && element.isStart() != isStart)
                continue;

            ClusteringBound<?> bound = element.value.isEmpty()
                ? builder.buildBound(isStart, element.isInclusive())
                : builder.buildBoundWith(element.value, isStart, element.isInclusive());

            set.add(bound);
        }
        return set.build();
    }
}

