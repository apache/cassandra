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

package org.apache.cassandra.cql3.restrictions;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.BoundType;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * One or more contiguous clusterings elements. A clustering element is composed of a clustering column and its
 * associated value. In practice, that class is not only used for clustering elements but also for partition key elements
 * and tokens expression.
 *
 * <p>There are some differences between how predicates are represented in a CQL query and how they have to be expressed
 * internally. Those differences are:
 * <ul>
 *     <li>The selected partition keys and clustering columns can be expressed with separate predicates in a CQL query
 *     but need to be grouped internally to define exact partition and clustering keys. For example,
 *     <pre>[..] WHERE pk1 = 2 AND pk2 = 3 AND c1 = 5 AND c2 IN (3, 4)</pre> will request 2 rows with clustering (5, 3) and
 *     (5, 4) from the partition (2, 3).</li>
 *     <li>When clustering slices are expressed in a CQL query they are expressed as if all columns were in
 *     ascending order. Internally, the engine handles slice according to the clustering columns real order.
 *     For example, if column c1 is descending and column c2 is ascending the predicate <pre>(c1, c2) >= (1, 2)</pre>
 *     should be translated internally in [(bottom) .. (1, bottom)),[(1, 2, bottom)..(1, top)].</li>
 * </ul>
 * This class is used to bridge the gap between the CQL expression and its internal representation. It allows for elements
 * to be appended together through the {@code extend} method and translate CQL ranges into their internal representation.
 * </p>
 * <p>It is important to realize that Guava {@code Range} instances are used in a slightly different way than how they were
 * designed to be used to deal with the tree model of Clusterings. {@code Range} returned by this class are never
 * unbounded so the methods {@code hasLowerEndpoint} and {@code hasUpperEndpoint} should not be used on those.
 * When a range does not have a lower endpoint its lower endpoint value will be (bottom) (e.g. ClusteringElements.of().bottom())
 * and if it does not have an upper endpoint its upper endpoint value will be (top) (e.g. ClusteringElements.of().top()).
 * Therefore a missing endpoint will always return {@code true} to {@code isEmpty()}.</p>
 */
public class ClusteringElements extends ForwardingList<ByteBuffer> implements Comparable<ClusteringElements>
{
    /**
     * The empty {@code ClusteringElements} instance used to avoid creating unecessary empty instances.
     */
    private static final ClusteringElements EMPTY = new ClusteringElements(ImmutableList.of(), ImmutableList.of());

    /**
     * A range representing all {@code ClusteringElements}.
     */
    private static final Range<ClusteringElements> ALL = Range.closed(EMPTY.bottom(), EMPTY.top());

    /**
     * The elements columns.
     */
    // We use ColumnSpecification to allow support for things like Token expressions that do not match real columns directly.
    private final ImmutableList<? extends ColumnSpecification> columns;

    /**
     * The elements values.
     */
    private final ImmutableList<ByteBuffer> values;

    private ClusteringElements(ImmutableList<? extends ColumnSpecification> columns, ImmutableList<ByteBuffer> values)
    {
        if (columns.size() != values.size())
            throw new IllegalArgumentException("columns and values should have the same size");

        checkColumnsOrder(columns);

        this.columns = columns;
        this.values = values;
    }

    private static void checkColumnsOrder(ImmutableList<? extends ColumnSpecification> columns)
    {
        if (columns.size() > 1)
        {
            // All the columns should be ColumnMetadata for partition key or clustering key
            int offset = ((ColumnMetadata) columns.get(0)).position();
            for (int i = 1, m = columns.size(); i < m; i++)
            {
                if (((ColumnMetadata) columns.get(i)).position() != (offset + i))
                    throw new IllegalArgumentException("columns should have increasing position");
            }
        }
    }

    private AbstractType<?> columnType(int index)
    {
        return columns.get(index).type;
    }

    @Override
    protected List<ByteBuffer> delegate()
    {
        return values;
    }

    /**
     * Returns an empty {@code ClusteringElements}.
     * @return an empty {@code ClusteringElements}.
     */
    public static ClusteringElements of()
    {
        return EMPTY;
    }

    /**
     * Returns a {@code ClusteringElements} with a single element.
     * @param column the element column
     * @param value the element value
     * @return a {@code ClusteringElements} with a single element.
     */
    public static ClusteringElements of(ColumnSpecification column, ByteBuffer value)
    {
        return new ClusteringElements(ImmutableList.of(column), ImmutableList.of(value));
    }

    /**
     * Returns a {@code ClusteringElements} with the specified elements.
     * @param columns the elements columns
     * @param values the elements values
     * @return a {@code ClusteringElements} with the specified elements.
     */
    public static ClusteringElements of(List<? extends ColumnSpecification> columns, List<ByteBuffer> values)
    {
        return new ClusteringElements(ImmutableList.copyOf(columns), ImmutableList.copyOf(values));
    }

    /**
     * Extends this set of elements with the specified ones. This method should only be called on {@code ClusteringElements}
     * corresponding to partition key or clustering elements.
     * @param suffix the elements to append to this ones
     * @return A new {@code ClusteringElements} instance composed of both set of elements
     */
    public ClusteringElements extend(ClusteringElements suffix)
    {
        // We cannot extend a Top or Bottom as those are only used for ranges and ranges endpoint should not be extended
        if (this instanceof Top || this instanceof Bottom)
            throw new UnsupportedOperationException("Range endpoints cannot be extended");

        checkSuffix(suffix);

        ImmutableList<? extends ColumnSpecification> newColumns = concat(columns, suffix.columns);
        ImmutableList<ByteBuffer> newValues = concat(values, suffix.values);

        return suffix instanceof Top ? new Top(newColumns, newValues)
                                     : suffix instanceof Bottom ? new Bottom(newColumns, newValues)
                                                                : new ClusteringElements(newColumns, newValues);
    }

    private void checkSuffix(ClusteringElements suffix)
    {
        // If the columns are not ColumnMetadata instances, we are dealing with a Token representation which cannot be extended
        if (!(columns.get(0) instanceof ColumnMetadata))
            throw new UnsupportedOperationException("Non partition key or clustering columns cannot be extended");

        if (!suffix.isEmpty()) // suffix can be empty if equal to (top) or (bottom)
        {
            ColumnMetadata lastPrefixElement = ((ColumnMetadata) last(this.columns));
            ColumnMetadata firstSuffixElement = ((ColumnMetadata) suffix.columns.get(0));
            if (firstSuffixElement.kind != lastPrefixElement.kind)
                throw new UnsupportedOperationException("Cannot extend elements with elements of a different kind");
            if (firstSuffixElement.position() != lastPrefixElement.position() + 1)
                throw new UnsupportedOperationException("Cannot extend elements with non consecutive elements");
        }
    }

    private static <T> ImmutableList<T> concat(ImmutableList<? extends T> prefix, ImmutableList<? extends T> suffix)
    {
        return ImmutableList.<T>builderWithExpectedSize(prefix.size() + suffix.size())
                            .addAll(prefix)
                            .addAll(suffix)
                            .build();
    }

    /**
     * Returns a {@code RangeSet} representing all the possible {@code ClusteringElements} values.
     * @return a {@code RangeSet} representing all the possible {@code ClusteringElements} values.
     */
    public static RangeSet<ClusteringElements> all()
    {
        TreeRangeSet<ClusteringElements> rangeSet = TreeRangeSet.create();
        rangeSet.add(ALL);
        return rangeSet;
    }

    /**
     * Returns a {@code RangeSet} that contains all values less than or equal to endpoint.
     * @return a {@code RangeSet} that contains all values less than or equal to endpoint.
     */
    public static RangeSet<ClusteringElements> atMost(ClusteringElements endpoint)
    {
        return buildRangeSet(endpoint, true, BoundType.CLOSED);
    }

    /**
     * Returns a {@code RangeSet} that contains all values less than endpoint.
     * @return a {@code RangeSet} that contains all values less than endpoint.
     */
    public static RangeSet<ClusteringElements> lessThan(ClusteringElements endpoint)
    {
        return buildRangeSet(endpoint, true, BoundType.OPEN);
    }

    /**
     * Returns a {@code RangeSet} that contains all values greater or equal to endpoint.
     * @return a {@code RangeSet} that contains all values greater or equal to endpoint.
     */
    public static RangeSet<ClusteringElements> atLeast(ClusteringElements endpoint)
    {
        return buildRangeSet(endpoint, false, BoundType.CLOSED);
    }

    /**
     * Returns a {@code RangeSet} that contains all values greater than endpoint.
     * @return a {@code RangeSet} that contains all values greater than endpoint.
     */
    public static RangeSet<ClusteringElements> greaterThan(ClusteringElements endpoint)
    {
        return buildRangeSet(endpoint, false, BoundType.OPEN);
    }

    private static RangeSet<ClusteringElements> buildRangeSet(ClusteringElements endpoint, boolean upperBound, BoundType boundType)
    {
        TreeRangeSet<ClusteringElements> rangeSet = TreeRangeSet.create();
        boolean reversed = endpoint.columnType(0).isReversed();
        if (reversed)
        {
            upperBound = !upperBound;
        }
        ClusteringElements oppositeEndpoint = upperBound ? ClusteringElements.of().bottom()
                                                         : ClusteringElements.of().top();

        for (int i = 0, m = endpoint.size(); i < m; i++)
        {
            AbstractType<?> type = endpoint.columnType(i);
            if (reversed != type.isReversed())
            {
                // The columns are changing directions therefore we need to create the range up to this point
                // and add it to the range set.
                // For example if we have c1 ASC and c2 DESC and (c1, c2) <= (1, 2) we should add the [(-∞)..(1))
                // range at this point. The second and last range: [(1, 2)..(1, -∞]) will be added after the loop.
                ClusteringElements e = ClusteringElements.of(endpoint.columns.subList(0, i), endpoint.subList(0, i));
                Range<ClusteringElements> range = upperBound ? Range.closedOpen(oppositeEndpoint, e.bottom())
                                                             : Range.openClosed(e.top(), oppositeEndpoint);

                rangeSet.add(range);
                reversed = !reversed;
                upperBound = !upperBound;
                oppositeEndpoint = upperBound ? e.bottom() : e.top();
            }
        }
        // We need to add the last range or the only one if there was no change of direction.
        Range<ClusteringElements> range = upperBound ? Range.range(oppositeEndpoint,
                                                                   BoundType.CLOSED,
                                                                   boundType == BoundType.OPEN ? endpoint.bottom() : endpoint.top(),
                                                                   boundType)
                                                     : Range.range(boundType == BoundType.OPEN ? endpoint.top() : endpoint.bottom(),
                                                                   boundType,
                                                                   oppositeEndpoint,
                                                                   BoundType.CLOSED);
        rangeSet.add(range);
        return rangeSet;
    }

    /**
     * Creates a bound representing the top value of this {@code ClusteringElements}.
     * @return a bound representing the top value of this {@code ClusteringElements}.
     */
    public ClusteringElements top()
    {
        return new Top(columns, values);
    }

    /**
     * Creates a bound representing the bottom value of this {@code ClusteringElements}.
     * @return a bound representing the bottom value of this {@code ClusteringElements}.
     */
    public ClusteringElements bottom()
    {
        return new Bottom(columns, values);
    }

    @Override
    public int compareTo(ClusteringElements that)
    {
        if (that == null)
            throw new NullPointerException();

        isComparableWith(that);

        int comparison = 0;

        int minSize = Math.min(this.size(), that.size());
        for (int i = 0; i < minSize; i++)
        {
            ByteBuffer thisValue = this.values.get(i);
            ByteBuffer thatValue = that.values.get(i);

            comparison = this.columnType(i).compare(thisValue, thatValue);

            if (comparison != 0)
                return comparison;
        }

        // If both sets of elements have the same size, it could mean:
        //  * that they are equal (e.g. this = (1, 2) and that = (1,2))
        //  * that one of them is a Top or Bottom boundary (e.g. this = (1) and that = (1, +∞))
        //  * that both of them are Top or Bottom boundaries ( e.g. this = (-∞) and that = (+∞)).
        if (this.size() == that.size())
        {
            comparison = Boolean.compare(this instanceof Top, that instanceof Top);

            if (comparison == 0) // either none is a Top or both are Tops
            {
                // If none is a Top, one can be a Bottom
                comparison = Boolean.compare(this instanceof Bottom, that instanceof Bottom);
                if (comparison == 0)
                    return 0; // this and that are equal

                return comparison > 0 ? -1 : 1; // If this is a Bottom that is greater and if that is a Bottom this is greater
            }

            // Either this or that is a top
            return comparison > 0 ? that instanceof Bottom ? 1 : 0  // this is a top
                                  : this instanceof Bottom ? -1 : 0; // that is a top
        }

        // If this size is smaller it means that we have 2 possible cases:
        //  * this with less column than that (e.g. (1) for this and (1, 0) for that)
        //  * a top or bottom for this (e.g. (1, +∞) for this and (1, 0) for that)
        // If we are in the first case then zero must be returned as that is included in this.
        if (this.size() < that.size())
        {
            return that.columns.get(minSize).type.isReversed() ? this instanceof Bottom ? -1 : 1
                                                               : this instanceof Top ? 1 : -1;
        }

        return this.columns.get(minSize).type.isReversed() ? that instanceof Bottom ? 1 : -1
                                                           : that instanceof Top ? -1 : 1;
    }

    /**
     * Checks if this {@code ClusteringElements} is comparable with that other one.
     * @param that the {@code ClusteringElements} to be compared with.
     */
    private void isComparableWith(ClusteringElements that)
    {
        int minSize = Math.min(this.columns.size(), that.columns.size());

        if (!this.columns.subList(0, minSize).equals(that.columns.subList(0, minSize)))
            throw new IllegalStateException("Cannot compare 2 lists containing different types");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ClusteringElements that = (ClusteringElements) o;
        return Objects.equals(columns, that.columns) && Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns, values);
    }

    public ClusteringBound<?> toBound(boolean isStart, boolean isInclusive)
    {
        return BufferClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive),
                                            values.toArray(new ByteBuffer[values.size()]));
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder().append('(');
        for (int i = 0, m = size(); i < m; i++)
        {
            if (i != 0)
                builder.append(", ");
            builder.append(columnType(i).toCQLString(values.get(i)));
        }

        if (this instanceof Top || this instanceof Bottom)
        {
            if (!isEmpty())
                builder.append(", ");

            builder.append(this instanceof Top ? "top" : "bottom");
        }
        return builder.append(')').toString();
    }

    private static <E> E last(List<E> elements)
    {
        return elements.get(elements.size() - 1);
    }

    /**
     * Represents the bottom of a column values. If the column is ascending in value this class will represent -∞.
     * If the column is descending in value it will represent +∞.
     * This class is used to define ranges internally.
     */
    private static class Bottom extends ClusteringElements
    {
        private Bottom(ImmutableList<? extends ColumnSpecification> columns, ImmutableList<ByteBuffer> values)
        {
            super(columns, values);
        }

        @Override
        public ClusteringBound<?> toBound(boolean isStart, boolean isInclusive)
        {
            return isEmpty() ? BufferClusteringBound.BOTTOM : super.toBound(isStart, isInclusive);
        }
    }

    /**
     * Represents the top of a column values. If the column is ascending in value this class will represent +∞.
     * If the column is descending in value it will represent -∞.
     * This class is used to define ranges internally.
     */
    private static class Top extends ClusteringElements
    {
        private Top(ImmutableList<? extends ColumnSpecification> columns, ImmutableList<ByteBuffer> values)
        {
            super(columns, values);
        }

        @Override
        public ClusteringBound<?> toBound(boolean isStart, boolean isInclusive)
        {
            return isEmpty() ? BufferClusteringBound.TOP : super.toBound(isStart, isInclusive);
        }
    }
}
