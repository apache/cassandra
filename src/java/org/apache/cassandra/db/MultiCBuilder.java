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
import java.util.Arrays;
import java.util.List;
import java.util.NavigableSet;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * Builder that allow to build multiple Clustering/ClusteringBound at the same time.
 */
public abstract class MultiCBuilder
{
    /**
     * The table comparator.
     */
    protected final ClusteringComparator comparator;

    /**
     * The number of clustering elements that have been added.
     */
    protected int size;

    /**
     * <code>true</code> if the clusterings have been build, <code>false</code> otherwise.
     */
    protected boolean built;

    /**
     * <code>true</code> if the clusterings contains some <code>null</code> elements.
     */
    protected boolean containsNull;

    /**
     * <code>true</code> if the composites contains some <code>unset</code> elements.
     */
    protected boolean containsUnset;

    /**
     * <code>true</code> if some empty collection have been added.
     */
    protected boolean hasMissingElements;

    protected MultiCBuilder(ClusteringComparator comparator)
    {
        this.comparator = comparator;
    }

    /**
     * Creates a new empty {@code MultiCBuilder}.
     */
    public static MultiCBuilder create(ClusteringComparator comparator, boolean forMultipleValues)
    {
        return forMultipleValues
             ? new MultiClusteringBuilder(comparator)
             : new OneClusteringBuilder(comparator);
    }

    /**
     * Adds the specified element to all the clusterings.
     * <p>
     * If this builder contains 2 clustering: A-B and A-C a call to this method to add D will result in the clusterings:
     * A-B-D and A-C-D.
     * </p>
     *
     * @param value the value of the next element
     * @return this <code>MulitCBuilder</code>
     */
    public abstract MultiCBuilder addElementToAll(ByteBuffer value);

    /**
     * Adds individually each of the specified elements to the end of all of the existing clusterings.
     * <p>
     * If this builder contains 2 clusterings: A-B and A-C a call to this method to add D and E will result in the 4
     * clusterings: A-B-D, A-B-E, A-C-D and A-C-E.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public abstract MultiCBuilder addEachElementToAll(List<ByteBuffer> values);

    /**
     * Adds individually each of the specified list of elements to the end of all of the existing composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [[D, E], [F, G]] will result in the 4
     * composites: A-B-D-E, A-B-F-G, A-C-D-E and A-C-F-G.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public abstract MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> values);

    protected void checkUpdateable()
    {
        if (!hasRemaining() || built)
            throw new IllegalStateException("this builder cannot be updated anymore");
    }

    /**
     * Returns the number of elements that can be added to the clusterings.
     *
     * @return the number of elements that can be added to the clusterings.
     */
    public int remainingCount()
    {
        return comparator.size() - size;
    }

    /**
     * Returns the current number of results when {@link #build()} is called
     *
     * @return the current number of build results
     */
    public abstract int buildSize();

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
     * Checks if some empty list of values have been added
     * @return <code>true</code> if the clusterings have some missing elements, <code>false</code> otherwise.
     */
    public boolean hasMissingElements()
    {
        return hasMissingElements;
    }

    /**
     * Builds the <code>clusterings</code>.
     *
     * @return the clusterings
     */
    public abstract NavigableSet<Clustering<?>> build();

    /**
     * Builds the <code>ClusteringBound</code>s for slice restrictions.
     *
     * @param isStart specify if the bound is a start one
     * @param isInclusive specify if the bound is inclusive or not
     * @param isOtherBoundInclusive specify if the other bound is inclusive or not
     * @param columnDefs the columns of the slice restriction
     * @return the <code>ClusteringBound</code>s
     */
    public abstract NavigableSet<ClusteringBound<?>> buildBoundForSlice(boolean isStart,
                                                                        boolean isInclusive,
                                                                        boolean isOtherBoundInclusive,
                                                                        List<ColumnMetadata> columnDefs);

    /**
     * Builds the <code>ClusteringBound</code>s
     *
     * @param isStart specify if the bound is a start one
     * @param isInclusive specify if the bound is inclusive or not
     * @return the <code>ClusteringBound</code>s
     */
    public abstract NavigableSet<ClusteringBound<?>> buildBound(boolean isStart, boolean isInclusive);

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
     * Specialization of MultiCBuilder when we know only one clustering/bound is created.
     */
    private static class OneClusteringBuilder extends MultiCBuilder
    {
        /**
         * The elements of the clusterings
         */
        private final ByteBuffer[] elements;

        public OneClusteringBuilder(ClusteringComparator comparator)
        {
            super(comparator);
            this.elements = new ByteBuffer[comparator.size()];
        }

        public MultiCBuilder addElementToAll(ByteBuffer value)
        {
            checkUpdateable();

            if (value == null)
                containsNull = true;
            if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                containsUnset = true;

            elements[size++] = value;
            return this;
        }

        public MultiCBuilder addEachElementToAll(List<ByteBuffer> values)
        {
            if (values.isEmpty())
            {
                hasMissingElements = true;
                return this;
            }

            assert values.size() == 1;

            return addElementToAll(values.get(0));
        }

        public MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> values)
        {
            if (values.isEmpty())
            {
                hasMissingElements = true;
                return this;
            }

            assert values.size() == 1;
            return addEachElementToAll(values.get(0));
        }

        @Override
        public int buildSize()
        {
            return hasMissingElements ? 0 : 1;
        }

        public NavigableSet<Clustering<?>> build()
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            return BTreeSet.of(comparator, size == 0 ? Clustering.EMPTY : Clustering.make(elements));
        }

        @Override
        public NavigableSet<ClusteringBound<?>> buildBoundForSlice(boolean isStart,
                                                                   boolean isInclusive,
                                                                   boolean isOtherBoundInclusive,
                                                                   List<ColumnMetadata> columnDefs)
        {
            return buildBound(isStart, columnDefs.get(0).isReversedType() ? isOtherBoundInclusive : isInclusive);
        }

        public NavigableSet<ClusteringBound<?>> buildBound(boolean isStart, boolean isInclusive)
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            if (size == 0)
                return BTreeSet.of(comparator, isStart ? BufferClusteringBound.BOTTOM : BufferClusteringBound.TOP);

            ByteBuffer[] newValues = size == elements.length
                                   ? elements
                                   : Arrays.copyOf(elements, size);

            return BTreeSet.of(comparator, BufferClusteringBound.create(ClusteringBound.boundKind(isStart, isInclusive), newValues));
        }
    }

    /**
     * MultiCBuilder implementation actually supporting the creation of multiple clustering/bound.
     */
    private static class MultiClusteringBuilder extends MultiCBuilder
    {
        /**
         * The elements of the clusterings
         */
        private final List<List<ByteBuffer>> elementsList = new ArrayList<>();

        public MultiClusteringBuilder(ClusteringComparator comparator)
        {
            super(comparator);
        }

        public MultiCBuilder addElementToAll(ByteBuffer value)
        {
            checkUpdateable();

            if (elementsList.isEmpty())
                elementsList.add(new ArrayList<>());

            if (value == null)
                containsNull = true;
            else if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                containsUnset = true;

            for (int i = 0, m = elementsList.size(); i < m; i++)
                elementsList.get(i).add(value);

            size++;
            return this;
        }

        public MultiCBuilder addEachElementToAll(List<ByteBuffer> values)
        {
            checkUpdateable();

            if (elementsList.isEmpty())
                elementsList.add(new ArrayList<>());

            if (values.isEmpty())
            {
                hasMissingElements = true;
            }
            else
            {
                for (int i = 0, m = elementsList.size(); i < m; i++)
                {
                    List<ByteBuffer> oldComposite = elementsList.remove(0);

                    for (int j = 0, n = values.size(); j < n; j++)
                    {
                        List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                        elementsList.add(newComposite);

                        ByteBuffer value = values.get(j);

                        if (value == null)
                            containsNull = true;
                        if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                            containsUnset = true;

                        newComposite.add(values.get(j));
                    }
                }
            }
            size++;
            return this;
        }

        public MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> values)
        {
            checkUpdateable();

            if (elementsList.isEmpty())
                elementsList.add(new ArrayList<>());

            if (values.isEmpty())
            {
                hasMissingElements = true;
            }
            else
            {
                for (int i = 0, m = elementsList.size(); i < m; i++)
                {
                    List<ByteBuffer> oldComposite = elementsList.remove(0);

                    for (int j = 0, n = values.size(); j < n; j++)
                    {
                        List<ByteBuffer> newComposite = new ArrayList<>(oldComposite);
                        elementsList.add(newComposite);

                        List<ByteBuffer> value = values.get(j);

                        if (value.contains(null))
                            containsNull = true;
                        if (value.contains(ByteBufferUtil.UNSET_BYTE_BUFFER))
                            containsUnset = true;

                        newComposite.addAll(value);
                    }
                }
                size += values.get(0).size();
            }
            return this;
        }

        @Override
        public int buildSize()
        {
            return hasMissingElements ? 0 : elementsList.size();
        }

        public NavigableSet<Clustering<?>> build()
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            CBuilder builder = CBuilder.create(comparator);

            if (elementsList.isEmpty())
                return BTreeSet.of(builder.comparator(), builder.build());

            BTreeSet.Builder<Clustering<?>> set = BTreeSet.builder(builder.comparator());
            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> elements = elementsList.get(i);
                set.add(builder.buildWith(elements));
            }
            return set.build();
        }

        public NavigableSet<ClusteringBound<?>> buildBoundForSlice(boolean isStart,
                                                                   boolean isInclusive,
                                                                   boolean isOtherBoundInclusive,
                                                                   List<ColumnMetadata> columnDefs)
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            CBuilder builder = CBuilder.create(comparator);

            if (elementsList.isEmpty())
                return BTreeSet.of(comparator, builder.buildBound(isStart, isInclusive));

            // Use a TreeSet to sort and eliminate duplicates
            BTreeSet.Builder<ClusteringBound<?>> set = BTreeSet.builder(comparator);

            // The first column of the slice might not be the first clustering column (e.g. clustering_0 = ? AND (clustering_1, clustering_2) >= (?, ?)
            int offset = columnDefs.get(0).position();

            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> elements = elementsList.get(i);

                // Handle the no bound case
                if (elements.size() == offset)
                {
                    set.add(builder.buildBoundWith(elements, isStart, true));
                    continue;
                }

                // In the case of mixed order columns, we will have some extra slices where the columns change directions.
                // For example: if we have clustering_0 DESC and clustering_1 ASC a slice like (clustering_0, clustering_1) > (1, 2)
                // will produce 2 slices: [BOTTOM, 1) and (1.2, 1]
                // So, the END bound will return 2 bounds with the same values 1
                ColumnMetadata lastColumn = columnDefs.get(columnDefs.size() - 1);
                if (elements.size() <= lastColumn.position() && i < m - 1 && elements.equals(elementsList.get(i + 1)))
                {
                    set.add(builder.buildBoundWith(elements, isStart, false));
                    set.add(builder.buildBoundWith(elementsList.get(i++), isStart, true));
                    continue;
                }

                // Handle the normal bounds
                ColumnMetadata column = columnDefs.get(elements.size() - 1 - offset);
                set.add(builder.buildBoundWith(elements, isStart, column.isReversedType() ? isOtherBoundInclusive : isInclusive));
            }
            return set.build();
        }

        public NavigableSet<ClusteringBound<?>> buildBound(boolean isStart, boolean isInclusive)
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            CBuilder builder = CBuilder.create(comparator);

            if (elementsList.isEmpty())
                return BTreeSet.of(comparator, builder.buildBound(isStart, isInclusive));

            // Use a TreeSet to sort and eliminate duplicates
            BTreeSet.Builder<ClusteringBound<?>> set = BTreeSet.builder(comparator);

            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> elements = elementsList.get(i);
                set.add(builder.buildBoundWith(elements, isStart, isInclusive));
            }
            return set.build();
        }
    }
}
