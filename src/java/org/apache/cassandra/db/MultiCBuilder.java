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
     * Adds individually each of the specified list of elements to the end of all the existing composites.
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [[D, E], [F, G]] will result in the 4
     * composites: A-B-D-E, A-B-F-G, A-C-D-E and A-C-F-G.
     * </p>
     *
     * @param values the elements to add
     * @return this <code>CompositeBuilder</code>
     */
    public abstract MultiCBuilder addAllElementsToAll(List<? extends List<ByteBuffer>> values);

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

        public MultiCBuilder addAllElementsToAll(List<? extends List<ByteBuffer>> values)
        {
            if (values.isEmpty())
            {
                hasMissingElements = true;
                return this;
            }
            assert values.size() == 1;
            List<ByteBuffer> buffers = values.get(0);
            for (int i = 0, m = buffers.size(); i < m; i++)
            {
                checkUpdateable();
                elements[size++] = buffers.get(i);
            }
            return this;
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
            ColumnMetadata column = columnDefs.get(0);
            // If the column position is equals to the size we are in a no bound case and inclusive should be true
            boolean inclusive = column.position() == size || (column.isReversedType() ? isOtherBoundInclusive : isInclusive);
            return buildBound(isStart, inclusive);
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

        public MultiCBuilder addAllElementsToAll(List<? extends List<ByteBuffer>> values)
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
