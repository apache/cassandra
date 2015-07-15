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
import java.util.*;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * Builder that allow to build multiple Clustering/Slice.Bound at the same time.
 */
public abstract class MultiCBuilder
{
    /**
     * Creates a new empty {@code MultiCBuilder}.
     */
    public static MultiCBuilder create(ClusteringComparator comparator)
    {
        return new ConcreteMultiCBuilder(comparator);
    }

    /**
     * Wraps an existing {@code CBuilder} to provide him with a MultiCBuilder interface
     * for the sake of passing it to {@link Restriction.appendTo}. The resulting
     * {@code MultiCBuilder} will still only be able to create a single clustering/bound
     * and an {@code IllegalArgumentException} will be thrown if elements that added that
     * would correspond to building multiple clusterings.
     */
    public static MultiCBuilder wrap(final CBuilder builder)
    {
        return new MultiCBuilder()
        {
            private boolean containsNull;
            private boolean containsUnset;
            private boolean hasMissingElements;

            public MultiCBuilder addElementToAll(ByteBuffer value)
            {
                builder.add(value);

                if (value == null)
                    containsNull = true;
                if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    containsUnset = true;

                return this;
            }

            public MultiCBuilder addEachElementToAll(List<ByteBuffer> values)
            {
                if (values.isEmpty())
                {
                    hasMissingElements = true;
                    return this;
                }

                if (values.size() > 1)
                    throw new IllegalArgumentException();

                return addElementToAll(values.get(0));
            }

            public MultiCBuilder addAllElementsToAll(List<List<ByteBuffer>> values)
            {
                if (values.isEmpty())
                {
                    hasMissingElements = true;
                    return this;
                }

                if (values.size() > 1)
                    throw new IllegalArgumentException();

                return addEachElementToAll(values.get(0));
            }

            public int remainingCount()
            {
                return builder.remainingCount();
            }

            public boolean containsNull()
            {
                return containsNull;
            }

            public boolean containsUnset()
            {
                return containsUnset;
            }

            public boolean hasMissingElements()
            {
                return hasMissingElements;
            }

            public NavigableSet<Clustering> build()
            {
                return BTreeSet.of(builder.comparator(), builder.build());
            }

            public NavigableSet<Slice.Bound> buildBound(boolean isStart, boolean isInclusive)
            {
                return BTreeSet.of(builder.comparator(), builder.buildBound(isStart, isInclusive));
            }
        };
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

    /**
     * Returns the number of elements that can be added to the clusterings.
     *
     * @return the number of elements that can be added to the clusterings.
     */
    public abstract int remainingCount();

    /**
     * Checks if the clusterings contains null elements.
     *
     * @return <code>true</code> if the clusterings contains <code>null</code> elements, <code>false</code> otherwise.
     */
    public abstract boolean containsNull();

    /**
     * Checks if the clusterings contains unset elements.
     *
     * @return <code>true</code> if the clusterings contains <code>unset</code> elements, <code>false</code> otherwise.
     */
    public abstract boolean containsUnset();

    /**
     * Checks if some empty list of values have been added
     * @return <code>true</code> if the clusterings have some missing elements, <code>false</code> otherwise.
     */
    public abstract boolean hasMissingElements();

    /**
     * Builds the <code>clusterings</code>.
     *
     * @return the clusterings
     */
    public abstract NavigableSet<Clustering> build();

    /**
     * Builds the <code>clusterings</code> with the specified EOC.
     *
     * @return the clusterings
     */
    public abstract NavigableSet<Slice.Bound> buildBound(boolean isStart, boolean isInclusive);

    /**
     * Checks if some elements can still be added to the clusterings.
     *
     * @return <code>true</code> if it is possible to add more elements to the clusterings, <code>false</code> otherwise.
     */
    public boolean hasRemaining()
    {
        return remainingCount() > 0;
    }


    private static class ConcreteMultiCBuilder extends MultiCBuilder
    {
        /**
         * The table comparator.
         */
        private final ClusteringComparator comparator;

        /**
         * The elements of the clusterings
         */
        private final List<List<ByteBuffer>> elementsList = new ArrayList<>();

        /**
         * The number of elements that have been added.
         */
        private int size;

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
         * <code>true</code> if some empty collection have been added.
         */
        private boolean hasMissingElements;

        public ConcreteMultiCBuilder(ClusteringComparator comparator)
        {
            this.comparator = comparator;
        }

        public MultiCBuilder addElementToAll(ByteBuffer value)
        {
            checkUpdateable();

            if (isEmpty())
                elementsList.add(new ArrayList<ByteBuffer>());

            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                if (value == null)
                    containsNull = true;
                if (value == ByteBufferUtil.UNSET_BYTE_BUFFER)
                    containsUnset = true;

                elementsList.get(i).add(value);
            }
            size++;
            return this;
        }

        public MultiCBuilder addEachElementToAll(List<ByteBuffer> values)
        {
            checkUpdateable();

            if (isEmpty())
                elementsList.add(new ArrayList<ByteBuffer>());

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

            if (isEmpty())
                elementsList.add(new ArrayList<ByteBuffer>());

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

                        if (value.isEmpty())
                            hasMissingElements = true;

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

        public int remainingCount()
        {
            return comparator.size() - size;
        }

        /**
         * Checks if this builder is empty.
         *
         * @return <code>true</code> if this builder is empty, <code>false</code> otherwise.
         */
        public boolean isEmpty()
        {
            return elementsList.isEmpty();
        }

        public boolean containsNull()
        {
            return containsNull;
        }

        public boolean containsUnset()
        {
            return containsUnset;
        }

        public boolean hasMissingElements()
        {
            return hasMissingElements;
        }

        public NavigableSet<Clustering> build()
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            CBuilder builder = CBuilder.create(comparator);

            if (elementsList.isEmpty())
                return BTreeSet.of(builder.comparator(), builder.build());

            BTreeSet.Builder<Clustering> set = BTreeSet.builder(builder.comparator());
            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> elements = elementsList.get(i);
                set.add(builder.buildWith(elements));
            }
            return set.build();
        }

        public NavigableSet<Slice.Bound> buildBound(boolean isStart, boolean isInclusive)
        {
            built = true;

            if (hasMissingElements)
                return BTreeSet.empty(comparator);

            CBuilder builder = CBuilder.create(comparator);

            if (elementsList.isEmpty())
                return BTreeSet.of(comparator, builder.buildBound(isStart, isInclusive));

            // Use a TreeSet to sort and eliminate duplicates
            BTreeSet.Builder<Slice.Bound> set = BTreeSet.builder(comparator);

            for (int i = 0, m = elementsList.size(); i < m; i++)
            {
                List<ByteBuffer> elements = elementsList.get(i);
                set.add(builder.buildBoundWith(elements, isStart, isInclusive));
            }
            return set.build();
        }

        private void checkUpdateable()
        {
            if (!hasRemaining() || built)
                throw new IllegalStateException("this builder cannot be updated anymore");
        }
    }
}
