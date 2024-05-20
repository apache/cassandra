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

import java.util.List;
import java.util.NavigableSet;
import java.util.Set;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

import org.apache.cassandra.cql3.restrictions.ClusteringElements;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * Builder that allows to build multiple Clusterings/Slices at the same time.
 */
public final class MultiCBuilder
{
    /**
     * The table comparator.
     */
    private final ClusteringComparator comparator;

    /**
     * {@code true} if the clusterings have been build, <code>false</code> otherwise.
     */
    private boolean built;

    /**
     * The clusterings set.
     */
    private List<ClusteringElements> clusterings = ImmutableList.of();

    /**
     * The clustering ranges.
     */
    private RangeSet<ClusteringElements> clusteringsRanges;

    /**
     * Track if some elements are missing. This can happen with a <pre>WHERE c IN ()</pre>.
     * making the query not returning any results.
     */
    private boolean hasMissingElements;


    public MultiCBuilder(ClusteringComparator comparator)
    {
        this.comparator = comparator;
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
     * @return this <code>MultiCBuilder</code>
     */
    public MultiCBuilder extend(List<ClusteringElements> suffixes)
    {
        checkUpdateable();

        if (suffixes.isEmpty())
        {
            hasMissingElements = true;
            return this;
        }
        this.clusterings = this.clusterings.isEmpty() ? suffixes : cartesianProduct(clusterings, suffixes);
        return this;
    }

    /**
     * Adds individually each of the specified elements ranges to the end of all the existing clusterings.
     * The number of result clusterings is the product of the number of current clusterings and the number
     * of range added.
     *
     * <p>
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [D..E) and (F..G] will result in the 4
     * clusterings range : [A-B-D..A-B-E), (A-B-F..A-B-G], [A-C-D..A-C-E) and (A-C-F..A-C-G].
     * </p>
     *
     * <p>
     * Added elements can be composites as well.
     * If this builder contains 2 composites: A-B and A-C a call to this method to add [[D-E..F-G), (H-I..J-K]] will result in
     * 4 composites: [A-B-D-E..A-B-F-G), (A-B-H-I..A-B-J-K], [A-C-D-E..A-C-F-G) and (A-C-H-I..A-C-J-K].
     * </p>
     *
     * @param suffixes the ranges to add
     * @return this <code>MultiCBuilder</code>
     */
    public MultiCBuilder extend(RangeSet<ClusteringElements> suffixes)
    {
        checkUpdateable();

        this.clusteringsRanges = this.clusterings.isEmpty() ? suffixes : cartesianProduct(clusterings, suffixes);
        this.clusterings = null;
        return this;
    }

    private static RangeSet<ClusteringElements> cartesianProduct(List<ClusteringElements> prefixes, RangeSet<ClusteringElements> suffixes)
    {
        ImmutableRangeSet.Builder<ClusteringElements> builder = ImmutableRangeSet.builder();
        for (ClusteringElements prefix: prefixes)
        {
            for (Range<ClusteringElements> suffix : suffixes.asRanges())
            {
                builder.add(Range.range(prefix.extend(suffix.lowerEndpoint()),
                                        suffix.lowerBoundType(),
                                        prefix.extend(suffix.upperEndpoint()),
                                        suffix.upperBoundType()));
            }
        }
        return builder.build();
    }

    private static List<ClusteringElements> cartesianProduct(List<ClusteringElements> prefixes, List<ClusteringElements> suffixes)
    {
        ImmutableList.Builder<ClusteringElements> builder = ImmutableList.builderWithExpectedSize(prefixes.size() * suffixes.size());
        for (ClusteringElements prefix: prefixes)
        {
            for (ClusteringElements suffix: suffixes)
            {
                builder.add(prefix.extend(suffix));
            }
        }
        return builder.build();
    }

    /**
     * Returns the current number of results when {@link #build()} is called
     *
     * @return the current number of build results
     */
    public int buildSize()
    {
        return hasMissingElements ? 0
                                  : clusterings != null ? clusterings.size()
                                                        : clusteringsRanges.asRanges().size();
    }

    /**
     * Checks if some clusterings have some missing elements due to a <pre>WHERE c IN ()</pre>.
     * @return {@code true} if the clusterings have some missing elements, {@code false} otherwise.
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
    public NavigableSet<Clustering<?>> build()
    {
        assert clusteringsRanges == null;
        built = true;

        if (hasMissingElements)
            return BTreeSet.empty(comparator);

        if (clusterings.isEmpty())
            return BTreeSet.of(comparator, Clustering.EMPTY);

        CBuilder builder = CBuilder.create(comparator);

        BTreeSet.Builder<Clustering<?>> set = BTreeSet.builder(builder.comparator());
        for (ClusteringElements clustering : clusterings)
        {
            set.add(builder.buildWith(clustering));
        }
        return set.build();
    }

    public Slices buildSlices()
    {
        if (clusterings != null)
        {
            if (hasMissingElements)
                return Slices.NONE;

            if (clusterings.isEmpty())
                return Slices.ALL;

            Slices.Builder builder = new Slices.Builder(comparator, clusterings.size());

            for (ClusteringElements clustering : clusterings)
            {
                builder.add(clustering.toBound(true, true),
                            clustering.toBound(false, true));
            }
            return builder.build();
        }

        Set<Range<ClusteringElements>> ranges = clusteringsRanges.asRanges();

        Slices.Builder builder = new Slices.Builder(comparator, ranges.size());
        for (Range<ClusteringElements> range : ranges)
        {
            builder.add(range.lowerEndpoint().toBound(true, isInclusive(range.lowerBoundType())),
                        range.upperEndpoint().toBound(false, isInclusive(range.upperBoundType())));
        }
        return builder.build();
    }

    private boolean isInclusive(BoundType boundType)
    {
        return boundType == BoundType.CLOSED;
    }

    private void checkUpdateable()
    {
       if (built)
            throw new IllegalStateException("This builder cannot be updated anymore");

        if (clusteringsRanges != null)
            throw new IllegalStateException("Cannot extend clusterings that contain ranges");

       if (hasMissingElements)
           throw new IllegalStateException("Cannot extend clusterings with missing elements");
    }
}
