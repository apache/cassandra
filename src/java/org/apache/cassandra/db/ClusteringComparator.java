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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.serializers.MarshalException;

import org.apache.cassandra.io.sstable.IndexInfo;

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

    private final Comparator<Row> rowComparator = (r1, r2) -> compare(r1.clustering(), r2.clustering());

    public ClusteringComparator(AbstractType<?>... clusteringTypes)
    {
        this(ImmutableList.copyOf(clusteringTypes));
    }

    public ClusteringComparator(List<AbstractType<?>> clusteringTypes)
    {
        // copy the list to ensure despatch is monomorphic
        this.clusteringTypes = ImmutableList.copyOf(clusteringTypes);

        this.indexComparator = (o1, o2) -> ClusteringComparator.this.compare(o1.lastName, o2.lastName);
        this.indexReverseComparator = (o1, o2) -> ClusteringComparator.this.compare(o1.firstName, o2.firstName);
        this.reverseComparator = (c1, c2) -> ClusteringComparator.this.compare(c2, c1);
        for (AbstractType<?> type : clusteringTypes)
            type.checkComparable(); // this should already be enforced by CFMetaData.rebuild, but we check again for other constructors
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
    public Clustering make(Object... values)
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
        return compare(c1.clustering(), c2.clustering());
    }

    public int compare(ClusteringPrefix c1, ClusteringPrefix c2)
    {
        int s1 = c1.size();
        int s2 = c2.size();
        int minSize = Math.min(s1, s2);

        for (int i = 0; i < minSize; i++)
        {
            int cmp = compareComponent(i, c1.get(i), c2.get(i));
            if (cmp != 0)
                return cmp;
        }

        if (s1 == s2)
            return ClusteringPrefix.Kind.compare(c1.kind(), c2.kind());

        return s1 < s2 ? c1.kind().comparedToClustering : -c2.kind().comparedToClustering;
    }

    public int compare(Clustering c1, Clustering c2)
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
    public int compare(Clustering c1, Clustering c2, int size)
    {
        for (int i = 0; i < size; i++)
        {
            int cmp = compareComponent(i, c1.get(i), c2.get(i));
            if (cmp != 0)
                return cmp;
        }
        return 0;
    }

    public int compareComponent(int i, ByteBuffer v1, ByteBuffer v2)
    {
        if (v1 == null)
            return v2 == null ? 0 : -1;
        if (v2 == null)
            return 1;

        return clusteringTypes.get(i).compare(v1, v2);
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
    public void validate(ClusteringPrefix clustering)
    {
        for (int i = 0; i < clustering.size(); i++)
        {
            ByteBuffer value = clustering.get(i);
            if (value != null)
                subtype(i).validate(value);
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
