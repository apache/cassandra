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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.*;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTreeSet;

/**
 * A filter selecting rows given their clustering value.
 */
public class ClusteringIndexNamesFilter extends AbstractClusteringIndexFilter
{
    static final InternalDeserializer deserializer = new NamesDeserializer();

    // This could be empty if selectedColumns only has static columns (in which case the filter still
    // selects the static row)
    private final NavigableSet<Clustering<?>> clusterings;

    // clusterings is always in clustering order (because we need it that way in some methods), but we also
    // sometimes need those clustering in "query" order (i.e. in reverse clustering order if the query is
    // reversed), so we keep that too for simplicity.
    private final NavigableSet<Clustering<?>> clusteringsInQueryOrder;

    public ClusteringIndexNamesFilter(NavigableSet<Clustering<?>> clusterings, boolean reversed)
    {
        super(reversed);
        assert !clusterings.contains(Clustering.STATIC_CLUSTERING);
        this.clusterings = clusterings;
        this.clusteringsInQueryOrder = reversed ? clusterings.descendingSet() : clusterings;
    }

    /**
     * The set of requested rows.
     *
     * Please note that this can be empty if only the static row is requested.
     *
     * @return the set of requested clustering in clustering order (note that
     * this is always in clustering order even if the query is reversed).
     */
    public NavigableSet<Clustering<?>> requestedRows()
    {
        return clusterings;
    }

    public boolean selectsAllPartition()
    {
        // if the clusterings set is empty we are selecting a static row and in this case we want to count
        // static rows so we return true
        return clusterings.isEmpty();
    }

    public boolean selects(Clustering<?> clustering)
    {
        return clusterings.contains(clustering);
    }

    public ClusteringIndexNamesFilter forPaging(ClusteringComparator comparator, Clustering<?> lastReturned, boolean inclusive)
    {
        NavigableSet<Clustering<?>> newClusterings = reversed ?
                                                     clusterings.headSet(lastReturned, inclusive) :
                                                     clusterings.tailSet(lastReturned, inclusive);

        return new ClusteringIndexNamesFilter(newClusterings, reversed);
    }

    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        if (partition.isEmpty())
            return false;

        // 'partition' contains all columns, so it covers our filter if our last clusterings
        // is smaller than the last in the cache
        return clusterings.comparator().compare(clusterings.last(), partition.lastRow().clustering()) <= 0;
    }

    public boolean isHeadFilter()
    {
        return false;
    }

    // Given another iterator, only return the rows that match this filter
    public UnfilteredRowIterator filterNotIndexed(ColumnFilter columnFilter, UnfilteredRowIterator iterator)
    {
        // Note that we don't filter markers because that's a bit trickier (we don't know in advance until when
        // the range extend) and it's harmless to left them.
        class FilterNotIndexed extends Transformation
        {
            @Override
            public Row applyToStatic(Row row)
            {
                return columnFilter.fetchedColumns().statics.isEmpty() ? null : row.filter(columnFilter, iterator.metadata());
            }

            @Override
            public Row applyToRow(Row row)
            {
                return clusterings.contains(row.clustering()) ? row.filter(columnFilter, iterator.metadata()) : null;
            }
        }
        return Transformation.apply(iterator, new FilterNotIndexed());
    }

    public Slices getSlices(TableMetadata metadata)
    {
        Slices.Builder builder = new Slices.Builder(metadata.comparator, clusteringsInQueryOrder.size());
        for (Clustering<?> clustering : clusteringsInQueryOrder)
            builder.add(Slice.make(clustering));
        return builder.build();
    }

    public UnfilteredRowIterator getUnfilteredRowIterator(final ColumnFilter columnFilter, final Partition partition)
    {
        return partition.unfilteredIterator(columnFilter, clusteringsInQueryOrder, isReversed());
    }

    public boolean intersects(ClusteringComparator comparator, Slice slice)
    {
        for (Clustering<?> clustering : clusterings)
        {
            if (slice.includes(comparator, clustering))
                return true;
        }
        return false;
    }

    public String toString(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("names(");
        int i = 0;
        for (Clustering<?> clustering : clusterings)
            sb.append(i++ == 0 ? "" : ", ").append(clustering.toString(metadata));
        if (reversed)
            sb.append(", reversed");
        return sb.append(')').toString();
    }

    @Override
    public String toCQLString(TableMetadata metadata, RowFilter rowFilter)
    {
        if (metadata.clusteringColumns().isEmpty() || clusterings.isEmpty())
            return rowFilter.toCQLString();

        boolean isSingleColumn = metadata.clusteringColumns().size() == 1;
        boolean isSingleClustering = clusterings.size() == 1;

        StringBuilder sb = new StringBuilder();
        sb.append(isSingleColumn ? "" : '(')
          .append(ColumnMetadata.toCQLString(metadata.clusteringColumns()))
          .append(isSingleColumn ? "" : ')');

        sb.append(isSingleClustering ? " = " : " IN (");
        int i = 0;
        for (Clustering<?> clustering : clusterings)
        {
            sb.append(i++ == 0 ? "" : ", ")
              .append(isSingleColumn ? "" : '(')
              .append(clustering.toCQLString(metadata))
              .append(isSingleColumn ? "" : ')');

            for (int j = 0; j < clustering.size(); j++)
                rowFilter = rowFilter.without(metadata.clusteringColumns().get(j), Operator.EQ, clustering.bufferAt(j));
        }
        sb.append(isSingleClustering ? "" : ")");

        if (!rowFilter.isEmpty())
            sb.append(" AND ").append(rowFilter.toCQLString());

        appendOrderByToCQLString(metadata, sb);
        return sb.toString();
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusteringIndexNamesFilter that = (ClusteringIndexNamesFilter) o;
        return Objects.equals(clusterings, that.clusterings) &&
               Objects.equals(reversed, that.reversed);
    }

    public int hashCode()
    {
        return Objects.hash(clusterings, reversed);
    }

    public Kind kind()
    {
        return Kind.NAMES;
    }

    protected void serializeInternal(DataOutputPlus out, int version) throws IOException
    {
        ClusteringComparator comparator = (ClusteringComparator)clusterings.comparator();
        out.writeUnsignedVInt32(clusterings.size());
        for (Clustering<?> clustering : clusterings)
            Clustering.serializer.serialize(clustering, out, version, comparator.subtypes());
    }

    protected long serializedSizeInternal(int version)
    {
        ClusteringComparator comparator = (ClusteringComparator)clusterings.comparator();
        long size = TypeSizes.sizeofUnsignedVInt(clusterings.size());
        for (Clustering<?> clustering : clusterings)
            size += Clustering.serializer.serializedSize(clustering, version, comparator.subtypes());
        return size;
    }

    private static class NamesDeserializer implements InternalDeserializer
    {
        public ClusteringIndexFilter deserialize(DataInputPlus in, int version, TableMetadata metadata, boolean reversed) throws IOException
        {
            ClusteringComparator comparator = metadata.comparator;
            int size = in.readUnsignedVInt32();
            try (BTree.FastBuilder<Clustering<?>> builder = BTree.fastBuilder())
            {
                for (int i = 0; i < size; i++)
                    builder.add(Clustering.serializer.deserialize(in, version, comparator.subtypes()));
                BTreeSet<Clustering<?>> clusterings = BTreeSet.wrap(builder.build(), comparator);
                return new ClusteringIndexNamesFilter(clusterings, reversed);
            }
        }
    }
}