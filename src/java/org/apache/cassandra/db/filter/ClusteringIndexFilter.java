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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * A filter that selects a subset of the rows of a given partition by using the "clustering index".
 * <p>
 * In CQL terms, this correspond to the clustering columns selection and correspond to what
 * the storage engine can do without filtering (and without 2ndary indexes). This does not include
 * the restrictions on non-PK columns which can be found in {@link RowFilter}.
 */
public interface ClusteringIndexFilter
{
    public static Serializer serializer = AbstractClusteringIndexFilter.serializer;

    public enum Kind
    {
        SLICE (ClusteringIndexSliceFilter.deserializer),
        NAMES (ClusteringIndexNamesFilter.deserializer);

        protected final InternalDeserializer deserializer;

        private Kind(InternalDeserializer deserializer)
        {
            this.deserializer = deserializer;
        }
    }

    static interface InternalDeserializer
    {
        public ClusteringIndexFilter deserialize(DataInputPlus in, int version, CFMetaData metadata, boolean reversed) throws IOException;
    }

    /**
     * Whether the filter query rows in reversed clustering order or not.
     *
     * @return whether the filter query rows in reversed clustering order or not.
     */
    public boolean isReversed();

    /**
     * Returns a filter for continuing the paging of this filter given the last returned clustering prefix.
     *
     * @param comparator the comparator for the table this is a filter for.
     * @param lastReturned the last clustering that was returned for the query we are paging for. The
     * resulting filter will be such that results coming after {@code lastReturned} are returned
     * (where coming after means "greater than" if the filter is not reversed, "lesser than" otherwise;
     * futher, whether the comparison is strict or not depends on {@code inclusive}).
     * @param inclusive whether or not we want to include the {@code lastReturned} in the newly returned
     * page of results.
     *
     * @return a new filter that selects results coming after {@code lastReturned}.
     */
    public ClusteringIndexFilter forPaging(ClusteringComparator comparator, Clustering lastReturned, boolean inclusive);

    /**
     * Returns whether we can guarantee that a given cached partition contains all the data selected by this filter.
     *
     * @param partition the cached partition. This method assumed that the rows of this partition contains all the table columns.
     *
     * @return whether we can guarantee that all data selected by this filter are in {@code partition}.
     */
    public boolean isFullyCoveredBy(CachedPartition partition);

    /**
     * Whether this filter selects the head of a partition (i.e. it isn't reversed and selects all rows up to a certain point).
     *
     * @return whether this filter selects the head of a partition.
     */
    public boolean isHeadFilter();

    /**
     * Whether this filter selects all the row of a partition (it's an "identity" filter).
     *
     * @return whether this filter selects all the row of a partition (it's an "identity" filter).
     */
    public boolean selectsAllPartition();

    /**
     * Whether a given row is selected by this filter.
     *
     * @param clustering the clustering of the row to test the selection of.
     *
     * @return whether the row with clustering {@code clustering} is selected by this filter.
     */
    public boolean selects(Clustering clustering);

    /**
     * Returns an iterator that only returns the rows of the provided iterator that this filter selects.
     * <p>
     * This method is the "dumb" counterpart to {@link #filter(SliceableUnfilteredRowIterator)} in that it has no way to quickly get
     * to what is actually selected, so it simply iterate over it all and filters out what shouldn't be returned. This should
     * be avoided in general, we should make sure to have {@code SliceableUnfilteredRowIterator} when we have filtering to do, but this
     * currently only used in {@link SinglePartitionReadCommand#getThroughCache} when we know this won't be a performance problem.
     * Another difference with {@link #filter(SliceableUnfilteredRowIterator)} is that this method also filter the queried
     * columns in the returned result, while the former assumes that the provided iterator has already done it.
     *
     * @param columnFilter the columns to include in the rows of the result iterator.
     * @param iterator the iterator for which we should filter rows.
     *
     * @return an iterator that only returns the rows (or rather Unfilted) from {@code iterator} that are selected by this filter.
     */
    public UnfilteredRowIterator filterNotIndexed(ColumnFilter columnFilter, UnfilteredRowIterator iterator);

    public Slices getSlices(CFMetaData metadata);

    /**
     * Given a partition, returns a row iterator for the rows of this partition that are selected by this filter.
     *
     * @param columnFilter the columns to include in the rows of the result iterator.
     * @param partition the partition containing the rows to filter.
     *
     * @return a unfiltered row iterator returning those rows (or rather Unfiltered) from {@code partition} that are selected by this filter.
     */
    // TODO: we could get rid of that if Partition was exposing a SliceableUnfilteredRowIterator (instead of the two searchIterator() and
    // unfilteredIterator() methods). However, for AtomicBtreePartition this would require changes to Btree so we'll leave that for later.
    public UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter columnFilter, Partition partition);

    /**
     * Whether the provided sstable may contain data that is selected by this filter (based on the sstable metadata).
     *
     * @param sstable the sstable for which we want to test the need for inclusion.
     *
     * @return whether {@code sstable} should be included to answer this filter.
     */
    public boolean shouldInclude(SSTableReader sstable);

    public Kind kind();

    public String toString(CFMetaData metadata);
    public String toCQLString(CFMetaData metadata);

    public interface Serializer
    {
        public void serialize(ClusteringIndexFilter filter, DataOutputPlus out, int version) throws IOException;
        public ClusteringIndexFilter deserialize(DataInputPlus in, int version, CFMetaData metadata) throws IOException;
        public long serializedSize(ClusteringIndexFilter filter, int version);
    }
}
