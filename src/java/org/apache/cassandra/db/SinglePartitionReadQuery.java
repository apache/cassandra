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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.MultiPartitionPager;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.service.pager.SinglePartitionPager;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * A {@code ReadQuery} for a single partition.
 */
public interface SinglePartitionReadQuery extends ReadQuery
{
    public static Group<? extends SinglePartitionReadQuery> createGroup(TableMetadata metadata,
                                                                        long nowInSec,
                                                                        ColumnFilter columnFilter,
                                                                        RowFilter rowFilter,
                                                                        DataLimits limits,
                                                                        List<DecoratedKey> partitionKeys,
                                                                        ClusteringIndexFilter clusteringIndexFilter)
    {
        return SinglePartitionReadCommand.Group.create(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKeys, clusteringIndexFilter);
    }


    /**
     * Creates a new read query on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param columnFilter the column filter to use for the query.
     * @param filter the clustering index filter to use for the query.
     *
     * @return a newly created read query. The returned query will use no row filter and have no limits.
     */
    public static SinglePartitionReadQuery create(TableMetadata metadata,
                                                  long nowInSec,
                                                  DecoratedKey key,
                                                  ColumnFilter columnFilter,
                                                  ClusteringIndexFilter filter)
    {
        return create(metadata, nowInSec, columnFilter, RowFilter.none(), DataLimits.NONE, key, filter);
    }

    /**
     * Creates a new read query on a single partition.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param columnFilter the column filter to use for the query.
     * @param rowFilter the row filter to use for the query.
     * @param limits the limits to use for the query.
     * @param partitionKey the partition key for the partition to query.
     * @param clusteringIndexFilter the clustering index filter to use for the query.
     *
     * @return a newly created read query.
     */
    public static SinglePartitionReadQuery create(TableMetadata metadata,
                                                  long nowInSec,
                                                  ColumnFilter columnFilter,
                                                  RowFilter rowFilter,
                                                  DataLimits limits,
                                                  DecoratedKey partitionKey,
                                                  ClusteringIndexFilter clusteringIndexFilter)
    {
        return SinglePartitionReadCommand.create(metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    /**
     * Returns the key of the partition queried by this {@code ReadQuery}
     * @return the key of the partition queried
     */
    DecoratedKey partitionKey();

    /**
     * Creates a new {@code SinglePartitionReadQuery} with the specified limits.
     *
     * @param newLimits the new limits
     * @return the new {@code SinglePartitionReadQuery}
     */
    SinglePartitionReadQuery withUpdatedLimit(DataLimits newLimits);

    /**
     * Returns a new {@code SinglePartitionReadQuery} suitable to paging from the last returned row.
     *
     * @param lastReturned the last row returned by the previous page. The newly created query
     * will only query row that comes after this (in query order). This can be {@code null} if this
     * is the first page.
     * @param limits the limits to use for the page to query.
     *
     * @return the newly create query.
     */
    SinglePartitionReadQuery forPaging(Clustering<?> lastReturned, DataLimits limits);

    @Override
    default SinglePartitionPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new SinglePartitionPager(this, pagingState, protocolVersion);
    }

    ClusteringIndexFilter clusteringIndexFilter();

    default boolean selectsKey(DecoratedKey key)
    {
        if (!this.partitionKey().equals(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
    }

    default boolean selectsClustering(DecoratedKey key, Clustering<?> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!clusteringIndexFilter().selects(clustering))
            return false;

        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    /**
     * Groups multiple single partition read queries.
     */
    abstract class Group<T extends SinglePartitionReadQuery> implements ReadQuery
    {
        public final List<T> queries;
        private final DataLimits limits;
        private final long nowInSec;
        private final boolean selectsFullPartitions;

        public Group(List<T> queries, DataLimits limits)
        {
            assert !queries.isEmpty();
            this.queries = queries;
            this.limits = limits;
            T firstQuery = queries.get(0);
            this.nowInSec = firstQuery.nowInSec();
            this.selectsFullPartitions = firstQuery.selectsFullPartition();
            for (int i = 1; i < queries.size(); i++)
                assert queries.get(i).nowInSec() == nowInSec;
        }

        public long nowInSec()
        {
            return nowInSec;
        }

        public DataLimits limits()
        {
            return limits;
        }

        public TableMetadata metadata()
        {
            return queries.get(0).metadata();
        }

        @Override
        public boolean selectsFullPartition()
        {
            return selectsFullPartitions;
        }

        public ReadExecutionController executionController()
        {
            // Note that the only difference between the queries in a group must be the partition key on which
            // they applied. So as far as ReadOrderGroup is concerned, we can use any of the queries to start one.
            return queries.get(0).executionController();
        }

        public PartitionIterator executeInternal(ReadExecutionController controller)
        {
            // Note that the only difference between the queries in a group must be the partition key on which
            // they applied.
            boolean enforceStrictLiveness = queries.get(0).metadata().enforceStrictLiveness();
            return limits.filter(UnfilteredPartitionIterators.filter(executeLocally(controller, false), nowInSec),
                                 nowInSec,
                                 selectsFullPartitions,
                                 enforceStrictLiveness);
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            return executeLocally(executionController, true);
        }

        /**
         * Implementation of {@link ReadQuery#executeLocally(ReadExecutionController)}.
         *
         * @param executionController - the {@code ReadExecutionController} protecting the read.
         * @param sort - whether to sort the inner queries by partition key, required for merging the iterator
         *               later on. This will be false when called by {@link ReadQuery#executeInternal(ReadExecutionController)}
         *               because in this case it is safe to do so as there is no merging involved and we don't want to
         *               change the old behavior which was to not sort by partition.
         *
         * @return - the iterator that can be used to retrieve the query result.
         */
        private UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController, boolean sort)
        {
            List<Pair<DecoratedKey, UnfilteredPartitionIterator>> partitions = new ArrayList<>(queries.size());
            for (T query : queries)
                partitions.add(Pair.of(query.partitionKey(), query.executeLocally(executionController)));

            if (sort)
                Collections.sort(partitions, (p1, p2) -> p1.getLeft().compareTo(p2.getLeft()));

            return UnfilteredPartitionIterators.concat(partitions.stream().map(p -> p.getRight()).collect(Collectors.toList()));
        }

        public QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
        {
            if (queries.size() == 1)
                return new SinglePartitionPager(queries.get(0), pagingState, protocolVersion);

            return new MultiPartitionPager<T>(this, pagingState, protocolVersion);
        }

        public boolean selectsKey(DecoratedKey key)
        {
            return Iterables.any(queries, c -> c.selectsKey(key));
        }

        public boolean selectsClustering(DecoratedKey key, Clustering<?> clustering)
        {
            return Iterables.any(queries, c -> c.selectsClustering(key, clustering));
        }

        @Override
        public RowFilter rowFilter()
        {
            // Note that the only difference between the query in a group must be the partition key on which
            // they applied.
            return queries.get(0).rowFilter();
        }

        @Override
        public ColumnFilter columnFilter()
        {
            // Note that the only difference between the query in a group must be the partition key on which
            // they applied.
            return queries.get(0).columnFilter();
        }

        @Override
        public void trackWarnings()
        {
            queries.forEach(ReadQuery::trackWarnings);
        }

        @Override
        public String toString()
        {
            return queries.toString();
        }
    }
}
