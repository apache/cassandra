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

import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.service.pager.PartitionRangeQueryPager;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 *  A {@code ReadQuery} for a range of partitions.
 */
public interface PartitionRangeReadQuery extends ReadQuery
{
    static ReadQuery create(TableMetadata table,
                            long nowInSec,
                            ColumnFilter columnFilter,
                            RowFilter rowFilter,
                            DataLimits limits,
                            DataRange dataRange)
    {
        return PartitionRangeReadCommand.create(table, nowInSec, columnFilter, rowFilter, limits, dataRange);
    }

    DataRange dataRange();

    /**
     * Creates a new {@code PartitionRangeReadQuery} with the updated limits.
     *
     * @param newLimits the new limits
     * @return the new {@code PartitionRangeReadQuery}
     */
    PartitionRangeReadQuery withUpdatedLimit(DataLimits newLimits);

    /**
     * Creates a new {@code PartitionRangeReadQuery} with the updated limits and data range.
     *
     * @param newLimits the new limits
     * @return the new {@code PartitionRangeReadQuery}
     */
    PartitionRangeReadQuery withUpdatedLimitsAndDataRange(DataLimits newLimits, DataRange newDataRange);

    default QueryPager getPager(PagingState pagingState, ProtocolVersion protocolVersion)
    {
        return new PartitionRangeQueryPager(this, pagingState, protocolVersion);
    }

    default boolean selectsKey(DecoratedKey key)
    {
        if (!dataRange().contains(key))
            return false;

        return rowFilter().partitionKeyRestrictionsAreSatisfiedBy(key, metadata().partitionKeyType);
    }

    default boolean selectsClustering(DecoratedKey key, Clustering<?> clustering)
    {
        if (clustering == Clustering.STATIC_CLUSTERING)
            return !columnFilter().fetchedColumns().statics.isEmpty();

        if (!dataRange().clusteringIndexFilter(key).selects(clustering))
            return false;
        return rowFilter().clusteringKeyRestrictionsAreSatisfiedBy(clustering);
    }

    default boolean selectsFullPartition()
    {
        if (metadata().isStaticCompactTable())
            return true;

        return dataRange().selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns();
    }
}
