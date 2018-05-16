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

import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

/**
 * A read query that selects a (part of a) single partition of a virtual table.
 */
public class VirtualTableSinglePartitionReadQuery extends VirtualTableReadQuery implements SinglePartitionReadQuery
{
    private final DecoratedKey partitionKey;
    private final ClusteringIndexFilter clusteringIndexFilter;

    public static VirtualTableSinglePartitionReadQuery create(TableMetadata metadata,
                                                              int nowInSec,
                                                              ColumnFilter columnFilter,
                                                              RowFilter rowFilter,
                                                              DataLimits limits,
                                                              DecoratedKey partitionKey,
                                                              ClusteringIndexFilter clusteringIndexFilter)
    {
        return new VirtualTableSinglePartitionReadQuery(metadata,
                                                        nowInSec,
                                                        columnFilter,
                                                        rowFilter,
                                                        limits,
                                                        partitionKey,
                                                        clusteringIndexFilter);
    }

    private VirtualTableSinglePartitionReadQuery(TableMetadata metadata,
                                                 int nowInSec,
                                                 ColumnFilter columnFilter,
                                                 RowFilter rowFilter,
                                                 DataLimits limits,
                                                 DecoratedKey partitionKey,
                                                 ClusteringIndexFilter clusteringIndexFilter)
    {
        super(metadata, nowInSec, columnFilter, rowFilter, limits);
        this.partitionKey = partitionKey;
        this.clusteringIndexFilter = clusteringIndexFilter;
    }

    @Override
    protected void appendCQLWhereClause(StringBuilder sb)
    {
        sb.append(" WHERE ");

        sb.append(ColumnMetadata.toCQLString(metadata().partitionKeyColumns())).append(" = ");
        DataRange.appendKeyString(sb, metadata().partitionKeyType, partitionKey().getKey());

        // We put the row filter first because the clustering index filter can end by "ORDER BY"
        if (!rowFilter().isEmpty())
            sb.append(" AND ").append(rowFilter());

        String filterString = clusteringIndexFilter().toCQLString(metadata());
        if (!filterString.isEmpty())
            sb.append(" AND ").append(filterString);
    }

    @Override
    public ClusteringIndexFilter clusteringIndexFilter()
    {
        return clusteringIndexFilter;
    }

    @Override
    public boolean selectsFullPartition()
    {
        return clusteringIndexFilter.selectsAllPartition() && !rowFilter().hasExpressionOnClusteringOrRegularColumns();
    }

    @Override
    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    @Override
    public SinglePartitionReadQuery withUpdatedLimit(DataLimits newLimits)
    {
        return new VirtualTableSinglePartitionReadQuery(metadata(),
                                                        nowInSec(),
                                                        columnFilter(),
                                                        rowFilter(),
                                                        newLimits,
                                                        partitionKey(),
                                                        clusteringIndexFilter);
    }

    @Override
    public SinglePartitionReadQuery forPaging(Clustering lastReturned, DataLimits limits)
    {
        return new VirtualTableSinglePartitionReadQuery(metadata(),
                                                        nowInSec(),
                                                        columnFilter(),
                                                        rowFilter(),
                                                        limits,
                                                        partitionKey(),
                                                      lastReturned == null ? clusteringIndexFilter
                                                              : clusteringIndexFilter.forPaging(metadata().comparator,
                                                                                                lastReturned,
                                                                                                false));
    }

    @Override
    protected UnfilteredPartitionIterator queryVirtualTable()
    {
        VirtualTable view = VirtualKeyspaceRegistry.instance.getTableNullable(metadata().id);
        return view.select(partitionKey, clusteringIndexFilter, columnFilter());
    }

    /**
     * Groups multiple single partition read queries.
     */
    public static class Group extends SinglePartitionReadQuery.Group<VirtualTableSinglePartitionReadQuery>
    {
        public static Group create(TableMetadata metadata,
                                   int nowInSec,
                                   ColumnFilter columnFilter,
                                   RowFilter rowFilter,
                                   DataLimits limits,
                                   List<DecoratedKey> partitionKeys,
                                   ClusteringIndexFilter clusteringIndexFilter)
        {
            List<VirtualTableSinglePartitionReadQuery> queries = new ArrayList<>(partitionKeys.size());
            for (DecoratedKey partitionKey : partitionKeys)
            {
                queries.add(VirtualTableSinglePartitionReadQuery.create(metadata,
                                                                        nowInSec,
                                                                        columnFilter,
                                                                        rowFilter,
                                                                        limits,
                                                                        partitionKey,
                                                                        clusteringIndexFilter));
            }

            return new Group(queries, limits);
        }

        public Group(List<VirtualTableSinglePartitionReadQuery> queries, DataLimits limits)
        {
            super(queries, limits);
        }

        public static Group one(VirtualTableSinglePartitionReadQuery query)
        {
            return new Group(Collections.singletonList(query), query.limits());
        }

        public PartitionIterator execute(ConsistencyLevel consistency, ClientState clientState, long queryStartNanoTime) throws RequestExecutionException
        {
            if (queries.size() == 1)
                return queries.get(0).execute(consistency, clientState, queryStartNanoTime);

            return PartitionIterators.concat(queries.stream()
                                                    .map(q -> q.execute(consistency, clientState, queryStartNanoTime))
                                                    .collect(Collectors.toList()));
        }
    }
}