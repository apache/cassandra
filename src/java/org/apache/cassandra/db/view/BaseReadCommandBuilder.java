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

package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class BaseReadCommandBuilder
{
    private final TableMetadata baseMetadata;
    private final TableMetadata viewMetadata;
    private final View view;
    private final Map<ColumnMetadata, ByteBuffer> viewRawDataMap;

    public BaseReadCommandBuilder(View view, ByteBuffer viewPartitionKey, Clustering<?> viewClustering)
    {
        this.view = view;
        this.baseMetadata = view.getDefinition().baseTableMetadata();
        this.viewMetadata = view.getDefinition().metadata;
        this.viewRawDataMap = getViewRawDataMap(viewPartitionKey, viewClustering);
    }

    Map<ColumnMetadata, ByteBuffer> viewRawDataMap()
    {
        return ImmutableMap.copyOf(viewRawDataMap);
    }

    public DecoratedKey buildBasePartitionKey()
    {
        List<ColumnMetadata> basePks = baseMetadata.partitionKeyColumns();
        ByteBuffer[] pkValues = new ByteBuffer[basePks.size()];
        for (int i = 0; i < basePks.size(); i++)
        {
            ColumnMetadata baseColumn = basePks.get(i);
            ColumnMetadata viewColumn = view.getViewColumn(baseColumn);
            ByteBuffer value = viewRawDataMap.get(viewColumn);
            if (value == null)
                throw new IllegalArgumentException("Missing value for base table partition key column: " + baseColumn.name);
            pkValues[i] = value;
        }

        ByteBuffer partitionKey;
        if (basePks.size() == 1)
            partitionKey = pkValues[0];
        else
        {
            AbstractType<?> partitionKeyType = baseMetadata.partitionKeyType;
            if (partitionKeyType instanceof CompositeType)
                partitionKey = CompositeType.build(ByteBufferAccessor.instance, pkValues);
            else
                throw new IllegalStateException("Expected CompositeType for multi-column partition key");
        }
        return baseMetadata.partitioner.decorateKey(partitionKey);
    }

    public Clustering<?> buildBaseClusteringKey()
    {
        List<ColumnMetadata> baseCks = baseMetadata.clusteringColumns();
        if (baseCks.isEmpty())
            return Clustering.EMPTY;

        ByteBuffer[] ckValues = new ByteBuffer[baseCks.size()];
        for (int i = 0; i < baseCks.size(); i++)
        {
            ColumnMetadata baseColumn = baseCks.get(i);
            ColumnMetadata viewColumn = view.getViewColumn(baseColumn);
            ByteBuffer value = viewRawDataMap.get(viewColumn);
            if (value == null)
                throw new IllegalArgumentException("Missing value for base cluster key column: " + baseColumn.name);
            ckValues[i] = value;
        }
        return Clustering.make(ckValues);
    }

    public ByteBuffer buildNonPrimaryKeyValue()
    {
        if (view.hasSamePrimaryKeyColumnsAsBaseTable())
            return null;
        ColumnMetadata baseNonPkColumn = view.baseNonPKColumnsInViewPK.get(0);
        ColumnMetadata viewColumn = view.getViewColumn(baseNonPkColumn);
        return viewRawDataMap.get(viewColumn);
    }

    /**
     * Returns a string representation of the view primary key (partition key + clustering key).
     * Format: (col1=val1, col2=val2, ...)
     */
    public String formatViewPrimaryKey()
    {
        StringBuilder sb = new StringBuilder("(");
        boolean first = true;

        // Add partition key columns
        for (ColumnMetadata col : viewMetadata.partitionKeyColumns())
        {
            if (!first) sb.append(", ");
            first = false;
            ByteBuffer value = viewRawDataMap.get(col);
            sb.append(col.name).append('=').append(formatValue(col, value));
        }

        // Add clustering columns
        for (ColumnMetadata col : viewMetadata.clusteringColumns())
        {
            if (!first) sb.append(", ");
            first = false;
            ByteBuffer value = viewRawDataMap.get(col);
            sb.append(col.name).append('=').append(formatValue(col, value));
        }

        sb.append(')');
        return sb.toString();
    }

    private String formatValue(ColumnMetadata col, ByteBuffer value)
    {
        if (value == null)
            return "NULL";
        return col.type.getString(value);
    }

    Map<ColumnMetadata, ByteBuffer> getViewRawDataMap(ByteBuffer viewPartitionKey, Clustering<?> viewClustering)
    {
        Map<ColumnMetadata, ByteBuffer> map = new HashMap<>(viewMetadata.partitionKeyColumns().size() + viewMetadata.clusteringColumns().size());
        List<ColumnMetadata> partitionKeyColumns = viewMetadata.partitionKeyColumns();

        if (viewMetadata.partitionKeyType instanceof CompositeType)
        {
            CompositeType compositeType = (CompositeType) viewMetadata.partitionKeyType;
            ByteBuffer[] components = compositeType.split(viewPartitionKey);
            for (ColumnMetadata column : viewMetadata.partitionKeyColumns())
                map.put(column, components[column.position()]);
        }
        else
            map.put(partitionKeyColumns.get(0), viewPartitionKey);

        ByteBuffer[] clusteringComponents = viewClustering.getBufferArray();
        for (ColumnMetadata column : viewMetadata.clusteringColumns())
            map.put(column, clusteringComponents[column.position()]);
        return map;
    }

    public SinglePartitionReadCommand buildBaseTableReadCommand(int nowInSec)
    {
        ClusteringIndexNamesFilter baseClusteringFilter = new ClusteringIndexNamesFilter(FBUtilities.singleton(buildBaseClusteringKey(),
                                                                                                               baseMetadata.comparator),
                                                                                         false);
        return SinglePartitionReadCommand.createUnfiltered(false,
                                                           0,
                                                           false,
                                                           baseMetadata,
                                                           nowInSec,
                                                           ColumnFilter.all(baseMetadata),
                                                           RowFilter.NONE,
                                                           DataLimits.NONE,
                                                           buildBasePartitionKey(),
                                                           baseClusteringFilter,
                                                           null,
                                                           false);
    }

    public SinglePartitionReadCommand buildViewTableReadCommand(ByteBuffer viewPartitionKey, Clustering<?> viewClustering, int nowInSec)
    {
        ClusteringIndexNamesFilter viewClusteringFilter = new ClusteringIndexNamesFilter(FBUtilities.singleton(viewClustering,
                                                                                                               viewMetadata.comparator),
                                                                                         false);
        return SinglePartitionReadCommand.createUnfiltered(false,
                                                           0,
                                                           false,
                                                           viewMetadata,
                                                           nowInSec,
                                                           ColumnFilter.all(viewMetadata),
                                                           RowFilter.NONE,
                                                           DataLimits.NONE,
                                                           viewMetadata.partitioner.decorateKey(viewPartitionKey),
                                                           viewClusteringFilter,
                                                           null,
                                                           false);
    }
}
