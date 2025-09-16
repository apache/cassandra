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

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;

/**
 * Utility class for translating base table rows to materialized view rows during backfill operations.
 * 
 * This class provides a dedicated path for MV backfill that generates view rows directly
 * without going through the mutation generation process. It's specifically designed for
 * SSTable-based backfill operations where we want to create view rows efficiently.
 * 
 * This is separate from ViewUpdateGenerator which handles regular MV updates through mutations.
 */
public class ViewRowTranslator
{
    /**
     * Result of translating a base row to a view row for backfill operations.
     */
    public static class ViewRowResult
    {
        public final Row viewRow;
        public final DecoratedKey viewPartitionKey;
        
        public ViewRowResult(Row viewRow, DecoratedKey viewPartitionKey)
        {
            this.viewRow = viewRow;
            this.viewPartitionKey = viewPartitionKey;
        }
    }
    
    /**
     * Translates a base table row to a materialized view row for backfill operations.
     * This method combines filtering, translation, and partition key calculation in one call.
     * 
     * @param view the materialized view
     * @param baseRow the base table row to translate
     * @param basePartitionKey the base table partition key
     * @param nowInSec current time in seconds
     * @return BackfillRowResult containing the view row and partition key, or null if filtered out
     */
    public static ViewRowResult translateForBackfill(View view, Row baseRow, DecoratedKey basePartitionKey, int nowInSec)
    {
        // Check if the base row matches the view filter
        if (!view.matchesViewFilter(basePartitionKey, baseRow, nowInSec))
            return null;
        Row viewRow = translateBaseRowToViewRow(view, baseRow, basePartitionKey, nowInSec);
        DecoratedKey viewPartitionKey = calculateViewPartitionKey(view, baseRow, basePartitionKey);
        return new ViewRowResult(viewRow, viewPartitionKey);
    }

    /**
     * Translates a base table row to a materialized view row.
     *
     * @param view the materialized view
     * @param baseRow the base table row to translate
     * @param basePartitionKey the base table partition key
     * @param nowInSec current time in seconds
     * @return the translated view row
     */
    public static Row translateBaseRowToViewRow(View view, Row baseRow, DecoratedKey basePartitionKey, int nowInSec)
    {

        TableMetadata baseMetadata = view.getDefinition().baseTableMetadata();
        TableMetadata viewMetadata = view.getDefinition().metadata;
        ByteBuffer[] basePartitionKeyComponents = ViewUtils.extractKeyComponents(basePartitionKey, baseMetadata.partitionKeyType);

        // Calculate view clustering
        ByteBuffer[] clusteringValues = new ByteBuffer[viewMetadata.clusteringColumns().size()];
        for (ColumnMetadata viewColumn : viewMetadata.clusteringColumns())
        {
            ColumnMetadata baseColumn = view.getBaseColumn(viewColumn);
            ByteBuffer value = ViewUtils.getValueForPK(baseColumn, baseRow, basePartitionKeyComponents);

            assert value != null;

            if (!viewColumn.isPartitionKey())
                clusteringValues[viewColumn.position()] = value;
        }

        // Build the view row
        Row.Builder viewRowBuilder = BTreeRow.sortedBuilder();
        viewRowBuilder.newRow(Clustering.make(clusteringValues));

        // Add primary key liveness info
        LivenessInfo livenessInfo = ViewUtils.computeLivenessInfoForEntry(view, baseRow, nowInSec);
        viewRowBuilder.addPrimaryKeyLivenessInfo(livenessInfo);

        // Add row deletion
        viewRowBuilder.addRowDeletion(baseRow.deletion());

        // Add column data
        for (ColumnData data : baseRow)
        {
            ColumnMetadata viewColumn = view.getViewColumn(data.column());
            // If that base table column is not denormalized in the view, we had nothing to do.
            // Also, if it's part of the view PK it's already been taken into account in the clustering.
            if (viewColumn == null || viewColumn.isPrimaryKeyColumn())
                continue;

            ViewUtils.addColumnDataToBuilder(viewRowBuilder, viewColumn, data);
        }

        return viewRowBuilder.build();
    }

    /**
     * Calculates the view partition key for a base table row.
     *
     * @param view the materialized view
     * @param baseRow the base table row
     * @param basePartitionKey the base table partition key
     * @return the calculated view partition key
     */
    public static DecoratedKey calculateViewPartitionKey(View view, Row baseRow, DecoratedKey basePartitionKey)
    {
        TableMetadata baseMetadata = view.getDefinition().baseTableMetadata();
        TableMetadata viewMetadata = view.getDefinition().metadata;
        ByteBuffer[] basePartitionKeyComponents = ViewUtils.extractKeyComponents(basePartitionKey, baseMetadata.partitionKeyType);

        ByteBuffer[] viewPartitionKeyComponents = new ByteBuffer[viewMetadata.partitionKeyColumns().size()];
        for (ColumnMetadata viewColumn : viewMetadata.partitionKeyColumns())
        {
            ColumnMetadata baseColumn = view.getBaseColumn(viewColumn);
            ByteBuffer value = ViewUtils.getValueForPK(baseColumn, baseRow, basePartitionKeyComponents);

            assert value != null;
                
            viewPartitionKeyComponents[viewColumn.position()] = value;
        }

        return ViewUtils.makeViewPartitionKey(viewMetadata, viewPartitionKeyComponents);
    }
}
