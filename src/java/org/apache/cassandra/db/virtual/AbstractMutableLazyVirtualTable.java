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
package org.apache.cassandra.db.virtual;

import java.util.Iterator;

import javax.annotation.Nullable;

import accord.utils.Invariants;

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.db.ClusteringPrefix.Kind.STATIC_CLUSTERING;

/**
 * An abstract virtual table implementation that builds the resultset on demand and allows fine-grained source
 * modification via INSERT/UPDATE, DELETE and TRUNCATE operations.
 * 
 * Virtual table implementation need to be thread-safe has they can be called from different threads.
 */
public abstract class AbstractMutableLazyVirtualTable extends AbstractLazyVirtualTable
{
    protected AbstractMutableLazyVirtualTable(TableMetadata metadata, OnTimeout onTimeout, Sorted sorted)
    {
        super(metadata, onTimeout, sorted);
    }

    protected AbstractMutableLazyVirtualTable(TableMetadata metadata, OnTimeout onTimeout, Sorted sorted, Sorted sortedByPartitionKey)
    {
        super(metadata, onTimeout, sorted, sortedByPartitionKey);
    }

    protected void applyPartitionDeletion(Object[] partitionKeys)
    {
        throw invalidRequest("Partition deletion is not supported by table %s", metadata());
    }

    protected void applyRangeTombstone(Object[] partitionKey, Object[] start, boolean startInclusive, Object[] end, boolean endInclusive)
    {
        throw invalidRequest("Range deletion is not supported by table %s", metadata());
    }

    protected void applyRowDeletion(Object[] partitionKey, @Nullable Object[] clusteringKeys)
    {
        throw invalidRequest("Row deletion is not supported by table %s", metadata());
    }

    protected void applyRowUpdate(Object[] partitionKeys, @Nullable Object[] clusteringKeys, ColumnMetadata[] columns, Object[] values)
    {
        throw invalidRequest("Column modification is not supported by table %s", metadata());
    }

    private void applyRangeTombstone(Object[] pks, RangeTombstone rt)
    {
        Slice slice = rt.deletedSlice();
        Object[] starts = composeClusterings(slice.start(), metadata());
        Object[] ends = composeClusterings(slice.end(), metadata());
        applyRangeTombstone(pks, starts, slice.start().isInclusive(), ends, slice.end().isInclusive());
    }

    private void applyRow(Object[] pks, Row row)
    {
        Object[] cks = row.clustering().kind() == STATIC_CLUSTERING ? null : composeClusterings(row.clustering(), metadata());
        if (!row.deletion().isLive())
        {
            applyRowDeletion(pks, cks);
        }
        else
        {
            ColumnMetadata[] columns = new ColumnMetadata[row.columnCount()];
            Object[] values = new Object[row.columnCount()];
            int i = 0;
            for (ColumnData cd : row)
            {
                ColumnMetadata cm = cd.column();
                if (cm.isComplex())
                    throw new InvalidRequestException(metadata() + " does not support complex column updates");
                Cell cell = (Cell)cd;
                columns[i] = cm;
                if (!cell.isTombstone())
                    values[i] = cm.type.compose(cell.value(), cell.accessor());
                ++i;
            }
            Invariants.require(i == columns.length);
            applyRowUpdate(pks, cks, columns, values);
        }
    }

    public void apply(PartitionUpdate update)
    {
        TableMetadata metadata = metadata();
        Object[] pks = composePartitionKeys(update.partitionKey(), metadata);

        DeletionInfo deletionInfo = update.deletionInfo();
        if (!deletionInfo.getPartitionDeletion().isLive())
        {
            applyPartitionDeletion(pks);
        }
        else if (deletionInfo.hasRanges())
        {
            Iterator<RangeTombstone> iter = deletionInfo.rangeIterator(false);
            while (iter.hasNext())
                applyRangeTombstone(pks, iter.next());
        }
        else
        {
            for (Row row : update)
                applyRow(pks, row);
            if (!update.staticRow().isEmpty())
                applyRow(pks, update.staticRow());
        }
    }
}
