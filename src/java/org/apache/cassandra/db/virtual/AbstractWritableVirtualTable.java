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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;

/**
 * An abstract virtual table implementation that builds the resultset on demand and allows source modification.
 */
public abstract class AbstractWritableVirtualTable extends AbstractVirtualTable
{

    protected AbstractWritableVirtualTable(TableMetadata metadata)
    {
        super(metadata);
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        if (update.deletionInfo().isLive())
        {
            update.forEach(row ->
            {
                if (row.deletion().isLive()) {
                    row.forEach(columnData ->
                    {
                        if (columnData.column().isComplex())
                            throw new InvalidRequestException("Complex types are not supported by table " + metadata);
                        Cell<?> cell = (Cell<?>) columnData;
                        if (cell.isTombstone())
                            applyColumnDeletion(update.partitionKey(), row.clustering(), cell);
                        else
                            applyUpdate(update.partitionKey(), row.clustering(), cell);
                    });
                }
                else
                {
                    // row deletion
                    applyRowDeletion(update.partitionKey(), row.clustering(), row.deletion());
                }
            });
        }
        else
        {
            if (update.deletionInfo().hasRanges())
                update.deletionInfo().rangeIterator(false).forEachRemaining(rt -> applyRangeTombstone(update.partitionKey(), rt));
            else
                applyPartitionDeletion(update.partitionKey(), update.deletionInfo().getPartitionDeletion());
        }
    }

    protected abstract void applyPartitionDeletion(DecoratedKey partitionKey, DeletionTime partitionDeletion);

    protected abstract void applyRangeTombstone(DecoratedKey partitionKey, RangeTombstone rt);

    protected abstract void applyRowDeletion(DecoratedKey partitionKey, Clustering<?> clustering, Row.Deletion rowDeletion);

    protected abstract void applyColumnDeletion(DecoratedKey partitionKey, Clustering<?> clustering, Cell<?> cell);

    protected abstract void applyUpdate(DecoratedKey partitionKey, Clustering<?> clustering, Cell<?> cell);
}
