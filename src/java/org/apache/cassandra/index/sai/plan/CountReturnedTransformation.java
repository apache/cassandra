/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.plan;

import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.sai.QueryContext;

/**
 * Counts the final number of partitions and rows returned by a query to the coordinator, after post-filtering and sorting.
 * Tombstones are not counted because they are not returned to the coordinator.
 */
class CountReturnedTransformation extends Transformation<UnfilteredRowIterator>
{
    private final QueryContext queryContext;
    private final Runnable onClose;
    private final Transformation<UnfilteredRowIterator> rowCounter;

    private CountReturnedTransformation(QueryContext queryContext, Runnable onClose)
    {
        this.queryContext = queryContext;
        this.onClose = onClose;
        rowCounter = new Transformation<>() {

            @Override
            protected Row applyToRow(Row row)
            {
                queryContext.checkpoint();
                queryContext.addRowsReturned(1);
                queryContext.addCellsReturned(row.cellsCount());
                return row;
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                queryContext.addCellsReturned(row.cellsCount());
                return row;
            }
        };
    }

    /**
     * Updates the query context metrics about the number of partitions and rows returned to the coordinator
     * with the contents of the provided partition iterator.
     *
     * @param partition the partition iterator containing the final results to return to the coordinator
     * @param queryContext the query context to update with the metrics
     * @param onClose a callback to run when the transformation is closed
     * @return a copy of the provided partition iterator, which will populate the query context as it is consumed
     */
    static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator partition, QueryContext queryContext, Runnable onClose)
    {
        return Transformation.apply(partition, new CountReturnedTransformation(queryContext, onClose));
    }

    @Override
    protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
    {
        queryContext.checkpoint();
        queryContext.addPartitionsReturned(1);
        return Transformation.apply(partition, rowCounter);
    }

    @Override
    protected void onClose()
    {
        onClose.run();
    }
}
