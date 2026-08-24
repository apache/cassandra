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

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.sai.QueryContext;

/**
 * Counts the number of partitions, rows and tombstones fetched by an index query, before post-filtering and sorting.
 */
class CountFetchedTransformation extends Transformation<UnfilteredRowIterator>
{
    private final QueryContext queryContext;
    private final int nowInSec;

    CountFetchedTransformation(QueryContext queryContext, int nowInSec)
    {
        this.queryContext = queryContext;
        this.nowInSec = nowInSec;
    }

    /**
     * Updates the query context metrics about the number of fetched partitions, rows and tombstones
     * with the contents of the provided partition iterator.
     *
     * @param partition the results of querying the base table with the indexed keys, before applying post-filtering and sorting
     * @return a copy of the provided row iterator, which will populate the query context as it is consumed
     */
    UnfilteredRowIterator apply(UnfilteredRowIterator partition)
    {
        return Transformation.apply(partition, this);
    }

    @Override
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        queryContext.checkpoint();
        if (deletionTime.deletes(nowInSec))
            queryContext.addPartitionTombstonesFetched(1);
        else
            queryContext.addPartitionsFetched(1);
        return deletionTime;
    }

    @Override
    protected Row applyToRow(Row row)
    {
        queryContext.checkpoint();

        if (row.hasLiveData(nowInSec, false))
            queryContext.addRowsFetched(1);
        else
            queryContext.addRowTombstonesFetched(1);

        queryContext.addCellsFetched(row.cellsCount());

        return row;
    }

    @Override
    protected Row applyToStatic(Row row)
    {
        queryContext.addCellsFetched(row.cellsCount());
        return row;
    }

    @Override
    protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        queryContext.checkpoint();
        queryContext.addRowTombstonesFetched(1);
        return marker;
    }
}
