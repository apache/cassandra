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
package org.apache.cassandra.cql3.selection;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.ResultSet.ResultMetadata;
import org.apache.cassandra.cql3.selection.Selection.Selectors;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class ResultSetBuilder
{
    private final ResultSet resultSet;

    /**
     * As multiple thread can access a <code>Selection</code> instance each <code>ResultSetBuilder</code> will use
     * its own <code>Selectors</code> instance.
     */
    private final Selectors selectors;

    /**
     * The <code>GroupMaker</code> used to build the aggregates.
     */
    private final GroupMaker groupMaker;

    /*
     * We'll build CQL3 row one by one.
     * The currentRow is the values for the (CQL3) columns we've fetched.
     * We also collect timestamps and ttls for the case where the writetime and
     * ttl functions are used. Note that we might collect timestamp and/or ttls
     * we don't care about, but since the array below are allocated just once,
     * it doesn't matter performance wise.
     */
    List<ByteBuffer> current;
    final long[] timestamps;
    final int[] ttls;

    public ResultSetBuilder(ResultMetadata metadata, Selectors selectors)
    {
        this(metadata, selectors, null);
    }

    public ResultSetBuilder(ResultMetadata metadata, Selectors selectors, GroupMaker groupMaker)
    {
        this.resultSet = new ResultSet(metadata.copy(), new ArrayList<List<ByteBuffer>>());
        this.selectors = selectors;
        this.groupMaker = groupMaker;
        this.timestamps = selectors.collectTimestamps() ? new long[selectors.numberOfFetchedColumns()] : null;
        this.ttls = selectors.collectTTLs() ? new int[selectors.numberOfFetchedColumns()] : null;

        // We use MIN_VALUE to indicate no timestamp and -1 for no ttl
        if (timestamps != null)
            Arrays.fill(timestamps, Long.MIN_VALUE);
        if (ttls != null)
            Arrays.fill(ttls, -1);
    }

    public void add(ByteBuffer v)
    {
        current.add(v);
    }

    public void add(Cell c, int nowInSec)
    {
        if (c == null)
        {
            current.add(null);
            return;
        }

        current.add(value(c));

        if (timestamps != null)
            timestamps[current.size() - 1] = c.timestamp();

        if (ttls != null)
            ttls[current.size() - 1] = remainingTTL(c, nowInSec);
    }

    private int remainingTTL(Cell c, int nowInSec)
    {
        if (!c.isExpiring())
            return -1;

        int remaining = c.localDeletionTime() - nowInSec;
        return remaining >= 0 ? remaining : -1;
    }

    private ByteBuffer value(Cell c)
    {
        return c.isCounterCell()
             ? ByteBufferUtil.bytes(CounterContext.instance().total(c.value()))
             : c.value();
    }

    /**
     * Notifies this <code>Builder</code> that a new row is being processed.
     *
     * @param partitionKey the partition key of the new row
     * @param clustering the clustering of the new row
     */
    public void newRow(DecoratedKey partitionKey, Clustering clustering)
    {
        // The groupMaker needs to be called for each row
        boolean isNewAggregate = groupMaker == null || groupMaker.isNewGroup(partitionKey, clustering);
        if (current != null)
        {
            selectors.addInputRow(this);
            if (isNewAggregate)
            {
                resultSet.addRow(getOutputRow());
                selectors.reset();
            }
        }
        current = new ArrayList<>(selectors.numberOfFetchedColumns());

        // Timestamps and TTLs are arrays per row, we must null them out between rows
        if (timestamps != null)
            Arrays.fill(timestamps, Long.MIN_VALUE);
        if (ttls != null)
            Arrays.fill(ttls, -1);
    }

    /**
     * Builds the <code>ResultSet</code>
     */
    public ResultSet build()
    {
        if (current != null)
        {
            selectors.addInputRow(this);
            resultSet.addRow(getOutputRow());
            selectors.reset();
            current = null;
        }

        // For aggregates we need to return a row even it no records have been found
        if (resultSet.isEmpty() && groupMaker != null && groupMaker.returnAtLeastOneRow())
            resultSet.addRow(getOutputRow());
        return resultSet;
    }

    private List<ByteBuffer> getOutputRow()
    {
        return selectors.getOutputRow();
    }
}