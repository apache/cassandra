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

package org.apache.cassandra.harry.dsl;

import java.io.Closeable;
import java.util.function.Consumer;

import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.model.OpSelectors;
import org.apache.cassandra.harry.visitors.ReplayingVisitor;

public class BatchVisitBuilder extends SingleOperationVisitBuilder implements Closeable
{
    private final HistoryBuilder historyBuilder;

    public BatchVisitBuilder(HistoryBuilder historyBuilder,
                             PartitionVisitStateImpl partitionState,
                             long lts,
                             OpSelectors.PureRng rng,
                             OpSelectors.DescriptorSelector descriptorSelector,
                             SchemaSpec schema,
                             ValueHelper valueHelper,
                             Consumer<ReplayingVisitor.Visit> appendToLog)
    {
        super(partitionState, lts, rng, descriptorSelector, schema, valueHelper, appendToLog);
        this.historyBuilder = historyBuilder;
    }

    @Override
    public int size()
    {
        return super.size();
    }

    @Override
    public BatchVisitBuilder insert()
    {
        super.insert();
        return this;
    }

    public BatchVisitBuilder inserts(int n)
    {
        assert n > 0;
        for (int i = 0; i < n; i++)
            insert();
        return this;
    }

    @Override
    public BatchVisitBuilder insert(int rowIdx)
    {
        super.insert(rowIdx);
        return this;
    }

    @Override
    public BatchVisitBuilder insert(int rowIdx, long[] valueIdxs)
    {
        super.insert(rowIdx, valueIdxs);
        return this;
    }

    @Override
    public BatchVisitBuilder deletePartition()
    {
        super.deletePartition();
        return this;
    }

    @Override
    public BatchVisitBuilder deleteRow()
    {
        super.deleteRow();
        return this;
    }

    @Override
    public BatchVisitBuilder deleteColumns()
    {
        super.deleteColumns();
        return this;
    }

    @Override
    public BatchVisitBuilder deleteRowRange()
    {
        super.deleteRowRange();
        return this;
    }

    @Override
    public BatchVisitBuilder deleteRowRange(int lowBoundRowIdx, int highBoundRowIdx, boolean isMinEq, boolean isMaxEq)
    {
        super.deleteRowRange(lowBoundRowIdx, highBoundRowIdx, isMinEq, isMaxEq);
        return this;
    }

    @Override
    public BatchVisitBuilder deleteRowSlice()
    {
        super.deleteRowSlice();
        return this;
    }

    // TODO: prevent from closing more than once
    public HistoryBuilder endBatch()
    {
        super.end();
        return this.historyBuilder;
    }

    /**
     * Implements closeable to instruct users to end batch before using.
     *
     * Non-finished batches are _not_ appended to the history and will appear as gaps in history.
     */
    @Override
    public void close()
    {
        endBatch();
    }
}