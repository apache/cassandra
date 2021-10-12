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

package org.apache.cassandra.distributed.fuzz;

import harry.core.Run;
import harry.model.OpSelectors;
import harry.visitors.VisitExecutor;
import org.apache.cassandra.db.Keyspace;

public class SSTableLoadingVisitor extends VisitExecutor
{
    private final SSTableGenerator gen;
    private final int forceFlushAfter;

    public SSTableLoadingVisitor(Run run, int forceFlushAfter)
    {
        this.forceFlushAfter = forceFlushAfter;
        gen = new SSTableGenerator(run, Keyspace.open(run.schemaSpec.keyspace).getColumnFamilyStore(run.schemaSpec.table));
        gen.mark();
    }

    @Override
    protected void beforeLts(long l, long l1)
    {

    }

    protected void afterLts(long lts, long pd) {
        if (lts > 0 && lts % forceFlushAfter == 0)
            forceFlush();
    }

    @Override
    protected void beforeBatch(long l, long l1, long l2)
    {

    }

    @Override
    protected void afterBatch(long l, long l1, long l2)
    {

    }

    @Override
    protected void operation(long lts, long pd, long cd, long m, long opId, OpSelectors.OperationKind operationKind)
    {
        switch (operationKind)
        {
            case INSERT:
                gen.write(lts, pd, cd, opId, true).applyUnsafe();
                break;
            case INSERT_WITH_STATICS:
                gen.write(lts, pd, cd, opId, true).applyUnsafe();
                gen.writeStatic(lts, pd, cd, opId, true).applyUnsafe();
                break;
            case UPDATE:
                gen.write(lts, pd, cd, opId, false).applyUnsafe();
                break;
            case UPDATE_WITH_STATICS:
                gen.write(lts, pd, cd, opId, false).applyUnsafe();
                gen.writeStatic(lts, pd, cd, opId, false).applyUnsafe();
                break;
            case DELETE_PARTITION:
                gen.deletePartition(lts, pd).applyUnsafe();
                break;
            case DELETE_ROW:
                gen.deleteRow(lts, pd, cd).applyUnsafe();
                break;
            case DELETE_COLUMN:
                gen.deleteColumn(lts, pd, cd, opId).applyUnsafe();
                break;
            case DELETE_COLUMN_WITH_STATICS:
                gen.deleteColumn(lts, pd, cd, opId).applyUnsafe();
                gen.deleteStatic(lts, pd, opId).applyUnsafe();
                break;
            case DELETE_RANGE:
                gen.deleteRange(lts, pd, opId).applyUnsafe();
                break;
            case DELETE_SLICE:
                gen.deleteSlice(lts, pd, opId).applyUnsafe();
                break;
        }
    }

    public void forceFlush()
    {
        gen.flush();
    }


    @Override
    public void shutdown() throws InterruptedException
    {

    }
}
