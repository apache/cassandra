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

package org.apache.cassandra.harry.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.execution.VisitExecutor;

import static org.apache.cassandra.harry.op.Operations.*;
import static org.apache.cassandra.harry.op.Operations.Kind.INSERT;

class PartitionStateBuilder extends VisitExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(PartitionStateBuilder.class);

    private final PartitionState partitionState;
    private boolean hadPartitionDeletion;
    private boolean hadTrackingRowWrite;
    private final List<Operation> rangeDeletes;
    private final List<Operation> writes;
    private final List<Operation> columnDeletes;
    private final ValueGenerators valueGenerators;

    PartitionStateBuilder(ValueGenerators valueGenerators,
                          PartitionState partitionState)
    {
        this.valueGenerators = valueGenerators;
        this.partitionState = partitionState;
        hadPartitionDeletion = false;
        hadTrackingRowWrite = false;
        rangeDeletes = new ArrayList<>();
        writes = new ArrayList<>();
        columnDeletes = new ArrayList<>();
    }

    public PartitionState build()
    {
        return partitionState;
    }

    protected void operation(Operation operation)
    {
        if (hadPartitionDeletion)
            return;

        long lts = operation.lts();

        switch (operation.kind())
        {
            case DELETE_RANGE:
                rangeDeletes.add(operation);
                break;
            case DELETE_ROW:
                long cd = ((DeleteRow) operation).cd();
                partitionState.delete(cd, lts);
                break;
            case DELETE_PARTITION:
                partitionState.deletePartition(lts);
                rangeDeletes.clear();
                writes.clear();
                columnDeletes.clear();
                hadPartitionDeletion = true;
                break;
            case INSERT:
            case UPDATE:
                writes.add(operation);
                break;
            case DELETE_COLUMNS:
                columnDeletes.add(operation);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + operation.kind());
        }
    }

    @Override
    protected void beginLts(long lts)
    {
        rangeDeletes.clear();
        writes.clear();
        columnDeletes.clear();
        hadPartitionDeletion = false;
    }

    @Override
    protected void endLts(long lts)
    {
        if (hadPartitionDeletion)
            return;

        for (Operation op : writes)
        {
            WriteOp writeOp = (WriteOp) op;
            long cd = writeOp.cd();

            if (hadTrackingRowWrite)
            {
                long[] statics = new long[valueGenerators.staticColumnCount()];
                Arrays.fill(statics, MagicConstants.UNSET_DESCR);
                partitionState.writeStatic(statics, lts);
            }

            switch (op.kind())
            {
                case INSERT:
                case UPDATE:
                    WriteOp writeStaticOp = (WriteOp) op;
                    // We could apply static columns during the first iteration, but it's more convenient
                    // to reconcile static-level deletions.
                    partitionState.writeStatic(writeStaticOp.sds(), lts);
                    partitionState.writeRegular(cd, writeOp.vds(), lts, op.kind() == INSERT);
                    break;
                default:
                    throw new IllegalStateException(op.kind().toString());
            }
        }

        for (Operation op : columnDeletes)
        {
            DeleteColumnsOp deleteColumnsOp = (DeleteColumnsOp) op;
            partitionState.deleteStaticColumns(lts, deleteColumnsOp.staticColumns());
            partitionState.deleteRegularColumns(lts, deleteColumnsOp.cd(), deleteColumnsOp.staticColumns());
        }

        for (Operation rangeDelete : rangeDeletes)
        {
            partitionState.delete((DeleteRange) rangeDelete, lts);
        }
    }
}
