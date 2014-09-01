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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.concurrent.OpOrder;

public abstract class MemtableBufferAllocator extends MemtableAllocator
{

    protected MemtableBufferAllocator(SubAllocator onHeap, SubAllocator offHeap)
    {
        super(onHeap, offHeap);
    }

    public MemtableRowData.ReusableRow newReusableRow()
    {
        return MemtableRowData.BufferRowData.createReusableRow();
    }

    public RowAllocator newRowAllocator(CFMetaData cfm, OpOrder.Group writeOp)
    {
        return new RowBufferAllocator(allocator(writeOp), cfm.isCounter());
    }

    public DecoratedKey clone(DecoratedKey key, OpOrder.Group writeOp)
    {
        return new BufferDecoratedKey(key.getToken(), allocator(writeOp).clone(key.getKey()));
    }

    public abstract ByteBuffer allocate(int size, OpOrder.Group opGroup);

    protected AbstractAllocator allocator(OpOrder.Group writeOp)
    {
        return new ContextAllocator(writeOp, this);
    }

    private static class RowBufferAllocator extends RowDataBlock.Writer implements RowAllocator
    {
        private final AbstractAllocator allocator;
        private final boolean isCounter;

        private MemtableRowData.BufferClustering clustering;
        private int clusteringIdx;
        private LivenessInfo info;
        private DeletionTime deletion;
        private RowDataBlock data;

        private RowBufferAllocator(AbstractAllocator allocator, boolean isCounter)
        {
            super(true);
            this.allocator = allocator;
            this.isCounter = isCounter;
        }

        public void allocateNewRow(int clusteringSize, Columns columns, boolean isStatic)
        {
            data = new RowDataBlock(columns, 1, false, isCounter);
            clustering = isStatic ? null : new MemtableRowData.BufferClustering(clusteringSize);
            clusteringIdx = 0;
            updateWriter(data);
        }

        public MemtableRowData allocatedRowData()
        {
            MemtableRowData row = new MemtableRowData.BufferRowData(clustering == null ? Clustering.STATIC_CLUSTERING : clustering,
                                                                    info,
                                                                    deletion,
                                                                    data);

            clustering = null;
            info = LivenessInfo.NONE;
            deletion = DeletionTime.LIVE;
            data = null;

            return row;
        }

        public void writeClusteringValue(ByteBuffer value)
        {
            clustering.setClusteringValue(clusteringIdx++, value == null ? null : allocator.clone(value));
        }

        public void writePartitionKeyLivenessInfo(LivenessInfo info)
        {
            this.info = info;
        }

        public void writeRowDeletion(DeletionTime deletion)
        {
            this.deletion = deletion;
        }

        @Override
        public void writeCell(ColumnDefinition column, boolean isCounter, ByteBuffer value, LivenessInfo info, CellPath path)
        {
            ByteBuffer v = allocator.clone(value);
            if (column.isComplex())
                complexWriter.addCell(column, v, info, MemtableRowData.BufferCellPath.clone(path, allocator));
            else
                simpleWriter.addCell(column, v, info);
        }
    }
}
