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
package org.apache.cassandra.db;

import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCounterCell extends NativeCell implements CounterCell
{
    private static final long SIZE = ObjectSizes.measure(new NativeCounterCell());

    private NativeCounterCell()
    {}

    public NativeCounterCell(NativeAllocator allocator, OpOrder.Group writeOp, CounterCell copyOf)
    {
        super(allocator, writeOp, copyOf);
    }

    @Override
    protected void construct(Cell from)
    {
        super.construct(from);
        setLong(internalSize() - 8, ((CounterCell) from).timestampOfLastDelete());
    }

    @Override
    protected int postfixSize()
    {
        return 8;
    }

    @Override
    protected int sizeOf(Cell cell)
    {
        return 8 + super.sizeOf(cell);
    }

    @Override
    public long timestampOfLastDelete()
    {
        return getLong(internalSize() - 8);
    }

    @Override
    public long total()
    {
        return contextManager.total(value());
    }

    @Override
    public boolean hasLegacyShards()
    {
        return contextManager.hasLegacyShards(value());
    }

    @Override
    public Cell markLocalToBeCleared()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cell diff(Cell cell)
    {
        return diffCounter(cell);
    }

    @Override
    public Cell reconcile(Cell cell)
    {
        return reconcileCounter(cell);
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_MASK;
    }

    @Override
    public int cellDataSize()
    {
        // A counter column adds 8 bytes for timestampOfLastDelete to Cell.
        return super.cellDataSize() + TypeSizes.NATIVE.sizeof(timestampOfLastDelete());
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return super.serializedSize(type, typeSizes) + typeSizes.sizeof(timestampOfLastDelete());
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
        // which is not the internal representation of counters
        contextManager.validateContext(value());
    }

    /*
     * We have to special case digest creation for counter column because
     * we don't want to include the information about which shard of the
     * context is a delta or not, since this information differs from node to
     * node.
     */
    @Override
    public void updateDigest(MessageDigest digest)
    {
        updateWithName(digest);

        // We don't take the deltas into account in a digest
        contextManager.updateDigest(digest, value());

        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithByte(digest, serializationFlags());
        FBUtilities.updateWithLong(digest, timestampOfLastDelete());
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s(%s:false:%s@%d!%d)",
                             getClass().getSimpleName(),
                             comparator.getString(name()),
                             contextManager.toString(value()),
                             timestamp(),
                             timestampOfLastDelete());
    }

    @Override
    public CounterCell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferCounterCell(copy(metadata, allocator), allocator.clone(value()), timestamp(), timestampOfLastDelete());
    }

    @Override
    public CounterCell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        return allocator.clone(this, metadata, opGroup);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return SIZE;
    }

    @Override
    public long unsharedHeapSize()
    {
        return SIZE;
    }

    @Override
    public boolean equals(Cell cell)
    {
        return super.equals(cell) && timestampOfLastDelete() == ((CounterCell) cell).timestampOfLastDelete();
    }
}
