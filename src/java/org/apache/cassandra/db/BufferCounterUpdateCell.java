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

import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class BufferCounterUpdateCell extends BufferCell implements CounterUpdateCell
{
    public BufferCounterUpdateCell(CellName name, long value, long timestamp)
    {
        this(name, ByteBufferUtil.bytes(value), timestamp);
    }

    public BufferCounterUpdateCell(CellName name, ByteBuffer value, long timestamp)
    {
        super(name, value, timestamp);
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferCounterUpdateCell(newName, value, timestamp);
    }

    public long delta()
    {
        return value().getLong(value.position());
    }

    @Override
    public Cell diff(Cell cell)
    {
        // Diff is used during reads, but we should never read those columns
        throw new UnsupportedOperationException("This operation is unsupported on CounterUpdateCell.");
    }

    @Override
    public Cell reconcile(Cell cell)
    {
        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (cell instanceof DeletedCell)
            return cell;

        assert cell instanceof CounterUpdateCell : "Wrong class type.";

        // The only time this could happen is if a batch ships two increments for the same cell. Hence we simply sum the deltas.
        return new BufferCounterUpdateCell(name, delta() + ((CounterUpdateCell) cell).delta(), Math.max(timestamp, cell.timestamp()));
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_UPDATE_MASK;
    }

    @Override
    public Cell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s:%s@%d", comparator.getString(name()), ByteBufferUtil.toLong(value), timestamp());
    }
}
