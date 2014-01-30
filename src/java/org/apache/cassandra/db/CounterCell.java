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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.HeapAllocator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.io.util.DataOutputBuffer;

/**
 * A column that represents a partitioned counter.
 */
public class CounterCell extends Cell
{
    protected static final CounterContext contextManager = CounterContext.instance();

    private final long timestampOfLastDelete;

    public CounterCell(CellName name, ByteBuffer value, long timestamp)
    {
        this(name, value, timestamp, Long.MIN_VALUE);
    }

    public CounterCell(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete)
    {
        super(name, value, timestamp);
        this.timestampOfLastDelete = timestampOfLastDelete;
    }

    public static CounterCell create(CellName name, ByteBuffer value, long timestamp, long timestampOfLastDelete, ColumnSerializer.Flag flag)
    {
        if (flag == ColumnSerializer.Flag.FROM_REMOTE || (flag == ColumnSerializer.Flag.LOCAL && contextManager.shouldClearLocal(value)))
            value = contextManager.clearAllLocal(value);
        return new CounterCell(name, value, timestamp, timestampOfLastDelete);
    }

    // For use by tests of compatibility with pre-2.1 counter only.
    public static CounterCell createLocal(CellName name, long value, long timestamp, long timestampOfLastDelete)
    {
        return new CounterCell(name, contextManager.createLocal(value, HeapAllocator.instance), timestamp, timestampOfLastDelete);
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new CounterCell(newName, value, timestamp, timestampOfLastDelete);
    }

    public long timestampOfLastDelete()
    {
        return timestampOfLastDelete;
    }

    public long total()
    {
        return contextManager.total(value);
    }

    @Override
    public int dataSize()
    {
        // A counter column adds 8 bytes for timestampOfLastDelete to Cell.
        return super.dataSize() + TypeSizes.NATIVE.sizeof(timestampOfLastDelete);
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        return super.serializedSize(type, typeSizes) + typeSizes.sizeof(timestampOfLastDelete);
    }

    @Override
    public Cell diff(Cell cell)
    {
        assert (cell instanceof CounterCell) || (cell instanceof DeletedCell) : "Wrong class type: " + cell.getClass();

        if (timestamp() < cell.timestamp())
            return cell;

        // Note that if at that point, cell can't be a tombstone. Indeed,
        // cell is the result of merging us with other nodes results, and
        // merging a CounterCell with a tombstone never return a tombstone
        // unless that tombstone timestamp is greater that the CounterCell
        // one.
        assert cell instanceof CounterCell : "Wrong class type: " + cell.getClass();

        if (timestampOfLastDelete() < ((CounterCell) cell).timestampOfLastDelete())
            return cell;
        CounterContext.Relationship rel = contextManager.diff(cell.value(), value());
        if (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT)
            return cell;
        return null;
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
        digest.update(name.toByteBuffer().duplicate());
        // We don't take the deltas into account in a digest
        contextManager.updateDigest(digest, value);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
            buffer.writeLong(timestampOfLastDelete);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public Cell reconcile(Cell cell, AbstractAllocator allocator)
    {
        // live + tombstone: track last tombstone
        if (cell.isMarkedForDelete(Long.MIN_VALUE)) // cannot be an expired cell, so the current time is irrelevant
        {
            // live < tombstone
            if (timestamp() < cell.timestamp())
            {
                return cell;
            }
            // live last delete >= tombstone
            if (timestampOfLastDelete() >= cell.timestamp())
            {
                return this;
            }
            // live last delete < tombstone
            return new CounterCell(name(), value(), timestamp(), cell.timestamp());
        }

        assert cell instanceof CounterCell : "Wrong class type: " + cell.getClass();

        // live < live last delete
        if (timestamp() < ((CounterCell) cell).timestampOfLastDelete())
            return cell;
        // live last delete > live
        if (timestampOfLastDelete() > cell.timestamp())
            return this;
        // live + live: merge clocks; update value
        return new CounterCell(name(),
                               contextManager.merge(value(), cell.value(), allocator),
                               Math.max(timestamp(), cell.timestamp()),
                               Math.max(timestampOfLastDelete(), ((CounterCell) cell).timestampOfLastDelete()));
    }

    @Override
    public boolean equals(Object o)
    {
        // super.equals() returns false if o is not a CounterCell
        return super.equals(o) && timestampOfLastDelete == ((CounterCell)o).timestampOfLastDelete;
    }

    @Override
    public int hashCode()
    {
        return 31 * super.hashCode() + (int)(timestampOfLastDelete ^ (timestampOfLastDelete >>> 32));
    }

    @Override
    public Cell localCopy(ColumnFamilyStore cfs, AbstractAllocator allocator)
    {
        return new CounterCell(name.copy(allocator), allocator.clone(value), timestamp, timestampOfLastDelete);
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s:false:%s@%d!%d",
                             comparator.getString(name),
                             contextManager.toString(value),
                             timestamp,
                             timestampOfLastDelete);
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.COUNTER_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        // We cannot use the value validator as for other columns as the CounterColumnType validate a long,
        // which is not the internal representation of counters
        contextManager.validateContext(value());
    }

    public Cell markLocalToBeCleared()
    {
        ByteBuffer marked = contextManager.markLocalToBeCleared(value);
        return marked == value ? this : new CounterCell(name, marked, timestamp, timestampOfLastDelete);
    }
}
