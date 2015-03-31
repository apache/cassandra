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

public class NativeExpiringCell extends NativeCell implements ExpiringCell
{
    private static final long SIZE = ObjectSizes.measure(new NativeExpiringCell());

    private NativeExpiringCell()
    {}

    public NativeExpiringCell(NativeAllocator allocator, OpOrder.Group writeOp, ExpiringCell copyOf)
    {
        super(allocator, writeOp, copyOf);
    }

    @Override
    protected int sizeOf(Cell cell)
    {
        return super.sizeOf(cell) + 8;
    }

    @Override
    protected void construct(Cell from)
    {
        ExpiringCell expiring = (ExpiringCell) from;

        setInt(internalSize() - 4, expiring.getTimeToLive());
        setInt(internalSize() - 8, expiring.getLocalDeletionTime());
        super.construct(from);
    }

    @Override
    protected int postfixSize()
    {
        return 8;
    }

    @Override
    public int getTimeToLive()
    {
        return getInt(internalSize() - 4);
    }

    @Override
    public int getLocalDeletionTime()
    {
        return getInt(internalSize() - 8);
    }

    @Override
    public boolean isLive()
    {
        return isLive(System.currentTimeMillis());
    }

    @Override
    public boolean isLive(long now)
    {
        return (int) (now / 1000) < getLocalDeletionTime();
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.EXPIRATION_MASK;
    }

    @Override
    public int cellDataSize()
    {
        return super.cellDataSize() + TypeSizes.NATIVE.sizeof(getLocalDeletionTime()) + TypeSizes.NATIVE.sizeof(getTimeToLive());
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        /*
         * An expired column adds to a Cell :
         *    4 bytes for the localExpirationTime
         *  + 4 bytes for the timeToLive
        */
        return super.serializedSize(type, typeSizes) + typeSizes.sizeof(getLocalDeletionTime()) + typeSizes.sizeof(getTimeToLive());
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        super.validateFields(metadata);

        if (getTimeToLive() <= 0)
            throw new MarshalException("A column TTL should be > 0");
        if (getLocalDeletionTime() < 0)
            throw new MarshalException("The local expiration time should not be negative");
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        super.updateDigest(digest);
        FBUtilities.updateWithInt(digest, getTimeToLive());
    }

    @Override
    public Cell reconcile(Cell cell)
    {
        long ts1 = timestamp(), ts2 = cell.timestamp();
        if (ts1 != ts2)
            return ts1 < ts2 ? cell : this;
        // we should prefer tombstones
        if (cell instanceof DeletedCell)
            return cell;
        int c = value().compareTo(cell.value());
        if (c != 0)
            return c < 0 ? cell : this;
        // If we have same timestamp and value, prefer the longest ttl
        if (cell instanceof ExpiringCell)
        {
            int let1 = getLocalDeletionTime(), let2 = cell.getLocalDeletionTime();
            if (let1 < let2)
                return cell;
        }
        return this;
    }

    public boolean equals(Cell cell)
    {
        if (!super.equals(cell))
            return false;
        ExpiringCell that = (ExpiringCell) cell;
        return getLocalDeletionTime() == that.getLocalDeletionTime() && getTimeToLive() == that.getTimeToLive();
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s(%s!%d)", getClass().getSimpleName(), super.getString(comparator), getTimeToLive());
    }

    @Override
    public ExpiringCell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferExpiringCell(name().copy(metadata, allocator), allocator.clone(value()), timestamp(), getTimeToLive(), getLocalDeletionTime());
    }

    @Override
    public ExpiringCell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
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
}
