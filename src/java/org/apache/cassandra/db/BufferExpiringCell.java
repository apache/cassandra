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
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class BufferExpiringCell extends BufferCell implements ExpiringCell
{
    private final int localExpirationTime;
    private final int timeToLive;

    public BufferExpiringCell(CellName name, ByteBuffer value, long timestamp, int timeToLive)
    {
        this(name, value, timestamp, timeToLive, (int) (System.currentTimeMillis() / 1000) + timeToLive);
    }

    public BufferExpiringCell(CellName name, ByteBuffer value, long timestamp, int timeToLive, int localExpirationTime)
    {
        super(name, value, timestamp);
        assert timeToLive > 0 : timeToLive;
        assert localExpirationTime > 0 : localExpirationTime;
        this.timeToLive = timeToLive;
        this.localExpirationTime = localExpirationTime;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferExpiringCell(newName, value(), timestamp(), timeToLive, localExpirationTime);
    }

    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new BufferExpiringCell(name(), value(), newTimestamp, timeToLive, localExpirationTime);
    }

    @Override
    public int cellDataSize()
    {
        return super.cellDataSize() + TypeSizes.NATIVE.sizeof(localExpirationTime) + TypeSizes.NATIVE.sizeof(timeToLive);
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        /*
         * An expired column adds to a Cell :
         *    4 bytes for the localExpirationTime
         *  + 4 bytes for the timeToLive
        */
        return super.serializedSize(type, typeSizes) + typeSizes.sizeof(localExpirationTime) + typeSizes.sizeof(timeToLive);
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        super.updateDigest(digest);
        FBUtilities.updateWithInt(digest, timeToLive);
    }

    @Override
    public int getLocalDeletionTime()
    {
        return localExpirationTime;
    }

    @Override
    public ExpiringCell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferExpiringCell(name.copy(metadata, allocator), allocator.clone(value), timestamp, timeToLive, localExpirationTime);
    }

    @Override
    public ExpiringCell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        return allocator.clone(this, metadata, opGroup);
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s!%d", super.getString(comparator), timeToLive);
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
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        super.validateFields(metadata);

        if (timeToLive <= 0)
            throw new MarshalException("A column TTL should be > 0");
        if (localExpirationTime < 0)
            throw new MarshalException("The local expiration time should not be negative");
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
            int let1 = localExpirationTime, let2 = cell.getLocalDeletionTime();
            if (let1 < let2)
                return cell;
        }
        return this;
    }

    @Override
    public boolean equals(Cell cell)
    {
        if (!super.equals(cell))
            return false;
        ExpiringCell that = (ExpiringCell) cell;
        return getLocalDeletionTime() == that.getLocalDeletionTime() && getTimeToLive() == that.getTimeToLive();
    }

    /** @return Either a DeletedCell, or an ExpiringCell. */
    public static Cell create(CellName name, ByteBuffer value, long timestamp, int timeToLive, int localExpirationTime, int expireBefore, ColumnSerializer.Flag flag)
    {
        if (localExpirationTime >= expireBefore || flag == ColumnSerializer.Flag.PRESERVE_SIZE)
            return new BufferExpiringCell(name, value, timestamp, timeToLive, localExpirationTime);
        // The column is now expired, we can safely return a simple tombstone. Note that
        // as long as the expiring column and the tombstone put together live longer than GC grace seconds,
        // we'll fulfil our responsibility to repair.  See discussion at
        // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
        return new BufferDeletedCell(name, localExpirationTime - timeToLive, timestamp);
    }
}
