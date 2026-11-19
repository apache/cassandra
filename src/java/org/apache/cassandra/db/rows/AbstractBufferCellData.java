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

package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.Cloner;

/// Base class for pathless [CellData] objects.
public abstract class AbstractBufferCellData implements CellData<ByteBuffer, AbstractBufferCellData>
{
    @Override
    public ValueAccessor<ByteBuffer> accessor()
    {
        return ByteBufferAccessor.instance;
    }

    @Override
    public BufferCellData withNewData(long timestamp, long localDeletionTime, int ttl, ByteBuffer value)
    {
        return new BufferCellData(value, timestamp, localDeletionTime, ttl, isCounterCell());
    }

    @Override
    public BufferCellData withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, long newLocalDeletionTime)
    {
        return new BufferCellData(value(), newTimestamp, newLocalDeletionTime, ttl(), isCounterCell());
    }

    @Override
    public BufferCellData updateAllTimestamp(long newTimestamp)
    {
        return new BufferCellData(value(), isTombstone() ? newTimestamp - 1 : newTimestamp, localDeletionTime(), ttl(), isCounterCell());
    }

    @Override
    public BufferCellData withSkippedValue()
    {
        return new BufferCellData(ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp(), localDeletionTime(), ttl(), isCounterCell());
    }

    @Override
    public AbstractBufferCellData clone(Cloner cloner)
    {
        if (!(cloner instanceof ByteBufferCloner))
            throw new AssertionError("Only byte buffer cloner supported for transient CellData.");

        return clone((ByteBufferCloner) cloner);
    }

    @Override
    public AbstractBufferCellData clone(ByteBufferCloner cloner)
    {
        ByteBuffer value = value();
        ByteBuffer newBuffer = cloner.clone(value);
        return newBuffer == value ? this : new BufferCellData(newBuffer, timestamp(), localDeletionTime(), ttl(), isCounterCell());
    }

    @Override
    public AbstractBufferCellData purge(DeletionPurger purger, long nowInSec)
    {
        if (!isLive(nowInSec))
        {
            if (purger.shouldPurge(timestamp(), localDeletionTime()))
                return null;

            // We slightly hijack purging to convert expired but not purgeable columns to tombstones. The reason we do that is
            // that once a column has expired it is equivalent to a tombstone but actually using a tombstone is more compact since
            // we don't keep the column value. The reason we do it here is that 1) it's somewhat related to dealing with tombstones
            // so hopefully not too surprising and 2) we want to this and purging at the same places, so it's simpler/more efficient
            // to do both here.
            if (isExpiring())
            {
                // Note that as long as the expiring column and the tombstone put together live longer than GC grace seconds,
                // we'll fulfil our responsibility to repair. See discussion at
                // http://cassandra-user-incubator-apache-org.3065146.n2.nabble.com/repair-compaction-and-tombstone-rows-td7583481.html
                return BufferCellData.tombstone(timestamp(), localDeletionTime() - ttl()).purge(purger, nowInSec);
            }
        }
        return this;
    }

    public AbstractBufferCellData purgeDataOlderThan(long timestamp)
    {
        return timestamp() < timestamp ? null : this;
    }

    @Override
    public AbstractBufferCellData markCounterLocalToBeCleared()
    {
        if (!isCounterCell())
            return this;

        ByteBuffer value = buffer();
        ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(value);
        return marked == value ? this : new BufferCellData(marked, timestamp(), localDeletionTime(), ttl(), true);
    }

    @Override
    public int dataSize()
    {
        return dataSizeWithoutPath();
    }


    @Override
    public String toString()
    {
        if (isCounterCell())
            return String.format("[?=%d ts=%d]", CounterContext.instance().total(value(), accessor()), timestamp());
        if (isTombstone())
            return String.format("[?=<tombstone> %s]", livenessInfoString());
        else
            return String.format("[?=%s %s]", ByteBufferUtil.bytesToHex(buffer()), livenessInfoString());
    }

    private String livenessInfoString()
    {
        if (isExpiring())
            return String.format("ts=%d ttl=%d ldt=%d", timestamp(), ttl(), localDeletionTime());
        else if (isTombstone())
            return String.format("ts=%d ldt=%d", timestamp(), localDeletionTime());
        else
            return String.format("ts=%d", timestamp());
    }

    @Override
    public BufferCell toCell(ColumnMetadata column, CellPath path)
    {
        return new BufferCell(column, timestamp(), ttl(), localDeletionTime(), value(), path);
    }
}
