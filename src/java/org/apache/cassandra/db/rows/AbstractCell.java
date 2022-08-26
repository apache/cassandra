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
import java.util.Objects;

import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

/**
 * Base abstract class for {@code Cell} implementations.
 *
 * Unless you have a very good reason not to, every cell implementation
 * should probably extend this class.
 */
public abstract class AbstractCell<V> extends Cell<V>
{
    protected AbstractCell(ColumnMetadata column)
    {
        super(column);
    }

    public boolean isCounterCell()
    {
        return !isTombstone() && column.isCounterColumn();
    }

    public boolean isLive(long nowInSec)
    {
        return localDeletionTime() == NO_DELETION_TIME || (ttl() != NO_TTL && nowInSec < localDeletionTime());
    }

    public boolean isTombstone()
    {
        return localDeletionTime() != NO_DELETION_TIME && ttl() == NO_TTL;
    }

    public boolean isExpiring()
    {
        return ttl() != NO_TTL;
    }

    public Cell<?> markCounterLocalToBeCleared()
    {
        if (!isCounterCell())
            return this;

        ByteBuffer value = buffer();
        ByteBuffer marked = CounterContext.instance().markLocalToBeCleared(value);
        return marked == value ? this : new BufferCell(column, timestamp(), ttl(), localDeletionTime(), marked, path());
    }

    public Cell<?> purge(DeletionPurger purger, long nowInSec)
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
                return BufferCell.tombstone(column, timestamp(), localDeletionTime() - ttl(), path()).purge(purger, nowInSec);
            }
        }
        return this;
    }


    public Cell<?> purgeDataOlderThan(long timestamp)
    {
        return this.timestamp() < timestamp ? null : this;
    }

    @Override
    public Cell<?> clone(ByteBufferCloner cloner)
    {
        CellPath path = path();
        return new BufferCell(column, timestamp(), ttl(), localDeletionTime(), cloner.clone(buffer()), path == null ? null : path.clone(cloner));
    }

    // note: while the cell returned may be different, the value is the same, so if the value is offheap it must be referenced inside a guarded context (or copied)
    public Cell<?> updateAllTimestamp(long newTimestamp)
    {
        return new BufferCell(column, isTombstone() ? newTimestamp - 1 : newTimestamp, ttl(), localDeletionTime(), buffer(), path());
    }

    public int dataSize()
    {
        CellPath path = path();
        return TypeSizes.sizeof(timestamp())
               + TypeSizes.sizeof(ttl())
               + TypeSizes.sizeof(localDeletionTime())
               + valueSize()
               + (path == null ? 0 : path.dataSize());
    }

    public void digest(Digest digest)
    {
        if (isCounterCell())
            digest.updateWithCounterContext(value(), accessor());
        else
            digest.update(value(), accessor());

        digest.updateWithLong(timestamp())
              .updateWithInt(ttl())
              .updateWithBoolean(isCounterCell());
        if (path() != null)
            path().digest(digest);
    }

    public void validate()
    {
        if (ttl() < 0)
            throw new MarshalException("A TTL should not be negative");
        if (localDeletionTime() < 0)
            throw new MarshalException("A local deletion time should not be negative");
        if (localDeletionTime() == INVALID_DELETION_TIME)
            throw new MarshalException("A local deletion time should not be a legacy overflowed value");
        if (isExpiring() && localDeletionTime() == NO_DELETION_TIME)
            throw new MarshalException("Shoud not have a TTL without an associated local deletion time");

        // non-frozen UDTs require both the cell path & value to validate,
        // so that logic is pushed down into ColumnMetadata. Tombstone
        // validation is done there too as it also involves the cell path
        // for complex columns
        column().validateCell(this);
    }

    public boolean hasInvalidDeletions()
    {
        if (ttl() < 0 || localDeletionTime() == INVALID_DELETION_TIME || localDeletionTime() < 0 || (isExpiring() && localDeletionTime() == NO_DELETION_TIME))
            return true;
        return false;
    }

    public long maxTimestamp()
    {
        return timestamp();
    }

    public static <V1, V2> boolean equals(Cell<V1> left, Cell<V2> right)
    {
        return left.column().equals(right.column())
               && left.isCounterCell() == right.isCounterCell()
               && left.timestamp() == right.timestamp()
               && left.ttl() == right.ttl()
               && left.localDeletionTime() == right.localDeletionTime()
               && ValueAccessor.equals(left.value(), left.accessor(), right.value(), right.accessor())
               && Objects.equals(left.path(), right.path());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if(!(other instanceof Cell))
            return false;

        return equals(this, (Cell<?>) other);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column(), isCounterCell(), timestamp(), ttl(), localDeletionTime(), accessor().hashCode(value()), path());
    }

    @Override
    public String toString()
    {
        if (isCounterCell())
            return String.format("[%s=%d ts=%d]", column().name, CounterContext.instance().total(value(), accessor()), timestamp());

        AbstractType<?> type = column().type;
        if (type instanceof CollectionType && type.isMultiCell())
        {
            CollectionType<?> ct = (CollectionType<?>) type;
            return String.format("[%s[%s]=%s %s]",
                                 column().name,
                                 ct.nameComparator().getString(path().get(0)),
                                 isTombstone() ? "<tombstone>" : ct.valueComparator().getString(value(), accessor()),
                                 livenessInfoString());
        }
        if (isTombstone())
            return String.format("[%s=<tombstone> %s]", column().name, livenessInfoString());
        else
            return String.format("[%s=%s %s]", column().name, safeToString(type), livenessInfoString());
    }

    private String safeToString(AbstractType<?> type)
    {
        try
        {
            return type.getString(value(), accessor());
        }
        catch (Exception e)
        {
            return "0x" + ByteBufferUtil.bytesToHex(buffer());
        }
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

}
