/**
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

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractCell implements Cell
{
    public static Iterator<OnDiskAtom> onDiskIterator(final DataInput in,
                                                      final ColumnSerializer.Flag flag,
                                                      final int expireBefore,
                                                      final Descriptor.Version version,
                                                      final CellNameType type)
    {
        return new AbstractIterator<OnDiskAtom>()
        {
            protected OnDiskAtom computeNext()
            {
                OnDiskAtom atom;
                try
                {
                    atom = type.onDiskAtomSerializer().deserializeFromSSTable(in, flag, expireBefore, version);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
                if (atom == null)
                    return endOfData();

                return atom;
            }
        };
    }

    @Override
    public boolean isMarkedForDelete(long now)
    {
        return false;
    }

    @Override
    public boolean isLive(long now)
    {
        return !isMarkedForDelete(now);
    }

    // Don't call unless the column is actually marked for delete.
    @Override
    public long getMarkedForDeleteAt()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public int cellDataSize()
    {
        return name().dataSize() + value().remaining() + TypeSizes.NATIVE.sizeof(timestamp());
    }

    @Override
    public int serializedSize(CellNameType type, TypeSizes typeSizes)
    {
        /*
         * Size of a column is =
         *   size of a name (short + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + 8 bytes for timestamp
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */
        int valueSize = value().remaining();
        return ((int)type.cellSerializer().serializedSize(name(), typeSizes)) + 1 + typeSizes.sizeof(timestamp()) + typeSizes.sizeof(valueSize) + valueSize;
    }

    @Override
    public int serializationFlags()
    {
        return 0;
    }

    @Override
    public Cell diff(Cell cell)
    {
        if (timestamp() < cell.timestamp())
            return cell;
        return null;
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name().toByteBuffer().duplicate());
        digest.update(value().duplicate());

        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithByte(digest, serializationFlags());
    }

    @Override
    public int getLocalDeletionTime()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public Cell reconcile(Cell cell)
    {
        // tombstones take precedence.  (if both are tombstones, then it doesn't matter which one we use.)
        if (isMarkedForDelete(System.currentTimeMillis()))
            return timestamp() < cell.timestamp() ? cell : this;
        if (cell.isMarkedForDelete(System.currentTimeMillis()))
            return timestamp() > cell.timestamp() ? this : cell;
        // break ties by comparing values.
        if (timestamp() == cell.timestamp())
            return value().compareTo(cell.value()) < 0 ? cell : this;
        // neither is tombstoned and timestamps are different
        return timestamp() < cell.timestamp() ? cell : this;
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Cell && equals((Cell) o));
    }

    public boolean equals(Cell cell)
    {
        return timestamp() == cell.timestamp() && name().equals(cell.name()) && value().equals(cell.value());
    }

    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(CellNameType comparator)
    {
        return String.format("%s:%b:%d@%d",
                comparator.getString(name()),
                isMarkedForDelete(System.currentTimeMillis()),
                value().remaining(),
                timestamp());
    }

    @Override
    public void validateName(CFMetaData metadata) throws MarshalException
    {
        metadata.comparator.validate(name());
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);

        AbstractType<?> valueValidator = metadata.getValueValidator(name());
        if (valueValidator != null)
            valueValidator.validate(value());
    }

    public static Cell create(CellName name, ByteBuffer value, long timestamp, int ttl, CFMetaData metadata)
    {
        if (ttl <= 0)
            ttl = metadata.getDefaultTimeToLive();

        return ttl > 0
                ? new BufferExpiringCell(name, value, timestamp, ttl)
                : new BufferCell(name, value, timestamp);
    }

    public static Cell diff(CounterCell a, Cell b)
    {
        if (a.timestamp() < b.timestamp())
            return b;

        // Note that if at that point, cell can't be a tombstone. Indeed,
        // cell is the result of merging us with other nodes results, and
        // merging a CounterCell with a tombstone never return a tombstone
        // unless that tombstone timestamp is greater that the CounterCell
        // one.
        assert b instanceof CounterCell : "Wrong class type: " + b.getClass();

        if (a.timestampOfLastDelete() < ((CounterCell) b).timestampOfLastDelete())
            return b;

        CounterContext.Relationship rel = CounterCell.contextManager.diff(b.value(), a.value());
        return (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT) ? b : null;
    }

    /** This is temporary until we start creating Cells of the different type (buffer vs. native) */
    public static Cell reconcile(CounterCell a, Cell b)
    {
        assert (b instanceof CounterCell) || (b instanceof DeletedCell) : "Wrong class type: " + b.getClass();

        // live + tombstone: track last tombstone
        if (b.isMarkedForDelete(Long.MIN_VALUE)) // cannot be an expired cell, so the current time is irrelevant
        {
            // live < tombstone
            if (a.timestamp() < b.timestamp())
                return b;

            // live last delete >= tombstone
            if (a.timestampOfLastDelete() >= b.timestamp())
                return a;

            // live last delete < tombstone
            return new BufferCounterCell(a.name(), a.value(), a.timestamp(), b.timestamp());
        }

        assert b instanceof CounterCell : "Wrong class type: " + b.getClass();

        // live < live last delete
        if (a.timestamp() < ((CounterCell) b).timestampOfLastDelete())
            return b;

        // live last delete > live
        if (a.timestampOfLastDelete() > b.timestamp())
            return a;

        // live + live. return one of the cells if its context is a superset of the other's, or merge them otherwise
        ByteBuffer context = CounterCell.contextManager.merge(a.value(), b.value());
        if (context == a.value() && a.timestamp() >= b.timestamp() && a.timestampOfLastDelete() >= ((CounterCell) b).timestampOfLastDelete())
            return a;
        else if (context == b.value() && b.timestamp() >= a.timestamp() && ((CounterCell) b).timestampOfLastDelete() >= a.timestampOfLastDelete())
            return b;
        else // merge clocks and timsestamps.
            return new BufferCounterCell(a.name(),
                                         context,
                                         Math.max(a.timestamp(), b.timestamp()),
                                         Math.max(a.timestampOfLastDelete(), ((CounterCell) b).timestampOfLastDelete()));
    }

}
