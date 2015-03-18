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

    public boolean isLive()
    {
        return true;
    }

    public boolean isLive(long now)
    {
        return true;
    }

    public int cellDataSize()
    {
        return name().dataSize() + value().remaining() + TypeSizes.NATIVE.sizeof(timestamp());
    }

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

    public int serializationFlags()
    {
        return 0;
    }

    public Cell diff(Cell cell)
    {
        if (timestamp() < cell.timestamp())
            return cell;
        return null;
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(name().toByteBuffer().duplicate());
        digest.update(value().duplicate());

        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithByte(digest, serializationFlags());
    }

    public int getLocalDeletionTime()
    {
        return Integer.MAX_VALUE;
    }

    public Cell reconcile(Cell cell)
    {
        long ts1 = timestamp(), ts2 = cell.timestamp();
        if (ts1 != ts2)
            return ts1 < ts2 ? cell : this;
        if (isLive() != cell.isLive())
            return isLive() ? cell : this;
        return value().compareTo(cell.value()) < 0 ? cell : this;
    }

    @Override
    public boolean equals(Object o)
    {
        return this == o || (o instanceof Cell && equals((Cell) o));
    }

    public boolean equals(Cell cell)
    {
        return timestamp() == cell.timestamp() && name().equals(cell.name()) && value().equals(cell.value())
               && serializationFlags() == cell.serializationFlags();
    }

    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public String getString(CellNameType comparator)
    {
        return String.format("%s:%b:%d@%d",
                             comparator.getString(name()),
                             !isLive(),
                             value().remaining(),
                             timestamp());
    }

    public void validateName(CFMetaData metadata) throws MarshalException
    {
        metadata.comparator.validate(name());
    }

    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);

        AbstractType<?> valueValidator = metadata.getValueValidator(name());
        if (valueValidator != null)
            valueValidator.validateCellValue(value());
    }

    public static Cell create(CellName name, ByteBuffer value, long timestamp, int ttl, CFMetaData metadata)
    {
        if (ttl <= 0)
            ttl = metadata.getDefaultTimeToLive();

        return ttl > 0
                ? new BufferExpiringCell(name, value, timestamp, ttl)
                : new BufferCell(name, value, timestamp);
    }

    public Cell diffCounter(Cell cell)
    {
        assert this instanceof CounterCell : "Wrong class type: " + getClass();

        if (timestamp() < cell.timestamp())
            return cell;

        // Note that if at that point, cell can't be a tombstone. Indeed,
        // cell is the result of merging us with other nodes results, and
        // merging a CounterCell with a tombstone never return a tombstone
        // unless that tombstone timestamp is greater that the CounterCell
        // one.
        assert cell instanceof CounterCell : "Wrong class type: " + cell.getClass();

        if (((CounterCell) this).timestampOfLastDelete() < ((CounterCell) cell).timestampOfLastDelete())
            return cell;

        CounterContext.Relationship rel = CounterCell.contextManager.diff(cell.value(), value());
        return (rel == CounterContext.Relationship.GREATER_THAN || rel == CounterContext.Relationship.DISJOINT) ? cell : null;
    }

    /** This is temporary until we start creating Cells of the different type (buffer vs. native) */
    public Cell reconcileCounter(Cell cell)
    {
        assert this instanceof CounterCell : "Wrong class type: " + getClass();

        // No matter what the counter cell's timestamp is, a tombstone always takes precedence. See CASSANDRA-7346.
        if (cell instanceof DeletedCell)
            return cell;

        assert (cell instanceof CounterCell) : "Wrong class type: " + cell.getClass();

        // live < live last delete
        if (timestamp() < ((CounterCell) cell).timestampOfLastDelete())
            return cell;

        long timestampOfLastDelete = ((CounterCell) this).timestampOfLastDelete();

        // live last delete > live
        if (timestampOfLastDelete > cell.timestamp())
            return this;

        // live + live. return one of the cells if its context is a superset of the other's, or merge them otherwise
        ByteBuffer context = CounterCell.contextManager.merge(value(), cell.value());
        if (context == value() && timestamp() >= cell.timestamp() && timestampOfLastDelete >= ((CounterCell) cell).timestampOfLastDelete())
            return this;
        else if (context == cell.value() && cell.timestamp() >= timestamp() && ((CounterCell) cell).timestampOfLastDelete() >= timestampOfLastDelete)
            return cell;
        else // merge clocks and timestamps.
            return new BufferCounterCell(name(),
                                         context,
                                         Math.max(timestamp(), cell.timestamp()),
                                         Math.max(timestampOfLastDelete, ((CounterCell) cell).timestampOfLastDelete()));
    }

}
