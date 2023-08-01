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
import java.nio.ByteOrder;

import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.MemoryUtil;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCell extends AbstractCell<ByteBuffer>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new NativeCell());

    private static final long HAS_CELLPATH = 0;
    private static final long TIMESTAMP = 1;
    private static final long TTL = 9;
    private static final long DELETION = 13;
    private static final long LENGTH = 17;
    private static final long VALUE = 21;

    private final long peer;

    private NativeCell()
    {
        super(null);
        this.peer = 0;
    }

    public NativeCell(NativeAllocator allocator,
                      OpOrder.Group writeOp,
                      Cell<?> cell)
    {
        this(allocator,
             writeOp,
             cell.column(),
             cell.timestamp(),
             cell.ttl(),
             cell.localDeletionTimeAsUnsignedInt(),
             cell.buffer(),
             cell.path());
    }

    // Please keep both int/long overloaded ctros public. Otherwise silent casts will mess timestamps when one is not
    // available.
    public NativeCell(NativeAllocator allocator,
                      OpOrder.Group writeOp,
                      ColumnMetadata column,
                      long timestamp,
                      int ttl,
                      long localDeletionTime,
                      ByteBuffer value,
                      CellPath path)
    {
        this(allocator, writeOp, column, timestamp, ttl, deletionTimeLongToUnsignedInteger(localDeletionTime), value, path);
    }

    public NativeCell(NativeAllocator allocator,
                      OpOrder.Group writeOp,
                      ColumnMetadata column,
                      long timestamp,
                      int ttl,
                      int localDeletionTimeUnsignedInteger,
                      ByteBuffer value,
                      CellPath path)
    {
        super(column);
        long size = offHeapSizeWithoutPath(value.remaining());

        assert value.order() == ByteOrder.BIG_ENDIAN;
        assert column.isComplex() == (path != null);
        if (path != null)
        {
            assert path.size() == 1 : String.format("Expected path size to be 1 but was not; %s", path);
            size += 4 + path.get(0).remaining();
        }

        if (size > Integer.MAX_VALUE)
            throw new IllegalStateException();

        // cellpath? : timestamp : ttl : localDeletionTime : length : <data> : [cell path length] : [<cell path data>]
        peer = allocator.allocate((int) size, writeOp);
        MemoryUtil.setByte(peer + HAS_CELLPATH, (byte)(path == null ? 0 : 1));
        MemoryUtil.setLong(peer + TIMESTAMP, timestamp);
        MemoryUtil.setInt(peer + TTL, ttl);
        MemoryUtil.setInt(peer + DELETION, localDeletionTimeUnsignedInteger);
        MemoryUtil.setInt(peer + LENGTH, value.remaining());
        MemoryUtil.setBytes(peer + VALUE, value);

        if (path != null)
        {
            ByteBuffer pathbuffer = path.get(0);
            assert pathbuffer.order() == ByteOrder.BIG_ENDIAN;

            long offset = peer + VALUE + value.remaining();
            MemoryUtil.setInt(offset, pathbuffer.remaining());
            MemoryUtil.setBytes(offset + 4, pathbuffer);
        }
    }

    private static long offHeapSizeWithoutPath(int length)
    {
        return VALUE + length;
    }

    public long timestamp()
    {
        return MemoryUtil.getLong(peer + TIMESTAMP);
    }

    public int ttl()
    {
        return MemoryUtil.getInt(peer + TTL);
    }

    public ByteBuffer value()// FIXME: add native accessor
    {
        int length = MemoryUtil.getInt(peer + LENGTH);
        return MemoryUtil.getByteBuffer(peer + VALUE, length, ByteOrder.BIG_ENDIAN);
    }

    public ValueAccessor<ByteBuffer> accessor()
    {
        return ByteBufferAccessor.instance;  // FIXME: add native accessor
    }

    public CellPath path()
    {
        if (!hasPath())
            return null;

        long offset = peer + VALUE + MemoryUtil.getInt(peer + LENGTH);
        int size = MemoryUtil.getInt(offset);
        return CellPath.create(MemoryUtil.getByteBuffer(offset + 4, size, ByteOrder.BIG_ENDIAN));
    }

    public Cell<?> withUpdatedValue(ByteBuffer newValue)
    {
        throw new UnsupportedOperationException();
    }

    public Cell<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, long newLocalDeletionTime)
    {
        return new BufferCell(column, newTimestamp, ttl(), newLocalDeletionTime, value(), path());
    }

    public Cell<?> withUpdatedColumn(ColumnMetadata column)
    {
        return new BufferCell(column, timestamp(), ttl(), localDeletionTimeAsUnsignedInt(), value(), path());
    }

    public Cell withSkippedValue()
    {
        return new BufferCell(column, timestamp(), ttl(), localDeletionTimeAsUnsignedInt(), ByteBufferUtil.EMPTY_BYTE_BUFFER, path());
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE;
    }

    public long offHeapSize()
    {
        long size = offHeapSizeWithoutPath(MemoryUtil.getInt(peer + LENGTH));
        if (hasPath())
            size += 4 + MemoryUtil.getInt(peer + size);
        return size;
    }

    private boolean hasPath()
    {
        return MemoryUtil.getByte(peer+ HAS_CELLPATH) != 0;
    }

    @Override
    protected int localDeletionTimeAsUnsignedInt()
    {
        return MemoryUtil.getInt(peer + DELETION);
    }
}
