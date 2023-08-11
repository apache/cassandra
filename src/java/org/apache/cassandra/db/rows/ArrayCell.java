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

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.ByteBufferCloner;

import static org.apache.cassandra.utils.ByteArrayUtil.EMPTY_BYTE_ARRAY;

public class ArrayCell extends AbstractCell<byte[]>
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new ArrayCell(ColumnMetadata.regularColumn("", "", "", ByteType.instance), 0L, 0, 0, EMPTY_BYTE_ARRAY, null));

    // Careful: Adding vars here has an impact on memtable size
    private final long timestamp;
    private final int ttl;
    private final int localDeletionTimeUnsignedInteger;

    private final byte[] value;
    private final CellPath path;

    // Please keep both int/long overloaded ctros public. Otherwise silent casts will mess timestamps when one is not
    // available.
    public ArrayCell(ColumnMetadata column, long timestamp, int ttl, long localDeletionTime, byte[] value, CellPath path)
    {
        this(column, timestamp, ttl, Cell.deletionTimeLongToUnsignedInteger(localDeletionTime), value, path);
    }

    public ArrayCell(ColumnMetadata column, long timestamp, int ttl, int localDeletionTimeUnsignedInteger, byte[] value, CellPath path)
    {
        super(column);
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.localDeletionTimeUnsignedInteger = localDeletionTimeUnsignedInteger;
        this.value = value;
        this.path = path;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public int ttl()
    {
        return ttl;
    }

    public byte[] value()
    {
        return value;
    }

    public ValueAccessor<byte[]> accessor()
    {
        return ByteArrayAccessor.instance;
    }

    public CellPath path()
    {
        return path;
    }

    public Cell<?> withUpdatedColumn(ColumnMetadata newColumn)
    {
        return new ArrayCell(newColumn, timestamp, ttl, localDeletionTimeUnsignedInteger, value, path);
    }

    public Cell<?> withUpdatedValue(ByteBuffer newValue)
    {
        return new ArrayCell(column, timestamp, ttl, localDeletionTimeUnsignedInteger, ByteBufferUtil.getArray(newValue), path);
    }

    public Cell<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, long newLocalDeletionTime)
    {
        return new ArrayCell(column, newTimestamp, ttl, newLocalDeletionTime, value, path);
    }

    @Override
    public Cell<?> withSkippedValue()
    {
        return new ArrayCell(column, timestamp, ttl, localDeletionTimeUnsignedInteger, EMPTY_BYTE_ARRAY, path);
    }

    @Override
    public Cell<?> clone(ByteBufferCloner cloner)
    {
        if (value.length == 0 && path == null)
            return this;

        return super.clone(cloner);
    }

    @Override
    public long unsharedHeapSize()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(value) + (path == null ? 0 : path.unsharedHeapSize());
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + ObjectSizes.sizeOfArray(value) - value.length + (path == null ? 0 : path.unsharedHeapSizeExcludingData());
    }

    @Override
    protected int localDeletionTimeAsUnsignedInt()
    {
        return localDeletionTimeUnsignedInteger;
    }
}
