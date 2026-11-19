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

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

public class BufferCellData extends AbstractBufferCellData
{
    static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCellData(null, 0, 0, 0, false));

    final ByteBuffer value;
    final long timestamp;
    final int localDeletionTimeAsUnsignedInt;
    final int ttl;
    final boolean isCounterCell;

    public BufferCellData(ByteBuffer value, long timestamp, long localDeletionTime, int ttl, boolean isCounterCell)
    {
        this.value = value;
        this.timestamp = timestamp;
        this.localDeletionTimeAsUnsignedInt = CellData.deletionTimeLongToUnsignedInteger(localDeletionTime);
        this.ttl = ttl;
        this.isCounterCell = isCounterCell;
    }

    public static BufferCellData tombstone(long timestamp, long localDeletionTime)
    {
        return new BufferCellData(ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp, localDeletionTime, NO_TTL, false);
    }

    @Override
    public boolean isCounterCell()
    {
        return isCounterCell;
    }

    @Override
    public ByteBuffer value()
    {
        return value;
    }

    @Override
    public long timestamp()
    {
        return timestamp;
    }

    @Override
    public int ttl()
    {
        return ttl;
    }

    @Override
    public int localDeletionTimeAsUnsignedInt()
    {
        return localDeletionTimeAsUnsignedInt;
    }

    @Override
    public long unsharedHeapSize()
    {
        return ObjectSizes.sizeOnHeapOf(value) + EMPTY_SIZE;
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE;
    }
}
