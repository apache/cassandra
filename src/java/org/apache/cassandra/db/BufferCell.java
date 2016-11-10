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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class BufferCell extends AbstractCell
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new BufferCell(CellNames.simpleDense(ByteBuffer.allocate(1))));

    protected final CellName name;
    protected final ByteBuffer value;
    protected final long timestamp;

    BufferCell(CellName name)
    {
        this(name, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public BufferCell(CellName name, ByteBuffer value)
    {
        this(name, value, 0);
    }

    public BufferCell(CellName name, ByteBuffer value, long timestamp)
    {
        assert name != null;
        assert value != null;

        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferCell(newName, value, timestamp);
    }

    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new BufferCell(name, value, newTimestamp);
    }

    @Override
    public CellName name() {
        return name;
    }

    @Override
    public ByteBuffer value() {
        return value;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE + name.unsharedHeapSizeExcludingData() + ObjectSizes.sizeOnHeapExcludingData(value);
    }

    @Override
    public Cell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferCell(name.copy(metadata, allocator), allocator.clone(value), timestamp);
    }

    @Override
    public Cell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        return allocator.clone(this, metadata, opGroup);
    }
}
