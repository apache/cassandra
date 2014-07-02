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
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.apache.cassandra.utils.memory.NativeAllocator;

public class NativeCell extends AbstractNativeCell
{
    private static final long SIZE = ObjectSizes.measure(new NativeCell());

    NativeCell()
    {}

    public NativeCell(NativeAllocator allocator, OpOrder.Group writeOp, Cell copyOf)
    {
        super(allocator, writeOp, copyOf);
    }

    @Override
    public CellName name()
    {
        return this;
    }

    @Override
    public long timestamp()
    {
        return getLong(TIMESTAMP_OFFSET);
    }

    @Override
    public Cell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferCell(copy(metadata, allocator), allocator.clone(value()), timestamp());
    }

    @Override
    public Cell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        return allocator.clone(this, metadata, opGroup);
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        updateWithName(digest);  // name
        updateWithValue(digest); // value

        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithByte(digest, serializationFlags());
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
