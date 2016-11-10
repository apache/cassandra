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
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.AbstractAllocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.MemtableAllocator;

public class BufferDeletedCell extends BufferCell implements DeletedCell
{
    public BufferDeletedCell(CellName name, int localDeletionTime, long timestamp)
    {
        this(name, ByteBufferUtil.bytes(localDeletionTime), timestamp);
    }

    public BufferDeletedCell(CellName name, ByteBuffer value, long timestamp)
    {
        super(name, value, timestamp);
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new BufferDeletedCell(newName, value, timestamp);
    }

    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new BufferDeletedCell(name, value, newTimestamp);
    }

    @Override
    public boolean isLive()
    {
        return false;
    }

    @Override
    public boolean isLive(long now)
    {
        return false;
    }

    @Override
    public int getLocalDeletionTime()
    {
       return value().getInt(value.position());
    }

    @Override
    public Cell reconcile(Cell cell)
    {
        if (cell instanceof DeletedCell)
            return super.reconcile(cell);
        return cell.reconcile(this);
    }

    @Override
    public DeletedCell localCopy(CFMetaData metadata, AbstractAllocator allocator)
    {
        return new BufferDeletedCell(name.copy(metadata, allocator), allocator.clone(value), timestamp);
    }

    @Override
    public DeletedCell localCopy(CFMetaData metadata, MemtableAllocator allocator, OpOrder.Group opGroup)
    {
        return allocator.clone(this, metadata, opGroup);
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.DELETION_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        if (value().remaining() != 4)
            throw new MarshalException("A tombstone value should be 4 bytes long");
        if (getLocalDeletionTime() < 0)
            throw new MarshalException("The local deletion time should not be negative");
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name().toByteBuffer().duplicate());

        FBUtilities.updateWithLong(digest, timestamp());
        FBUtilities.updateWithByte(digest, serializationFlags());
    }
}
