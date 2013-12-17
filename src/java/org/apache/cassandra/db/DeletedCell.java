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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

public class DeletedCell extends Cell
{
    public DeletedCell(CellName name, int localDeletionTime, long timestamp)
    {
        this(name, ByteBufferUtil.bytes(localDeletionTime), timestamp);
    }

    public DeletedCell(CellName name, ByteBuffer value, long timestamp)
    {
        super(name, value, timestamp);
    }

    @Override
    public Cell withUpdatedName(CellName newName)
    {
        return new DeletedCell(newName, value, timestamp);
    }

    @Override
    public Cell withUpdatedTimestamp(long newTimestamp)
    {
        return new DeletedCell(name, value, newTimestamp);
    }

    @Override
    public boolean isMarkedForDelete(long now)
    {
        return true;
    }

    @Override
    public long getMarkedForDeleteAt()
    {
        return timestamp;
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.toByteBuffer().duplicate());

        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    @Override
    public int getLocalDeletionTime()
    {
       return value.getInt(value.position());
    }

    @Override
    public Cell reconcile(Cell cell, Allocator allocator)
    {
        if (cell instanceof DeletedCell)
            return super.reconcile(cell, allocator);
        return cell.reconcile(this, allocator);
    }

    @Override
    public Cell localCopy(ColumnFamilyStore cfs)
    {
        return new DeletedCell(name.copy(HeapAllocator.instance), ByteBufferUtil.clone(value), timestamp);
    }

    @Override
    public Cell localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new DeletedCell(name.copy(allocator), allocator.clone(value), timestamp);
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
}
