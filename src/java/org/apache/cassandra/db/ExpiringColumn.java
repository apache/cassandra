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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;

/**
 * Alternative to Column that have an expiring time.
 * ExpiringColumn is immutable (as Column is).
 *
 * Note that ExpiringColumn does not override Column.getMarkedForDeleteAt,
 * which means that it's in the somewhat unintuitive position of being deleted (after its expiration)
 * without having a time-at-which-it-became-deleted.  (Because ttl is a server-side measurement,
 * we can't mix it with the timestamp field, which is client-supplied and whose resolution we
 * can't assume anything about.)
 */
public class ExpiringColumn extends Column
{
    private final int localExpirationTime;
    private final int timeToLive;

    public ExpiringColumn(ByteBuffer name, ByteBuffer value, long timestamp, int timeToLive)
    {
      this(name, value, timestamp, timeToLive, (int) (System.currentTimeMillis() / 1000) + timeToLive);
    }

    public ExpiringColumn(ByteBuffer name, ByteBuffer value, long timestamp, int timeToLive, int localExpirationTime)
    {
        super(name, value, timestamp);
        assert timeToLive > 0 : timeToLive;
        assert localExpirationTime > 0 : localExpirationTime;
        this.timeToLive = timeToLive;
        this.localExpirationTime = localExpirationTime;
    }

    /** @return Either a DeletedColumn, or an ExpiringColumn. */
    public static Column create(ByteBuffer name, ByteBuffer value, long timestamp, int timeToLive, int localExpirationTime, int expireBefore, IColumnSerializer.Flag flag)
    {
        if (localExpirationTime >= expireBefore || flag == IColumnSerializer.Flag.PRESERVE_SIZE)
            return new ExpiringColumn(name, value, timestamp, timeToLive, localExpirationTime);
        // the column is now expired, we can safely return a simple tombstone
        return new DeletedColumn(name, localExpirationTime, timestamp);
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    @Override
    public boolean isMarkedForDelete()
    {
        return (int) (System.currentTimeMillis() / 1000 ) > localExpirationTime;
    }

    @Override
    public int size()
    {
        /*
         * An expired column adds to a Column : 
         *    4 bytes for the localExpirationTime
         *  + 4 bytes for the timeToLive
        */
        return super.size() + DBConstants.intSize + DBConstants.intSize;
    }

    @Override
    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.duplicate());
        digest.update(value.duplicate());

        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            buffer.writeLong(timestamp);
            buffer.writeByte(serializationFlags());
            buffer.writeInt(timeToLive);
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
        return localExpirationTime;
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs)
    {
        return new ExpiringColumn(cfs.internOrCopy(name, HeapAllocator.instance), ByteBufferUtil.clone(value), timestamp, timeToLive, localExpirationTime);
    }

    @Override
    public IColumn localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        ByteBuffer clonedName = cfs.maybeIntern(name);
        if (clonedName == null)
            clonedName = allocator.clone(name);
        return new ExpiringColumn(clonedName, allocator.clone(value), timestamp, timeToLive, localExpirationTime);
    }
    
    @Override
    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getString(comparator));
        sb.append("!");
        sb.append(timeToLive);
        return sb.toString();
    }

    @Override
    public long getMarkedForDeleteAt()
    {
        if (isMarkedForDelete())
        {
            return timestamp;
        }
        else
        {
            throw new IllegalStateException("column is not marked for delete");
        }
    }

    @Override
    public int serializationFlags()
    {
        return ColumnSerializer.EXPIRATION_MASK;
    }

    @Override
    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        super.validateFields(metadata);
        if (timeToLive <= 0)
            throw new MarshalException("A column TTL should be > 0");
        if (localExpirationTime < 0)
            throw new MarshalException("The local expiration time should not be negative");
    }
}
