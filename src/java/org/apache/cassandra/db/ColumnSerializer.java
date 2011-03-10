package org.apache.cassandra.db;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSerializer implements IColumnSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnSerializer.class);
    
    public final static int DELETION_MASK       = 0x01;
    public final static int EXPIRATION_MASK     = 0x02;
    public final static int COUNTER_MASK        = 0x04;
    public final static int COUNTER_UPDATE_MASK = 0x08;

    public void serialize(IColumn column, DataOutput dos)
    {
        assert column.name().remaining() > 0;
        ByteBufferUtil.writeWithShortLength(column.name(), dos);
        try
        {
            dos.writeByte(column.serializationFlags());
            if (column instanceof CounterColumn)
            {
                dos.writeLong(((CounterColumn)column).timestampOfLastDelete());
            }
            else if (column instanceof ExpiringColumn)
            {
                dos.writeInt(((ExpiringColumn) column).getTimeToLive());
                dos.writeInt(column.getLocalDeletionTime());
            }
            dos.writeLong(column.timestamp());
            ByteBufferUtil.writeWithLength(column.value(), dos);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Column deserialize(DataInput dis) throws IOException
    {
        return deserialize(dis, null, false);
    }

    /*
     * For counter columns, we must know when we deserialize them if what we
     * deserialize comes from a remote host. If it does, then we must clear
     * the delta.
     */
    public Column deserialize(DataInput dis, ColumnFamilyStore interner, boolean fromRemote) throws IOException
    {
        ByteBuffer name = ByteBufferUtil.readWithShortLength(dis);
        if (name.remaining() <= 0)
            throw new CorruptColumnException("invalid column name length " + name.remaining());
        if (interner != null)
            name = interner.maybeIntern(name);

        int b = dis.readUnsignedByte();
        if ((b & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = dis.readLong();
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            if (fromRemote)
                value = CounterContext.instance().clearAllDelta(value);
            return new CounterColumn(name, value, ts, timestampOfLastDelete);
        }
        else if ((b & EXPIRATION_MASK) != 0)
        {
            int ttl = dis.readInt();
            int expiration = dis.readInt();
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            if ((int) (System.currentTimeMillis() / 1000 ) > expiration)
            {
                // the column is now expired, we can safely return a simple
                // tombstone
                ByteBuffer bytes = ByteBuffer.allocate(4);
                bytes.putInt(expiration);
                bytes.rewind();
                return new DeletedColumn(name, bytes, ts);
            }
            else
            {
                return new ExpiringColumn(name, value, ts, ttl, expiration);
            }
        }
        else
        {
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            return (b & COUNTER_UPDATE_MASK) != 0
                   ? new CounterUpdateColumn(name, value, ts)
                   : ((b & DELETION_MASK) == 0
                      ? new Column(name, value, ts)
                      : new DeletedColumn(name, value, ts));
        }
    }

    private static class CorruptColumnException extends IOException
    {
        public CorruptColumnException(String s)
        {
            super(s);
        }
    }
}
