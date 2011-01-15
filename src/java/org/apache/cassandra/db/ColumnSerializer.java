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

import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class ColumnSerializer implements ICompactSerializer2<IColumn>
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnSerializer.class);
    
    public final static int DELETION_MASK = 0x01;
    public final static int EXPIRATION_MASK = 0x02;
    public final static int COUNTER_MASK    = 0x04;

    public void serialize(IColumn column, DataOutput dos)
    {
        assert column.name().remaining() > 0;
        ByteBufferUtil.writeWithShortLength(column.name(), dos);
        try
        {
            if (column instanceof CounterColumn)
            {
                dos.writeByte(COUNTER_MASK);
                dos.writeLong(((CounterColumn)column).timestampOfLastDelete());
                ByteBufferUtil.writeWithShortLength(ByteBuffer.wrap(((CounterColumn)column).partitionedCounter()), dos);
            }
            else if (column instanceof ExpiringColumn)
            {
                dos.writeByte(EXPIRATION_MASK);
                dos.writeInt(((ExpiringColumn) column).getTimeToLive());
                dos.writeInt(column.getLocalDeletionTime());
            }
            else
            {
                dos.writeByte((column.isMarkedForDelete()) ? DELETION_MASK : 0);
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
        ByteBuffer name = FBUtilities.readShortByteArray(dis);
        if (name.remaining() <= 0)
            throw new CorruptColumnException("invalid column name length " + name.remaining());

        int b = dis.readUnsignedByte();
        if ((b & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = dis.readLong();
            ByteBuffer pc = FBUtilities.readShortByteArray(dis);
            byte[] partitionedCounter = Arrays.copyOfRange(pc.array(), pc.position() + pc.arrayOffset(), pc.limit());
            long timestamp = dis.readLong();
            ByteBuffer value = FBUtilities.readByteArray(dis);
            return new CounterColumn(name, value, timestamp, partitionedCounter, timestampOfLastDelete);
        }
        else if ((b & EXPIRATION_MASK) != 0)
        {
            int ttl = dis.readInt();
            int expiration = dis.readInt();
            long ts = dis.readLong();
            ByteBuffer value = FBUtilities.readByteArray(dis);
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
            ByteBuffer value = FBUtilities.readByteArray(dis);
            return (b & DELETION_MASK) == 0
                   ? new Column(name, value, ts)
                   : new DeletedColumn(name, value, ts);
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
