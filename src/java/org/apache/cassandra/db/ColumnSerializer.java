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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSerializer implements IColumnSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnSerializer.class);

    public final static int DELETION_MASK        = 0x01;
    public final static int EXPIRATION_MASK      = 0x02;
    public final static int COUNTER_MASK         = 0x04;
    public final static int COUNTER_UPDATE_MASK  = 0x08;
    public final static int RANGE_TOMBSTONE_MASK = 0x10;

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
        return deserialize(dis, Flag.LOCAL);
    }

    /*
     * For counter columns, we must know when we deserialize them if what we
     * deserialize comes from a remote host. If it does, then we must clear
     * the delta.
     */
    public Column deserialize(DataInput dis, IColumnSerializer.Flag flag) throws IOException
    {
        return deserialize(dis, flag, (int) (System.currentTimeMillis() / 1000));
    }

    public Column deserialize(DataInput dis, IColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        ByteBuffer name = ByteBufferUtil.readWithShortLength(dis);
        if (name.remaining() <= 0)
            throw CorruptColumnException.create(dis, name);

        int b = dis.readUnsignedByte();
        return deserializeColumnBody(dis, name, b, flag, expireBefore);
    }

    Column deserializeColumnBody(DataInput dis, ByteBuffer name, int mask, IColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = dis.readLong();
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            return CounterColumn.create(name, value, ts, timestampOfLastDelete, flag);
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            int ttl = dis.readInt();
            int expiration = dis.readInt();
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            return ExpiringColumn.create(name, value, ts, ttl, expiration, expireBefore, flag);
        }
        else
        {
            long ts = dis.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(dis);
            return (mask & COUNTER_UPDATE_MASK) != 0
                   ? new CounterUpdateColumn(name, value, ts)
                   : ((mask & DELETION_MASK) == 0
                      ? new Column(name, value, ts)
                      : new DeletedColumn(name, value, ts));
        }
    }

    public long serializedSize(IColumn column, TypeSizes type)
    {
        return column.serializedSize(type);
    }

    public static class CorruptColumnException extends IOException
    {
        public CorruptColumnException(String s)
        {
            super(s);
        }

        public static CorruptColumnException create(DataInput dis, ByteBuffer name)
        {
            assert name.remaining() <= 0;
            String format = "invalid column name length %d%s";
            String details = "";
            if (dis instanceof FileDataInput)
            {
                try
                {
                    FileDataInput fdis = (FileDataInput)dis;
                    details = String.format(" (%s, %d bytes remaining)", fdis.getPath(), fdis.bytesRemaining());
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
            return new CorruptColumnException(String.format(format, name.remaining(), details));
        }
    }
}
