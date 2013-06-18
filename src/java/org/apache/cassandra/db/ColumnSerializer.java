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
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSerializer implements ISerializer<Column>
{
    public final static int DELETION_MASK        = 0x01;
    public final static int EXPIRATION_MASK      = 0x02;
    public final static int COUNTER_MASK         = 0x04;
    public final static int COUNTER_UPDATE_MASK  = 0x08;
    public final static int RANGE_TOMBSTONE_MASK = 0x10;

    /**
     * Flag affecting deserialization behavior.
     *  - LOCAL: for deserialization of local data (Expired columns are
     *      converted to tombstones (to gain disk space)).
     *  - FROM_REMOTE: for deserialization of data received from remote hosts
     *      (Expired columns are converted to tombstone and counters have
     *      their delta cleared)
     *  - PRESERVE_SIZE: used when no transformation must be performed, i.e,
     *      when we must ensure that deserializing and reserializing the
     *      result yield the exact same bytes. Streaming uses this.
     */
    public static enum Flag
    {
        LOCAL, FROM_REMOTE, PRESERVE_SIZE;
    }

    public void serialize(Column column, DataOutput out) throws IOException
    {
        assert column.name().remaining() > 0;
        ByteBufferUtil.writeWithShortLength(column.name(), out);
        try
        {
            out.writeByte(column.serializationFlags());
            if (column instanceof CounterColumn)
            {
                out.writeLong(((CounterColumn)column).timestampOfLastDelete());
            }
            else if (column instanceof ExpiringColumn)
            {
                out.writeInt(((ExpiringColumn) column).getTimeToLive());
                out.writeInt(column.getLocalDeletionTime());
            }
            out.writeLong(column.timestamp());
            ByteBufferUtil.writeWithLength(column.value(), out);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Column deserialize(DataInput in) throws IOException
    {
        return deserialize(in, Flag.LOCAL);
    }

    /*
     * For counter columns, we must know when we deserialize them if what we
     * deserialize comes from a remote host. If it does, then we must clear
     * the delta.
     */
    public Column deserialize(DataInput in, ColumnSerializer.Flag flag) throws IOException
    {
        return deserialize(in, flag, Integer.MIN_VALUE);
    }

    public Column deserialize(DataInput in, ColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
        if (name.remaining() <= 0)
            throw CorruptColumnException.create(in, name);

        int b = in.readUnsignedByte();
        return deserializeColumnBody(in, name, b, flag, expireBefore);
    }

    Column deserializeColumnBody(DataInput in, ByteBuffer name, int mask, ColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = in.readLong();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return CounterColumn.create(name, value, ts, timestampOfLastDelete, flag);
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            int ttl = in.readInt();
            int expiration = in.readInt();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return ExpiringColumn.create(name, value, ts, ttl, expiration, expireBefore, flag);
        }
        else
        {
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return (mask & COUNTER_UPDATE_MASK) != 0
                   ? new CounterUpdateColumn(name, value, ts)
                   : ((mask & DELETION_MASK) == 0
                      ? new Column(name, value, ts)
                      : new DeletedColumn(name, value, ts));
        }
    }

    public long serializedSize(Column column, TypeSizes type)
    {
        return column.serializedSize(type);
    }

    public static class CorruptColumnException extends IOException
    {
        public CorruptColumnException(String s)
        {
            super(s);
        }

        public static CorruptColumnException create(DataInput in, ByteBuffer name)
        {
            assert name.remaining() <= 0;
            String format = "invalid column name length %d%s";
            String details = "";
            if (in instanceof FileDataInput)
            {
                FileDataInput fdis = (FileDataInput)in;
                long remaining;
                try
                {
                    remaining = fdis.bytesRemaining();
                }
                catch (IOException e)
                {
                    throw new FSReadError(e, fdis.getPath());
                }
                details = String.format(" (%s, %d bytes remaining)", fdis.getPath(), remaining);
            }
            return new CorruptColumnException(String.format(format, name.remaining(), details));
        }
    }
}
