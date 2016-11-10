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
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ColumnSerializer implements ISerializer<Cell>
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

    private final CellNameType type;

    public ColumnSerializer(CellNameType type)
    {
        this.type = type;
    }

    public void serialize(Cell cell, DataOutputPlus out) throws IOException
    {
        assert !cell.name().isEmpty();
        type.cellSerializer().serialize(cell.name(), out);
        try
        {
            out.writeByte(cell.serializationFlags());
            if (cell instanceof CounterCell)
            {
                out.writeLong(((CounterCell) cell).timestampOfLastDelete());
            }
            else if (cell instanceof ExpiringCell)
            {
                out.writeInt(((ExpiringCell) cell).getTimeToLive());
                out.writeInt(cell.getLocalDeletionTime());
            }
            out.writeLong(cell.timestamp());
            ByteBufferUtil.writeWithLength(cell.value(), out);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public Cell deserialize(DataInput in) throws IOException
    {
        return deserialize(in, Flag.LOCAL);
    }

    /*
     * For counter columns, we must know when we deserialize them if what we
     * deserialize comes from a remote host. If it does, then we must clear
     * the delta.
     */
    public Cell deserialize(DataInput in, ColumnSerializer.Flag flag) throws IOException
    {
        return deserialize(in, flag, Integer.MIN_VALUE);
    }

    public Cell deserialize(DataInput in, ColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        CellName name = type.cellSerializer().deserialize(in);

        int b = in.readUnsignedByte();
        return deserializeColumnBody(in, name, b, flag, expireBefore);
    }

    Cell deserializeColumnBody(DataInput in, CellName name, int mask, ColumnSerializer.Flag flag, int expireBefore) throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
        {
            long timestampOfLastDelete = in.readLong();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return BufferCounterCell.create(name, value, ts, timestampOfLastDelete, flag);
        }
        else if ((mask & EXPIRATION_MASK) != 0)
        {
            int ttl = in.readInt();
            int expiration = in.readInt();
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return BufferExpiringCell.create(name, value, ts, ttl, expiration, expireBefore, flag);
        }
        else
        {
            long ts = in.readLong();
            ByteBuffer value = ByteBufferUtil.readWithLength(in);
            return (mask & COUNTER_UPDATE_MASK) != 0
                   ? new BufferCounterUpdateCell(name, value, ts)
                   : ((mask & DELETION_MASK) == 0
                      ? new BufferCell(name, value, ts)
                      : new BufferDeletedCell(name, value, ts));
        }
    }

    void skipColumnBody(DataInput in, int mask) throws IOException
    {
        if ((mask & COUNTER_MASK) != 0)
            FileUtils.skipBytesFully(in, 16);
        else if ((mask & EXPIRATION_MASK) != 0)
            FileUtils.skipBytesFully(in, 16);
        else
            FileUtils.skipBytesFully(in, 8);

        int length = in.readInt();
        FileUtils.skipBytesFully(in, length);
    }

    public long serializedSize(Cell cell, TypeSizes typeSizes)
    {
        return cell.serializedSize(type, typeSizes);
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
