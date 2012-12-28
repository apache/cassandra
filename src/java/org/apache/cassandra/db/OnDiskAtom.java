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

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface OnDiskAtom
{
    public ByteBuffer name();

    /**
     * For a standard column, this is the same as timestamp().
     * For a super column, this is the min/max column timestamp of the sub columns.
     */
    public long minTimestamp();
    public long maxTimestamp();
    public int getLocalDeletionTime(); // for tombstone GC, so int is sufficient granularity

    public int serializedSize(TypeSizes typeSizes);
    public long serializedSizeForSSTable();

    public void validateFields(CFMetaData metadata) throws MarshalException;
    public void updateDigest(MessageDigest digest);

    public static class Serializer implements ISSTableSerializer<OnDiskAtom>
    {
        private final IColumnSerializer columnSerializer;

        public Serializer(IColumnSerializer columnSerializer)
        {
            this.columnSerializer = columnSerializer;
        }

        public void serializeForSSTable(OnDiskAtom atom, DataOutput dos) throws IOException
        {
            if (atom instanceof IColumn)
            {
                columnSerializer.serialize((IColumn)atom, dos);
            }
            else
            {
                assert atom instanceof RangeTombstone;
                RangeTombstone.serializer.serializeForSSTable((RangeTombstone)atom, dos);
            }
        }

        public OnDiskAtom deserializeFromSSTable(DataInput dis, Descriptor.Version version) throws IOException
        {
            return deserializeFromSSTable(dis, IColumnSerializer.Flag.LOCAL, (int)(System.currentTimeMillis() / 1000), version);
        }

        public OnDiskAtom deserializeFromSSTable(DataInput dis, IColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
        {
            if (columnSerializer instanceof SuperColumnSerializer)
            {
                return columnSerializer.deserialize(dis, flag, expireBefore);
            }
            else
            {
                ByteBuffer name = ByteBufferUtil.readWithShortLength(dis);
                if (name.remaining() <= 0)
                    throw ColumnSerializer.CorruptColumnException.create(dis, name);

                int b = dis.readUnsignedByte();
                if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
                    return RangeTombstone.serializer.deserializeBody(dis, name, version);
                else
                    return ((ColumnSerializer)columnSerializer).deserializeColumnBody(dis, name, b, flag, expireBefore);
            }
        }
    }
}
