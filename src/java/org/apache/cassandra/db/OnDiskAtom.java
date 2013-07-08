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
import org.apache.cassandra.io.ISSTableSerializer;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.serializers.MarshalException;
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
        public static Serializer instance = new Serializer();

        private Serializer() {}

        public void serializeForSSTable(OnDiskAtom atom, DataOutput out) throws IOException
        {
            if (atom instanceof Column)
            {
                Column.serializer.serialize((Column) atom, out);
            }
            else
            {
                assert atom instanceof RangeTombstone;
                RangeTombstone.serializer.serializeForSSTable((RangeTombstone)atom, out);
            }
        }

        public OnDiskAtom deserializeFromSSTable(DataInput in, Descriptor.Version version) throws IOException
        {
            return deserializeFromSSTable(in, ColumnSerializer.Flag.LOCAL, Integer.MIN_VALUE, version);
        }

        public OnDiskAtom deserializeFromSSTable(DataInput in, ColumnSerializer.Flag flag, int expireBefore, Descriptor.Version version) throws IOException
        {
            ByteBuffer name = ByteBufferUtil.readWithShortLength(in);
            if (name.remaining() <= 0)
            {
                // SSTableWriter.END_OF_ROW
                return null;
            }

            int b = in.readUnsignedByte();
            if ((b & ColumnSerializer.RANGE_TOMBSTONE_MASK) != 0)
                return RangeTombstone.serializer.deserializeBody(in, name, version);
            else
                return Column.serializer.deserializeColumnBody(in, name, b, flag, expireBefore);
        }
    }
}
