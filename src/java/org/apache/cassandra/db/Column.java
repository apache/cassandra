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
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.HeapAllocator;

/**
 * Column is immutable, which prevents all kinds of confusion in a multithreaded environment.
 */
public class Column implements OnDiskAtom
{
    public static final int MAX_NAME_LENGTH = FBUtilities.MAX_UNSIGNED_SHORT;

    public static final ColumnSerializer serializer = new ColumnSerializer();

    public static OnDiskAtom.Serializer onDiskSerializer()
    {
        return OnDiskAtom.Serializer.instance;
    }

    public static Iterator<OnDiskAtom> onDiskIterator(final DataInput in, final int count, final ColumnSerializer.Flag flag, final int expireBefore, final Descriptor.Version version)
    {
        return new Iterator<OnDiskAtom>()
        {
            int i = 0;

            public boolean hasNext()
            {
                return i < count;
            }

            public OnDiskAtom next()
            {
                ++i;
                try
                {
                    return onDiskSerializer().deserializeFromSSTable(in, flag, expireBefore, version);
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }

            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    protected final ByteBuffer name;
    protected final ByteBuffer value;
    protected final long timestamp;

    Column(ByteBuffer name)
    {
        this(name, ByteBufferUtil.EMPTY_BYTE_BUFFER);
    }

    public Column(ByteBuffer name, ByteBuffer value)
    {
        this(name, value, 0);
    }

    public Column(ByteBuffer name, ByteBuffer value, long timestamp)
    {
        assert name != null;
        assert value != null;
        assert name.remaining() <= Column.MAX_NAME_LENGTH;
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Column withUpdatedName(ByteBuffer newName)
    {
        return new Column(newName, value, timestamp);
    }

    public ByteBuffer name()
    {
        return name;
    }

    public ByteBuffer value()
    {
        return value;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public long minTimestamp()
    {
        return timestamp;
    }

    public long maxTimestamp()
    {
        return timestamp;
    }

    public boolean isMarkedForDelete()
    {
        return (int) (System.currentTimeMillis() / 1000) >= getLocalDeletionTime();
    }

    public long getMarkedForDeleteAt()
    {
        throw new IllegalStateException("column is not marked for delete");
    }

    public long mostRecentLiveChangeAt()
    {
        return timestamp;
    }

    public long mostRecentNonGCableChangeAt(int gcbefore)
    {
        return timestamp;
    }

    public int dataSize()
    {
        return name().remaining() + value.remaining() + TypeSizes.NATIVE.sizeof(timestamp);
    }

    public int serializedSize(TypeSizes typeSizes)
    {
        /*
         * Size of a column is =
         *   size of a name (short + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + 8 bytes for timestamp
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */
        int nameSize = name.remaining();
        int valueSize = value.remaining();
        return typeSizes.sizeof((short) nameSize) + nameSize + 1 + typeSizes.sizeof(timestamp) + typeSizes.sizeof(valueSize) + valueSize;
    }

    public long serializedSizeForSSTable()
    {
        return serializedSize(TypeSizes.NATIVE);
    }

    public int serializationFlags()
    {
        return 0;
    }

    public Column diff(Column column)
    {
        if (timestamp() < column.timestamp())
        {
            return column;
        }
        return null;
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(name.duplicate());
        digest.update(value.duplicate());

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

    public int getLocalDeletionTime()
    {
        return Integer.MAX_VALUE;
    }

    public Column reconcile(Column column)
    {
        return reconcile(column, HeapAllocator.instance);
    }

    public Column reconcile(Column column, Allocator allocator)
    {
        // tombstones take precedence.  (if both are tombstones, then it doesn't matter which one we use.)
        if (isMarkedForDelete())
            return timestamp() < column.timestamp() ? column : this;
        if (column.isMarkedForDelete())
            return timestamp() > column.timestamp() ? this : column;
        // break ties by comparing values.
        if (timestamp() == column.timestamp())
            return value().compareTo(column.value()) < 0 ? column : this;
        // neither is tombstoned and timestamps are different
        return timestamp() < column.timestamp() ? column : this;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Column column = (Column)o;

        if (timestamp != column.timestamp)
            return false;
        if (!name.equals(column.name))
            return false;

        return value.equals(column.value);
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (int)(timestamp ^ (timestamp >>> 32));
        return result;
    }

    public Column localCopy(ColumnFamilyStore cfs)
    {
        return localCopy(cfs, HeapAllocator.instance);
    }

    public Column localCopy(ColumnFamilyStore cfs, Allocator allocator)
    {
        return new Column(cfs.internOrCopy(name, allocator), allocator.clone(value), timestamp);
    }

    public String getString(AbstractType<?> comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(value.remaining());
        sb.append("@");
        sb.append(timestamp());
        return sb.toString();
    }

    public boolean isLive()
    {
        return !isMarkedForDelete();
    }

    protected void validateName(CFMetaData metadata) throws MarshalException
    {
        metadata.comparator.validate(name());
    }

    public void validateFields(CFMetaData metadata) throws MarshalException
    {
        validateName(metadata);
        AbstractType<?> valueValidator = metadata.getValueValidator(name());
        if (valueValidator != null)
            valueValidator.validate(value());
    }

    public boolean hasIrrelevantData(int gcBefore)
    {
        return getLocalDeletionTime() < gcBefore;
    }

    public static Column create(ByteBuffer name, ByteBuffer value, long timestamp, int ttl, CFMetaData metadata)
    {
        if (ttl <= 0)
            ttl = metadata.getDefaultTimeToLive();

        return ttl > 0
               ? new ExpiringColumn(name, value, timestamp, ttl)
               : new Column(name, value, timestamp);
    }

    public static Column create(String value, long timestamp, String... names)
    {
        return new Column(decomposeName(names), UTF8Type.instance.decompose(value), timestamp);
    }

    public static Column create(int value, long timestamp, String... names)
    {
        return new Column(decomposeName(names), Int32Type.instance.decompose(value), timestamp);
    }

    public static Column create(boolean value, long timestamp, String... names)
    {
        return new Column(decomposeName(names), BooleanType.instance.decompose(value), timestamp);
    }

    public static Column create(double value, long timestamp, String... names)
    {
        return new Column(decomposeName(names), DoubleType.instance.decompose(value), timestamp);
    }

    public static Column create(ByteBuffer value, long timestamp, String... names)
    {
        return new Column(decomposeName(names), value, timestamp);
    }

    public static Column create(InetAddress value, long timestamp, String... names)
    {
        return new Column(decomposeName(names), InetAddressType.instance.decompose(value), timestamp);
    }

    static ByteBuffer decomposeName(String... names)
    {
        assert names.length > 0;

        if (names.length == 1)
            return UTF8Type.instance.decompose(names[0]);

        // not super performant.  at this time, only infrequently called schema code uses this.
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(names.length);
        for (int i = 0; i < names.length; i++)
            types.add(UTF8Type.instance);

        CompositeType.Builder builder = new CompositeType.Builder(CompositeType.getInstance(types));
        for (String name : names)
            builder.add(UTF8Type.instance.decompose(name));
        return builder.build();
    }
}

