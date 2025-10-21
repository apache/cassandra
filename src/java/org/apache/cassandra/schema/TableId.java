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
package org.apache.cassandra.schema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.LongUnaryOperator;

import javax.annotation.Nullable;

import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

/**
 * The unique identifier of a table.
 * <p>
 * This is essentially a UUID, but we wrap it as it's used quite a bit in the code and having a nicely named class make
 * the code more readable.
 */
public final class TableId implements Comparable<TableId>
{
    public static final long MAGIC = 1956074401491665062L;
    /**
     * Represents a placeholder for cases where a table id is defined, but has no meaning.
     */
    public static final TableId UNDEFINED = TableId.fromRaw(Long.MIN_VALUE, Long.MIN_VALUE);
    public static final long EMPTY_SIZE = ObjectSizes.measureDeep(new UUID(0, 0));
    private static final int MAGIC_BYTE = (int) ((flipSign(MAGIC) >>> 56) & 0xf0);

    final long msb, lsb;

    private TableId(UUID id)
    {
        this(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    private TableId(long msb, long lsb)
    {
        this.msb = msb;
        this.lsb = lsb;
    }

    public static TableId fromUUID(UUID id)
    {
        return new TableId(id);
    }

    public static TableId fromRaw(long msb, long lsb)
    {
        return new TableId(msb, lsb);
    }

    // TODO: should we be using UUID.randomUUID()?
    public static TableId generate()
    {
        return new TableId(nextTimeUUID().asUUID());
    }

    public static TableId fromString(String idString)
    {
        if (idString.startsWith("tid:"))
            return new TableId(MAGIC, Hex.parseLong(idString, 4, idString.length()));
        return new TableId(UUID.fromString(idString));
    }

    public static TableId get(ClusterMetadata prev)
    {
        int i = 0;
        while (true)
        {
            TableId tableId = TableId.fromLong(prev.epoch.getEpoch() + i);
            if (!tableIdExists(prev, tableId))
                return tableId;
            i++;
        }
    }

    private static boolean tableIdExists(ClusterMetadata metadata, TableId tableId)
    {
        return metadata.schema.getKeyspaces().stream().anyMatch(ks -> ks.tables.containsTable(tableId));
    }

    @Nullable
    public static Pair<String, TableId> tableNameAndIdFromFilename(String filename)
    {
        int dash = filename.lastIndexOf('-');
        if (dash <= 0 || dash != filename.length() - 32 - 1)
            return null;

        TableId id = fromHexString(filename.substring(dash + 1));
        String tableName = filename.substring(0, dash);

        return Pair.create(tableName, id);
    }

    private static TableId fromHexString(String nonDashUUID)
    {
        ByteBuffer bytes = ByteBufferUtil.hexToBytes(nonDashUUID);
        long msb = bytes.getLong(0);
        long lsb = bytes.getLong(8);
        return fromUUID(new UUID(msb, lsb));
    }

    public static TableId fromLong(long start)
    {
        return TableId.fromUUID(new UUID(MAGIC, start));
    }

    /**
     * Creates the UUID of a system table.
     *
     * This is deterministically based on the table name as system tables are hardcoded and initialized independently
     * on each node (they don't go through a CREATE), but we still want them to have the same ID everywhere.
     *
     * We shouldn't use this for any other table.
     */
    public static TableId forSystemTable(String keyspace, String table)
    {
        assert SchemaConstants.isSystemKeyspace(keyspace) : String.format("Table %s.%s is not a system table; only keyspaces allowed are %s", keyspace, table, SchemaConstants.getSystemKeyspaces());
        return unsafeDeterministic(keyspace, table);
    }

    public static TableId unsafeDeterministic(String keyspace, String table)
    {
        return new TableId(UUID.nameUUIDFromBytes(ArrayUtils.addAll(keyspace.getBytes(UTF_8), table.getBytes(UTF_8))));
    }

    public String toHexString()
    {
        return ByteBufferUtil.bytesToHex(ByteBuffer.wrap(UUIDGen.decompose(msb, lsb)));
    }

    public UUID asUUID()
    {
        return new UUID(msb, lsb);
    }

    @Override
    public int hashCode()
    {
        long hilo = msb ^ lsb;
        return ((int)(hilo >> 32)) ^ (int) hilo;
    }

    @Override
    public final boolean equals(Object o)
    {
        if (o == this | o == null) return o == this;
        if (o.getClass() != TableId.class) return false;
        TableId that = (TableId) o;
        return this.msb == that.msb && this.lsb == that.lsb;
    }

    @Override
    public String toString()
    {
        if (msb == MAGIC)
            return "tid:" + Long.toHexString(lsb);
        return asUUID().toString();
    }

    public String toLongString()
    {
        return asUUID().toString();
    }

    public void serialize(DataOutput out) throws IOException
    {
        out.writeLong(msb);
        out.writeLong(lsb);
    }

    public <V> int serialize(V dst, ValueAccessor<V> accessor, int offset)
    {
        int position = offset;
        position += accessor.putLong(dst, position, msb);
        position += accessor.putLong(dst, position, lsb);
        return position - offset;
    }

    public final long msb()
    {
        return msb;
    }

    public final long lsb()
    {
        return lsb;
    }

    public final int serializedSize()
    {
        return 16;
    }

    public void serializeCompact(DataOutputPlus out) throws IOException
    {
        serializeCompact(out, Long.compare(msb, MAGIC), msb, lsb);
    }

    public void serializeCompactComparable(DataOutputPlus out) throws IOException
    {
        serializeCompact(out, Long.compare(msb, MAGIC), flipSign(msb), flipSign(lsb));
    }

    private static void serializeCompact(DataOutputPlus out, int compareMagic, long msb, long lsb) throws IOException
    {
        // make this an ordered compact serialization at the cost of one byte
        // TODO (desired): we could use 6 bits of the byte for encoding the vint header and avoid any extra bytes in most cases
        if (compareMagic == 0)
        {
            int bytes = numberOfBytes(lsb);
            out.writeByte(MAGIC_BYTE | bytes);
            out.writeLeastSignificantBytes(lsb, bytes);
        }
        else
        {
            out.writeByte(MAGIC_BYTE + (compareMagic > 0 ? 0x10 : -0x10));
            out.writeLong(msb);
            out.writeLong(lsb);
        }
    }

    public <V> int serializeCompact(V dst, ValueAccessor<V> accessor, int offset)
    {
        return serializeCompact(dst, accessor, offset, Long.compare(msb, MAGIC), msb, lsb);
    }

    public <V> int serializeCompactComparable(V dst, ValueAccessor<V> accessor, int offset)
    {
        return serializeCompact(dst, accessor, offset, Long.compare(msb, MAGIC), flipSign(msb), flipSign(lsb));
    }

    private static <V> int serializeCompact(V dst, ValueAccessor<V> accessor, int offset, int compareMagic, long msb, long lsb)
    {
        if (compareMagic == 0)
        {
            int bytes = numberOfBytes(lsb);
            accessor.putByte(dst, offset, (byte) (MAGIC_BYTE | bytes));
            accessor.putLeastSignificantBytes(dst, offset + 1, lsb, bytes);
            return 1 + bytes;
        }
        else
        {
            int position = offset;
            position += accessor.putByte(dst, position, (byte) (MAGIC_BYTE + (compareMagic > 0 ? 0x10 : -0x10)));
            position += accessor.putLong(dst, position, msb);
            position += accessor.putLong(dst, position, lsb);
            return position - offset;
        }
    }

    private static int numberOfBytes(long lsb)
    {
        return (64 + 7 - Long.numberOfLeadingZeros(lsb)) / 8;
    }

    public final int serializedCompactSize()
    {
        // make this an ordered compact serialization at the cost of one byte
        return msb == MAGIC ? 1 + numberOfBytes(lsb) : 17;
    }

    public final int serializedCompactComparableSize()
    {
        // make this an ordered compact serialization at the cost of one byte
        return msb == MAGIC ? 1 + numberOfBytes(flipSign(lsb)) : 17;
    }

    public static int staticSerializedSize()
    {
        return 16;
    }

    public static void skip(DataInputPlus in) throws IOException
    {
        in.skipBytesFully(16);
    }

    public static void skipCompact(DataInputPlus in) throws IOException
    {
        int b = in.readByte();
        if ((b & 0xf0) != MAGIC_BYTE)
            in.skipBytesFully(16);
        else
            in.skipBytesFully(b & 0xf);
    }

    public static TableId deserialize(DataInput in) throws IOException
    {
        return new TableId(in.readLong(), in.readLong());
    }

    private static TableId deserialize(DataInput in, LongUnaryOperator transform) throws IOException
    {
        return new TableId(transform.applyAsLong(in.readLong()), transform.applyAsLong(in.readLong()));
    }

    private static long flipSign(long bits)
    {
        return bits ^ Long.MIN_VALUE;
    }

    private static long keepSign(long bits)
    {
        return bits;
    }

    public static <V> TableId deserialize(V src, ValueAccessor<V> accessor, int offset)
    {
        return deserialize(src, accessor, offset, TableId::keepSign);
    }

    public static <V> TableId deserializeComparable(V src, ValueAccessor<V> accessor, int offset)
    {
        return deserialize(src, accessor, offset, TableId::flipSign);
    }

    private static <V> TableId deserialize(V src, ValueAccessor<V> accessor, int offset, LongUnaryOperator transform)
    {
        return new TableId(transform.applyAsLong(accessor.getLong(src, offset)), transform.applyAsLong(accessor.getLong(src, offset + TypeSizes.LONG_SIZE)));
    }

    public static TableId deserializeCompact(DataInputPlus in) throws IOException
    {
        return deserializeCompact(in, TableId::keepSign);
    }

    public static TableId deserializeCompactComparable(DataInputPlus in) throws IOException
    {
        return deserializeCompact(in, TableId::flipSign);
    }

    private static TableId deserializeCompact(DataInputPlus in, LongUnaryOperator transform) throws IOException
    {
        int b = in.readByte();
        if ((b & 0xf0) != MAGIC_BYTE) return deserialize(in, transform);
        else return new TableId(MAGIC, transform.applyAsLong(in.readLeastSignificantBytes(b & 0xf)));
    }

    public static <V> TableId deserializeCompact(V src, ValueAccessor<V> accessor, int offset)
    {
        return deserializeCompact(src, accessor, offset, TableId::keepSign);
    }

    public static <V> TableId deserializeCompactComparable(V src, ValueAccessor<V> accessor, int offset)
    {
        return deserializeCompact(src, accessor, offset, TableId::flipSign);
    }

    private static <V> TableId deserializeCompact(V src, ValueAccessor<V> accessor, int offset, LongUnaryOperator transform)
    {
        int b = accessor.getByte(src, offset++);
        if ((b & 0xf0) != MAGIC_BYTE) return deserialize(src, accessor, offset, transform);
        else return new TableId(MAGIC, transform.applyAsLong(accessor.getLeastSignificantBytes(src, offset, b & 0x0f)));
    }

    @Override
    public int compareTo(TableId that)
    {
        int c = Long.compare(this.msb, that.msb);
        return c != 0 ? c : Long.compare(this.lsb, that.lsb);
    }

    public static final IVersionedSerializer<TableId> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TableId t, DataOutputPlus out, int version) throws IOException
        {
            t.serialize(out);
        }

        @Override
        public TableId deserialize(DataInputPlus in, int version) throws IOException
        {
            return TableId.deserialize(in);
        }

        @Override
        public long serializedSize(TableId t, int version)
        {
            return t.serializedSize();
        }
    };

    public static final UnversionedSerializer<TableId> compactComparableSerializer = new UnversionedSerializer<TableId>()
    {
        @Override
        public void serialize(TableId t, DataOutputPlus out) throws IOException
        {
            t.serializeCompactComparable(out);
        }

        @Override
        public TableId deserialize(DataInputPlus in) throws IOException
        {
            return TableId.deserializeCompactComparable(in);
        }

        @Override
        public long serializedSize(TableId t)
        {
            return t.serializedCompactComparableSize();
        }
    };

    public static final MetadataSerializer<TableId> metadataSerializer = new MetadataSerializer<TableId>()
    {
        @Override
        public void serialize(TableId t, DataOutputPlus out, Version version) throws IOException
        {
            t.serialize(out);
        }

        @Override
        public TableId deserialize(DataInputPlus in, Version version) throws IOException
        {
            return TableId.deserialize(in);
        }

        @Override
        public long serializedSize(TableId t, Version version)
        {
            return t.serializedSize();
        }
    };
}
