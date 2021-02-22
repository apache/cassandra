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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.CBCodec;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.DataType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;

public class ResultSet
{
    public static final Codec codec = new Codec();

    public final ResultMetadata metadata;
    public final List<List<ByteBuffer>> rows;

    public ResultSet(ResultMetadata resultMetadata)
    {
        this(resultMetadata, new ArrayList<List<ByteBuffer>>());
    }

    public ResultSet(ResultMetadata resultMetadata, List<List<ByteBuffer>> rows)
    {
        this.metadata = resultMetadata;
        this.rows = rows;
    }

    public int size()
    {
        return rows.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void addRow(List<ByteBuffer> row)
    {
        assert row.size() == metadata.valueCount();
        rows.add(row);
    }

    public void addColumnValue(ByteBuffer value)
    {
        if (rows.isEmpty() || lastRow().size() == metadata.valueCount())
            rows.add(new ArrayList<ByteBuffer>(metadata.valueCount()));

        lastRow().add(value);
    }

    private List<ByteBuffer> lastRow()
    {
        return rows.get(rows.size() - 1);
    }

    public void reverse()
    {
        Collections.reverse(rows);
    }

    public void trim(int limit)
    {
        int toRemove = rows.size() - limit;
        if (toRemove > 0)
        {
            for (int i = 0; i < toRemove; i++)
                rows.remove(rows.size() - 1);
        }
    }

    @Override
    public String toString()
    {
        try
        {
            StringBuilder sb = new StringBuilder();
            sb.append(metadata).append('\n');
            for (List<ByteBuffer> row : rows)
            {
                for (int i = 0; i < row.size(); i++)
                {
                    ByteBuffer v = row.get(i);
                    if (v == null)
                    {
                        sb.append(" | null");
                    }
                    else
                    {
                        sb.append(" | ");
                        if (metadata.flags.contains(Flag.NO_METADATA))
                            sb.append("0x").append(ByteBufferUtil.bytesToHex(v));
                        else
                            sb.append(metadata.names.get(i).type.getString(v));
                    }
                }
                sb.append('\n');
            }
            sb.append("---");
            return sb.toString();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static class Codec implements CBCodec<ResultSet>
    {
        /*
         * Format:
         *   - metadata
         *   - rows count (4 bytes)
         *   - rows
         */
        public ResultSet decode(ByteBuf body, ProtocolVersion version)
        {
            ResultMetadata m = ResultMetadata.codec.decode(body, version);
            int rowCount = body.readInt();
            ResultSet rs = new ResultSet(m, new ArrayList<List<ByteBuffer>>(rowCount));

            // rows
            int totalValues = rowCount * m.columnCount;
            for (int i = 0; i < totalValues; i++)
                rs.addColumnValue(CBUtil.readValue(body));

            return rs;
        }

        public void encode(ResultSet rs, ByteBuf dest, ProtocolVersion version)
        {
            ResultMetadata.codec.encode(rs.metadata, dest, version);
            dest.writeInt(rs.rows.size());
            for (List<ByteBuffer> row : rs.rows)
            {
                // Note that we do only want to serialize only the first columnCount values, even if the row
                // as more: see comment on ResultMetadata.names field.
                for (int i = 0; i < rs.metadata.columnCount; i++)
                    CBUtil.writeValue(row.get(i), dest);
            }
        }

        public int encodedSize(ResultSet rs, ProtocolVersion version)
        {
            int size = ResultMetadata.codec.encodedSize(rs.metadata, version) + 4;
            for (List<ByteBuffer> row : rs.rows)
            {
                for (int i = 0; i < rs.metadata.columnCount; i++)
                    size += CBUtil.sizeOfValue(row.get(i));
            }
            return size;
        }
    }

    /**
     * The metadata for the results of executing a query or prepared statement.
     */
    public static class ResultMetadata
    {
        public static final CBCodec<ResultMetadata> codec = new Codec();

        public static final ResultMetadata EMPTY = new ResultMetadata(MD5Digest.compute(new byte[0]), EnumSet.of(Flag.NO_METADATA), null, 0, null);

        private final EnumSet<Flag> flags;
        // Please note that columnCount can actually be smaller than names, even if names is not null. This is
        // used to include columns in the resultSet that we need to do post-query re-orderings
        // (SelectStatement.orderResults) but that shouldn't be sent to the user as they haven't been requested
        // (CASSANDRA-4911). So the serialization code will exclude any columns in name whose index is >= columnCount.
        public final List<ColumnSpecification> names;
        private final int columnCount;
        private PagingState pagingState;
        private final MD5Digest resultMetadataId;

        public ResultMetadata(MD5Digest digest, List<ColumnSpecification> names)
        {
            this(digest, EnumSet.noneOf(Flag.class), names, names.size(), null);
            if (!names.isEmpty() && ColumnSpecification.allInSameTable(names))
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        // Problem is that we compute the metadata from the columns on creation;
        // when re-preparing we create the intermediate object
        public ResultMetadata(List<ColumnSpecification> names)
        {
            this(names, null);
        }

        // Problem is that we compute the metadata from the columns on creation;
        // when re-preparing we create the intermediate object
        public ResultMetadata(List<ColumnSpecification> names, PagingState pagingState)
        {
            this(computeResultMetadataId(names), EnumSet.noneOf(Flag.class), names, names.size(), pagingState);
            if (!names.isEmpty() && ColumnSpecification.allInSameTable(names))
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        private ResultMetadata(MD5Digest resultMetadataId, EnumSet<Flag> flags, List<ColumnSpecification> names, int columnCount, PagingState pagingState)
        {
            this.resultMetadataId = resultMetadataId;
            this.flags = flags;
            this.names = names;
            this.columnCount = columnCount;
            this.pagingState = pagingState;
        }

        public ResultMetadata copy()
        {
            return new ResultMetadata(resultMetadataId, EnumSet.copyOf(flags), names, columnCount, pagingState);
        }

        /**
         * Return only the column names requested by the user, excluding those added for post-query re-orderings,
         * see definition of names and columnCount.
         **/
        public List<ColumnSpecification> requestNames()
        {
            return names.subList(0, columnCount);
        }

        // The maximum number of values that the ResultSet can hold. This can be bigger than columnCount due to CASSANDRA-4911
        public int valueCount()
        {
            return names == null ? columnCount : names.size();
        }

        @VisibleForTesting
        public EnumSet<Flag> getFlags()
        {
            return flags;
        }

        @VisibleForTesting
        public int getColumnCount()
        {
            return columnCount;
        }

        @VisibleForTesting
        public PagingState getPagingState()
        {
            return pagingState;
        }

        /**
         * Adds the specified columns which will not be serialized.
         *
         * @param columns the columns
         */
        public ResultMetadata addNonSerializedColumns(Collection<? extends ColumnSpecification> columns)
        {
            // See comment above. Because columnCount doesn't account the newly added name, it
            // won't be serialized.
            names.addAll(columns);
            return this;
        }

        public void setHasMorePages(PagingState pagingState)
        {
            this.pagingState = pagingState;
            if (pagingState == null)
                flags.remove(Flag.HAS_MORE_PAGES);
            else
                flags.add(Flag.HAS_MORE_PAGES);
        }

        public void setSkipMetadata()
        {
            flags.add(Flag.NO_METADATA);
        }

        public void setMetadataChanged()
        {
            flags.add(Flag.METADATA_CHANGED);
        }

        public MD5Digest getResultMetadataId()
        {
            return resultMetadataId;
        }

        public static ResultMetadata fromPrepared(CQLStatement statement)
        {
            if (statement instanceof SelectStatement)
                return ((SelectStatement)statement).getResultMetadata();

            return ResultSet.ResultMetadata.EMPTY;
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;

            if (!(other instanceof ResultMetadata))
                return false;

            ResultMetadata that = (ResultMetadata) other;

            return Objects.equals(flags, that.flags)
                   && Objects.equals(names, that.names)
                   && columnCount == that.columnCount
                   && Objects.equals(pagingState, that.pagingState);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(flags, names, columnCount, pagingState);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (names == null)
            {
                sb.append("[").append(columnCount).append(" columns]");
            }
            else
            {
                for (ColumnSpecification name : names)
                {
                    sb.append("[").append(name.name);
                    sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
                    sb.append(", ").append(name.type).append("]");
                }
            }
            if (flags.contains(Flag.HAS_MORE_PAGES))
                sb.append(" (to be continued)");
            return sb.toString();
        }

        private static class Codec implements CBCodec<ResultMetadata>
        {
            public ResultMetadata decode(ByteBuf body, ProtocolVersion version)
            {
                // flags & column count
                int iflags = body.readInt();
                int columnCount = body.readInt();

                EnumSet<Flag> flags = Flag.deserialize(iflags);

                MD5Digest resultMetadataId = null;
                if (flags.contains(Flag.METADATA_CHANGED))
                {
                    assert version.isGreaterOrEqualTo(ProtocolVersion.V5) : "MetadataChanged flag is not supported before native protocol v5";
                    assert !flags.contains(Flag.NO_METADATA) : "MetadataChanged and NoMetadata are mutually exclusive flags";

                    resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
                }

                PagingState state = null;
                if (flags.contains(Flag.HAS_MORE_PAGES))
                    state = PagingState.deserialize(CBUtil.readValueNoCopy(body), version);

                if (flags.contains(Flag.NO_METADATA))
                    return new ResultMetadata(null, flags, null, columnCount, state);

                boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

                String globalKsName = null;
                String globalCfName = null;
                if (globalTablesSpec)
                {
                    globalKsName = CBUtil.readString(body);
                    globalCfName = CBUtil.readString(body);
                }

                // metadata (names/types)
                List<ColumnSpecification> names = new ArrayList<ColumnSpecification>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
                    String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
                    ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
                    AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
                    names.add(new ColumnSpecification(ksName, cfName, colName, type));
                }
                return new ResultMetadata(resultMetadataId, flags, names, names.size(), state);
            }

            public void encode(ResultMetadata m, ByteBuf dest, ProtocolVersion version)
            {
                boolean noMetadata = m.flags.contains(Flag.NO_METADATA);
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                boolean hasMorePages = m.flags.contains(Flag.HAS_MORE_PAGES);
                boolean metadataChanged = m.flags.contains(Flag.METADATA_CHANGED);
                assert version.isGreaterThan(ProtocolVersion.V1) || (!hasMorePages && !noMetadata)
                    : "version = " + version + ", flags = " + m.flags;

                dest.writeInt(Flag.serialize(m.flags));
                dest.writeInt(m.columnCount);

                if (hasMorePages)
                    CBUtil.writeValue(m.pagingState.serialize(version), dest);

                if (version.isGreaterOrEqualTo(ProtocolVersion.V5)  && metadataChanged)
                {
                    assert !noMetadata : "MetadataChanged and NoMetadata are mutually exclusive flags";
                    CBUtil.writeBytes(m.getResultMetadataId().bytes, dest);
                }

                if (!noMetadata)
                {
                    if (globalTablesSpec)
                    {
                        CBUtil.writeAsciiString(m.names.get(0).ksName, dest);
                        CBUtil.writeAsciiString(m.names.get(0).cfName, dest);
                    }

                    for (int i = 0; i < m.columnCount; i++)
                    {
                        ColumnSpecification name = m.names.get(i);
                        if (!globalTablesSpec)
                        {
                            CBUtil.writeAsciiString(name.ksName, dest);
                            CBUtil.writeAsciiString(name.cfName, dest);
                        }
                        CBUtil.writeString(name.name.toString(), dest);
                        DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
                    }
                }
            }

            public int encodedSize(ResultMetadata m, ProtocolVersion version)
            {
                boolean noMetadata = m.flags.contains(Flag.NO_METADATA);
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                boolean hasMorePages = m.flags.contains(Flag.HAS_MORE_PAGES);
                boolean metadataChanged = m.flags.contains(Flag.METADATA_CHANGED);

                int size = 8;
                if (hasMorePages)
                    size += CBUtil.sizeOfValue(m.pagingState.serializedSize(version));

                if (version.isGreaterOrEqualTo(ProtocolVersion.V5) && metadataChanged)
                    size += CBUtil.sizeOfBytes(m.getResultMetadataId().bytes);

                if (!noMetadata)
                {
                    if (globalTablesSpec)
                    {
                        size += CBUtil.sizeOfAsciiString(m.names.get(0).ksName);
                        size += CBUtil.sizeOfAsciiString(m.names.get(0).cfName);
                    }

                    for (int i = 0; i < m.columnCount; i++)
                    {
                        ColumnSpecification name = m.names.get(i);
                        if (!globalTablesSpec)
                        {
                            size += CBUtil.sizeOfAsciiString(name.ksName);
                            size += CBUtil.sizeOfAsciiString(name.cfName);
                        }
                        size += CBUtil.sizeOfString(name.name.toString());
                        size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version);
                    }
                }
                return size;
            }
        }
    }

    /**
     * The metadata for the query parameters in a prepared statement.
     */
    public static class PreparedMetadata
    {
        public static final CBCodec<PreparedMetadata> codec = new Codec();

        private final EnumSet<Flag> flags;
        public final List<ColumnSpecification> names;
        private final short[] partitionKeyBindIndexes;

        public PreparedMetadata(List<ColumnSpecification> names, short[] partitionKeyBindIndexes)
        {
            this(EnumSet.noneOf(Flag.class), names, partitionKeyBindIndexes);
            if (!names.isEmpty() && ColumnSpecification.allInSameTable(names))
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        private PreparedMetadata(EnumSet<Flag> flags, List<ColumnSpecification> names, short[] partitionKeyBindIndexes)
        {
            this.flags = flags;
            this.names = names;
            this.partitionKeyBindIndexes = partitionKeyBindIndexes;
        }

        public PreparedMetadata copy()
        {
            return new PreparedMetadata(EnumSet.copyOf(flags), names, partitionKeyBindIndexes);
        }

        @Override
        public boolean equals(Object other)
        {
            if (this == other)
                return true;

            if (!(other instanceof PreparedMetadata))
                return false;

            PreparedMetadata that = (PreparedMetadata) other;
            return this.names.equals(that.names) &&
                   this.flags.equals(that.flags) &&
                   Arrays.equals(this.partitionKeyBindIndexes, that.partitionKeyBindIndexes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(names, flags) + Arrays.hashCode(partitionKeyBindIndexes);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (ColumnSpecification name : names)
            {
                sb.append("[").append(name.name);
                sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
                sb.append(", ").append(name.type).append("]");
            }

            sb.append(", bindIndexes=[");
            if (partitionKeyBindIndexes != null)
            {
                for (int i = 0; i < partitionKeyBindIndexes.length; i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(partitionKeyBindIndexes[i]);
                }
            }
            sb.append("]");
            return sb.toString();
        }

        public static PreparedMetadata fromPrepared(CQLStatement statement)
        {
            return new PreparedMetadata(statement.getBindVariables(), statement.getPartitionKeyBindVariableIndexes());
        }

        private static class Codec implements CBCodec<PreparedMetadata>
        {
            public PreparedMetadata decode(ByteBuf body, ProtocolVersion version)
            {
                // flags & column count
                int iflags = body.readInt();
                int columnCount = body.readInt();

                EnumSet<Flag> flags = Flag.deserialize(iflags);

                short[] partitionKeyBindIndexes = null;
                if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
                {
                    int numPKNames = body.readInt();
                    if (numPKNames > 0)
                    {
                        partitionKeyBindIndexes = new short[numPKNames];
                        for (int i = 0; i < numPKNames; i++)
                            partitionKeyBindIndexes[i] = body.readShort();
                    }
                }

                boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

                String globalKsName = null;
                String globalCfName = null;
                if (globalTablesSpec)
                {
                    globalKsName = CBUtil.readString(body);
                    globalCfName = CBUtil.readString(body);
                }

                // metadata (names/types)
                List<ColumnSpecification> names = new ArrayList<>(columnCount);
                for (int i = 0; i < columnCount; i++)
                {
                    String ksName = globalTablesSpec ? globalKsName : CBUtil.readString(body);
                    String cfName = globalTablesSpec ? globalCfName : CBUtil.readString(body);
                    ColumnIdentifier colName = new ColumnIdentifier(CBUtil.readString(body), true);
                    AbstractType type = DataType.toType(DataType.codec.decodeOne(body, version));
                    names.add(new ColumnSpecification(ksName, cfName, colName, type));
                }
                return new PreparedMetadata(flags, names, partitionKeyBindIndexes);
            }

            public void encode(PreparedMetadata m, ByteBuf dest, ProtocolVersion version)
            {
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                dest.writeInt(Flag.serialize(m.flags));
                dest.writeInt(m.names.size());

                if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
                {
                    // there's no point in providing partition key bind indexes if the statements affect multiple tables
                    if (m.partitionKeyBindIndexes == null || !globalTablesSpec)
                    {
                        dest.writeInt(0);
                    }
                    else
                    {
                        dest.writeInt(m.partitionKeyBindIndexes.length);
                        for (Short bindIndex : m.partitionKeyBindIndexes)
                            dest.writeShort(bindIndex);
                    }
                }

                if (globalTablesSpec)
                {
                    CBUtil.writeAsciiString(m.names.get(0).ksName, dest);
                    CBUtil.writeAsciiString(m.names.get(0).cfName, dest);
                }

                for (ColumnSpecification name : m.names)
                {
                    if (!globalTablesSpec)
                    {
                        CBUtil.writeAsciiString(name.ksName, dest);
                        CBUtil.writeAsciiString(name.cfName, dest);
                    }
                    CBUtil.writeString(name.name.toString(), dest);
                    DataType.codec.writeOne(DataType.fromType(name.type, version), dest, version);
                }
            }

            public int encodedSize(PreparedMetadata m, ProtocolVersion version)
            {
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);
                int size = 8;
                if (globalTablesSpec)
                {
                    size += CBUtil.sizeOfAsciiString(m.names.get(0).ksName);
                    size += CBUtil.sizeOfAsciiString(m.names.get(0).cfName);
                }

                if (m.partitionKeyBindIndexes != null && version.isGreaterOrEqualTo(ProtocolVersion.V4))
                    size += 4 + 2 * m.partitionKeyBindIndexes.length;

                for (ColumnSpecification name : m.names)
                {
                    if (!globalTablesSpec)
                    {
                        size += CBUtil.sizeOfAsciiString(name.ksName);
                        size += CBUtil.sizeOfAsciiString(name.cfName);
                    }
                    size += CBUtil.sizeOfString(name.name.toString());
                    size += DataType.codec.oneSerializedSize(DataType.fromType(name.type, version), version);
                }
                return size;
            }
        }
    }

    public enum Flag
    {
        // The order of that enum matters!!
        GLOBAL_TABLES_SPEC,
        HAS_MORE_PAGES,
        NO_METADATA,
        METADATA_CHANGED;

        public static EnumSet<Flag> deserialize(int flags)
        {
            EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
            Flag[] values = Flag.values();
            for (int n = 0; n < values.length; n++)
            {
                if ((flags & (1 << n)) != 0)
                    set.add(values[n]);
            }
            return set;
        }

        public static int serialize(EnumSet<Flag> flags)
        {
            int i = 0;
            for (Flag flag : flags)
                i |= 1 << flag.ordinal();
            return i;
        }
    }

    static MD5Digest computeResultMetadataId(List<ColumnSpecification> columnSpecifications)
    {
        // still using the MD5 MessageDigest thread local here instead of a HashingUtils/Guava
        // Hasher, as ResultSet will need to be changed alongside other usages of MD5
        // in the native transport/protocol and it seems to make more sense to do that
        // then than as part of the Guava Hasher refactor which is focused on non-client
        // protocol digests
        MessageDigest md = MD5Digest.threadLocalMD5Digest();

        if (columnSpecifications != null)
        {
            for (ColumnSpecification cs : columnSpecifications)
            {
                md.update(cs.name.bytes.duplicate());
                md.update((byte) 0);
                md.update(cs.type.toString().getBytes(StandardCharsets.UTF_8));
                md.update((byte) 0);
                md.update((byte) 0);
            }
        }

        return MD5Digest.wrap(md.digest());
    }
}
