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
import java.util.*;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.transport.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlMetadata;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlResultType;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ResultSet
{
    public static final Codec codec = new Codec();
    private static final ColumnIdentifier COUNT_COLUMN = new ColumnIdentifier("count", false);

    public final Metadata metadata;
    public final List<List<ByteBuffer>> rows;

    public ResultSet(List<ColumnSpecification> metadata)
    {
        this(new Metadata(metadata), new ArrayList<List<ByteBuffer>>());
    }

    private ResultSet(Metadata metadata, List<List<ByteBuffer>> rows)
    {
        this.metadata = metadata;
        this.rows = rows;
    }

    public int size()
    {
        return rows.size();
    }

    public void addColumnValue(ByteBuffer value)
    {
        if (rows.isEmpty() || lastRow().size() == metadata.names.size())
            rows.add(new ArrayList<ByteBuffer>(metadata.names.size()));

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

    public ResultSet makeCountResult()
    {
        String ksName = metadata.names.get(0).ksName;
        String cfName = metadata.names.get(0).cfName;
        metadata.names.clear();
        metadata.names.add(new ColumnSpecification(ksName, cfName, COUNT_COLUMN, LongType.instance));

        long count = rows.size();
        rows.clear();
        rows.add(Collections.singletonList(ByteBufferUtil.bytes(count)));
        return this;
    }

    public CqlResult toThriftResult()
    {
        String UTF8 = "UTF8Type";
        CqlMetadata schema = new CqlMetadata(new HashMap<ByteBuffer, String>(),
                new HashMap<ByteBuffer, String>(),
                // The 2 following ones shouldn't be needed in CQL3
                UTF8, UTF8);

        for (ColumnSpecification name : metadata.names)
        {
            ByteBuffer colName = ByteBufferUtil.bytes(name.toString());
            schema.name_types.put(colName, UTF8);
            schema.value_types.put(colName, TypeParser.getShortName(name.type));
        }

        List<CqlRow> cqlRows = new ArrayList<CqlRow>(rows.size());
        for (List<ByteBuffer> row : rows)
        {
            List<Column> thriftCols = new ArrayList<Column>(metadata.names.size());
            for (int i = 0; i < metadata.names.size(); i++)
            {
                Column col = new Column(ByteBufferUtil.bytes(metadata.names.get(i).toString()));
                col.setValue(row.get(i));
                thriftCols.add(col);
            }
            // The key of CqlRow shoudn't be needed in CQL3
            cqlRows.add(new CqlRow(ByteBufferUtil.EMPTY_BYTE_BUFFER, thriftCols));
        }
        CqlResult res = new CqlResult(CqlResultType.ROWS);
        res.setRows(cqlRows).setSchema(schema);
        return res;
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
                        sb.append(" | null");
                    else
                        sb.append(" | ").append(metadata.names.get(i).type.getString(v));
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
        public ResultSet decode(ChannelBuffer body)
        {
            Metadata m = Metadata.codec.decode(body);
            int rowCount = body.readInt();
            ResultSet rs = new ResultSet(m, new ArrayList<List<ByteBuffer>>(rowCount));

            // rows
            int totalValues = rowCount * m.names.size();
            for (int i = 0; i < totalValues; i++)
                rs.addColumnValue(CBUtil.readValue(body));

            return rs;
        }

        public ChannelBuffer encode(ResultSet rs)
        {
            CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(2, 0, rs.metadata.names.size() * rs.rows.size());
            builder.add(Metadata.codec.encode(rs.metadata));
            builder.add(CBUtil.intToCB(rs.rows.size()));

            for (List<ByteBuffer> row : rs.rows)
            {
                for (ByteBuffer bb : row)
                    builder.addValue(bb);
            }

            return builder.build();
        }
    }

    public static class Metadata
    {
        private static OptionCodec<DataType> dataTypeCodec = new OptionCodec<DataType>(DataType.class);
        public static final CBCodec<Metadata> codec = new Codec();

        public final EnumSet<Flag> flags;
        public final List<ColumnSpecification> names;

        public Metadata(List<ColumnSpecification> names)
        {
            this(EnumSet.noneOf(Flag.class), names);
            if (allInSameCF())
                flags.add(Flag.GLOBAL_TABLES_SPEC);
        }

        private Metadata(EnumSet<Flag> flags, List<ColumnSpecification> names)
        {
            this.flags = flags;
            this.names = names;
        }

        private boolean allInSameCF()
        {
            assert !names.isEmpty();

            Iterator<ColumnSpecification> iter = names.iterator();
            ColumnSpecification first = iter.next();
            while (iter.hasNext())
            {
                ColumnSpecification name = iter.next();
                if (!name.ksName.equals(first.ksName) || !name.cfName.equals(first.cfName))
                    return false;
            }
            return true;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            for (ColumnSpecification name : names)
            {
                sb.append("[").append(name.toString());
                sb.append("(").append(name.ksName).append(", ").append(name.cfName).append(")");
                sb.append(", ").append(name.type).append("]");
            }
            return sb.toString();
        }

        private static class Codec implements CBCodec<Metadata>
        {
            public Metadata decode(ChannelBuffer body)
            {
                // flags & column count
                int iflags = body.readInt();
                int columnCount = body.readInt();

                EnumSet<Flag> flags = Flag.deserialize(iflags);
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
                    AbstractType type = DataType.toType(dataTypeCodec.decodeOne(body));
                    names.add(new ColumnSpecification(ksName, cfName, colName, type));
                }
                return new Metadata(flags, names);
            }

            public ChannelBuffer encode(Metadata m)
            {
                boolean globalTablesSpec = m.flags.contains(Flag.GLOBAL_TABLES_SPEC);

                int stringCount = globalTablesSpec ? 2 + m.names.size() : 3* m.names.size();
                CBUtil.BufferBuilder builder = new CBUtil.BufferBuilder(1 + m.names.size(), stringCount, 0);

                ChannelBuffer header = ChannelBuffers.buffer(8);
                header.writeInt(Flag.serialize(m.flags));
                header.writeInt(m.names.size());
                builder.add(header);

                if (globalTablesSpec)
                {
                    builder.addString(m.names.get(0).ksName);
                    builder.addString(m.names.get(0).cfName);
                }

                for (ColumnSpecification name : m.names)
                {
                    if (!globalTablesSpec)
                    {
                        builder.addString(name.ksName);
                        builder.addString(name.cfName);
                    }
                    builder.addString(name.toString());
                    builder.add(dataTypeCodec.encodeOne(DataType.fromType(name.type)));
                }
                return builder.build();
            }
        }
    }

    public static enum Flag
    {
        // The order of that enum matters!!
        GLOBAL_TABLES_SPEC;

        public static EnumSet<Flag> deserialize(int flags)
        {
            EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
            Flag[] values = Flag.values();
            for (int n = 0; n < 32; n++)
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
}
