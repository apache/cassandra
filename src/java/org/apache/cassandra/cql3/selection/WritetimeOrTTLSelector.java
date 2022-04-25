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
package org.apache.cassandra.cql3.selection;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.base.Objects;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

final class WritetimeOrTTLSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            ByteBuffer columnName = ByteBufferUtil.readWithVIntLength(in);
            ColumnMetadata column = metadata.getColumn(columnName);
            int idx = in.readInt();
            boolean isWritetime = in.readBoolean();
            return new WritetimeOrTTLSelector(column, idx, isWritetime);
        }
    };

    private final ColumnMetadata column;
    private final int idx;
    private final boolean isWritetime;
    private ByteBuffer current;
    private boolean isSet;

    public static Factory newFactory(final ColumnMetadata def, final int idx, final boolean isWritetime)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("%s(%s)", isWritetime ? "writetime" : "ttl", def.name.toString());
            }

            protected AbstractType<?> getReturnType()
            {
                return isWritetime ? LongType.instance : Int32Type.instance;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
               mapping.addMapping(resultsColumn, def);
            }

            public Selector newInstance(QueryOptions options)
            {
                return new WritetimeOrTTLSelector(def, idx, isWritetime);
            }

            public boolean isWritetimeSelectorFactory()
            {
                return isWritetime;
            }

            public boolean isTTLSelectorFactory()
            {
                return !isWritetime;
            }

            public boolean areAllFetchedColumnsKnown()
            {
                return true;
            }

            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                builder.add(def);
            }
        };
    }

    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        builder.add(column);
    }

    public void addInput(ProtocolVersion protocolVersion, InputRow input)
    {
        if (isSet)
            return;

        isSet = true;

        if (isWritetime)
        {
            long ts = input.getTimestamp(idx);
            current = ts != Long.MIN_VALUE ? ByteBufferUtil.bytes(ts) : null;
        }
        else
        {
            int ttl = input.getTtl(idx);
            current = ttl > 0 ? ByteBufferUtil.bytes(ttl) : null;
        }
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return current;
    }

    public void reset()
    {
        isSet = false;
        current = null;
    }

    public AbstractType<?> getType()
    {
        return isWritetime ? LongType.instance : Int32Type.instance;
    }

    @Override
    public String toString()
    {
        return column.name.toString();
    }

    private WritetimeOrTTLSelector(ColumnMetadata column, int idx, boolean isWritetime)
    {
        super(Kind.WRITETIME_OR_TTL_SELECTOR);
        this.column = column;
        this.idx = idx;
        this.isWritetime = isWritetime;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WritetimeOrTTLSelector))
            return false;

        WritetimeOrTTLSelector s = (WritetimeOrTTLSelector) o;

        return Objects.equal(column, s.column)
            && Objects.equal(idx, s.idx)
            && Objects.equal(isWritetime, s.isWritetime);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, idx, isWritetime);
    }

    @Override
    protected int serializedSize(int version)
    {
        return ByteBufferUtil.serializedSizeWithVIntLength(column.name.bytes)
                + TypeSizes.sizeof(idx)
                + TypeSizes.sizeof(isWritetime);
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        ByteBufferUtil.writeWithVIntLength(column.name.bytes, out);
        out.writeInt(idx);
        out.writeBoolean(isWritetime);
    }
}
