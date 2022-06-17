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

import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;

final class WritetimeOrTTLSelector extends Selector
{
    static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        @Override
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            Selector selected = serializer.deserialize(in, version, metadata);
            int idx = in.readInt();
            int ordinal = in.readByte();
            Selectable.WritetimeOrTTL.Kind kind = Selectable.WritetimeOrTTL.Kind.fromOrdinal(ordinal);
            boolean isMultiCell = in.readBoolean();
            return new WritetimeOrTTLSelector(selected, idx, kind, isMultiCell);
        }
    };

    private final Selector selected;
    private final int columnIndex;
    private final Selectable.WritetimeOrTTL.Kind kind;
    private ByteBuffer current;
    private final boolean isMultiCell;
    private boolean isSet;

    public static Factory newFactory(final Selector.Factory factory, final int columnIndex, final Selectable.WritetimeOrTTL.Kind kind, boolean isMultiCell)
    {
        return new Factory()
        {
            @Override
            protected String getColumnName()
            {
                return String.format("%s(%s)", kind.name, factory.getColumnName());
            }

            @Override
            protected AbstractType<?> getReturnType()
            {
                AbstractType<?> type = kind.returnType;
                return isMultiCell && !kind.aggregatesMultiCell() ? ListType.getInstance(type, false) : type;
            }

            @Override
            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                factory.addColumnMapping(mapping, resultsColumn);
            }

            @Override
            public Selector newInstance(QueryOptions options)
            {
                return new WritetimeOrTTLSelector(factory.newInstance(options), columnIndex, kind, isMultiCell);
            }

            @Override
            public boolean isWritetimeSelectorFactory()
            {
                return kind != Selectable.WritetimeOrTTL.Kind.TTL;
            }

            @Override
            public boolean isTTLSelectorFactory()
            {
                return kind == Selectable.WritetimeOrTTL.Kind.TTL;
            }

            @Override
            public boolean isMaxWritetimeSelectorFactory()
            {
                return kind == Selectable.WritetimeOrTTL.Kind.MAX_WRITE_TIME;
            }

            @Override
            public boolean areAllFetchedColumnsKnown()
            {
                return true;
            }

            @Override
            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                factory.addFetchedColumns(builder);
            }
        };
    }

    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        selected.addFetchedColumns(builder);
    }

    public void addInput(InputRow input)
    {
        if (isSet)
            return;

        isSet = true;

        selected.addInput(input);
        ProtocolVersion protocolVersion = input.getProtocolVersion();

        switch (kind)
        {
            case WRITE_TIME:
                current = selected.getWritetimes(protocolVersion).toByteBuffer(protocolVersion);
                break;
            case MAX_WRITE_TIME:
                current = selected.getWritetimes(protocolVersion).max().toByteBuffer(protocolVersion);
                break;
            case TTL:
                current = selected.getTTLs(protocolVersion).toByteBuffer(protocolVersion);
                break;
        }
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return current;
    }

    public void reset()
    {
        selected.reset();
        isSet = false;
        current = null;
    }

    public AbstractType<?> getType()
    {
        AbstractType<?> type = kind.returnType;
        return isMultiCell ? ListType.getInstance(type, false) : type;
    }

    @Override
    public String toString()
    {
        return selected.toString();
    }

    private WritetimeOrTTLSelector(Selector selected, int idx, Selectable.WritetimeOrTTL.Kind kind, boolean isMultiCell)
    {
        super(Kind.WRITETIME_OR_TTL_SELECTOR);
        this.selected = selected;
        this.columnIndex = idx;
        this.kind = kind;
        this.isMultiCell = isMultiCell;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof WritetimeOrTTLSelector))
            return false;

        WritetimeOrTTLSelector s = (WritetimeOrTTLSelector) o;

        return Objects.equal(selected, s.selected) && kind == s.kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(selected, kind);
    }

    @Override
    protected int serializedSize(int version)
    {
        return serializer.serializedSize(selected, version)
                + TypeSizes.sizeof(columnIndex)
                + TypeSizes.sizeofUnsignedVInt(kind.ordinal())
                + TypeSizes.sizeof(isMultiCell);
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        serializer.serialize(selected, out, version);
        out.writeInt(columnIndex);
        out.writeByte(kind.ordinal());
        out.writeBoolean(isMultiCell);
    }
}
