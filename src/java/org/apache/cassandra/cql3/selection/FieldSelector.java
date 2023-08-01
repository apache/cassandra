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

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

final class FieldSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            UserType type = (UserType) readType(metadata, in);
            int field = in.readUnsignedVInt32();
            Selector selected = Selector.serializer.deserialize(in, version, metadata);

            return new FieldSelector(type, field, selected);
        }
    };

    private final UserType type;
    private final int field;
    private final Selector selected;

    public static Factory newFactory(final UserType type, final int field, final Selector.Factory factory)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("%s.%s", factory.getColumnName(), type.fieldName(field));
            }

            protected AbstractType<?> getReturnType()
            {
                return type.fieldType(field);
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                factory.addColumnMapping(mapping, resultsColumn);
            }

            public Selector newInstance(QueryOptions options) throws InvalidRequestException
            {
                return new FieldSelector(type, field, factory.newInstance(options));
            }

            public boolean isAggregateSelectorFactory()
            {
                return factory.isAggregateSelectorFactory();
            }

            public boolean areAllFetchedColumnsKnown()
            {
                return factory.areAllFetchedColumnsKnown();
            }

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
        selected.addInput(input);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        ByteBuffer value = selected.getOutput(protocolVersion);
        if (value == null)
            return null;
        ByteBuffer[] buffers = type.split(ByteBufferAccessor.instance, value);
        return field < buffers.length ? buffers[field] : null;
    }

    @Override
    protected ColumnTimestamps getWritetimes(ProtocolVersion protocolVersion)
    {
        return getOutput(protocolVersion) == null
               ? ColumnTimestamps.NO_TIMESTAMP
               : selected.getWritetimes(protocolVersion).get(field);
    }

    @Override
    protected ColumnTimestamps getTTLs(ProtocolVersion protocolVersion)
    {
        return getOutput(protocolVersion) == null
               ? ColumnTimestamps.NO_TIMESTAMP
               : selected.getTTLs(protocolVersion).get(field);
    }

    public AbstractType<?> getType()
    {
        return type.fieldType(field);
    }

    public void reset()
    {
        selected.reset();
    }

    @Override
    public boolean isTerminal()
    {
        return selected.isTerminal();
    }

    @Override
    public String toString()
    {
        return String.format("%s.%s", selected, type.fieldName(field));
    }

    private FieldSelector(UserType type, int field, Selector selected)
    {
        super(Kind.FIELD_SELECTOR);
        this.type = type;
        this.field = field;
        this.selected = selected;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof FieldSelector))
            return false;

        FieldSelector s = (FieldSelector) o;

        return Objects.equal(type, s.type)
            && Objects.equal(field, s.field)
            && Objects.equal(selected, s.selected);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, field, selected);
    }

    @Override
    protected int serializedSize(int version)
    {
        return sizeOf(type) + TypeSizes.sizeofUnsignedVInt(field) + serializer.serializedSize(selected, version);
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, type);
        out.writeUnsignedVInt32(field);
        serializer.serialize(selected, out, version);
    }
}
