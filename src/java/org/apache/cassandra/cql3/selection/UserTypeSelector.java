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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Objects;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * <code>Selector</code> for literal map (e.g. {'min' : min(value), 'max' : max(value), 'count' : count(value)}).
 *
 */
final class UserTypeSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            UserType type = (UserType) readType(metadata, in);
            int size = in.readUnsignedVInt32();
            Map<FieldIdentifier, Selector> fields = new HashMap<>(size);
            for (int i = 0; i < size; i++)
            {
                FieldIdentifier identifier = new FieldIdentifier(ByteBufferUtil.readWithVIntLength(in));
                Selector selector = serializer.deserialize(in, version, metadata);
                fields.put(identifier, selector);
            }
            return new UserTypeSelector(type, fields);
        }
    };

    /**
     * The map type.
     */
    private final AbstractType<?> type;

    /**
     * The user type fields
     */
    private final Map<FieldIdentifier, Selector> fields;

    public static Factory newFactory(final AbstractType<?> type, final Map<FieldIdentifier, Factory> factories)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return UserTypes.userTypeToString(factories, Factory::getColumnName);
            }

            protected AbstractType<?> getReturnType()
            {
                return type;
            }

            protected final void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                SelectionColumnMapping tmpMapping = SelectionColumnMapping.newMapping();
                for (Factory factory : factories.values())
                {
                    factory.addColumnMapping(tmpMapping, resultsColumn);
                }

                if (tmpMapping.getMappings().get(resultsColumn).isEmpty())
                    // add a null mapping for cases where the collection is empty
                    mapping.addMapping(resultsColumn, (ColumnMetadata)null);
                else
                    // collate the mapped columns from the child factories & add those
                    mapping.addMapping(resultsColumn, tmpMapping.getMappings().values());
            }

            public Selector newInstance(final QueryOptions options)
            {
                Map<FieldIdentifier, Selector> fields = new HashMap<>(factories.size());
                for (Entry<FieldIdentifier, Factory> factory : factories.entrySet())
                    fields.put(factory.getKey(), factory.getValue().newInstance(options));

                return new UserTypeSelector(type, fields);
            }

            @Override
            public boolean isAggregateSelectorFactory()
            {
                for (Factory factory : factories.values())
                {
                    if (factory.isAggregateSelectorFactory())
                        return true;
                }
                return false;
            }

            @Override
            public void addFunctionsTo(List<Function> functions)
            {
                for (Factory factory : factories.values())
                    factory.addFunctionsTo(functions);
            }

            @Override
            public boolean isWritetimeSelectorFactory()
            {
                for (Factory factory : factories.values())
                {
                    if (factory.isWritetimeSelectorFactory())
                        return true;
                }
                return false;
            }

            @Override
            public boolean isTTLSelectorFactory()
            {
                for (Factory factory : factories.values())
                {
                    if (factory.isTTLSelectorFactory())
                        return true;
                }
                return false;
            }

            @Override
            boolean areAllFetchedColumnsKnown()
            {
                for (Factory factory : factories.values())
                {
                    if (!factory.areAllFetchedColumnsKnown())
                        return false;
                }
                return true;
            }

            @Override
            void addFetchedColumns(Builder builder)
            {
                for (Factory factory : factories.values())
                    factory.addFetchedColumns(builder);
            }
        };
    }

    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        for (Selector field : fields.values())
            field.addFetchedColumns(builder);
    }

    public void addInput(InputRow input)
    {
        for (Selector field : fields.values())
            field.addInput(input);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        UserType userType = (UserType) type;
        ByteBuffer[] buffers = new ByteBuffer[userType.size()];
        for (int i = 0, m = userType.size(); i < m; i++)
        {
            Selector selector = fields.get(userType.fieldName(i));
            if (selector != null)
                buffers[i] = selector.getOutput(protocolVersion);
        }
        return TupleType.buildValue(buffers);
    }

    public void reset()
    {
        for (Selector field : fields.values())
            field.reset();
    }

    @Override
    public boolean isTerminal()
    {
        for (Selector field : fields.values())
        {
            if(!field.isTerminal())
                return false;
        }
        return true;
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return UserTypes.userTypeToString(fields);
    }

    private UserTypeSelector(AbstractType<?> type, Map<FieldIdentifier, Selector> fields)
    {
        super(Kind.USER_TYPE_SELECTOR);
        this.type = type;
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof UserTypeSelector))
            return false;

        UserTypeSelector s = (UserTypeSelector) o;

        return Objects.equal(type, s.type)
            && Objects.equal(fields, s.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, fields);
    }

    @Override
    protected int serializedSize(int version)
    {
        int size = sizeOf(type) + TypeSizes.sizeofUnsignedVInt(fields.size());

        for (Map.Entry<FieldIdentifier, Selector> field : fields.entrySet())
            size += ByteBufferUtil.serializedSizeWithVIntLength(field.getKey().bytes) + serializer.serializedSize(field.getValue(), version);

        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, type);
        out.writeUnsignedVInt32(fields.size());

        for (Map.Entry<FieldIdentifier, Selector> field : fields.entrySet())
        {
            ByteBufferUtil.writeWithVIntLength(field.getKey().bytes, out);
            serializer.serialize(field.getValue(), out, version);
        }
    }
}
