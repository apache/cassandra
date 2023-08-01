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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * <code>Selector</code> for literal set (e.g. {min(value), max(value), count(value)}).
 *
 */
final class SetSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            SetType<?> type = (SetType<?>) readType(metadata, in);
            int size = in.readUnsignedVInt32();
            List<Selector> elements = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                elements.add(serializer.deserialize(in, version, metadata));

            return new SetSelector(type, elements);
        }
    };

    /**
     * The set type.
     */
    private final SetType<?> type;

    /**
     * The set elements
     */
    private final List<Selector> elements;

    public static Factory newFactory(final AbstractType<?> type, final SelectorFactories factories)
    {
        return new MultiElementFactory(type, factories)
        {
            protected String getColumnName()
            {
                return Sets.setToString(factories, Factory::getColumnName);
            }

            public Selector newInstance(final QueryOptions options)
            {
                return new SetSelector(type, factories.newInstances(options));
            }
        };
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addFetchedColumns(builder);
    }

    public void addInput(InputRow input)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addInput(input);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        Set<ByteBuffer> buffers = new TreeSet<>(type.getElementsType());
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            buffers.add(elements.get(i).getOutput(protocolVersion));
        }
        return CollectionSerializer.pack(buffers, buffers.size());
    }

    public void reset()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).reset();
    }

    @Override
    public boolean isTerminal()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            if (!elements.get(i).isTerminal())
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
        return Sets.setToString(elements);
    }

    private SetSelector(AbstractType<?> type, List<Selector> elements)
    {
        super(Kind.SET_SELECTOR);
        this.type = (SetType<?>) type;
        this.elements = elements;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof SetSelector))
            return false;

        SetSelector s = (SetSelector) o;

        return Objects.equal(type, s.type)
            && Objects.equal(elements, s.elements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(type, elements);
    }

    @Override
    protected int serializedSize(int version)
    {
        int size = sizeOf(type) + TypeSizes.sizeofUnsignedVInt(elements.size());

        for (int i = 0, m = elements.size(); i < m; i++)
            size += serializer.serializedSize(elements.get(i), version);

        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, type);
        out.writeUnsignedVInt32(elements.size());
        for (int i = 0, m = elements.size(); i < m; i++)
            serializer.serialize(elements.get(i), out, version);
    }
}
