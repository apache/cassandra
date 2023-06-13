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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

public class VectorSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        @Override
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            VectorType<?> type = (VectorType<?>) readType(metadata, in);
            List<Selector> elements = new ArrayList<>(type.dimension);
            for (int i = 0; i < type.dimension; i++)
                elements.add(serializer.deserialize(in, version, metadata));

            return new VectorSelector(type, elements);
        }
    };

    /**
     * The vector type.
     */
    private final VectorType<?> type;

    /**
     * The list elements
     */
    private final List<Selector> elements;

    private VectorSelector(VectorType<?> type, List<Selector> elements)
    {
        super(Kind.VECTOR_SELECTOR);
        Preconditions.checkArgument(elements.size() == type.dimension,
                                    "Unable to create a vector select of type %s from %s elements",
                                    type.asCQL3Type(),
                                    elements.size());
        this.type = type;
        this.elements = elements;
    }

    public static Factory newFactory(final AbstractType<?> type, final SelectorFactories factories)
    {
        assert type.isVector() : String.format("Unable to create vector selector from typs %s", type.asCQL3Type());
        VectorType<?> vt = (VectorType<?>) type;
        return new MultiElementFactory(type, factories)
        {
            protected String getColumnName()
            {
                return Lists.listToString(factories, Factory::getColumnName);
            }

            public Selector newInstance(final QueryOptions options)
            {
                return new VectorSelector(vt, factories.newInstances(options));
            }
        };
    }

    @Override
    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addFetchedColumns(builder);
    }

    @Override
    public void addInput(InputRow input)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addInput(input);
    }

    @Override
    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public void reset()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).reset();
    }

    @Override
    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = new ArrayList<>(elements.size());
        for (int i = 0, m = elements.size(); i < m; i++)
            buffers.add(elements.get(i).getOutput(protocolVersion));

        return type.decomposeRaw(buffers);
    }

    @Override
    protected int serializedSize(int version)
    {
        int size = sizeOf(type);
        for (int i = 0, m = elements.size(); i < m; i++)
            size += serializer.serializedSize(elements.get(i), version);

        return size;
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        writeType(out, type);
        for (int i = 0, m = elements.size(); i < m; i++)
            serializer.serialize(elements.get(i), out, version);
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

    @Override
    public String toString()
    {
        return Lists.listToString(elements);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorSelector that = (VectorSelector) o;
        return type.equals(that.type) && elements.equals(that.elements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, elements);
    }
}
