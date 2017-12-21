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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * <code>Selector</code> for literal list (e.g. [min(value), max(value), count(value)]).
 *
 */
final class ListSelector extends Selector
{
    /**
     * The list type.
     */
    private final AbstractType<?> type;

    /**
     * The list elements
     */
    private final List<Selector> elements;

    public static Factory newFactory(final AbstractType<?> type, final SelectorFactories factories)
    {
        return new CollectionFactory(type, factories)
        {
            protected String getColumnName()
            {
                return Lists.listToString(factories, Factory::getColumnName);
            }

            public Selector newInstance(final QueryOptions options)
            {
                return new ListSelector(type, factories.newInstances(options));
            }
        };
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addFetchedColumns(builder);
    }

    public void addInput(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).addInput(protocolVersion, rs);
    }

    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        List<ByteBuffer> buffers = new ArrayList<>(elements.size());
        for (int i = 0, m = elements.size(); i < m; i++)
        {
            buffers.add(elements.get(i).getOutput(protocolVersion));
        }
        return CollectionSerializer.pack(buffers, buffers.size(), protocolVersion);
    }

    public void reset()
    {
        for (int i = 0, m = elements.size(); i < m; i++)
            elements.get(i).reset();
    }

    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return Lists.listToString(elements);
    }

    private ListSelector(AbstractType<?> type, List<Selector> elements)
    {
        this.type = type;
        this.elements = elements;
    }
}
