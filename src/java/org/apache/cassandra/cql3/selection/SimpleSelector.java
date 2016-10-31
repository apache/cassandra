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

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.selection.Selection.ResultSetBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public final class SimpleSelector extends Selector
{
    private final String columnName;
    private final int idx;
    private final AbstractType<?> type;
    private ByteBuffer current;
    private boolean isSet;

    public static Factory newFactory(final ColumnDefinition def, final int idx)
    {
        return new Factory()
        {
            @Override
            protected String getColumnName()
            {
                return def.name.toString();
            }

            @Override
            protected AbstractType<?> getReturnType()
            {
                return def.type;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn)
            {
               mapping.addMapping(resultColumn, def);
            }

            @Override
            public Selector newInstance(QueryOptions options)
            {
                return new SimpleSelector(def.name.toString(), idx, def.type);
            }

            @Override
            public boolean isSimpleSelectorFactory(int index)
            {
                return index == idx;
            }
        };
    }

    @Override
    public void addInput(ProtocolVersion protocolVersion, ResultSetBuilder rs) throws InvalidRequestException
    {
        if (!isSet)
        {
            isSet = true;
            current = rs.current.get(idx);
        }
    }

    @Override
    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        return current;
    }

    @Override
    public void reset()
    {
        isSet = false;
        current = null;
    }

    @Override
    public AbstractType<?> getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return columnName;
    }

    private SimpleSelector(String columnName, int idx, AbstractType<?> type)
    {
        this.columnName = columnName;
        this.idx = idx;
        this.type = type;
    }
}
