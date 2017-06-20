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

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.ProtocolVersion;

public final class SimpleSelector extends Selector
{
    /**
     * The Factory for {@code SimpleSelector}.
     */
    public static final class SimpleSelectorFactory extends Factory
    {
        private final int idx;

        private final ColumnMetadata column;

        private SimpleSelectorFactory(int idx, ColumnMetadata def)
        {
            this.idx = idx;
            this.column = def;
        }

        @Override
        protected String getColumnName()
        {
            return column.name.toString();
        }

        @Override
        protected AbstractType<?> getReturnType()
        {
            return column.type;
        }

        protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultColumn)
        {
           mapping.addMapping(resultColumn, column);
        }

        @Override
        public Selector newInstance(QueryOptions options)
        {
            return new SimpleSelector(column, idx);
        }

        @Override
        public boolean isSimpleSelectorFactory()
        {
            return true;
        }

        @Override
        public boolean isSimpleSelectorFactoryFor(int index)
        {
            return index == idx;
        }

        public boolean areAllFetchedColumnsKnown()
        {
            return true;
        }

        public void addFetchedColumns(ColumnFilter.Builder builder)
        {
            builder.add(column);
        }

        public ColumnMetadata getColumn()
        {
            return column;
        }
    }

    public final ColumnMetadata column;
    private final int idx;
    private ByteBuffer current;
    private boolean isSet;

    public static Factory newFactory(final ColumnMetadata def, final int idx)
    {
        return new SimpleSelectorFactory(idx, def);
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        builder.add(column);
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
        return column.type;
    }

    @Override
    public String toString()
    {
        return column.name.toString();
    }

    private SimpleSelector(ColumnMetadata column, int idx)
    {
        this.column = column;
        this.idx = idx;
    }
}
