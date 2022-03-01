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

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class MaxWritetimeSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            ByteBuffer columnName = ByteBufferUtil.readWithVIntLength(in);
            ColumnMetadata column = metadata.getColumn(columnName);
            int idx = in.readInt();
            return new MaxWritetimeSelector(column, idx);
        }
    };

    private final ColumnMetadata column;
    private final int idx;
    private ByteBuffer current;
    private boolean isSet;

    public static Factory newFactory(final ColumnMetadata def, final int idx)
    {
        return new Factory()
        {
            protected String getColumnName()
            {
                return String.format("maxwritetime(%s)", def.name.toString());
            }

            protected AbstractType<?> getReturnType()
            {
                return LongType.instance;
            }

            protected void addColumnMapping(SelectionColumnMapping mapping, ColumnSpecification resultsColumn)
            {
                mapping.addMapping(resultsColumn, def);
            }

            public Selector newInstance(QueryOptions options)
            {
                return new MaxWritetimeSelector(def, idx);
            }

            public boolean areAllFetchedColumnsKnown()
            {
                return true;
            }

            public void addFetchedColumns(ColumnFilter.Builder builder)
            {
                builder.add(def);
            }

            @Override
            public boolean isMaxWritetimeSelectorFactory() {
                return true;
            }
        };
    }

    private MaxWritetimeSelector(ColumnMetadata column, int idx)
    {
        super(Kind.MAX_WRITETIME_SELECTOR);
        this.column = column;
        this.idx = idx;
    }

    @Override
    public void addFetchedColumns(ColumnFilter.Builder builder)
    {
        builder.add(column);
    }

    @Override
    public void addInput(ProtocolVersion protocolVersion, InputRow input) {
        if (isSet)
            return;

        isSet = true;
        long ts = input.getTimestamp(idx);
        current = ts != Long.MIN_VALUE ? ByteBufferUtil.bytes(ts) : null;
    }

    @Override
    public ByteBuffer getOutput(ProtocolVersion protocolVersion) throws InvalidRequestException
    {
        return current;
    }

    @Override
    public AbstractType<?> getType()
    {
        return LongType.instance;
    }

    @Override
    public void reset()
    {
        isSet = false;
        current = null;
    }

    @Override
    protected int serializedSize(int version)
    {
        return ByteBufferUtil.serializedSizeWithVIntLength(column.name.bytes)
               + TypeSizes.sizeof(idx);
    }

    @Override
    protected void serialize(DataOutputPlus out, int version) throws IOException
    {
        ByteBufferUtil.writeWithVIntLength(column.name.bytes, out);
        out.writeInt(idx);
    }
}
