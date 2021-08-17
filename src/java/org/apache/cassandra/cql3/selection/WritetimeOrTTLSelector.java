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
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

final class WritetimeOrTTLSelector extends Selector
{
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

    public void addInput(ProtocolVersion protocolVersion, ResultSetBuilder rs)
    {
        if (isSet)
            return;

        isSet = true;

        if (isWritetime)
        {
            long ts = rs.timestamps[idx];
            current = ts != Long.MIN_VALUE ? ByteBufferUtil.bytes(ts) : null;
        }
        else
        {
            int ttl = rs.ttls[idx];
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
        this.column = column;
        this.idx = idx;
        this.isWritetime = isWritetime;
    }
}
