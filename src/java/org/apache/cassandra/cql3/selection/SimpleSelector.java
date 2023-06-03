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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.functions.masking.ColumnMask;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.ColumnFilter.Builder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class SimpleSelector extends Selector
{
    protected static final SelectorDeserializer deserializer = new SelectorDeserializer()
    {
        protected Selector deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            ByteBuffer columnName = ByteBufferUtil.readWithVIntLength(in);
            ColumnMetadata column = metadata.getColumn(columnName);
            int idx = in.readInt();
            return new SimpleSelector(column, idx, false, ProtocolVersion.CURRENT);
        }
    };

    /**
     * The Factory for {@code SimpleSelector}.
     */
    public static final class SimpleSelectorFactory extends Factory
    {
        private final int idx;
        private final ColumnMetadata column;
        private final boolean useForPostOrdering;

        private SimpleSelectorFactory(int idx, ColumnMetadata def, boolean useForPostOrdering)
        {
            this.idx = idx;
            this.column = def;
            this.useForPostOrdering = useForPostOrdering;
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
            return new SimpleSelector(column, idx, useForPostOrdering, options.getProtocolVersion());
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
    private final ColumnMask.Masker masker;
    private ByteBuffer current;
    private ColumnTimestamps writetimes;
    private ColumnTimestamps ttls;
    private boolean isSet;

    public static Factory newFactory(final ColumnMetadata def, final int idx, boolean useForPostOrdering)
    {
        return new SimpleSelectorFactory(idx, def, useForPostOrdering);
    }

    @Override
    public void addFetchedColumns(Builder builder)
    {
        builder.add(column);
    }

    @Override
    public void addInput(InputRow input) throws InvalidRequestException
    {
        if (!isSet)
        {
            isSet = true;
            writetimes = input.getWritetimes(idx);
            ttls = input.getTtls(idx);

            /*
            We apply the column masker of the column unless:
            - The column doesn't have a mask
            - The input row is for a user with UNMASK permission, indicated by input.unmask()
            - Dynamic data masking is globally disabled
             */
            ByteBuffer value = input.getValue(idx);
            current = masker == null || input.unmask() || !DatabaseDescriptor.getDynamicDataMaskingEnabled()
                      ? value : masker.mask(value);
        }
    }

    @Override
    public ByteBuffer getOutput(ProtocolVersion protocolVersion)
    {
        return current;
    }

    @Override
    protected ColumnTimestamps getWritetimes(ProtocolVersion protocolVersion)
    {
        return writetimes;
    }

    @Override
    protected ColumnTimestamps getTTLs(ProtocolVersion protocolVersion)
    {
        return ttls;
    }

    @Override
    public void reset()
    {
        isSet = false;
        current = null;
        writetimes = null;
        ttls = null;
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

    private SimpleSelector(ColumnMetadata column, int idx, boolean useForPostOrdering, ProtocolVersion version)
    {
        super(Kind.SIMPLE_SELECTOR);
        this.column = column;
        this.idx = idx;
        /*
         We apply the column mask of the column unless:
         - The column doesn't have a mask
         - This selector is for a query with ORDER BY post-ordering
          */
        this.masker = useForPostOrdering || column.getMask() == null
                      ? null
                      : column.getMask().masker(version);
    }

    @Override
    public void validateForGroupBy()
    {
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof SimpleSelector))
            return false;

        SimpleSelector s = (SimpleSelector) o;

        return Objects.equal(column, s.column)
            && Objects.equal(idx, s.idx);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(column, idx);
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
