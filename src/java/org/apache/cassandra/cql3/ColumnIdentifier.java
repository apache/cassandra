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
package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.MapMaker;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.selection.Selector;
import org.apache.cassandra.cql3.selection.SimpleSelector;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Represents an identifer for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
public class ColumnIdentifier extends org.apache.cassandra.cql3.selection.Selectable implements IMeasurableMemory
{
    public final ByteBuffer bytes;
    private final String text;
    private final boolean interned;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ColumnIdentifier(ByteBufferUtil.EMPTY_BYTE_BUFFER, "", false));

    private static final ConcurrentMap<ByteBuffer, ColumnIdentifier> internedInstances = new MapMaker().weakValues().makeMap();

    public ColumnIdentifier(String rawText, boolean keepCase)
    {
        this.text = keepCase ? rawText : rawText.toLowerCase(Locale.US);
        this.bytes = ByteBufferUtil.bytes(this.text);
        this.interned = false;
    }

    public ColumnIdentifier(ByteBuffer bytes, AbstractType<?> type)
    {
        this(bytes, type.getString(bytes), false);
    }

    private ColumnIdentifier(ByteBuffer bytes, String text, boolean interned)
    {
        this.bytes = bytes;
        this.text = text;
        this.interned = interned;
    }

    public static ColumnIdentifier getInterned(ByteBuffer bytes, AbstractType<?> type)
    {
        return getInterned(bytes, type.getString(bytes));
    }

    public static ColumnIdentifier getInterned(String rawText, boolean keepCase)
    {
        String text = keepCase ? rawText : rawText.toLowerCase(Locale.US);
        ByteBuffer bytes = ByteBufferUtil.bytes(text);
        return getInterned(bytes, text);
    }

    public static ColumnIdentifier getInterned(ByteBuffer bytes, String text)
    {
        ColumnIdentifier id = internedInstances.get(bytes);
        if (id != null)
            return id;

        ColumnIdentifier created = new ColumnIdentifier(bytes, text, true);
        ColumnIdentifier previous = internedInstances.putIfAbsent(bytes, created);
        return previous == null ? created : previous;
    }

    public boolean isInterned()
    {
        return interned;
    }

    @Override
    public final int hashCode()
    {
        return bytes.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o)
            return true;

        if(!(o instanceof ColumnIdentifier))
            return false;
        ColumnIdentifier that = (ColumnIdentifier)o;
        return bytes.equals(that.bytes);
    }

    @Override
    public String toString()
    {
        return text;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapOf(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public long unsharedHeapSizeExcludingData()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapExcludingData(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public ColumnIdentifier clone(AbstractAllocator allocator)
    {
        return interned ? this : new ColumnIdentifier(allocator.clone(bytes), text, false);
    }

    public Selector.Factory newSelectorFactory(CFMetaData cfm, List<ColumnDefinition> defs) throws InvalidRequestException
    {
        ColumnDefinition def = cfm.getColumnDefinition(this);
        if (def == null)
            throw new InvalidRequestException(String.format("Undefined name %s in selection clause", this));

        return SimpleSelector.newFactory(def, addAndGetIndex(def, defs));
    }

    /**
     * Because Thrift-created tables may have a non-text comparator, we cannot determine the proper 'key' until
     * we know the comparator. ColumnIdentifier.Raw is a placeholder that can be converted to a real ColumnIdentifier
     * once the comparator is known with prepare(). This should only be used with identifiers that are actual
     * column names. See CASSANDRA-8178 for more background.
     */
    public static class Raw implements Selectable.Raw
    {
        private final String rawText;
        private final String text;

        public Raw(String rawText, boolean keepCase)
        {
            this.rawText = rawText;
            this.text =  keepCase ? rawText : rawText.toLowerCase(Locale.US);
        }

        public ColumnIdentifier prepare(CFMetaData cfm)
        {
            if (!cfm.isStaticCompactTable())
                return getInterned(text, true);

            AbstractType<?> thriftColumnNameType = cfm.thriftColumnNameType();
            if (thriftColumnNameType instanceof UTF8Type)
                return getInterned(text, true);

            // We have a Thrift-created table with a non-text comparator. Check if we have a match column, otherwise assume we should use
            // thriftColumnNameType
            ByteBuffer bufferName = ByteBufferUtil.bytes(text);
            for (ColumnDefinition def : cfm.allColumns())
            {
                if (def.name.bytes.equals(bufferName))
                    return def.name;
            }
            return getInterned(thriftColumnNameType.fromString(rawText), text);
        }

        public boolean processesSelection()
        {
            return false;
        }

        @Override
        public final int hashCode()
        {
            return text.hashCode();
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof ColumnIdentifier.Raw))
                return false;
            ColumnIdentifier.Raw that = (ColumnIdentifier.Raw)o;
            return text.equals(that.text);
        }

        @Override
        public String toString()
        {
            return text;
        }
    }
}
