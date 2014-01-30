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

import java.util.Locale;
import java.nio.ByteBuffer;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.cql3.statements.Selectable;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Represents an identifer for a CQL column definition.
 * TODO : should support light-weight mode without text representation for when not interned
 */
public class ColumnIdentifier implements Selectable, Comparable<ColumnIdentifier>, IMeasurableMemory
{
    public final ByteBuffer bytes;
    private final String text;

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ColumnIdentifier("", true));

    public ColumnIdentifier(String rawText, boolean keepCase)
    {
        this.text = keepCase ? rawText : rawText.toLowerCase(Locale.US);
        this.bytes = ByteBufferUtil.bytes(this.text);
    }

    public ColumnIdentifier(ByteBuffer bytes, AbstractType type)
    {
        this.bytes = bytes;
        this.text = type.getString(bytes);
    }

    private ColumnIdentifier(ByteBuffer bytes, String text)
    {
        this.bytes = bytes;
        this.text = text;
    }

    @Override
    public final int hashCode()
    {
        return bytes.hashCode();
    }

    @Override
    public final boolean equals(Object o)
    {
        // Note: it's worth checking for reference equality since we intern those
        // in SparseCellNameType
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

    public long excessHeapSizeExcludingData()
    {
        return EMPTY_SIZE
             + ObjectSizes.sizeOnHeapExcludingData(bytes)
             + ObjectSizes.sizeOf(text);
    }

    public int compareTo(ColumnIdentifier other)
    {
        if (this == other)
            return 0;

        return bytes.compareTo(other.bytes);
    }

    public ColumnIdentifier clone(AbstractAllocator allocator)
    {
        return new ColumnIdentifier(allocator.clone(bytes), text);
    }

}
