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
package org.apache.cassandra.db.composites;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Row;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;

public class CompoundDenseCellNameType extends AbstractCompoundCellNameType
{
    public CompoundDenseCellNameType(List<AbstractType<?>> types)
    {
        this(new CompoundCType(types));
    }

    private CompoundDenseCellNameType(CompoundCType type)
    {
        super(type, type);
    }

    public CellNameType setSubtype(int position, AbstractType<?> newType)
    {
        if (position != 0)
            throw new IllegalArgumentException();
        return new SimpleDenseCellNameType(newType);
    }

    public boolean isDense()
    {
        return true;
    }

    public CellName create(Composite prefix, ColumnDefinition column)
    {
        // We ignore the column because it's just the COMPACT_VALUE name which is not store in the cell name (and it can be null anyway)
        assert prefix.size() == fullSize;
        if (prefix instanceof CellName)
            return (CellName)prefix;

        assert prefix instanceof CompoundComposite;
        CompoundComposite lc = (CompoundComposite)prefix;
        assert lc.elements.length == lc.size;
        return new CompoundDenseCellName(lc.elements);
    }

    protected Composite makeWith(ByteBuffer[] components, int size, Composite.EOC eoc, boolean isStatic)
    {
        assert !isStatic;
        if (size < fullSize || eoc != Composite.EOC.NONE)
            return new CompoundComposite(components, size, false).withEOC(eoc);

        assert components.length == size;
        return new CompoundDenseCellName(components);
    }

    protected Composite copyAndMakeWith(ByteBuffer[] components, int size, Composite.EOC eoc, boolean isStatic)
    {
        return makeWith(Arrays.copyOfRange(components, 0, size), size, eoc, isStatic);
    }

    public void addCQL3Column(ColumnIdentifier id) {}
    public void removeCQL3Column(ColumnIdentifier id) {}

    public CQL3Row.Builder CQL3RowBuilder(long now)
    {
        return makeDenseCQL3RowBuilder(now);
    }
}
