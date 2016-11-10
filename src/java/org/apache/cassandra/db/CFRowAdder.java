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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Convenience object to populate a given CQL3 row in a ColumnFamily object.
 *
 * This is meant for when performance is not of the utmost importance. When
 * performance matters, it might be worth allocating such builder.
 */
public class CFRowAdder
{
    public final ColumnFamily cf;
    public final Composite prefix;
    public final long timestamp;
    public final int ttl;
    private final int ldt;

    public CFRowAdder(ColumnFamily cf, Composite prefix, long timestamp)
    {
        this(cf, prefix, timestamp, 0);
    }

    public CFRowAdder(ColumnFamily cf, Composite prefix, long timestamp, int ttl)
    {
        this.cf = cf;
        this.prefix = prefix;
        this.timestamp = timestamp;
        this.ttl = ttl;
        this.ldt = (int) (System.currentTimeMillis() / 1000);

        // If a CQL3 table, add the row marker
        if (cf.metadata().isCQL3Table() && !prefix.isStatic())
            cf.addColumn(new BufferCell(cf.getComparator().rowMarker(prefix), ByteBufferUtil.EMPTY_BYTE_BUFFER, timestamp));
    }

    public CFRowAdder add(String cql3ColumnName, Object value)
    {
        ColumnDefinition def = getDefinition(cql3ColumnName);
        return add(cf.getComparator().create(prefix, def), def, value);
    }

    public CFRowAdder resetCollection(String cql3ColumnName)
    {
        ColumnDefinition def = getDefinition(cql3ColumnName);
        assert def.type.isCollection() && def.type.isMultiCell();
        Composite name = cf.getComparator().create(prefix, def);
        cf.addAtom(new RangeTombstone(name.start(), name.end(), timestamp - 1, ldt));
        return this;
    }

    public CFRowAdder addMapEntry(String cql3ColumnName, Object key, Object value)
    {
        ColumnDefinition def = getDefinition(cql3ColumnName);
        assert def.type instanceof MapType;
        MapType mt = (MapType)def.type;
        CellName name = cf.getComparator().create(prefix, def, mt.getKeysType().decompose(key));
        return add(name, def, value);
    }

    public CFRowAdder addListEntry(String cql3ColumnName, Object value)
    {
        ColumnDefinition def = getDefinition(cql3ColumnName);
        assert def.type instanceof ListType;
        CellName name = cf.getComparator().create(prefix, def, ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));
        return add(name, def, value);
    }

    private ColumnDefinition getDefinition(String name)
    {
        return cf.metadata().getColumnDefinition(new ColumnIdentifier(name, false));
    }

    private CFRowAdder add(CellName name, ColumnDefinition def, Object value)
    {
        if (value == null)
        {
            cf.addColumn(new BufferDeletedCell(name, ldt, timestamp));
        }
        else
        {
            AbstractType valueType = def.type.isCollection()
                                   ? ((CollectionType) def.type).valueComparator()
                                   : def.type;
            ByteBuffer valueBytes = value instanceof ByteBuffer ? (ByteBuffer)value : valueType.decompose(value);
            if (ttl == 0)
                cf.addColumn(new BufferCell(name, valueBytes, timestamp));
            else
                cf.addColumn(new BufferExpiringCell(name, valueBytes, timestamp, ttl));
        }
        return this;
    }
}
