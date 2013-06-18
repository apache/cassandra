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
package org.apache.cassandra.db.index.composites;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.marshal.*;

/**
 * Index on a REGULAR column definition on a composite type.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name : v
 * where ck_i are the cluster keys, c_name the last component of the cell
 * composite name (or second to last if collections are in use, but this
 * has no impact) and v the cell value.
 *
 * Such a cell is indexed if c_name == columnDef.name, and it will generate
 * (makeIndexColumnName()) an index entry whose:
 *   - row key will be the value v (getIndexedValue()).
 *   - cell name will
 *       rk ck_0 ... ck_n
 *     where rk is the row key of the initial cell. I.e. the index entry store
 *     all the information require to locate back the indexed cell.
 */
public class CompositesIndexOnRegular extends CompositesIndex
{
    public static CompositeType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.componentIndex;
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(prefixSize + 1);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(((CompositeType)baseMetadata.comparator).types.get(i));
        return CompositeType.getInstance(types);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Column column)
    {
        return column.value();
    }

    protected ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer rowKey, ByteBuffer columnName)
    {
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(columnName);
        CompositeType.Builder builder = getIndexComparator().builder();
        builder.add(rowKey);
        for (int i = 0; i < Math.min(columnDef.componentIndex, components.length); i++)
            builder.add(components[i]);
        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Column indexEntry)
    {
        ByteBuffer[] components = getIndexComparator().split(indexEntry.name());
        CompositeType.Builder builder = getBaseComparator().builder();
        for (int i = 0; i < columnDef.componentIndex; i++)
            builder.add(components[i + 1]);
        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), components[0], builder);
    }

    @Override
    public boolean indexes(ByteBuffer name)
    {
        ByteBuffer[] components = getBaseComparator().split(name);
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return components.length > columnDef.componentIndex
            && comp.compare(components[columnDef.componentIndex], columnDef.name) == 0;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        ByteBuffer bb = entry.indexedEntryNameBuilder.copy().add(columnDef.name).build();
        Column liveColumn = data.getColumn(bb);
        if (liveColumn == null || liveColumn.isMarkedForDelete(now))
            return true;

        ByteBuffer liveValue = liveColumn.value();
        return columnDef.getValidator().compare(entry.indexValue.key, liveValue) != 0;
    }
}
