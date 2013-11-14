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
import org.apache.cassandra.dht.LocalToken;

/**
 * Index the value of a collection cell.
 *
 * This is a lot like an index on REGULAR, except that we also need to make
 * the collection key part of the index entry so that:
 *   1) we don't have to scan the whole collection at query time to know the
 *   entry is stale and if it still satisfies the query.
 *   2) if a collection has multiple time the same value, we need one entry
 *   for each so that if we delete one of the value only we only delete the
 *   entry corresponding to that value.
 */
public class CompositesIndexOnCollectionValue extends CompositesIndex
{
    public static CompositeType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int prefixSize = columnDef.position();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(prefixSize + 2);
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < prefixSize; i++)
            types.add(((CompositeType)baseMetadata.comparator).types.get(i));
        types.add(((CollectionType)columnDef.type).nameComparator()); // collection key
        return CompositeType.getInstance(types);
    }

    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).valueComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Column column)
    {
        return column.value();
    }

    protected ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer rowKey, ByteBuffer columnName)
    {
        int prefixSize = columnDef.position();
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(columnName);
        assert components.length == baseComparator.types.size();
        CompositeType.Builder builder = getIndexComparator().builder();
        builder.add(rowKey);
        for (int i = 0; i < prefixSize; i++)
            builder.add(components[i]);
        builder.add(components[prefixSize + 1]);
        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Column indexEntry)
    {
        int prefixSize = columnDef.position();
        ByteBuffer[] components = getIndexComparator().split(indexEntry.name());
        CompositeType.Builder builder = getBaseComparator().builder();
        for (int i = 0; i < prefixSize; i++)
            builder.add(components[i + 1]);
        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), components[0], builder, components[prefixSize + 1]);
    }

    @Override
    public boolean indexes(ByteBuffer name)
    {
        ByteBuffer[] components = getBaseComparator().split(name);
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return components.length > columnDef.position()
            && comp.compare(components[columnDef.position()], columnDef.name.bytes) == 0;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        ByteBuffer bb = entry.indexedEntryNameBuilder.copy().add(columnDef.name).add(entry.indexedEntryCollectionKey).build();
        Column liveColumn = data.getColumn(bb);
        if (liveColumn == null || liveColumn.isMarkedForDelete(now))
            return true;

        ByteBuffer liveValue = liveColumn.value();
        return ((CollectionType)columnDef.type).valueComparator().compare(entry.indexValue.key, liveValue) != 0;
    }
}
