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
 * Index on the collection element of the cell name of a collection.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name [col_elt] : v
 * where ck_i are the cluster keys, c_name the CQL3 column name, col_elt the
 * collection element that we want to index (which may or may not be there depending
 * on whether c_name is the collection we're indexing) and v the cell value.
 *
 * Such a cell is indexed if c_name is the indexed collection (in which case we are guaranteed to have
 * col_elt). The index entry will be:
 *   - row key will be col_elt value (getIndexedValue()).
 *   - cell name will be 'rk ck_0 ... ck_n' where rk is the row key of the initial cell.
 */
public class CompositesIndexOnCollectionKey extends CompositesIndex
{
    public static CompositeType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int count = 1 + baseMetadata.clusteringColumns().size(); // row key + clustering prefix
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(count);
        List<AbstractType<?>> ckTypes = baseMetadata.comparator.getComponents();
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < count - 1; i++)
            types.add(ckTypes.get(i));
        return CompositeType.getInstance(types);
    }

    @Override
    protected AbstractType<?> getIndexKeyComparator()
    {
        return ((CollectionType)columnDef.type).nameComparator();
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Column column)
    {
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(column.name());
        return components[columnDef.position() + 1];
    }

    protected ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer rowKey, ByteBuffer columnName)
    {
        int count = 1 + baseCfs.metadata.clusteringColumns().size();
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(columnName);
        CompositeType.Builder builder = getIndexComparator().builder();
        builder.add(rowKey);
        for (int i = 0; i < count - 1; i++)
            builder.add(components[i]);
        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Column indexEntry)
    {
        int count = 1 + baseCfs.metadata.clusteringColumns().size();
        ByteBuffer[] components = getIndexComparator().split(indexEntry.name());

        ColumnNameBuilder builder = getBaseComparator().builder();
        for (int i = 0; i < count - 1; i++)
            builder.add(components[i + 1]);

        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), components[0], builder);
    }

    @Override
    public boolean indexes(ByteBuffer name)
    {
        // We index if the CQL3 column name is the one of the collection we index
        ByteBuffer[] components = getBaseComparator().split(name);
        AbstractType<?> comp = baseCfs.metadata.getColumnDefinitionComparator(columnDef);
        return components.length > columnDef.position()
            && comp.compare(components[columnDef.position()], columnDef.name.bytes) == 0;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        ByteBuffer bb = entry.indexedEntryNameBuilder.copy().add(columnDef.name).add(entry.indexValue.key).build();
        Column liveColumn = data.getColumn(bb);
        return (liveColumn == null || liveColumn.isMarkedForDelete(now));
    }
}
