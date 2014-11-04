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
 * Index on a CLUSTERING_KEY column definition.
 *
 * A cell indexed by this index will have the general form:
 *   ck_0 ... ck_n c_name : v
 * where ck_i are the cluster keys, c_name the last component of the cell
 * composite name (or second to last if collections are in use, but this
 * has no impact) and v the cell value.
 *
 * Such a cell is always indexed by this index (or rather, it is indexed if
 * n >= columnDef.componentIndex, which will always be the case in practice)
 * and it will generate (makeIndexColumnName()) an index entry whose:
 *   - row key will be ck_i (getIndexedValue()) where i == columnDef.componentIndex.
 *   - cell name will
 *       rk ck_0 ... ck_{i-1} ck_{i+1} ck_n
 *     where rk is the row key of the initial cell and i == columnDef.componentIndex.
 */
public class CompositesIndexOnClusteringKey extends CompositesIndex
{
    public static CompositeType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        // Index cell names are rk ck_0 ... ck_{i-1} ck_{i+1} ck_n, so n
        // components total (where n is the number of clustering keys)
        int ckCount = baseMetadata.clusteringKeyColumns().size();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(ckCount);
        List<AbstractType<?>> ckTypes = baseMetadata.comparator.getComponents();
        types.add(SecondaryIndex.keyComparator);
        for (int i = 0; i < columnDef.componentIndex; i++)
            types.add(ckTypes.get(i));
        for (int i = columnDef.componentIndex + 1; i < ckCount; i++)
            types.add(ckTypes.get(i));
        return CompositeType.getInstance(types);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Column column)
    {
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(column.name());
        return components[columnDef.componentIndex];
    }

    protected ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer rowKey, ByteBuffer columnName)
    {
        int ckCount = baseCfs.metadata.clusteringKeyColumns().size();
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(columnName);
        CompositeType.Builder builder = getIndexComparator().builder();
        builder.add(rowKey);

        for (int i = 0; i < Math.min(components.length, columnDef.componentIndex); i++)
            builder.add(components[i]);
        for (int i = columnDef.componentIndex + 1; i < Math.min(components.length, ckCount); i++)
            builder.add(components[i]);
        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Column indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringKeyColumns().size();
        ByteBuffer[] components = getIndexComparator().split(indexEntry.name());

        ColumnNameBuilder builder = getBaseComparator().builder();
        for (int i = 0; i < columnDef.componentIndex; i++)
            builder.add(components[i + 1]);

        builder.add(indexedValue.key);

        for (int i = columnDef.componentIndex + 1; i < ckCount; i++)
            builder.add(components[i]);

        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), components[0], builder);
    }

    @Override
    public boolean indexes(ByteBuffer name)
    {
        // For now, assume this is only used in CQL3 when we know name has enough component.
        return true;
    }

    public boolean isStale(IndexedEntry entry, ColumnFamily data, long now)
    {
        return data.hasOnlyTombstones(now);
    }

    @Override
    public void delete(ByteBuffer rowKey, Column column)
    {
        // We only know that one column of the CQL row has been updated/deleted, but we don't know if the
        // full row has been deleted so we should not do anything. If it ends up that the whole row has
        // been deleted, it will be eventually cleaned up on read because the entry will be detected stale.
    }
}
