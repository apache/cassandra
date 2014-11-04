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
 * Index on a PARTITION_KEY column definition.
 *
 * This suppose a composite row key:
 *   rk = rk_0 ... rk_n
 *
 * The corresponding index entry will be:
 *   - index row key will be rk_i (where i == columnDef.componentIndex)
 *   - cell name will be: rk ck
 *     where rk is the fully partition key and ck the clustering keys of the
 *     original cell names (thus excluding the last column name as we want to refer to
 *     the whole CQL3 row, not just the cell itself)
 *
 * Note that contrarily to other type of index, we repeat the indexed value in
 * the index cell name (we use the whole partition key). The reason is that we
 * want to order the index cell name by partitioner first, and skipping a part
 * of the row key would change the order.
 */
public class CompositesIndexOnPartitionKey extends CompositesIndex
{
    public static CompositeType buildIndexComparator(CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        int ckCount = baseMetadata.clusteringKeyColumns().size();
        List<AbstractType<?>> types = new ArrayList<AbstractType<?>>(ckCount + 1);
        types.add(SecondaryIndex.keyComparator);
        types.addAll(baseMetadata.comparator.getComponents());
        return CompositeType.getInstance(types);
    }

    protected ByteBuffer getIndexedValue(ByteBuffer rowKey, Column column)
    {
        CompositeType keyComparator = (CompositeType)baseCfs.metadata.getKeyValidator();
        ByteBuffer[] components = keyComparator.split(rowKey);
        return components[columnDef.componentIndex];
    }

    protected ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer rowKey, ByteBuffer columnName)
    {
        int ckCount = baseCfs.metadata.clusteringKeyColumns().size();
        CompositeType baseComparator = (CompositeType)baseCfs.getComparator();
        ByteBuffer[] components = baseComparator.split(columnName);
        CompositeType.Builder builder = getIndexComparator().builder();
        builder.add(rowKey);
        for (int i = 0; i < ckCount; i++)
            builder.add(components[i]);
        return builder;
    }

    public IndexedEntry decodeEntry(DecoratedKey indexedValue, Column indexEntry)
    {
        int ckCount = baseCfs.metadata.clusteringKeyColumns().size();
        ByteBuffer[] components = getIndexComparator().split(indexEntry.name());

        ColumnNameBuilder builder = getBaseComparator().builder();
        for (int i = 0; i < ckCount; i++)
            builder.add(components[i + 1]);

        return new IndexedEntry(indexedValue, indexEntry.name(), indexEntry.timestamp(), components[0], builder);
    }

    @Override
    public boolean indexes(ByteBuffer name)
    {
        // Since a partition key is always full, we always index it
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
