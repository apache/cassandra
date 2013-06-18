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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Base class for secondary indexes where composites are involved.
 */
public abstract class CompositesIndex extends AbstractSimplePerColumnSecondaryIndex
{
    private volatile CompositeType indexComparator;

    protected CompositeType getIndexComparator()
    {
        // Yes, this is racy, but doing this more than once is not a big deal, we just want to avoid doing it every time
        // More seriously, we should fix that whole SecondaryIndex API so this can be a final and avoid all that non-sense.
        if (indexComparator == null)
        {
            assert columnDef != null;
            indexComparator = getIndexComparator(baseCfs.metadata, columnDef);
        }
        return indexComparator;
    }

    public static CompositesIndex create(ColumnDefinition cfDef)
    {
        switch (cfDef.type)
        {
            case CLUSTERING_KEY:
                return new CompositesIndexOnClusteringKey();
            case REGULAR:
                return new CompositesIndexOnRegular();
            case PARTITION_KEY:
                return new CompositesIndexOnPartitionKey();
            //case COMPACT_VALUE:
            //    return new CompositesIndexOnCompactValue();
        }
        throw new AssertionError();
    }

    // Check SecondaryIndex.getIndexComparator if you want to know why this is static
    public static CompositeType getIndexComparator(CFMetaData baseMetadata, ColumnDefinition cfDef)
    {
        switch (cfDef.type)
        {
            case CLUSTERING_KEY:
                return CompositesIndexOnClusteringKey.buildIndexComparator(baseMetadata, cfDef);
            case REGULAR:
                return CompositesIndexOnRegular.buildIndexComparator(baseMetadata, cfDef);
            case PARTITION_KEY:
                return CompositesIndexOnPartitionKey.buildIndexComparator(baseMetadata, cfDef);
            //case COMPACT_VALUE:
            //    return CompositesIndexOnCompactValue.buildIndexComparator(baseMetadata, cfDef);
        }
        throw new AssertionError();
    }

    protected ByteBuffer makeIndexColumnName(ByteBuffer rowKey, Column column)
    {
        return makeIndexColumnNameBuilder(rowKey, column.name()).build();
    }

    protected abstract ColumnNameBuilder makeIndexColumnNameBuilder(ByteBuffer rowKey, ByteBuffer columnName);

    public abstract IndexedEntry decodeEntry(DecoratedKey indexedValue, Column indexEntry);

    public abstract boolean isStale(IndexedEntry entry, ColumnFamily data, long now);

    public void delete(IndexedEntry entry)
    {
        int localDeletionTime = (int) (System.currentTimeMillis() / 1000);
        ColumnFamily cfi = ArrayBackedSortedColumns.factory.create(indexCfs.metadata);
        cfi.addTombstone(entry.indexEntry, localDeletionTime, entry.timestamp);
        indexCfs.apply(entry.indexValue, cfi, SecondaryIndexManager.nullUpdater);
        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", entry.indexValue, cfi);

    }

    protected AbstractType getExpressionComparator()
    {
        return baseCfs.metadata.getColumnDefinitionComparator(columnDef);
    }

    protected CompositeType getBaseComparator()
    {
        assert baseCfs.getComparator() instanceof CompositeType;
        return (CompositeType)baseCfs.getComparator();
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
    {
        return new CompositesSearcher(baseCfs.indexManager, columns);
    }

    public void validateOptions() throws ConfigurationException
    {
        ColumnDefinition columnDef = columnDefs.iterator().next();
        Map<String, String> options = new HashMap<String, String>(columnDef.getIndexOptions());

        // We take no options though we used to have one called "prefix_size",
        // so skip it silently for backward compatibility sake.
        options.remove("prefix_size");

        if (!options.isEmpty())
            throw new ConfigurationException("Unknown options provided for COMPOSITES index: " + options.keySet());
    }

    public static class IndexedEntry
    {
        public final DecoratedKey indexValue;
        public final ByteBuffer indexEntry;
        public final long timestamp;

        public final ByteBuffer indexedKey;
        public final ColumnNameBuilder indexedEntryNameBuilder;

        public IndexedEntry(DecoratedKey indexValue, ByteBuffer indexEntry, long timestamp, ByteBuffer indexedKey, ColumnNameBuilder indexedEntryNameBuilder)
        {
            this.indexValue = indexValue;
            this.indexEntry = indexEntry;
            this.timestamp = timestamp;
            this.indexedKey = indexedKey;
            this.indexedEntryNameBuilder = indexedEntryNameBuilder;
        }

        public ByteBuffer indexedEntryStart()
        {
            return indexedEntryNameBuilder.build();
        }

        public ByteBuffer indexedEntryEnd()
        {
            return indexedEntryNameBuilder.buildAsEndOfRange();
        }
    }
}
