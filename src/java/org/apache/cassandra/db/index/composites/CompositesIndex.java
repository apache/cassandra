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

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.index.AbstractSimplePerColumnSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexManager;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Base class for internal secondary indexes (this could be merged with AbstractSimplePerColumnSecondaryIndex).
 */
public abstract class CompositesIndex extends AbstractSimplePerColumnSecondaryIndex
{
    public static CompositesIndex create(ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection() && cfDef.type.isMultiCell())
        {
            switch (((CollectionType)cfDef.type).kind)
            {
                case LIST:
                    return new CompositesIndexOnCollectionValue();
                case SET:
                    return new CompositesIndexOnCollectionKey();
                case MAP:
                    if (cfDef.hasIndexOption(SecondaryIndex.INDEX_KEYS_OPTION_NAME))
                        return new CompositesIndexOnCollectionKey();
                    else if (cfDef.hasIndexOption(SecondaryIndex.INDEX_ENTRIES_OPTION_NAME))
                        return new CompositesIndexOnCollectionKeyAndValue();
                    else
                        return new CompositesIndexOnCollectionValue();
            }
        }

        switch (cfDef.kind)
        {
            case CLUSTERING:
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

    public static void addIndexClusteringColumns(CFMetaData.Builder indexMetadata, CFMetaData baseMetadata, ColumnDefinition cfDef)
    {
        if (cfDef.type.isCollection() && cfDef.type.isMultiCell())
        {
            CollectionType type = (CollectionType)cfDef.type;
            if (type.kind == CollectionType.Kind.LIST
                || (type.kind == CollectionType.Kind.MAP && cfDef.hasIndexOption(SecondaryIndex.INDEX_VALUES_OPTION_NAME)))
            {
                CompositesIndexOnCollectionValue.addClusteringColumns(indexMetadata, baseMetadata, cfDef);
            }
            else
            {
                addGenericClusteringColumns(indexMetadata, baseMetadata, cfDef);
            }
        }
        else if (cfDef.isClusteringColumn())
        {
            CompositesIndexOnClusteringKey.addClusteringColumns(indexMetadata, baseMetadata, cfDef);
        }
        else
        {
            addGenericClusteringColumns(indexMetadata, baseMetadata, cfDef);
        }
    }

    protected static void addGenericClusteringColumns(CFMetaData.Builder indexMetadata, CFMetaData baseMetadata, ColumnDefinition columnDef)
    {
        indexMetadata.addClusteringColumn("partition_key", SecondaryIndex.keyComparator);
        for (ColumnDefinition def : baseMetadata.clusteringColumns())
            indexMetadata.addClusteringColumn(def.name, def.type);
    }

    public abstract IndexedEntry decodeEntry(DecoratedKey indexedValue, Row indexEntry);

    public abstract boolean isStale(Row row, ByteBuffer indexValue, int nowInSec);

    public void delete(IndexedEntry entry, OpOrder.Group opGroup, int nowInSec)
    {
        Row row = ArrayBackedRow.emptyDeletedRow(entry.indexClustering, new DeletionTime(entry.timestamp, nowInSec));
        PartitionUpdate upd = PartitionUpdate.singleRowUpdate(indexCfs.metadata, entry.indexValue, row);
        indexCfs.apply(upd, SecondaryIndexManager.nullUpdater, opGroup, null);

        if (logger.isDebugEnabled())
            logger.debug("removed index entry for cleaned-up value {}:{}", entry.indexValue, upd);
    }

    public SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ColumnDefinition> columns)
    {
        return new CompositesSearcher(baseCfs.indexManager, columns);
    }

    public void validateOptions() throws ConfigurationException
    {
        ColumnDefinition columnDef = columnDefs.iterator().next();
        Map<String, String> options = new HashMap<String, String>(columnDef.getIndexOptions());

        // We used to have an option called "prefix_size" so skip it silently for backward compatibility sake.
        options.remove("prefix_size");

        if (columnDef.type.isCollection())
        {
            options.remove(SecondaryIndex.INDEX_VALUES_OPTION_NAME);
            options.remove(SecondaryIndex.INDEX_KEYS_OPTION_NAME);
            options.remove(SecondaryIndex.INDEX_ENTRIES_OPTION_NAME);
        }

        if (!options.isEmpty())
            throw new ConfigurationException("Unknown options provided for COMPOSITES index: " + options.keySet());
    }

    public static class IndexedEntry
    {
        public final DecoratedKey indexValue;
        public final Clustering indexClustering;
        public final long timestamp;

        public final ByteBuffer indexedKey;
        public final Clustering indexedEntryClustering;

        public IndexedEntry(DecoratedKey indexValue, Clustering indexClustering, long timestamp, ByteBuffer indexedKey, Clustering indexedEntryClustering)
        {
            this.indexValue = indexValue;
            this.indexClustering = indexClustering;
            this.timestamp = timestamp;
            this.indexedKey = indexedKey;
            this.indexedEntryClustering = indexedEntryClustering;
        }
    }
}
