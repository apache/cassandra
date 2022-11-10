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

package org.apache.cassandra.index.internal;

import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.index.internal.composites.*;
import org.apache.cassandra.index.internal.keys.KeysIndex;
import org.apache.cassandra.schema.IndexMetadata;

public interface CassandraIndexFunctions
{
    /**
     *
     * @param baseCfs
     * @param indexMetadata
     * @return
     */
    public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata);

    /**
     * Returns the type of the the values in the index. For most columns this is simply its type, but for collections
     * it depends on whether the index is on the collection name/value element or on a frozen collection
     * @param indexedColumn
     * @return
     */
    default AbstractType<?> getIndexedValueType(ColumnDefinition indexedColumn)
    {
        return indexedColumn.type;
    }

    public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn);
    
    /**
     * Add the clustering columns for a specific type of index table to the a CFMetaData.Builder (which is being
     * used to construct the index table's CFMetadata. In the default implementation, the clustering columns of the
     * index table hold the partition key and clustering columns of the base table. This is overridden in several cases:
     * * When the indexed value is itself a clustering column, in which case, we only need store the base table's
     *   *other* clustering values in the index - the indexed value being the index table's partition key
     * * When the indexed value is a collection value, in which case we also need to capture the cell path from the base
     *   table
     * * In a KEYS index (for thrift/compact storage/static column indexes), where only the base partition key is
     *   held in the index table.
     *
     * Called from indexCfsMetadata
     * @param builder
     * @param baseMetadata
     * @param cfDef
     * @return
     */
    default CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                         CFMetaData baseMetadata,
                                                         ColumnDefinition cfDef)
    {
        for (ColumnDefinition def : baseMetadata.clusteringColumns())
            builder.addClusteringColumn(def.name, def.type);
        return builder;
    }

    /*
     * implementations providing specializations for the built in index types
     */

    static final CassandraIndexFunctions KEYS_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new KeysIndex(baseCfs, indexMetadata);
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            return indexedColumn.type;
        }
    };

    static final CassandraIndexFunctions REGULAR_COLUMN_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new RegularColumnIndex(baseCfs, indexMetadata);
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            return indexedColumn.type;
        }
    };

    static final CassandraIndexFunctions CLUSTERING_COLUMN_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new ClusteringColumnIndex(baseCfs, indexMetadata);
        }

        public CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                            CFMetaData baseMetadata,
                                                            ColumnDefinition columnDef)
        {
            List<ColumnDefinition> cks = baseMetadata.clusteringColumns();
            for (int i = 0; i < columnDef.position(); i++)
            {
                ColumnDefinition def = cks.get(i);
                builder.addClusteringColumn(def.name, def.type);
            }
            for (int i = columnDef.position() + 1; i < cks.size(); i++)
            {
                ColumnDefinition def = cks.get(i);
                builder.addClusteringColumn(def.name, def.type);
            }
            return builder;
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            return indexedColumn.type;
        }
    };

    static final CassandraIndexFunctions PARTITION_KEY_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new PartitionKeyIndex(baseCfs, indexMetadata);
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            return indexedColumn.type;
        }
    };

    static final CassandraIndexFunctions COLLECTION_KEY_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new CollectionKeyIndex(baseCfs, indexMetadata);
        }

        public AbstractType<?> getIndexedValueType(ColumnDefinition indexedColumn)
        {
            return ((CollectionType) indexedColumn.type).nameComparator();
        }


        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            assert indexedColumn.type.isCollection() ;
            switch (((CollectionType)indexedColumn.type).kind)
            {
                case LIST:
                    return ((ListType)indexedColumn.type).getElementsType();
                case SET:
                    return ((SetType)indexedColumn.type).getElementsType();
                case MAP:
                    return ((MapType)indexedColumn.type).getKeysType();
            }
            throw new RuntimeException("Error collection type " + indexedColumn.type);
        }
    };

    static final CassandraIndexFunctions COLLECTION_VALUE_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {

        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new CollectionValueIndex(baseCfs, indexMetadata);
        }

        public AbstractType<?> getIndexedValueType(ColumnDefinition indexedColumn)
        {
            return ((CollectionType)indexedColumn.type).valueComparator();
        }

        public CFMetaData.Builder addIndexClusteringColumns(CFMetaData.Builder builder,
                                                            CFMetaData baseMetadata,
                                                            ColumnDefinition columnDef)
        {
            for (ColumnDefinition def : baseMetadata.clusteringColumns())
                builder.addClusteringColumn(def.name, def.type);

            // collection key
            builder.addClusteringColumn("cell_path", ((CollectionType)columnDef.type).nameComparator());
            return builder;
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            assert indexedColumn.type.isCollection() ;
            switch (((CollectionType)indexedColumn.type).kind)
            {
                case LIST:
                    return ((ListType)indexedColumn.type).getElementsType();
                case SET:
                    return ((SetType)indexedColumn.type).getElementsType();
                case MAP:
                    return ((MapType)indexedColumn.type).getValuesType();
            }
            throw new RuntimeException("Error collection type " + indexedColumn.type);
        }
    };

    static final CassandraIndexFunctions COLLECTION_ENTRY_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new CollectionEntryIndex(baseCfs, indexMetadata);
        }

        public AbstractType<?> getIndexedValueType(ColumnDefinition indexedColumn)
        {
            CollectionType colType = (CollectionType)indexedColumn.type;
            return CompositeType.getInstance(colType.nameComparator(), colType.valueComparator());
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnDefinition indexedColumn)
        {
            assert indexedColumn.type.isCollection();
            return indexedColumn.type;
        }
    };
}
