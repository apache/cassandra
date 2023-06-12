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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.index.internal.composites.ClusteringColumnIndex;
import org.apache.cassandra.index.internal.composites.CollectionEntryIndex;
import org.apache.cassandra.index.internal.composites.CollectionKeyIndex;
import org.apache.cassandra.index.internal.composites.CollectionValueIndex;
import org.apache.cassandra.index.internal.composites.PartitionKeyIndex;
import org.apache.cassandra.index.internal.composites.RegularColumnIndex;
import org.apache.cassandra.index.internal.keys.KeysIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;

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
    default AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn)
    {
        return indexedColumn.type;
    }

    default AbstractType<?> getIndexedPartitionKeyType(ColumnMetadata indexedColumn)
    {
        return indexedColumn.type;
    }

    /**
     * Add the clustering columns for a specific type of index table to the a TableMetadata.Builder (which is being
     * used to construct the index table's TableMetadata. In the default implementation, the clustering columns of the
     * index table hold the partition key and clustering columns of the base table. This is overridden in several cases:
     * * When the indexed value is itself a clustering column, in which case, we only need store the base table's
     *   *other* clustering values in the index - the indexed value being the index table's partition key
     * * When the indexed value is a collection value, in which case we also need to capture the cell path from the base
     *   table
     * * In a KEYS index (for compact storage/static column indexes), where only the base partition key is
     *   held in the index table.
     *
     * Called from indexCfsMetadata
     * @param builder
     * @param baseMetadata
     * @param cfDef
     * @return
     */
    default TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder,
                                                            TableMetadata baseMetadata,
                                                            ColumnMetadata cfDef)
    {
        for (ColumnMetadata def : baseMetadata.clusteringColumns())
            builder.addClusteringColumn(def.name, def.type);
        return builder;
    }

    /*
     * implementations providing specializations for the built in index types
     */

    static final CassandraIndexFunctions KEYS_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        @Override
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new KeysIndex(baseCfs, indexMetadata);
        }

        @Override
        public TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder,
                                                               TableMetadata baseMetadata,
                                                               ColumnMetadata columnDef)
        {
            // KEYS index are indexing the whole partition so have no clustering columns (outside of the
            // "partition_key" one that all the 'CassandraIndex' gets (see CassandraIndex#indexCfsMetadata)).
            return builder;
        }
    };

    static final CassandraIndexFunctions REGULAR_COLUMN_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        @Override
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new RegularColumnIndex(baseCfs, indexMetadata);
        }
    };

    static final CassandraIndexFunctions CLUSTERING_COLUMN_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        @Override
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new ClusteringColumnIndex(baseCfs, indexMetadata);
        }

        @Override
        public TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder,
                                                               TableMetadata baseMetadata,
                                                               ColumnMetadata columnDef)
        {
            List<ColumnMetadata> cks = baseMetadata.clusteringColumns();
            for (int i = 0; i < columnDef.position(); i++)
            {
                ColumnMetadata def = cks.get(i);
                builder.addClusteringColumn(def.name, def.type);
            }
            for (int i = columnDef.position() + 1; i < cks.size(); i++)
            {
                ColumnMetadata def = cks.get(i);
                builder.addClusteringColumn(def.name, def.type);
            }

            return builder;
        }
    };

    static final CassandraIndexFunctions COLLECTION_KEY_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new CollectionKeyIndex(baseCfs, indexMetadata);
        }

        public AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn)
        {
            return ((CollectionType) indexedColumn.type).nameComparator();
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnMetadata indexedColumn)
        {
            assert indexedColumn.type.isCollection();
            switch (((CollectionType<?>)indexedColumn.type).kind)
            {
                case LIST:
                    return ((ListType<?>)indexedColumn.type).getElementsType();
                case SET:
                    return ((SetType<?>)indexedColumn.type).getElementsType();
                case MAP:
                    return ((MapType<?, ?>)indexedColumn.type).getKeysType();
            }
            throw new RuntimeException("Error collection type " + indexedColumn.type);
        }
    };

    static final CassandraIndexFunctions PARTITION_KEY_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {
        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new PartitionKeyIndex(baseCfs, indexMetadata);
        }
    };

    static final CassandraIndexFunctions COLLECTION_VALUE_INDEX_FUNCTIONS = new CassandraIndexFunctions()
    {

        public CassandraIndex newIndexInstance(ColumnFamilyStore baseCfs, IndexMetadata indexMetadata)
        {
            return new CollectionValueIndex(baseCfs, indexMetadata);
        }

        public AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn)
        {
            return ((CollectionType)indexedColumn.type).valueComparator();
        }

        public TableMetadata.Builder addIndexClusteringColumns(TableMetadata.Builder builder,
                                                               TableMetadata baseMetadata,
                                                               ColumnMetadata columnDef)
        {
            for (ColumnMetadata def : baseMetadata.clusteringColumns())
                builder.addClusteringColumn(def.name, def.type);

            // collection key
            builder.addClusteringColumn("cell_path", ((CollectionType)columnDef.type).nameComparator());
            return builder;
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnMetadata indexedColumn)
        {
            assert indexedColumn.type.isCollection();
            switch (((CollectionType<?>) indexedColumn.type).kind)
            {
                case LIST:
                    return ((ListType<?>) indexedColumn.type).getElementsType();
                case SET:
                    return ((SetType<?>) indexedColumn.type).getElementsType();
                case MAP:
                    return ((MapType<?, ?>) indexedColumn.type).getValuesType();
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

        public AbstractType<?> getIndexedValueType(ColumnMetadata indexedColumn)
        {
            CollectionType colType = (CollectionType)indexedColumn.type;
            return CompositeType.getInstance(colType.nameComparator(), colType.valueComparator());
        }

        @Override
        public AbstractType<?> getIndexedPartitionKeyType(ColumnMetadata indexedColumn)
        {
            assert indexedColumn.type.isCollection();
            return indexedColumn.type;
        }
    };
}
