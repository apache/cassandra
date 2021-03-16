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
package org.apache.cassandra.index.sai.virtual;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} providing a system view of per-column storage-attached index metadata.
 */
public class IndexesSystemView extends AbstractVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(IndexesSystemView.class);

    static final String NAME = "indexes";

    static final String KEYSPACE_NAME = "keyspace_name";
    static final String INDEX_NAME = "index_name";
    static final String TABLE_NAME = "table_name";
    static final String COLUMN_NAME = "column_name";
    static final String IS_QUERYABLE = "is_queryable";
    static final String IS_BUILDING = "is_building";
    static final String IS_STRING = "is_string";
    static final String ANALYZER = "analyzer";
    static final String INDEXED_SSTABLE_COUNT = "indexed_sstable_count";
    static final String CELL_COUNT = "cell_count";
    static final String PER_TABLE_DISK_SIZE = "per_table_disk_size";
    static final String PER_COLUMN_DISK_SIZE = "per_column_disk_size";
    static final String PER_TABLE_FILE_CACHE_SIZE = "per_table_file_cache_size";
    static final String PER_COLUMN_FILE_CACHE_SIZE = "per_column_file_cache_size";

    public IndexesSystemView(String keyspace)
    {
        super(TableMetadata.builder(keyspace, NAME)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .comment("Storage-attached column index metadata")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(INDEX_NAME, UTF8Type.instance)
                           .addRegularColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(IS_QUERYABLE, BooleanType.instance)
                           .addRegularColumn(IS_BUILDING, BooleanType.instance)
                           .addRegularColumn(IS_STRING, BooleanType.instance)
                           .addRegularColumn(ANALYZER, UTF8Type.instance)
                           .addRegularColumn(INDEXED_SSTABLE_COUNT, Int32Type.instance)
                           .addRegularColumn(CELL_COUNT, LongType.instance)
                           .addRegularColumn(PER_TABLE_DISK_SIZE, LongType.instance)
                           .addRegularColumn(PER_COLUMN_DISK_SIZE, LongType.instance)
                           .build());
    }


    @Override
    public void apply(PartitionUpdate update)
    {
        // TODO port DataSet. Now we can't change index queryability via system view
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        for (String ks : Schema.instance.getUserKeyspaces().names())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
            if (keyspace == null)
                throw new IllegalArgumentException("Unknown keyspace " + ks);

            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                SecondaryIndexManager manager = cfs.indexManager;
                StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

                if (group != null)
                {
                    for (Index index : group.getIndexes())
                    {
                        ColumnContext context = ((StorageAttachedIndex)index).getContext();
                        String indexName = context.getIndexName();
                        View view = context.getView();

                        dataset.row(ks, indexName)
                               .column(TABLE_NAME, cfs.name)
                               .column(COLUMN_NAME, context.getColumnName())
                               .column(IS_QUERYABLE, manager.isIndexQueryable(index))
                               .column(IS_BUILDING, manager.isIndexBuilding(indexName))
                               .column(IS_STRING, context.isLiteral())
                               .column(ANALYZER, context.getAnalyzer().toString())
                               .column(INDEXED_SSTABLE_COUNT, view.size())
                               .column(CELL_COUNT, context.getCellCount())
                               .column(PER_TABLE_DISK_SIZE, group.diskUsage())
                               .column(PER_COLUMN_DISK_SIZE, context.diskUsage());
                    }
                }
            }
        }

        return dataset;
    }

    private static Consumer<Boolean> isQueryableUpdateConsumer(SecondaryIndexManager manager, StorageAttachedIndex index)
    {
        return isQueryable -> {
            logger.debug(index.getContext().logMessage("Index is now {}queryable."), isQueryable ? "" : "non-");

            if (isQueryable)
                manager.makeIndexQueryable(index, Index.Status.BUILD_SUCCEEDED);
            else
                manager.makeIndexNonQueryable(index, Index.Status.BUILD_FAILED);
        };
    }
}
