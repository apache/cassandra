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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} providing a system view of per-column storage-attached index metadata.
 */
public class ColumnIndexesSystemView extends AbstractVirtualTable
{
    public static final String NAME = "sai_column_indexes";

    static final String KEYSPACE_NAME = "keyspace_name";
    static final String INDEX_NAME = "index_name";
    static final String TABLE_NAME = "table_name";
    static final String COLUMN_NAME = "column_name";
    static final String IS_QUERYABLE = "is_queryable";
    static final String IS_BUILDING = "is_building";
    static final String IS_STRING = "is_string";
    static final String ANALYZER = "analyzer";

    public ColumnIndexesSystemView(String keyspace)
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
                           .build());
    }

    @Override
    public void apply(PartitionUpdate update)
    {
        throw new InvalidRequestException("Modification is not supported by table " + metadata);
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        for (String ks : Schema.instance.getUserKeyspaces())
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
                        IndexContext context = ((StorageAttachedIndex) index).getIndexContext();
                        String indexName = context.getIndexName();

                        dataset.row(ks, indexName)
                               .column(TABLE_NAME, cfs.name)
                               .column(COLUMN_NAME, context.getColumnName())
                               .column(IS_QUERYABLE, manager.isIndexQueryable(index))
                               .column(IS_BUILDING, manager.isIndexBuilding(indexName))
                               .column(IS_STRING, context.isLiteral())
                               .column(ANALYZER, context.getAnalyzerFactory().toString());
                    }
                }
            }
        }

        return dataset;
    }
}
