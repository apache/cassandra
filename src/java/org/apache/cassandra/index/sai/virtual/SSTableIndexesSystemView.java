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
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} providing a system view of SSTable index metadata.
 */
public class SSTableIndexesSystemView extends AbstractVirtualTable
{
    static final String NAME = "sai_sstable_indexes";

    static final String KEYSPACE_NAME = "keyspace_name";
    static final String INDEX_NAME = "index_name";
    static final String SSTABLE_NAME = "sstable_name";
    static final String TABLE_NAME = "table_name";
    static final String COLUMN_NAME = "column_name";
    static final String FORMAT_VERSION = "format_version";
    static final String CELL_COUNT = "cell_count";
    static final String MIN_ROW_ID = "min_row_id";
    static final String MAX_ROW_ID = "max_row_id";
    static final String START_TOKEN = "start_token";
    static final String END_TOKEN = "end_token";
    static final String PER_TABLE_DISK_SIZE = "per_table_disk_size";
    static final String PER_COLUMN_DISK_SIZE = "per_column_disk_size";

    public SSTableIndexesSystemView(String keyspace)
    {
        super(TableMetadata.builder(keyspace, NAME)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .comment("SSTable index metadata")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(INDEX_NAME, UTF8Type.instance)
                           .addClusteringColumn(SSTABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(FORMAT_VERSION, UTF8Type.instance)
                           .addRegularColumn(CELL_COUNT, LongType.instance)
                           .addRegularColumn(MIN_ROW_ID, LongType.instance)
                           .addRegularColumn(MAX_ROW_ID, LongType.instance)
                           .addRegularColumn(START_TOKEN, UTF8Type.instance)
                           .addRegularColumn(END_TOKEN, UTF8Type.instance)
                           .addRegularColumn(PER_TABLE_DISK_SIZE, LongType.instance)
                           .addRegularColumn(PER_COLUMN_DISK_SIZE, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        for (String ks : Schema.instance.getUserKeyspaces())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
            if (keyspace == null)
                throw new IllegalStateException("Unknown keyspace " + ks + ". This can occur if the keyspace is being dropped.");

            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

                if (group != null)
                {
                    Token.TokenFactory tokenFactory = cfs.metadata().partitioner.getTokenFactory();

                    group.getIndexes().forEach(i -> {
                        StorageAttachedIndex index = (StorageAttachedIndex)i;

                        for (SSTableIndex sstableIndex : index.view())
                        {
                            SSTableReader sstable = sstableIndex.getSSTable();
                            Descriptor descriptor = sstable.descriptor;
                            AbstractBounds<Token> bounds = sstable.getBounds();

                            dataset.row(ks, index.identifier().indexName, sstable.getFilename())
                                   .column(TABLE_NAME, descriptor.cfname)
                                   .column(COLUMN_NAME, index.termType().columnName())
                                   .column(FORMAT_VERSION, sstableIndex.getVersion().toString())
                                   .column(CELL_COUNT, sstableIndex.getRowCount())
                                   .column(MIN_ROW_ID, sstableIndex.minSSTableRowId())
                                   .column(MAX_ROW_ID, sstableIndex.maxSSTableRowId())
                                   .column(START_TOKEN, tokenFactory.toString(bounds.left))
                                   .column(END_TOKEN, tokenFactory.toString(bounds.right))
                                   .column(PER_TABLE_DISK_SIZE, sstableIndex.getSSTableContext().diskUsage())
                                   .column(PER_COLUMN_DISK_SIZE, sstableIndex.sizeOfPerColumnComponents());
                        }
                    });
                }
            }
        }

        return dataset;
    }
}
