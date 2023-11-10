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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} providing a system view of SSTable index segment metadata.
 */
public class SegmentsSystemView extends AbstractVirtualTable
{
    public static final String NAME = "sai_sstable_index_segments";

    public static final String KEYSPACE_NAME = "keyspace_name";
    public static final String INDEX_NAME = "index_name";
    public static final String SSTABLE_NAME = "sstable_name";
    public static final String TABLE_NAME = "table_name";
    public static final String COLUMN_NAME = "column_name";
    public static final String CELL_COUNT = "cell_count";
    public static final String SEGMENT_ROW_ID_OFFSET = "segment_row_id_offset";
    public static final String MIN_SSTABLE_ROW_ID = "min_sstable_row_id";
    public static final String MAX_SSTABLE_ROW_ID = "max_sstable_row_id";
    public static final String START_TOKEN = "start_token";
    public static final String END_TOKEN = "end_token";
    public static final String MIN_TERM = "min_term";
    public static final String MAX_TERM = "max_term";
    public static final String COMPONENT_METADATA = "component_metadata";

    public SegmentsSystemView(String keyspace)
    {
        super(TableMetadata.builder(keyspace, NAME)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .comment("SSTable index segment metadata")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(INDEX_NAME, UTF8Type.instance)
                           .addClusteringColumn(SSTABLE_NAME, UTF8Type.instance)
                           .addClusteringColumn(SEGMENT_ROW_ID_OFFSET, LongType.instance)
                           .addRegularColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(CELL_COUNT, LongType.instance)
                           .addRegularColumn(MIN_SSTABLE_ROW_ID, LongType.instance)
                           .addRegularColumn(MAX_SSTABLE_ROW_ID, LongType.instance)
                           .addRegularColumn(START_TOKEN, UTF8Type.instance)
                           .addRegularColumn(END_TOKEN, UTF8Type.instance)
                           .addRegularColumn(MIN_TERM, UTF8Type.instance)
                           .addRegularColumn(MAX_TERM, UTF8Type.instance)
                           .addRegularColumn(COMPONENT_METADATA,
                                             MapType.getInstance(UTF8Type.instance,
                                                                 MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false),
                                                                 false))
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet dataset = new SimpleDataSet(metadata());

        forEachIndex(index -> {
            for (SSTableIndex sstableIndex : index.view())
            {
                sstableIndex.populateSegmentView(dataset);
            }
        });

        return dataset;
    }

    private void forEachIndex(Consumer<StorageAttachedIndex> process)
    {
        for (String ks : Schema.instance.getUserKeyspaces())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
            if (keyspace == null)
                throw new IllegalStateException("Unknown keyspace " + ks + ". This can occur if the keyspace is being dropped.");

            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

                if (group != null)
                    group.getIndexes().stream().map(index -> (StorageAttachedIndex) index).forEach(process);
            }
        }
    }
}
