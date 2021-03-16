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

import java.util.List;
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
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

/**
 * A {@link VirtualTable} providing a system view of SSTable index segment metadata.
 */
public class SegmentsSystemView extends AbstractVirtualTable
{
    static final String NAME = "sstable_index_segments";

    static final String KEYSPACE_NAME = "keyspace_name";
    static final String INDEX_NAME = "index_name";
    static final String SSTABLE_NAME = "sstable_name";
    static final String TABLE_NAME = "table_name";
    static final String COLUMN_NAME = "column_name";
    static final String CELL_COUNT = "cell_count";
    static final String SEGMENT_ROW_ID_OFFSET = "segment_row_id_offset";
    static final String MIN_SSTABLE_ROW_ID = "min_sstable_row_id";
    static final String MAX_SSTABLE_ROW_ID = "max_sstable_row_id";
    static final String START_TOKEN = "start_token";
    static final String END_TOKEN = "end_token";
    static final String MIN_TERM = "min_term";
    static final String MAX_TERM = "max_term";
    static final String COMPONENT_METADATA = "component_metadata";

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

        forEachIndex(columnContext -> {
            for (SSTableIndex sstableIndex : columnContext.getView())
            {
                SSTableReader sstable = sstableIndex.getSSTable();
                List<SegmentMetadata> segments = sstableIndex.segments();
                Descriptor descriptor = sstable.descriptor;
                Token.TokenFactory tokenFactory = sstable.metadata().partitioner.getTokenFactory();

                for (SegmentMetadata metadata : segments)
                {
                    dataset.row(sstable.metadata().keyspace, columnContext.getIndexName(), sstable.getFilename(), metadata.segmentRowIdOffset)
                           .column(TABLE_NAME, descriptor.cfname)
                           .column(COLUMN_NAME, columnContext.getColumnName())
                           .column(CELL_COUNT, metadata.numRows)
                           .column(MIN_SSTABLE_ROW_ID, metadata.minSSTableRowId)
                           .column(MAX_SSTABLE_ROW_ID, metadata.maxSSTableRowId)
                           .column(START_TOKEN, tokenFactory.toString(metadata.minKey.getToken()))
                           .column(END_TOKEN, tokenFactory.toString(metadata.maxKey.getToken()))
                           .column(MIN_TERM, columnContext.getValidator().getSerializer().deserialize(metadata.minTerm).toString())
                           .column(MAX_TERM, columnContext.getValidator().getSerializer().deserialize(metadata.maxTerm).toString())
                           .column(COMPONENT_METADATA, metadata.componentMetadatas.asMap());
                }
            }
        });

        return dataset;
    }

    private void forEachIndex(Consumer<ColumnContext> process)
    {
        for (String ks : Schema.instance.getUserKeyspaces().names())
        {
            Keyspace keyspace = Schema.instance.getKeyspaceInstance(ks);
            if (keyspace == null)
                throw new IllegalArgumentException("Unknown keyspace " + ks);

            for (ColumnFamilyStore cfs : keyspace.getColumnFamilyStores())
            {
                StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);

                if (group != null)
                {
                    for (Index index : group.getIndexes())
                    {
                        process.accept(((StorageAttachedIndex)index).getContext());
                    }
                }
            }
        }
    }
}
