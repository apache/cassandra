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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1.keystore.KeyStoreWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class SSTableComponentsWriter implements PerSSTableIndexWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final MetadataWriter metadataWriter;
    private final NumericValuesWriter partitionSizeWriter;
    private final NumericValuesWriter tokenWriter;
    private final KeyStoreWriter partitionKeysWriter;
    private final KeyStoreWriter clusteringKeysWriter;

    private long partitionId = -1;

    @SuppressWarnings({"resource", "RedundantSuppression"})
    public SSTableComponentsWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
        this.tokenWriter = new NumericValuesWriter(indexDescriptor, IndexComponent.TOKEN_VALUES, metadataWriter, false);
        this.partitionSizeWriter = new NumericValuesWriter(indexDescriptor, IndexComponent.PARTITION_SIZES, metadataWriter, true);
        IndexOutputWriter partitionKeyBlocksWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PARTITION_KEY_BLOCKS);
        NumericValuesWriter partitionKeyBlockOffsetWriter = new NumericValuesWriter(indexDescriptor, IndexComponent.PARTITION_KEY_BLOCK_OFFSETS, metadataWriter, true);
        this.partitionKeysWriter = new KeyStoreWriter(indexDescriptor.componentName(IndexComponent.PARTITION_KEY_BLOCKS),
                                                      metadataWriter,
                                                      partitionKeyBlocksWriter,
                                                      partitionKeyBlockOffsetWriter,
                                                      CassandraRelevantProperties.SAI_SORTED_TERMS_PARTITION_BLOCK_SHIFT.getInt(),
                                                      false);
        if (indexDescriptor.hasClustering())
        {
            IndexOutputWriter clusteringKeyBlocksWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.CLUSTERING_KEY_BLOCKS);
            NumericValuesWriter clusteringKeyBlockOffsetWriter = new NumericValuesWriter(indexDescriptor, IndexComponent.CLUSTERING_KEY_BLOCK_OFFSETS, metadataWriter, true);
            this.clusteringKeysWriter = new KeyStoreWriter(indexDescriptor.componentName(IndexComponent.CLUSTERING_KEY_BLOCKS),
                                                           metadataWriter,
                                                           clusteringKeyBlocksWriter,
                                                           clusteringKeyBlockOffsetWriter,
                                                           CassandraRelevantProperties.SAI_SORTED_TERMS_CLUSTERING_BLOCK_SHIFT.getInt(),
                                                           true);
        }
        else
        {
            this.clusteringKeysWriter = null;
        }
    }

    @Override
    public void startPartition(DecoratedKey partitionKey) throws IOException
    {
        partitionId++;
        partitionKeysWriter.add(v -> ByteSource.of(partitionKey.getKey(), v));
        if (indexDescriptor.hasClustering())
            clusteringKeysWriter.startPartition();
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        tokenWriter.add(primaryKey.token().getLongValue());
        partitionSizeWriter.add(partitionId);
        if (indexDescriptor.hasClustering())
            clusteringKeysWriter.add(indexDescriptor.clusteringComparator.asByteComparable(primaryKey.clustering()));
    }

    @Override
    public void complete() throws IOException
    {
        try
        {
            indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
        }
        finally
        {
            FileUtils.close(tokenWriter, partitionSizeWriter, partitionKeysWriter, clusteringKeysWriter, metadataWriter);
        }
    }

    @Override
    public void abort()
    {
        logger.debug(indexDescriptor.logMessage("Aborting per-SSTable index component writer for {}..."), indexDescriptor.sstableDescriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }
}
