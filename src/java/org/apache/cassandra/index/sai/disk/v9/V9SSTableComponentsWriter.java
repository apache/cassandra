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

package org.apache.cassandra.index.sai.disk.v9;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v9.keystore.KeyStoreWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.IOUtils;

public class V9SSTableComponentsWriter implements PerSSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(V9SSTableComponentsWriter.class);

    /**
     * Default block shift value for partition keys.
     * Used to determine the block size and block mask for the partition key store writer.
     * The blocks should not be too small and not be too large.
     * See {@link KeyStoreWriter} for details on how this affects index size and performance.
     */
    private static final int DEFAULT_PARTITION_BLOCK_SHIFT = 4;

    /**
     * Default block shift value for clustering keys.
     * Used to determine the block size and block mask for the clustering key store writer.
     * The blocks should not be too small and not be too large.
     * See {@link KeyStoreWriter} for details on how this affects index size and performance.
     */
    private static final int DEFAULT_CLUSTERING_BLOCK_SHIFT = 4;

    /**
     * Configurable partition block shift. Can be set for testing/benchmarking purposes.
     */
    private static volatile int partitionBlockShift = DEFAULT_PARTITION_BLOCK_SHIFT;

    /**
     * Configurable clustering block shift. Can be set for testing/benchmarking purposes.
     */
    private static volatile int clusteringBlockShift = DEFAULT_CLUSTERING_BLOCK_SHIFT;

    private final IndexComponents.ForWrite perSSTableComponents;
    private final MetadataWriter metadataWriter;
    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter partitionSizeWriter;
    private final NumericValuesWriter partitionRowsWriter;
    private final KeyStoreWriter partitionKeysWriter;
    private final KeyStoreWriter clusteringKeysWriter;

    private Token prevToken = null;
    private long partitionId = -1;
    // This is used to record the number of rows in each partition
    private long partitionRowCount = 0;

    @SuppressWarnings("resource")
    public V9SSTableComponentsWriter(IndexComponents.ForWrite perSSTableComponents) throws IOException
    {
        this.perSSTableComponents = perSSTableComponents;
        this.metadataWriter = new MetadataWriter(perSSTableComponents);
        this.tokenWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.ROW_TO_TOKEN),
                                                   metadataWriter, false);

        this.partitionRowsWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.ROW_TO_PARTITION), metadataWriter, true);
        this.partitionSizeWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.PARTITION_TO_SIZE), metadataWriter, false);
        NumericValuesWriter partitionKeyBlockOffsetWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.PARTITION_KEY_BLOCK_OFFSETS), metadataWriter, true);
        this.partitionKeysWriter = new KeyStoreWriter(perSSTableComponents.addOrGet(IndexComponentType.PARTITION_KEY_BLOCKS),
                                                      metadataWriter,
                                                      partitionKeyBlockOffsetWriter,
                                                      partitionBlockShift,
                                                      false);
        if (perSSTableComponents.hasClustering())
        {
            NumericValuesWriter clusteringKeyBlockOffsetWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.CLUSTERING_KEY_BLOCK_OFFSETS), metadataWriter, true);
            this.clusteringKeysWriter = new KeyStoreWriter(perSSTableComponents.addOrGet(IndexComponentType.CLUSTERING_KEY_BLOCKS),
                                                           metadataWriter,
                                                           clusteringKeyBlockOffsetWriter,
                                                           clusteringBlockShift,
                                                           true);
        }
        else
        {
            this.clusteringKeysWriter = null;
        }
    }

    /**
     * Sets the partition block shift value. Primarily for testing and benchmarking.
     *
     * @param shift the block shift value
     */
    @VisibleForTesting
    public static void setPartitionBlockShift(int shift)
    {
        partitionBlockShift = shift;
    }

    /**
     * Sets the clustering block shift value. Primarily for testing and benchmarking.
     *
     * @param shift the block shift value
     */
    @VisibleForTesting
    public static void setClusteringBlockShift(int shift)
    {
        clusteringBlockShift = shift;
    }

    @Override
    public void startPartition(DecoratedKey partitionKey, long position) throws IOException
    {
        if (partitionId >= 0)
        {
            if (prevToken.compareTo(partitionKey.getToken()) >= 0)
                throw new IllegalArgumentException("Partition keys must be in ascending token order");

            partitionSizeWriter.add(partitionRowCount);
        }
        prevToken = partitionKey.getToken();

        partitionId++;
        partitionRowCount = 0;
        partitionKeysWriter.add(v -> ByteSource.of(partitionKey.getKey(), v));
        if (perSSTableComponents.hasClustering())
            clusteringKeysWriter.startPartition();
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        assert partitionId >= 0;

        tokenWriter.add(primaryKey.token().getLongValue());
        partitionRowsWriter.add(partitionId);
        partitionRowCount++;
        if (perSSTableComponents.hasClustering())
            clusteringKeysWriter.add(perSSTableComponents.comparator().asByteComparable(primaryKey.clustering()));
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        partitionSizeWriter.add(partitionRowCount);
        IOUtils.close(tokenWriter, partitionSizeWriter, partitionRowsWriter,
                      partitionKeysWriter, clusteringKeysWriter, metadataWriter);
        perSSTableComponents.markComplete();
    }

    @Override
    @SuppressWarnings("ThrowableNotThrown")
    public void abort(Throwable accumulator)
    {
        logger.debug(perSSTableComponents.logMessage("Aborting per-SSTable index component writer for {}..."), perSSTableComponents.descriptor());
        Throwables.close(accumulator, tokenWriter, partitionSizeWriter, partitionRowsWriter,
                         partitionKeysWriter, clusteringKeysWriter, metadataWriter);
        perSSTableComponents.forceDeleteAllComponents();
    }
}
