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

import org.apache.cassandra.index.sai.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.IOUtils;

public class SSTableComponentsWriter implements PerSSTableIndexWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final MetadataWriter metadataWriter;
    private final NumericValuesWriter partitionWriter;
    private final NumericValuesWriter tokenWriter;
    private final SortedTermsWriter sortedTermsWriter;
    private long partitionId = -1;

    @SuppressWarnings({"resource", "RedundantSuppression"})
    public SSTableComponentsWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
        this.tokenWriter = NumericValuesWriter.create(indexDescriptor, IndexComponent.TOKEN_VALUES, metadataWriter, false);
        this.partitionWriter = indexDescriptor.hasClustering() ? NumericValuesWriter.create(indexDescriptor, IndexComponent.PARTITION_SIZES, metadataWriter, true)
                                                               : NumericValuesWriter.NOOP_WRITER;
        IndexOutputWriter primaryKeyBlocksWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCKS);
        NumericValuesWriter primaryKeyBlockOffsetWriter = NumericValuesWriter.create(indexDescriptor, IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS, metadataWriter, true);
        this.sortedTermsWriter = new SortedTermsWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS),
                                                       metadataWriter,
                                                       primaryKeyBlocksWriter,
                                                       primaryKeyBlockOffsetWriter);
    }

    @Override
    public void startPartition()
    {
        partitionId++;
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        tokenWriter.add(primaryKey.token().getLongValue());
        sortedTermsWriter.add(primaryKey);
        partitionWriter.add(partitionId);
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
            IOUtils.close(tokenWriter, partitionWriter, sortedTermsWriter, metadataWriter);
        }
    }

    @Override
    public void abort()
    {
        logger.debug(indexDescriptor.logMessage("Aborting per-SSTable index component writer for {}..."), indexDescriptor.sstableDescriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }
}
