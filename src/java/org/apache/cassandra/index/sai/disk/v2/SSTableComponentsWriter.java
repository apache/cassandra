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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;

import com.google.common.base.Stopwatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.IOUtils;

public class SSTableComponentsWriter implements PerSSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final MetadataWriter metadataWriter;
    private final NumericValuesWriter tokenWriter;
    private final IndexOutputWriter trieWriter;
    private final IndexOutputWriter bytesWriter;
    private final NumericValuesWriter blockFPWriter;
    private final SortedTermsWriter sortedTermsWriter;

    public SSTableComponentsWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
        this.tokenWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES),
                                                   indexDescriptor.openPerSSTableOutput(IndexComponent.TOKEN_VALUES),
                                                   metadataWriter, false);
        this.trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_TRIE);
        this.bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCKS);
        this.blockFPWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS),
                                                     indexDescriptor.openPerSSTableOutput(IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS),
                                                     metadataWriter, true);
        this.sortedTermsWriter = new SortedTermsWriter(indexDescriptor.componentName(IndexComponent.PRIMARY_KEY_BLOCKS),
                                                       metadataWriter,
                                                       bytesWriter,
                                                       blockFPWriter,
                                                       trieWriter);
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        tokenWriter.add(primaryKey.token().getLongValue());
        sortedTermsWriter.add(v -> primaryKey.asComparableBytes(v));
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        try
        {
            indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
        }
        finally
        {
            IOUtils.close(tokenWriter, sortedTermsWriter, metadataWriter);
        }
    }

    @Override
    public void abort(Throwable accumulator)
    {
        logger.debug(indexDescriptor.logMessage("Aborting per-SSTable index component writer for {}..."), indexDescriptor.descriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }
}
