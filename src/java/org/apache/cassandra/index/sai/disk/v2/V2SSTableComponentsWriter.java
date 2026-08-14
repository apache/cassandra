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
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.util.IOUtils;

public class V2SSTableComponentsWriter implements PerSSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(V2SSTableComponentsWriter.class);

    private final IndexComponents.ForWrite perSSTableComponents;
    private final MetadataWriter metadataWriter;
    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter blockFPWriter;
    private final SortedTermsWriter sortedTermsWriter;

    public V2SSTableComponentsWriter(IndexComponents.ForWrite perSSTableComponents) throws IOException
    {
        this.perSSTableComponents = perSSTableComponents;
        this.metadataWriter = new MetadataWriter(perSSTableComponents);
        this.tokenWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.TOKEN_VALUES),
                                                   metadataWriter, false);

        this.blockFPWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCK_OFFSETS),
                                                     metadataWriter, true);
        this.sortedTermsWriter = new SortedTermsWriter(perSSTableComponents.addOrGet(IndexComponentType.PRIMARY_KEY_BLOCKS),
                                                       metadataWriter,
                                                       blockFPWriter,
                                                       perSSTableComponents.addOrGet(IndexComponentType.PRIMARY_KEY_TRIE));
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
        IOUtils.close(tokenWriter, sortedTermsWriter, metadataWriter);
        perSSTableComponents.markComplete();
    }

    @Override
    @SuppressWarnings("ThrowableNotThrown")
    public void abort(Throwable accumulator)
    {
        logger.debug(perSSTableComponents.logMessage("Aborting per-SSTable index component writer for {}..."), perSSTableComponents.descriptor());
        Throwables.close(accumulator, tokenWriter, sortedTermsWriter, metadataWriter);
        perSSTableComponents.forceDeleteAllComponents();
    }
}
