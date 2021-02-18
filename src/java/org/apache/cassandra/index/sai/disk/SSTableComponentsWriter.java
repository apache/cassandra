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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesWriter;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.util.IOUtils;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public class SSTableComponentsWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(SSTableComponentsWriter.class);

    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter offsetWriter;
    private final MetadataWriter metadataWriter;

    private final Descriptor descriptor;
    private final IndexComponents indexComponents;

    private DecoratedKey currentKey;

    private long currentKeyPartitionOffset;

    public SSTableComponentsWriter(Descriptor descriptor, CompressionParams compressionParams) throws IOException
    {
        this.descriptor = descriptor;

        indexComponents = IndexComponents.perSSTable(descriptor, compressionParams);
        this.metadataWriter = new MetadataWriter(indexComponents.createOutput(IndexComponents.GROUP_META));

        this.tokenWriter = new NumericValuesWriter(IndexComponents.TOKEN_VALUES,
                                                   indexComponents.createOutput(IndexComponents.TOKEN_VALUES),
                                                   metadataWriter, false);
        this.offsetWriter = new NumericValuesWriter(IndexComponents.OFFSETS_VALUES,
                                                    indexComponents.createOutput(IndexComponents.OFFSETS_VALUES),
                                                    metadataWriter, true);
    }

    private SSTableComponentsWriter()
    {
        this.descriptor = null;
        this.indexComponents = null;
        this.metadataWriter = null;
        this.tokenWriter = null;
        this.offsetWriter = null;
    }

    public void startPartition(DecoratedKey key, long position)
    {
        currentKey = key;
        currentKeyPartitionOffset = position;
    }

    public void nextUnfilteredCluster(Unfiltered unfiltered, long position) throws IOException
    {
        recordCurrentTokenOffset();
    }

    public void staticRow(Row staticRow, long position) throws IOException
    {
        recordCurrentTokenOffset();
    }

    private void recordCurrentTokenOffset() throws IOException
    {
        recordCurrentTokenOffset((long) currentKey.getToken().getTokenValue(), currentKeyPartitionOffset);
    }

    @VisibleForTesting
    public void recordCurrentTokenOffset(long tokenValue, long keyOffset) throws IOException
    {
        tokenWriter.add(tokenValue);
        offsetWriter.add(keyOffset);
    }

    public void complete() throws IOException
    {
        IOUtils.close(tokenWriter, offsetWriter, metadataWriter);
        indexComponents.createGroupCompletionMarker();
    }

    public void abort(Throwable accumulator)
    {
        logger.debug(indexComponents.logMessage("Aborting token/offset writer for {}..."), descriptor);
        IndexComponents.deletePerSSTableIndexComponents(descriptor);
    }

    public static final SSTableComponentsWriter NONE = new SSTableComponentsWriter() {

        @Override
        public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
        {
        }

        @Override
        public void startPartition(DecoratedKey key, long position)
        {
        }

        @Override
        public void staticRow(Row staticRow, long position)
        {
        }

        @Override
        public void complete()
        {
        }

        @Override
        public void abort(Throwable accumulate)
        {
        }
    };
}
