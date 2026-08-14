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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.util.IOUtils;

/**
 * Writes all SSTable-attached index token and offset structures.
 */
public class V1SSTableComponentsWriter implements PerSSTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(V1SSTableComponentsWriter.class);

    private final IndexComponents.ForWrite perSSTableComponents;
    private final NumericValuesWriter tokenWriter;
    private final NumericValuesWriter offsetWriter;
    private final MetadataWriter metadataWriter;

    private long currentKeyPartitionOffset;

    public V1SSTableComponentsWriter(IndexComponents.ForWrite perSSTableComponents) throws IOException
    {
        this.perSSTableComponents = perSSTableComponents;
        this.metadataWriter = new MetadataWriter(perSSTableComponents);
        this.tokenWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.TOKEN_VALUES),
                                                   metadataWriter, false);
        this.offsetWriter = new NumericValuesWriter(perSSTableComponents.addOrGet(IndexComponentType.OFFSETS_VALUES),
                                                    metadataWriter, true);
    }

    @Override
    public void startPartition(DecoratedKey decoratedKey, long position)
    {
        currentKeyPartitionOffset = position;
    }

    @Override
    public void nextRow(PrimaryKey primaryKey) throws IOException
    {
        recordCurrentTokenOffset(primaryKey.token().getLongValue(), currentKeyPartitionOffset);
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        IOUtils.close(tokenWriter, offsetWriter, metadataWriter);
        perSSTableComponents.markComplete();
    }

    @Override
    @SuppressWarnings("ThrowableNotThrown")
    public void abort(Throwable accumulator)
    {
        logger.debug(perSSTableComponents.logMessage("Aborting token/offset writer for {}..."), perSSTableComponents.descriptor());
        Throwables.close(accumulator, tokenWriter, offsetWriter, metadataWriter);
        perSSTableComponents.forceDeleteAllComponents();
    }

    @VisibleForTesting
    public void recordCurrentTokenOffset(long tokenValue, long keyOffset) throws IOException
    {
        tokenWriter.add(tokenValue);
        offsetWriter.add(keyOffset);
    }
}
