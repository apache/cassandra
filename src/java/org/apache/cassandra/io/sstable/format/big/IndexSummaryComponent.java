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

package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IndexSummaryComponent
{
    private static final Logger logger = LoggerFactory.getLogger(IndexSummaryComponent.class);

    public final IndexSummary indexSummary;
    public final DecoratedKey first;
    public final DecoratedKey last;

    public IndexSummaryComponent(IndexSummary indexSummary, DecoratedKey first, DecoratedKey last)
    {
        this.indexSummary = indexSummary;
        this.first = first;
        this.last = last;
    }

    /**
     * Load index summary, first key and last key from Summary.db file if it exists.
     * <p>
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and need to be rebuilt.
     */
    public static IndexSummaryComponent load(Descriptor descriptor, TableMetadata metadata) throws IOException
    {
        File summaryFile = descriptor.fileFor(Component.SUMMARY);
        if (!summaryFile.exists())
        {
            if (logger.isDebugEnabled())
                logger.debug("SSTable Summary File {} does not exist", summaryFile.absolutePath());
            return null;
        }

        IndexSummary summary = null;
        try (FileInputStreamPlus iStream = summaryFile.newInputStream())
        {
            summary = IndexSummary.serializer.deserialize(iStream,
                                                          metadata.partitioner,
                                                          metadata.params.minIndexInterval,
                                                          metadata.params.maxIndexInterval);
            DecoratedKey first = metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));
            DecoratedKey last = metadata.partitioner.decorateKey(ByteBufferUtil.readWithLength(iStream));

            return new IndexSummaryComponent(summary, first, last);
        }
        catch (IOException ex)
        {
            if (summary != null)
                summary.close();

            throw new IOException(String.format("Cannot deserialize SSTable %s component: %s", Component.SUMMARY.name, summaryFile), ex);
        }
    }

    public static IndexSummaryComponent loadOrDeleteCorrupted(Descriptor descriptor, TableMetadata metadata) throws IOException
    {
        try
        {
            return load(descriptor, metadata);
        }
        catch (IOException ex)
        {
            descriptor.fileFor(Component.SUMMARY).deleteIfExists();
            throw ex;
        }
    }

    /**
     * Save index summary to Summary.db file.
     */
    public void save(Descriptor descriptor) throws IOException
    {
        File summaryFile = descriptor.fileFor(Component.SUMMARY);
        if (summaryFile.exists())
            summaryFile.delete();

        try (DataOutputStreamPlus oStream = summaryFile.newOutputStream(File.WriteMode.OVERWRITE))
        {
            IndexSummary.serializer.serialize(indexSummary, oStream);
            ByteBufferUtil.writeWithLength(first.getKey(), oStream);
            ByteBufferUtil.writeWithLength(last.getKey(), oStream);
        }
        catch (IOException ex)
        {
            throw new IOException("Failed to save index summary for SSTable: " + descriptor.baseFilename(), ex);
        }
    }

    public void saveOrDeleteCorrupted(Descriptor descriptor) throws IOException
    {
        try
        {
            save(descriptor);
        }
        catch (IOException ex)
        {
            descriptor.fileFor(Component.SUMMARY).deleteIfExists();
            throw ex;
        }
    }
}
