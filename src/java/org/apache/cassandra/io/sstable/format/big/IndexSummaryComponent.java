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
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

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

    public static Pair<DecoratedKey, DecoratedKey> loadFirstAndLastKey(File summaryFile, IPartitioner partitioner) throws IOException
    {
        if (!summaryFile.exists())
        {
            if (logger.isDebugEnabled())
                logger.debug("Index summary {} does not exist", summaryFile.absolutePath());
            return null;
        }

        try (FileInputStreamPlus iStream = summaryFile.newInputStream())
        {
            return new IndexSummary.IndexSummarySerializer().deserializeFirstLastKey(iStream, partitioner);
        }
    }

    /**
     * Load index summary, first key and last key from Summary.db file if it exists.
     * <p>
     * if loaded index summary has different index interval from current value stored in schema,
     * then Summary.db file will be deleted and need to be rebuilt.
     */
    public static IndexSummaryComponent load(File summaryFile, TableMetadata metadata) throws IOException
    {
        if (!summaryFile.exists())
        {
            if (logger.isDebugEnabled())
                logger.debug("Index summary {} does not exist", summaryFile.absolutePath());
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

            throw new IOException(String.format("Cannot deserialize index summary from %s", summaryFile), ex);
        }
    }

    public static IndexSummaryComponent loadOrDeleteCorrupted(File summaryFile, TableMetadata metadata) throws IOException
    {
        try
        {
            return load(summaryFile, metadata);
        }
        catch (IOException ex)
        {
            summaryFile.deleteIfExists();
            throw ex;
        }
    }

    /**
     * Save index summary to Summary.db file.
     */
    public void save(File summaryFile, boolean deleteOnFailure) throws IOException
    {
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
            if (deleteOnFailure)
                summaryFile.deleteIfExists();
            throw new IOException("Failed to save index summary to " + summaryFile, ex);
        }
    }
}
