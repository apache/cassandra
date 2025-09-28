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

package org.apache.cassandra.db.view;

import java.io.IOException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadataRef;

/**
 * BackfillSink implementation that writes translated view rows to SSTables using SSTableSimpleUnsortedWriter.
 * The generated SSTables are placed in a "mv_backfill" subdirectory within the materialized view's data directory.
 */
public class MVBackfillSSTableSink implements MVBackfillManager.BackfillSink
{
    private static final Logger logger = LoggerFactory.getLogger(MVBackfillSSTableSink.class);
    private static final long DEFAULT_BUFFER_SIZE_MB = 128; // 128MB buffer size

    private final ColumnFamilyStore viewCfs;
    private final File backfillDirectory;
    private final SSTableSimpleUnsortedWriter writer;

    /**
     * Creates a new MVBackfillSSTableSink for the given materialized view.
     *
     * @param viewCfs the materialized view column family store
     * @throws IOException if there's an error creating the backfill directory or writer
     */
    public MVBackfillSSTableSink(ColumnFamilyStore viewCfs) throws IOException
    {
        this(viewCfs, DEFAULT_BUFFER_SIZE_MB);
    }

    /**
     * Creates a new MVBackfillSSTableSink for the given materialized view with custom buffer size.
     *
     * @param viewCfs the materialized view column family store
     * @param bufferSizeMB the buffer size in MB for the SSTableSimpleUnsortedWriter
     * @throws IOException if there's an error creating the backfill directory or writer
     */
    public MVBackfillSSTableSink(ColumnFamilyStore viewCfs, long bufferSizeMB) throws IOException
    {
        this.viewCfs = viewCfs;
        this.backfillDirectory = getBackfillDirectory(viewCfs);
        
        // Create the SSTableSimpleUnsortedWriter for the view
        TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(viewCfs.metadata.get());
        this.writer = new SSTableSimpleUnsortedWriter(backfillDirectory, metadataRef, 
                                                      viewCfs.metadata.get().regularAndStaticColumns(), 
                                                      bufferSizeMB);
        
        logger.info("Created MV backfill SSTable sink for view {}.{} writing to directory: {}", 
                   viewCfs.keyspace.getName(), viewCfs.name, backfillDirectory);
    }

    /**
     * Creates the mv_backfill subdirectory in the materialized view's data directory.
     */
    private static File getBackfillDirectory(ColumnFamilyStore viewCfs)
    {
        File viewDataDir = viewCfs.getDirectories().getDirectoryForNewSSTables();
        return Directories.getMVBackfillDirectory(viewDataDir);
    }

    @Override
    public void processViewRow(ViewRowTranslator.ViewRowResult viewResult) throws Exception
    {
        if (viewResult == null)
            return;

        // Get the partition update builder for this view partition key
        PartitionUpdate.Builder updateBuilder = writer.getUpdateFor(viewResult.viewPartitionKey);
        
        // Add the view row to the partition update
        updateBuilder.add(viewResult.viewRow);
    }

    @Override
    public void complete() throws Exception
    {
        try
        {
            writer.close();
            logger.info("Successfully completed MV backfill SSTable writing for view {}.{}", 
                       viewCfs.keyspace.getName(), viewCfs.name);
        }
        catch (Exception e)
        {
            logger.error("Error completing MV backfill SSTable writing for view {}.{}", 
                        viewCfs.keyspace.getName(), viewCfs.name, e);
            throw e;
        }
    }

    @Override
    public void fail(Exception e)
    {
        try
        {
            writer.close();
            logger.error("MV backfill SSTable writing failed for view {}.{}, cleaned up resources", 
                        viewCfs.keyspace.getName(), viewCfs.name, e);
        }
        catch (Exception cleanupException)
        {
            logger.error("Error during cleanup after MV backfill failure for view {}.{}", 
                        viewCfs.keyspace.getName(), viewCfs.name, cleanupException);
            // Suppress cleanup exception and let the original exception propagate
            e.addSuppressed(cleanupException);
        }
    }

    /**
     * Gets the directory where the backfill SSTables are being written.
     */
    public File getBackfillDirectory()
    {
        return backfillDirectory;
    }
}
