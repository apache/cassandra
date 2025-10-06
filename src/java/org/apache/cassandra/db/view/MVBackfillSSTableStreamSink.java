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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableSimpleUnsortedWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.cassandra.streaming.StreamOperation.MV_BACKFILL;

/**
 * BackfillSink implementation that writes translated view rows to SSTables using SSTableSimpleUnsortedWriter
 * and streams them to appropriate replica nodes.
 * 
 * The generated SSTables are placed in a "mv_backfill" subdirectory within the materialized view's data directory,
 * and then streamed to the correct replicas based on the cluster topology.
 */
public class MVBackfillSSTableStreamSink implements MVBackfillManager.BackfillSink, StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(MVBackfillSSTableStreamSink.class);
    private static final long DEFAULT_BUFFER_SIZE_MB = 128; // 128MB buffer size
    private static final int DEFAULT_CONNECTIONS_PER_HOST = 1;

    protected final ColumnFamilyStore viewCfs;
    private final File backfillDirectory;
    private final SSTableSimpleUnsortedWriter writer;
    private final int connectionsPerHost;
    private final Set<InetAddressAndPort> failedHosts = new HashSet<>();
    
    // Streaming state
    private final List<SSTableReader> generatedSSTables = new ArrayList<>();
    private final Multimap<InetAddressAndPort, OutgoingStream> streamingDetails = HashMultimap.create();

    /**
     * Creates a new MVBackfillSSTableSink for the given materialized view.
     *
     * @param viewCfs the materialized view column family store
     * @throws IOException if there's an error creating the backfill directory or writer
     */
    public MVBackfillSSTableStreamSink(ColumnFamilyStore viewCfs) throws IOException
    {
        this(viewCfs, DEFAULT_BUFFER_SIZE_MB, DEFAULT_CONNECTIONS_PER_HOST);
    }

    /**
     * Creates a new MVBackfillSSTableSink for the given materialized view with custom buffer size.
     *
     * @param viewCfs the materialized view column family store
     * @param bufferSizeMB the buffer size in MB for the SSTableSimpleUnsortedWriter
     * @throws IOException if there's an error creating the backfill directory or writer
     */
    public MVBackfillSSTableStreamSink(ColumnFamilyStore viewCfs, long bufferSizeMB) throws IOException
    {
        this(viewCfs, bufferSizeMB, DEFAULT_CONNECTIONS_PER_HOST);
    }

    /**
     * Creates a new MVBackfillSSTableSink for the given materialized view with custom settings.
     *
     * @param viewCfs the materialized view column family store
     * @param bufferSizeMB the buffer size in MB for the SSTableSimpleUnsortedWriter
     * @param connectionsPerHost the number of connections per host for streaming
     * @throws IOException if there's an error creating the backfill directory or writer
     */
    public MVBackfillSSTableStreamSink(ColumnFamilyStore viewCfs, long bufferSizeMB, int connectionsPerHost) throws IOException
    {
        this.viewCfs = viewCfs;
        this.connectionsPerHost = connectionsPerHost;
        this.backfillDirectory = getBackfillDirectory(viewCfs);
        
        // Create the SSTableSimpleUnsortedWriter for the view
        TableMetadataRef metadataRef = TableMetadataRef.forOfflineTools(viewCfs.metadata.get());
        this.writer = new SSTableSimpleUnsortedWriter(backfillDirectory, metadataRef, 
                                                      viewCfs.metadata.get().regularAndStaticColumns(), 
                                                      bufferSizeMB);
        
        logger.info("Created MV backfill SSTable sink for view {}.{} writing to directory: {} with {} connections per host", 
                   viewCfs.keyspace.getName(), viewCfs.name, backfillDirectory, connectionsPerHost);
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
    public void postRowProcess(Collection<Range<Token>> baseTableRanges) throws Exception
    {
        try
        {
            logger.info("Starting MV backfill streaming for view {}.{} with base table ranges: {}",
                       viewCfs.keyspace.getName(), viewCfs.name, baseTableRanges);

            // 1. Open generated SSTables and prepare for streaming
            Collection<SSTableReader> sstables = openGeneratedSSTables();
            if (sstables.isEmpty())
            {
                logger.info("No SSTables generated for MV backfill of view {}.{} in base table ranges: {}",
                           viewCfs.keyspace.getName(), viewCfs.name, baseTableRanges);
                return;
            }

            // 2. Stream the SSTables to replica nodes
            Future<StreamState> streamingFuture = streamToReplicas(sstables);
            
            // 3. Wait for streaming to complete
            StreamState finalState = streamingFuture.get();
            
            if (finalState.hasFailedSession())
            {
                throw new RuntimeException("MV backfill streaming failed for some replicas: " + failedHosts);
            }

            logger.info("Successfully completed MV backfill streaming for view {}.{} base table ranges: {}",
                       viewCfs.keyspace.getName(), viewCfs.name, baseTableRanges);
        }
        finally
        {
            // Clean up resources
            cleanup();
        }
    }

    @Override
    public void rowProcessComplete() throws Exception
    {
        try
        {
            // TODO: Persist the filenames generated in system table, if retry is needed, we don't need to scan the base table again
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
    public void complete() throws Exception
    {
        deleteMVBackfillFiles();
    }

    private void deleteMVBackfillFiles()
    {
        logger.info("MV backfill finished for view {}.{}, deleting the generated files.", viewCfs.keyspace.getName(), viewCfs.name);
        File mvBackfillDir = getBackfillDirectory();
        // make sure the MV backfill dir is not pointing to some critical directories
        assert mvBackfillDir.isDirectory() && mvBackfillDir.absolutePath().contains(Directories.MV_BACKFILL_SUBDIR);
        mvBackfillDir.deleteRecursive();
    }


    @Override
    public void fail(Exception e)
    {
        try
        {
            writer.close();
            logger.error("MV backfill SSTable writing or streaming failed for view {}.{}.",
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

    /**
     * Opens the generated SSTables from the backfill directory and prepares them for streaming.
     */
    @VisibleForTesting
    public Collection<SSTableReader> openGeneratedSSTables() throws IOException
    {
        logger.info("Opening generated SSTables for streaming from directory: {}", backfillDirectory);

        // Get endpoint to ranges mapping for the materialized view
        Keyspace keyspace = Keyspace.open(viewCfs.keyspace.getName());
        RangesByEndpoint rangesByEndpoint = keyspace.getReplicationStrategy().getAddressReplicas();

        // Scan the backfill directory for SSTable files
        List<File> dataFiles = new ArrayList<>();
        if (!backfillDirectory.exists())
        {
            logger.warn("Backfill directory does not exist: {}", backfillDirectory);
            return Collections.emptyList();
        }

        for (String fileName : writer.getGeneratedFileNames())
        {
            File file = new File(fileName);
            if (file.exists())
            {
                dataFiles.add(file);
            }
            else
            {
                throw new RuntimeException("Not able to find generated file: " + fileName);
            }
        }

        for (File dataFile : dataFiles)
        {
            Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dataFile);
            if (p == null)
                continue;

            Descriptor desc = p.left;
            
            // Verify required components exist
            if (!new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists())
            {
                throw new RuntimeException("SSTable " + dataFile.name() + " is corrupted because index is missing");
            }

            try
            {
                // Open SSTable for streaming (without bloom filters to conserve memory)
                Set<Component> components = getRequiredComponents(desc);
                SSTableReader sstable = SSTableReader.openForBatch(desc, components, viewCfs.metadata);
                generatedSSTables.add(sstable);

                // Calculate streaming sections for each endpoint
                calculateStreamingSections(sstable, rangesByEndpoint);

                // Release summary to conserve memory
                sstable.releaseSummary();
            }
            catch (Exception e)
            {
                logger.error("Error opening SSTable {} for streaming", dataFile.name(), e);
                throw new IOException("Failed to open generated SSTable for streaming", e);
            }
        }

        return generatedSSTables;
    }

    /**
     * Gets the required SSTable components for streaming.
     */
    private Set<Component> getRequiredComponents(Descriptor desc)
    {
        Set<Component> components = new HashSet<>();
        components.add(Component.DATA);
        components.add(Component.PRIMARY_INDEX);
        
        if (new File(desc.filenameFor(Component.SUMMARY)).exists())
            components.add(Component.SUMMARY);
        if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists())
            components.add(Component.COMPRESSION_INFO);
        if (new File(desc.filenameFor(Component.STATS)).exists())
            components.add(Component.STATS);
            
        return components;
    }

    /**
     * Calculates which sections of the SSTable to stream to each endpoint.
     */
    private void calculateStreamingSections(SSTableReader sstable,
                                            RangesByEndpoint rangesByEndpoint)
    {
        for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> entry : rangesByEndpoint.asMap().entrySet())
        {
            InetAddressAndPort endpoint = entry.getKey();
            List<Range<Token>> tokenRanges = Range.normalize(entry.getValue().ranges());

            List<SSTableReader.PartitionPositionBounds> sstableSections = sstable.getPositionsForRanges(tokenRanges);
            
            // Skip endpoints that don't own any part of this SSTable
            if (sstableSections.isEmpty())
                continue;

            long estimatedKeys = sstable.estimatedKeysForRanges(tokenRanges);
            Ref<SSTableReader> ref = sstable.ref();
            OutgoingStream stream = new CassandraOutgoingFile(MV_BACKFILL, ref,
                                                              sstableSections, tokenRanges, estimatedKeys);
            streamingDetails.put(endpoint, stream);
        }
    }

    /**
     * Streams the SSTables to replica nodes using Cassandra's streaming infrastructure.
     */
    private Future<StreamState> streamToReplicas(Collection<SSTableReader> sstables) throws Exception
    {
        logger.info("Streaming {} SSTables for MV {}.{} to {} endpoints",
                   sstables.size(), viewCfs.keyspace.getName(), viewCfs.name, streamingDetails.keySet().size());

        StreamPlan plan = new StreamPlan(MV_BACKFILL, connectionsPerHost, false, null, PreviewKind.NONE);

        // Add streams for each endpoint
        for (Map.Entry<InetAddressAndPort, Collection<OutgoingStream>> entry : streamingDetails.asMap().entrySet())
        {
            InetAddressAndPort remote = entry.getKey();
            Collection<OutgoingStream> streams = entry.getValue();
            
            if (!streams.isEmpty())
            {
                plan.transferStreams(remote, streams);
            }
        }

        // Add this sink as a stream event handler
        plan.listeners(this);

        // Execute the streaming plan
        StreamResultFuture future = plan.execute();
        return future;
    }

    @Override
    public void handleStreamEvent(StreamEvent event)
    {
        if (event.eventType == StreamEvent.Type.STREAM_COMPLETE)
        {
            StreamEvent.SessionCompleteEvent se = (StreamEvent.SessionCompleteEvent) event;
            if (!se.success)
            {
                failedHosts.add(se.peer);
                logger.error("Streaming failed to endpoint {} for MV backfill of view {}.{}", 
                           se.peer, viewCfs.keyspace.getName(), viewCfs.name);
            }
            else
            {
                logger.debug("Streaming completed successfully to endpoint {} for MV backfill of view {}.{}", 
                           se.peer, viewCfs.keyspace.getName(), viewCfs.name);
            }
        }
    }

    /**
     * Cleans up resources and temporary files.
     */
    private void cleanup()
    {
        try
        {
            // Release SSTable references
            for (SSTableReader sstable : generatedSSTables)
            {
                if (sstable.selfRef() != null)
                {
                    sstable.selfRef().release();
                }
            }

            // Clear collections
            generatedSSTables.clear();
            streamingDetails.clear();

            logger.debug("Cleaned up MV backfill streaming resources for view {}.{}", 
                        viewCfs.keyspace.getName(), viewCfs.name);
        }
        catch (Exception e)
        {
            logger.warn("Error during cleanup of MV backfill streaming resources for view {}.{}", 
                       viewCfs.keyspace.getName(), viewCfs.name, e);
        }
    }

    /**
     * Gets the set of hosts that failed during streaming.
     * 
     * @return set of failed host endpoints
     */
    public Set<InetAddressAndPort> getFailedHosts()
    {
        return failedHosts;
    }

    @Override
    public void onSuccess(@Nullable StreamState streamState)
    {
        logger.info("MV backfill succeeded for view {}.{}", viewCfs.keyspace.getName(), viewCfs.name);
    }

    @Override
    public void onFailure(Throwable throwable)
    {
        logger.info("MV backfill failed for view {}.{}", viewCfs.keyspace.getName(), viewCfs.name, throwable);
    }
}
