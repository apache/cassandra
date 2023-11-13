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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.util.Collection;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.IOOptions;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.SSTableZeroCopyWriter;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReceiver;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;

import static java.lang.String.format;
import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

/**
 * CassandraEntireSSTableStreamReader reads SSTable off the wire and writes it to disk.
 */
public class CassandraEntireSSTableStreamReader implements IStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraEntireSSTableStreamReader.class);

    private final TableId tableId;
    private final StreamSession session;
    private final StreamMessageHeader messageHeader;
    private final CassandraStreamHeader header;
    private final int fileSequenceNumber;

    public CassandraEntireSSTableStreamReader(StreamMessageHeader messageHeader, CassandraStreamHeader streamHeader, StreamSession session)
    {
        if (session.getPendingRepair() != null)
        {
            // we should only ever be streaming pending repair sstables if the session has a pending repair id
            if (!session.getPendingRepair().equals(messageHeader.pendingRepair))
                throw new IllegalStateException(format("Stream Session & SSTable (%s) pendingRepair UUID mismatch.", messageHeader.tableId));
        }

        this.header = streamHeader;
        this.session = session;
        this.messageHeader = messageHeader;
        this.tableId = messageHeader.tableId;
        this.fileSequenceNumber = messageHeader.sequenceNumber;
    }

    /**
     * @param in where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @Override
    public SSTableMultiWriter read(DataInputPlus in) throws IOException
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);
        if (cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("Table " + tableId + " was dropped during streaming");
        }

        ComponentManifest manifest = header.componentManifest;
        long totalSize = manifest.totalSize();

        logger.debug("[Stream #{}] Started receiving sstable #{} from {}, size = {}, table = {}",
                     session.planId(),
                     fileSequenceNumber,
                     session.peer,
                     prettyPrintMemory(totalSize),
                     cfs.metadata());

        SSTableZeroCopyWriter writer = null;

        try
        {
            writer = createWriter(cfs, totalSize, manifest.components());
            long bytesRead = 0;
            for (Component component : manifest.components())
            {
                long length = manifest.sizeOf(component);

                logger.debug("[Stream #{}] Started receiving {} component from {}, componentSize = {}, readBytes = {}, totalSize = {}",
                             session.planId(),
                             component,
                             session.peer,
                             prettyPrintMemory(length),
                             prettyPrintMemory(bytesRead),
                             prettyPrintMemory(totalSize));

                writer.writeComponent(component, in, length);
                session.progress(writer.descriptor.fileFor(component).toString(), ProgressInfo.Direction.IN, length, length, length);
                bytesRead += length;

                logger.debug("[Stream #{}] Finished receiving {} component from {}, componentSize = {}, readBytes = {}, totalSize = {}",
                             session.planId(),
                             component,
                             session.peer,
                             prettyPrintMemory(length),
                             prettyPrintMemory(bytesRead),
                             prettyPrintMemory(totalSize));
            }

            UnaryOperator<StatsMetadata> transform = stats -> stats.mutateLevel(header.sstableLevel)
                                                                   .mutateRepairedMetadata(messageHeader.repairedAt, messageHeader.pendingRepair, false);
            String description = String.format("level %s and repairedAt time %s and pendingRepair %s",
                                               header.sstableLevel, messageHeader.repairedAt, messageHeader.pendingRepair);
            writer.descriptor.getMetadataSerializer().mutate(writer.descriptor, description, transform);
            return writer;
        }
        catch (Throwable e)
        {
            logger.error("[Stream {}] Error while reading sstable from stream for table = {}", session.planId(), cfs.metadata(), e);
            if (writer != null)
            {
                Throwable e2 = writer.abort(null);
                if (e2 != null)
                    e.addSuppressed(e2);
            }
            throw e;
        }
    }

    private File getDataDir(ColumnFamilyStore cfs, long totalSize) throws IOException
    {
        Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException(format("Insufficient disk space to store %s", prettyPrintMemory(totalSize)));

        File dir = cfs.getDirectories().getLocationForDisk(cfs.getDiskBoundaries().getCorrectDiskForKey(header.firstKey));

        if (dir == null)
            return cfs.getDirectories().getDirectoryForNewSSTables();

        return dir;
    }

    protected SSTableZeroCopyWriter createWriter(ColumnFamilyStore cfs, long totalSize, Collection<Component> components) throws IOException
    {
        File dataDir = getDataDir(cfs, totalSize);

        StreamReceiver streamReceiver = session.getAggregator(tableId);
        assert streamReceiver instanceof CassandraStreamReceiver;

        LifecycleNewTracker lifecycleNewTracker = CassandraStreamReceiver.fromReceiver(session.getAggregator(tableId)).createLifecycleNewTracker();

        Descriptor desc = cfs.newSSTableDescriptor(dataDir, header.version);

        IOOptions ioOptions = new IOOptions(DatabaseDescriptor.getDiskOptimizationStrategy(),
                                            DatabaseDescriptor.getDiskAccessMode(),
                                            DatabaseDescriptor.getIndexAccessMode(),
                                            DatabaseDescriptor.getDiskOptimizationEstimatePercentile(),
                                            SequentialWriterOption.newBuilder()
                                                                  .trickleFsync(false)
                                                                  .bufferSize(2 << 20)
                                                                  .bufferType(BufferType.OFF_HEAP)
                                                                  .build(),
                                            DatabaseDescriptor.getFlushCompression());

        logger.debug("[Table #{}] {} Components to write: {}", cfs.metadata(), desc, components);
        return desc.getFormat()
                   .getWriterFactory()
                   .builder(desc)
                   .setComponents(components)
                   .setTableMetadataRef(cfs.metadata)
                   .setIOOptions(ioOptions)
                   .createZeroCopyWriter(lifecycleNewTracker, cfs);
    }
}
