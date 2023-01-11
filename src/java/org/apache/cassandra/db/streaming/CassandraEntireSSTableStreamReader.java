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

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.big.BigTableZeroCopyWriter;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.File;
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
        if (streamHeader.format != SSTableFormat.Type.BIG)
            throw new AssertionError("Unsupported SSTable format " + streamHeader.format);

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
    @SuppressWarnings("resource") // input needs to remain open, streams on top of it can't be closed
    @Override
    public SSTableMultiWriter read(DataInputPlus in) throws Throwable
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

        BigTableZeroCopyWriter writer = null;

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

                writer.writeComponent(component.type, in, length);
                session.progress(writer.descriptor.filenameFor(component), ProgressInfo.Direction.IN, length, length, length);
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
                e = writer.abort(e);
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

    @SuppressWarnings("resource")
    protected BigTableZeroCopyWriter createWriter(ColumnFamilyStore cfs, long totalSize, Collection<Component> components) throws IOException
    {
        File dataDir = getDataDir(cfs, totalSize);

        StreamReceiver streamReceiver = session.getAggregator(tableId);
        assert streamReceiver instanceof CassandraStreamReceiver;

        LifecycleNewTracker lifecycleNewTracker = CassandraStreamReceiver.fromReceiver(session.getAggregator(tableId)).createLifecycleNewTracker();

        Descriptor desc = cfs.newSSTableDescriptor(dataDir, header.version, header.format);

        logger.debug("[Table #{}] {} Components to write: {}", cfs.metadata(), desc.filenameFor(Component.DATA), components);

        return new BigTableZeroCopyWriter(desc, cfs.metadata, lifecycleNewTracker, components);
    }
}
