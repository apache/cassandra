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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigTableBlockWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamReceiver;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.Collectors3;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CassandraBlockStreamReader reads SSTable off the wire and writes it to disk.
 */
public class CassandraBlockStreamReader implements IStreamReader
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraBlockStreamReader.class);
    protected final TableId tableId;
    protected final StreamSession session;
    protected final int sstableLevel;
    protected final SerializationHeader.Component header;
    protected final int fileSeqNum;
    private final List<ComponentInfo> components;
    private final SSTableFormat.Type format;
    private final Version version;
    private final DecoratedKey firstKey;

    public CassandraBlockStreamReader(StreamMessageHeader header, CassandraStreamHeader streamHeader, StreamSession session)
    {
        if (session.getPendingRepair() != null)
        {
            // we should only ever be streaming pending repair
            // sstables if the session has a pending repair id
            if (!session.getPendingRepair().equals(header.pendingRepair))
                throw new IllegalStateException(String.format("Stream Session & SSTable ({}) pendingRepair UUID mismatch.",
                                                              header.tableId));
        }
        this.session = session;
        this.tableId = header.tableId;
        this.components = streamHeader.components;
        this.sstableLevel = streamHeader.sstableLevel;
        this.header = streamHeader.header;
        this.format = streamHeader.format;
        this.fileSeqNum = header.sequenceNumber;
        this.version = streamHeader.version;
        this.firstKey = streamHeader.firstKey;
    }

    /**
     * @param inputPlus where this reads data from
     * @return SSTable transferred
     * @throws IOException if reading the remote sstable fails. Will throw an RTE if local write fails.
     */
    @SuppressWarnings("resource") // input needs to remain open, streams on top of it can't be closed
    @Override
    public SSTableMultiWriter read(DataInputPlus inputPlus) throws IOException
    {
        long totalSize = totalSize();

        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(tableId);

        if (cfs == null)
        {
            // schema was dropped during streaming
            throw new IOException("CF " + tableId + " was dropped during streaming");
        }

        logger.debug("[Stream #{}] Start receiving file #{} from {}, size = {}, ks = '{}', table = '{}'.",
                     session.planId(), fileSeqNum, session.peer, totalSize, cfs.keyspace.getName(), cfs.getTableName());

        BigTableBlockWriter writer = null;

        try
        {
            Set<Component> componentsToWrite = components.stream()
                                                         .map(p -> Component.parse(p.type.repr))
                                                         .collect(Collectors3.toImmutableSet());

            writer = createWriter(cfs, totalSize, componentsToWrite);
            long bytesRead = 0;
            for (ComponentInfo info : components)
            {
                logger.debug("[Stream #{}] About to receive file {} from {} readBytes = {}, componentSize = {}, totalSize = {}",
                            session.planId(), info.type, session.peer, FBUtilities.prettyPrintMemory(bytesRead),
                            FBUtilities.prettyPrintMemory(info.length), FBUtilities.prettyPrintMemory(totalSize));
                writer.writeComponent(info.type, inputPlus, info.length);
                session.progress(writer.descriptor.filenameFor(Component.parse(info.type.repr)), ProgressInfo.Direction.IN, info.length, info.length);
                bytesRead += info.length;
                logger.debug("[Stream #{}] Finished receiving file {} from {} readBytes = {}, componentSize = {}, totalSize = {}",
                            session.planId(), info.type, session.peer, FBUtilities.prettyPrintMemory(bytesRead),
                            FBUtilities.prettyPrintMemory(info.length), FBUtilities.prettyPrintMemory(totalSize));
            }

            return writer;
        }
        catch (Throwable e)
        {
            logger.error("[Stream {}] Error while reading from stream on ks='{}' and table='{}'.",
                        session.planId(), cfs.keyspace.getName(), cfs.getTableName(), e);
            if (writer != null)
            {
                writer.abort(e);
            }
            throw Throwables.propagate(e);
        }
    }

    private File getDataDir(ColumnFamilyStore cfs, long totalSize) throws IOException
    {
        Directories.DataDirectory localDir = cfs.getDirectories().getWriteableLocation(totalSize);
        if (localDir == null)
            throw new IOException(String.format("Insufficient disk space to store %s", FBUtilities.prettyPrintMemory(totalSize)));

        File dir = cfs.getDirectories().getLocationForDisk(cfs.getDiskBoundaries().getCorrectDiskForKey(firstKey));

        if (dir == null)
            return cfs.getDirectories().getDirectoryForNewSSTables();

        return dir;
    }

    @SuppressWarnings("resource")
    protected BigTableBlockWriter createWriter(ColumnFamilyStore cfs, long totalSize, Set<Component> componentsToWrite) throws IOException
    {
        File dataDir = getDataDir(cfs, totalSize);

        StreamReceiver streamReceiver = session.getAggregator(tableId);
        assert streamReceiver instanceof CassandraStreamReceiver;

        LifecycleTransaction txn = CassandraStreamReceiver.fromReceiver(session.getAggregator(tableId)).getTransaction();

        Descriptor desc = cfs.newSSTableDescriptor(dataDir, version, format);

        logger.debug("[Table #{}] {} Components to write - {}", tableId, desc.filenameFor(Component.DATA), componentsToWrite);
        BigTableBlockWriter writer = new BigTableBlockWriter(desc, cfs.metadata, txn, componentsToWrite);
        return writer;
    }

    protected long totalSize()
    {
        long size = 0;
        for (ComponentInfo component : components)
            size += component.length;
        return size;
    }
}
