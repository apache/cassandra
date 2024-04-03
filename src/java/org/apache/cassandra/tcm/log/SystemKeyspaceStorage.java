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

package org.apache.cassandra.tcm.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Transformation;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class SystemKeyspaceStorage implements LogStorage
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspaceStorage.class);
    public static final String NAME =  org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     * <p>
     * gen 0: original definition in 5.1
     */
    public static final long GENERATION = 0;

    private final Supplier<MetadataSnapshots> snapshots;

    public SystemKeyspaceStorage()
    {
        this(() -> ClusterMetadataService.instance().snapshotManager());
    }

    @VisibleForTesting
    public SystemKeyspaceStorage(Supplier<MetadataSnapshots> snapshots)
    {
        this.snapshots = snapshots;
    }

    // This method is always called from a single thread, so doesn't have to be synchonised.
    public void append(Entry entry)
    {
        try
        {
            // TODO get lowest supported metadata version from ClusterMetadata
            ByteBuffer serializedTransformation = entry.transform.kind().toVersionedBytes(entry.transform);
            String query = String.format("INSERT INTO %s.%s (epoch, entry_id, transformation, kind) VALUES (?,?,?,?)",
                                         SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME);
            executeInternal(query, entry.epoch.getEpoch(), entry.id.entryId, serializedTransformation, entry.transform.kind().id);
            // todo; should probably not flush every time, but it simplifies tests
            Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(NAME).forceBlockingFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED);
        }
        catch (Throwable t)
        {
            logger.error("Could not persist the entry {} proceeding with in-memory commit.", entry, t);
        }
    }

    public synchronized static boolean hasAnyEpoch()
    {
        String query = String.format("SELECT epoch FROM %s.%s LIMIT 1", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME);

        for (UntypedResultSet.Row row : executeInternal(query))
            return true;

        return false;
    }

    @Override
    public MetadataSnapshots snapshots()
    {
        return snapshots.get();
    }

    public void truncate()
    {
        Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(NAME).truncateBlockingWithoutSnapshot();
    }

    /**
     * Gets the persisted log state for replaying log on startup
     *
     * Slow, only to be used on startup
     *
     * @return
     */
    @Override
    public LogState getPersistedLogState()
    {
        ClusterMetadata base = snapshots.get().getLatestSnapshot();
        return getLogStateBetween(base, Epoch.create(Long.MAX_VALUE));
    }

    @Override
    public EntryHolder getEntries(Epoch since) throws IOException
    {
        // during gossip upgrade we have epoch = Long.MIN_VALUE + 1 (and the reverse partitioner doesn't support negative keys)
        since = since.isBefore(Epoch.EMPTY) ? Epoch.EMPTY : since;
        UntypedResultSet resultSet = executeInternal(String.format("SELECT epoch, kind, transformation, entry_id FROM %s.%s WHERE token(epoch) <= token(?)", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME),
                                                     since.getEpoch());
        return toEntryHolder(since, resultSet);
    }

    public EntryHolder getEntries(Epoch since, Epoch until) throws IOException
    {
        // during gossip upgrade we have epoch = Long.MIN_VALUE + 1 (and the reverse partitioner doesn't support negative keys)
        since = since.isBefore(Epoch.EMPTY) ? Epoch.EMPTY : since;
        UntypedResultSet resultSet = executeInternal(String.format("SELECT epoch, kind, transformation, entry_id " +
                                                                   "FROM %s.%s " +
                                                                   "WHERE token(epoch) <= token(?) AND token(epoch) >= token(?)",
                                                                   SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME),
                                                     since.getEpoch(), until.getEpoch());
        return toEntryHolder(since, resultSet);
    }

    private static EntryHolder toEntryHolder(Epoch since, UntypedResultSet resultSet) throws IOException
    {
        EntryHolder holder = new EntryHolder(since);
        for (UntypedResultSet.Row row : resultSet)
        {
            long entryId = row.getLong("entry_id");
            Epoch epoch = Epoch.create(row.getLong("epoch"));
            Transformation.Kind kind = Transformation.Kind.fromId(row.getInt("kind"));
            Transformation transform = kind.fromVersionedBytes(row.getBlob("transformation"));
            holder.add(new Entry(new Entry.Id(entryId), epoch, transform));
        }
        return holder;
    }

    @Override
    public LogState getLogStateBetween(ClusterMetadata base, Epoch end)
    {
        try
        {
            Epoch epoch = base == null ? Epoch.EMPTY : base.epoch;
            EntryHolder entryHolder = getEntries(epoch, end);
            ImmutableList.Builder<Entry> entries = ImmutableList.builder();
            Epoch prevEpoch = epoch;
            for (Entry e : entryHolder.entries)
            {
                if (!prevEpoch.nextEpoch().is(e.epoch))
                    throw new IllegalStateException("Can't get replication between " + epoch + " and " + end + " - incomplete local log?");
                prevEpoch = e.epoch;
                entries.add(e);
            }
            return new LogState(base, entries.build());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}