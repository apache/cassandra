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
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataSnapshots;
import org.apache.cassandra.tcm.Period;
import org.apache.cassandra.tcm.Transformation;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.db.SystemKeyspace.LocalMetadataLog;
import static org.apache.cassandra.tcm.Epoch.FIRST;

public class SystemKeyspaceStorage implements LogStorage
{
    private static final Logger logger = LoggerFactory.getLogger(SystemKeyspaceStorage.class);
    public static final String NAME =  org.apache.cassandra.db.SystemKeyspace.METADATA_LOG;

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     * <p>
     * gen 0: original definition in 5.0
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
    public void append(long period, Entry entry)
    {
        try
        {
            // TODO get lowest supported metadata version from ClusterMetadata
            ByteBuffer serializedTransformation = entry.transform.kind().toVersionedBytes(entry.transform);
            String query = String.format("INSERT INTO %s.%s (period, epoch, current_epoch, entry_id, transformation, kind) VALUES (?,?,?,?,?,?)",
                                         SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME);
            executeInternal(query, period, entry.epoch.getEpoch(), entry.epoch.getEpoch(),
                            entry.id.entryId, serializedTransformation, entry.transform.kind().toString());
            // todo; should probably not flush every time, but it simplifies tests
            Keyspace.open(SchemaConstants.SYSTEM_KEYSPACE_NAME).getColumnFamilyStore(NAME).forceBlockingFlush(ColumnFamilyStore.FlushReason.INTERNALLY_FORCED);
        }
        catch (Throwable t)
        {
            logger.error("Could not persist the entry {} proceeding with in-memory commit.", entry, t);
        }
    }

    public synchronized static boolean hasFirstEpoch()
    {
        String query = String.format("SELECT epoch FROM %s.%s WHERE period = ? and epoch > ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME);

        for (UntypedResultSet.Row row : executeInternal(query, Period.FIRST, FIRST.getEpoch()))
            return true;

        return false;
    }

    public LogState getLogState(Epoch since)
    {
        return LogState.getLogState(since, snapshots.get(), this);
    }

    /**
     * Uses the supplied period as a starting point to iterate through the log table
     * collating log entries which follow the supplied epoch. It is assumed that the
     * target epoch is found in the starting period, so any entries returned will be
     * from either the starting period or subsequent periods.
     * @param since target epoch
     * @return contiguous list of log entries which follow the given epoch,
     *         which may be empty
     * @param startPeriod
     * @param since
     * @return
     */
    public Replication getReplication(long startPeriod, Epoch since)
    {
        try
        {
            if (startPeriod == Period.EMPTY)
            {
                startPeriod = Period.scanLogForPeriod(LocalMetadataLog, since);
                if (startPeriod == Period.EMPTY)
                    return Replication.EMPTY;
            }

            long period = startPeriod;
            ImmutableList.Builder<Entry> entries = new ImmutableList.Builder<>();
            while (true)
            {
                boolean empty = true;

                UntypedResultSet resultSet = executeInternal(String.format("SELECT epoch, kind, transformation, entry_id FROM %s.%s WHERE period = ? and epoch > ?", SchemaConstants.SYSTEM_KEYSPACE_NAME, NAME),
                                                             period, since.getEpoch());

                for (UntypedResultSet.Row row : resultSet)
                {
                    long entryId = row.getLong("entry_id");
                    Epoch epoch = Epoch.create(row.getLong("epoch"));
                    ByteBuffer transformationBlob = row.getBlob("transformation");
                    Transformation.Kind kind = Transformation.Kind.valueOf(row.getString("kind"));
                    Transformation transform = kind.fromVersionedBytes(transformationBlob);
                    kind.fromVersionedBytes(transformationBlob);
                    entries.add(new Entry(new Entry.Id(entryId), epoch, transform));
                    empty = false;
                }

                if (period != startPeriod && empty)
                    break;

                period++;
            }

            return new Replication(entries.build());
        }
        catch (Exception e)
        {
            logger.error("Could not restore the state.", e);
            throw new RuntimeException(e);
        }
    }
}