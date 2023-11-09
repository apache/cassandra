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

package org.apache.cassandra.service.paxos.uncommitted;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.cleanup.PaxosTableRepairs;
import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.config.CassandraRelevantProperties.AUTO_REPAIR_FREQUENCY_SECONDS;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_PAXOS_AUTO_REPAIRS;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_PAXOS_STATE_FLUSH;
import static org.apache.cassandra.config.DatabaseDescriptor.paxosRepairEnabled;
import static org.apache.cassandra.service.paxos.uncommitted.PaxosKeyState.mergeUncommitted;

/**
 * Tracks uncommitted paxos operations to enable operation completion as part of repair by returning an iterator of
 * partition keys with uncommitted paxos operations (and their consistency levels) for a given table and token range(s)
 *
 * There are 2 parts to the uncommitted states it tracks: operations flushed to disk, and updates still in memory. This
 * class handles merging these two sources for queries and for merging states as part of flush. In practice, in memory
 * updates are the contents of the system.paxos memtables, although this has been generalized into an "UpdateSupplier"
 * interface to accomodate testing.
 */
public class PaxosUncommittedTracker
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosUncommittedTracker.class);
    private static final Range<Token> FULL_RANGE = new Range<>(DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                               DatabaseDescriptor.getPartitioner().getMinimumToken());

    private static volatile UpdateSupplier updateSupplier;

    private volatile boolean autoRepairsEnabled = !DISABLE_PAXOS_AUTO_REPAIRS.getBoolean();
    private volatile boolean stateFlushEnabled = !DISABLE_PAXOS_STATE_FLUSH.getBoolean();

    private boolean started = false;
    private boolean autoRepairStarted = false;

    private final Set<TableId> autoRepairTableIds = Sets.newConcurrentHashSet();

    public interface UpdateSupplier
    {
        CloseableIterator<PaxosKeyState> repairIterator(TableId tableId, Collection<Range<Token>> ranges);
        CloseableIterator<PaxosKeyState> flushIterator(Memtable paxos);
    }

    private final File dataDirectory;
    private volatile ImmutableMap<TableId, UncommittedTableData> tableStates;

    public PaxosUncommittedTracker(File dataDirectory, ImmutableMap<TableId, UncommittedTableData> tableStates)
    {
        this.dataDirectory = dataDirectory;
        this.tableStates = tableStates;
    }

    public PaxosUncommittedTracker(File dataDirectory)
    {
        this(dataDirectory, ImmutableMap.of());
    }

    public File getDirectory()
    {
        return dataDirectory;
    }

    public static void truncate(File dataDirectory)
    {
        logger.info("truncating paxos uncommitted metadata in {}", dataDirectory);
        for (File file : dataDirectory.tryList())
        {
            if (file.name().equals(PaxosBallotTracker.FNAME))
                continue;

            if (file.isDirectory())
                FileUtils.deleteRecursive(file);
            else
                FileUtils.deleteWithConfirm(file);
        }
    }

    public static PaxosUncommittedTracker load(File dataDirectory)
    {
        ImmutableMap.Builder<TableId, UncommittedTableData> builder = ImmutableMap.builder();
        for (TableId tableId : UncommittedTableData.listTableIds(dataDirectory))
        {
            builder.put(tableId, UncommittedTableData.load(dataDirectory, tableId));
        }

        return new PaxosUncommittedTracker(dataDirectory, builder.build());
    }

    @VisibleForTesting
    UncommittedTableData getOrCreateTableState(TableId tableId)
    {
        UncommittedTableData state = tableStates.get(tableId);
        if (state == null)
        {
            synchronized (this)
            {
                state = tableStates.get(tableId);
                if (state != null)
                    return state;

                state = UncommittedTableData.load(dataDirectory, tableId);
                tableStates = ImmutableMap.<TableId, UncommittedTableData>builder()
                              .putAll(tableStates).put(tableId, state)
                              .build();
            }
        }
        return state;
    }

    synchronized void flushUpdates(Memtable paxos) throws IOException
    {
        if (!stateFlushEnabled || !started)
            return;

        Map<TableId, UncommittedTableData.FlushWriter> flushWriters = new HashMap<>();
        try (CloseableIterator<PaxosKeyState> iterator = updateSupplier.flushIterator(paxos))
        {
            while (iterator.hasNext())
            {
                PaxosKeyState next = iterator.next();
                UncommittedTableData.FlushWriter writer = flushWriters.get(next.tableId);
                if (writer == null)
                {
                    writer = getOrCreateTableState(next.tableId).flushWriter();
                    flushWriters.put(next.tableId, writer);
                }
                writer.append(next);
            }
        }
        catch (Throwable t)
        {
            for (UncommittedTableData.FlushWriter writer : flushWriters.values())
                t = writer.abort(t);
            throw new IOException(t);
        }

        for (UncommittedTableData.FlushWriter writer : flushWriters.values())
            writer.finish();
    }

    @VisibleForTesting
    UncommittedTableData getTableState(TableId tableId)
    {
        return tableStates.get(tableId);
    }

    public CloseableIterator<UncommittedPaxosKey> uncommittedKeyIterator(TableId tableId, Collection<Range<Token>> ranges)
    {
        ranges = (ranges == null || ranges.isEmpty()) ? Collections.singleton(FULL_RANGE) : Range.normalize(ranges);
        CloseableIterator<PaxosKeyState> updates = updateSupplier.repairIterator(tableId, ranges);

        try
        {
            UncommittedTableData state = tableStates.get(tableId);
            if (state == null)
                return PaxosKeyState.toUncommittedInfo(updates);

            CloseableIterator<PaxosKeyState> fileIter = state.iterator(ranges);
            try
            {
                @SuppressWarnings("unchecked") CloseableIterator<PaxosKeyState> merged = mergeUncommitted(updates, fileIter);

                return PaxosKeyState.toUncommittedInfo(merged);
            }
            catch (Throwable t)
            {
                fileIter.close();
                throw t;
            }
        }
        catch (Throwable t)
        {
            updates.close();
            throw t;
        }
    }

    synchronized void truncate()
    {
        logger.info("truncating paxos uncommitted info");
        tableStates.values().forEach(UncommittedTableData::truncate);
        tableStates = ImmutableMap.of();
    }

    public synchronized void start()
    {
        if (started)
            return;

        logger.info("enabling PaxosUncommittedTracker");
        started = true;
    }

    public synchronized void rebuild(Iterator<PaxosKeyState> iterator) throws IOException
    {
        Preconditions.checkState(!started);
        truncate();

        Map<TableId, UncommittedTableData.FlushWriter> flushWriters = new HashMap<>();
        try
        {
            while (iterator.hasNext())
            {
                PaxosKeyState next = iterator.next();
                UncommittedTableData.FlushWriter writer = flushWriters.get(next.tableId);
                if (writer == null)
                {
                    writer = getOrCreateTableState(next.tableId).rebuildWriter();
                    flushWriters.put(next.tableId, writer);
                }
                writer.append(next);
            }
            for (UncommittedTableData.FlushWriter writer : flushWriters.values())
                writer.finish();
        }
        catch (Throwable t)
        {
            for (UncommittedTableData.FlushWriter writer : flushWriters.values())
                t = writer.abort(t);
            throw new IOException(t);
        }

        start();
    }

    synchronized void consolidateFiles()
    {
        tableStates.values().forEach(UncommittedTableData::maybeScheduleMerge);
    }

    synchronized void schedulePaxosAutoRepairs()
    {
        if (!paxosRepairEnabled() || !autoRepairsEnabled)
            return;

        for (UncommittedTableData tableData : tableStates.values())
        {
            if (tableData.numFiles() == 0)
                continue;

            if (SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(tableData.keyspace()))
                continue;

            TableId tableId = tableData.tableId();
            if (Schema.instance.getTableMetadata(tableId) == null)
                continue;

            logger.debug("Starting paxos auto repair for {}.{}", tableData.keyspace(), tableData.table());

            if (!autoRepairTableIds.add(tableId))
            {
                logger.debug("Skipping paxos auto repair for {}.{}, another auto repair is already in progress", tableData.keyspace(), tableData.table());
                continue;
            }

            StorageService.instance.autoRepairPaxos(tableId).addCallback((success, failure) -> {
                if (failure != null) logger.error("Paxos auto repair for {}.{} failed", tableData.keyspace(), tableData.table(), failure);
                else logger.debug("Paxos auto repair for {}.{} completed", tableData.keyspace(), tableData.table());
                autoRepairTableIds.remove(tableId);
            });
        }
    }

    private static void runAndLogException(String desc, Runnable runnable)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable t)
        {
            logger.error("Unhandled exception running " + desc, t);
        }
    }

    void maintenance()
    {
        runAndLogException("file consolidation", this::consolidateFiles);
        runAndLogException("schedule auto repairs", this::schedulePaxosAutoRepairs);
        runAndLogException("evict hung repairs", PaxosTableRepairs::evictHungRepairs);
    }

    public synchronized void startAutoRepairs()
    {
        if (autoRepairStarted)
            return;
        int seconds = AUTO_REPAIR_FREQUENCY_SECONDS.getInt();
        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(this::maintenance, seconds, seconds, TimeUnit.SECONDS);
        autoRepairStarted = true;
    }

    @VisibleForTesting
    public boolean hasInflightAutoRepairs()
    {
        return !autoRepairTableIds.isEmpty();
    }

    public boolean isAutoRepairsEnabled()
    {
        return autoRepairsEnabled;
    }

    public void setAutoRepairsEnabled(boolean autoRepairsEnabled)
    {
        this.autoRepairsEnabled = autoRepairsEnabled;
    }

    public boolean isStateFlushEnabled()
    {
        return stateFlushEnabled;
    }

    public void setStateFlushEnabled(boolean enabled)
    {
        this.stateFlushEnabled = enabled;
    }

    public Set<TableId> tableIds()
    {
        return tableStates.keySet();
    }

    public static UpdateSupplier unsafGetUpdateSupplier()
    {
        return updateSupplier;
    }

    public static void unsafSetUpdateSupplier(UpdateSupplier updateSupplier)
    {
        Preconditions.checkArgument(updateSupplier != null);
        PaxosUncommittedTracker.updateSupplier = updateSupplier;
    }

}
