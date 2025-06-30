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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import accord.api.DataStore;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.progress.ProgressEventType;

import static accord.utils.Invariants.require;
import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.ACCORD_TXN_GC;

public class AccordDataStore implements DataStore
{
    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        AccordFetchCoordinator coordinator = new AccordFetchCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore());
        coordinator.start();
        return coordinator.result();
    }

    @Override
    public FetchResult sync(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        Map<String, List<org.apache.cassandra.dht.Range<Token>>> tableToRanges = new Object2ObjectHashMap<>();
        for (Range range : ranges)
        {
            TokenRange tokenRange = (TokenRange) range;
            TableMetadata table = metadata.schema.getKeyspaces().getTableOrViewNullable(tokenRange.table());
            Invariants.require(table != null, "Table with id %s not found", tokenRange.table());
            tableToRanges.computeIfAbsent(table.keyspace, k -> new ArrayList<>()).add(tokenRange.toKeyspaceRange());
        }

        class SyncResult extends AsyncResults.SettableResult<Ranges> implements BiConsumer<Object, Throwable>, FetchResult
        {
            final AtomicInteger total = new AtomicInteger(tableToRanges.size());
            final List<Throwable> failures = new CopyOnWriteArrayList<>();

            @Override
            public void accept(Object o, Throwable throwable)
            {
                if (throwable != null)
                    failures.add(throwable);

                if (total.decrementAndGet() == 0)
                {
                    if (failures.isEmpty())
                    {
                        setSuccess(null);
                    }
                    else
                    {
                        Throwable e = new ExecutionException("Could not sync", failures.get(0));
                        for (int i = 1; i < failures.size(); i++)
                            e.addSuppressed(failures.get(i));
                        setFailure(e);

                    }
                }
            }

            @Override
            public void abort(Ranges ranges)
            {
                throw new UnsupportedOperationException("Can not abort sync task");
            }
        }
        SyncResult syncResult = new SyncResult();

        for (Map.Entry<String,  List<org.apache.cassandra.dht.Range<Token>>> e : tableToRanges.entrySet())
        {

            RepairCoordinator coord = StorageService.instance.repairAccordKeyspace(e.getKey(), e.getValue());
            coord.addProgressListener((tag, event) -> {
                if (event.getType() == ProgressEventType.ERROR)
                    syncResult.accept(null, new IllegalStateException(String.format("Streaming errored out: %s", event)));
                else if (event.getType() == ProgressEventType.SUCCESS)
                    syncResult.accept(null, null);
            });

            ScheduledExecutors.optionalTasks.submit(coord);
        }

        return syncResult;
    }

    static class SnapshotBounds
    {
        final List<org.apache.cassandra.dht.Range<Token>> ranges = new ArrayList<>();
        long id;
    }

    @Override
    public AsyncResult<Void> snapshot(Ranges ranges, TxnId before)
    {
        AsyncResults.SettableResult<Void> result = new AsyncResults.SettableResult<>();
        // TODO (desired): maintain a list of Accord tables, perhaps in ClusterMetadata?
        ClusterMetadata metadata = ClusterMetadata.current();
        Object2ObjectHashMap<TableId, SnapshotBounds> tables = new Object2ObjectHashMap<>();
        for (Range range : ranges)
        {
            tables.computeIfAbsent(((TokenRange)range).table(), ignore -> new SnapshotBounds())
            .ranges.add(((TokenRange) range).toKeyspaceRange());
        }

        for (Map.Entry<TableId, SnapshotBounds> e : tables.entrySet())
        {
            // TODO (required): is it safe to ignore null table metadata / cfs?
            TableMetadata tableMetadata = metadata.schema.getTableMetadata(e.getKey());
            if (tableMetadata == null || !tableMetadata.isAccordEnabled())
                continue;

            ColumnFamilyStore cfs = Keyspace.openAndGetStoreIfExists(tableMetadata);
            if (cfs == null)
                continue;

            // TODO (required): when we can safely map TxnId.hlc() -> local timestamp, consult Memtable timestamps
            Memtable memtable = cfs.getCurrentMemtable();
            e.getValue().id = memtable.getMemtableId();
        }

        ScheduledExecutors.scheduledTasks.schedule(() -> {
            List<Future<?>> futures = new ArrayList<>();
            for (Map.Entry<TableId, SnapshotBounds> e : tables.entrySet())
            {
                // TODO (required): is it safe to ignore null tableMetadata (or ColumnFamilyStore below)?
                TableMetadata tableMetadata = metadata.schema.getTableMetadata(e.getKey());
                if (tableMetadata == null) continue;

                ColumnFamilyStore cfs = Keyspace.openAndGetStoreIfExists(tableMetadata);
                if (cfs == null) continue;

                SnapshotBounds bounds = e.getValue();
                View view = cfs.getTracker().getView();
                for (Memtable memtable : view.getAllMemtables())
                {
                    if (memtable.getMemtableId() > bounds.id) continue;
                    if (!intersects(cfs, memtable, bounds.ranges)) continue;

                    futures.add(cfs.forceFlush(ACCORD_TXN_GC));
                    break;
                }
            }

            FutureCombiner.allOf(futures).addCallback((objects, throwable) -> {
                if (throwable != null)
                    result.setFailure(throwable);
                else
                    result.setSuccess(null);
            });
        }, DatabaseDescriptor.getAccordGCDelay(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);

        return result;
    }

    private boolean intersects(ColumnFamilyStore cfs, Memtable memtable, List<org.apache.cassandra.dht.Range<Token>> tableRanges)
    {
        boolean intersects = false;
        // TrieMemtable doesn't support reverse iteration so can't find the last token
        if (memtable instanceof TrieMemtable)
            intersects = true;
        else
        {
            Token firstToken = null;
            try (UnfilteredPartitionIterator iterator = memtable.partitionIterator(ColumnFilter.all(cfs.metadata()), DataRange.allData(cfs.getPartitioner()), SSTableReadsListener.NOOP_LISTENER))
            {
                if (iterator.hasNext())
                    firstToken = iterator.next().partitionKey().getToken();
            }
            Token lastToken = memtable.lastToken();

            if (firstToken != null)
            {
                require(lastToken != null);
                if (firstToken.equals(lastToken))
                {
                    for (org.apache.cassandra.dht.Range<Token> tableRange : tableRanges)
                    {
                        if (tableRange.contains(firstToken))
                        {
                            intersects = true;
                            break;
                        }
                    }
                }
                else
                {
                    require(firstToken.compareTo(lastToken) < 0);
                    org.apache.cassandra.dht.Range<Token> memtableRange = new org.apache.cassandra.dht.Range<>(firstToken, lastToken);
                    for (org.apache.cassandra.dht.Range<Token> tableRange : tableRanges)
                    {
                        if (tableRange.intersects(memtableRange))
                        {
                            intersects = true;
                            break;
                        }
                    }
                }
            }
        }

        return intersects;
    }
}
