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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.DataStore;
import accord.local.CommandStore;
import accord.local.CommandStores.RestrictedStoreSelector;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncResults;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordDurableOnFlush.ReportDurable;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressListener;

import static accord.utils.Invariants.illegalArgument;

public class AccordDataStore implements DataStore
{
    private static final Logger logger = LoggerFactory.getLogger(AccordDataStore.class);
    enum FlushListenerKey { KEY }

    /**
     * Ensures data for the intersecting ranges is flushed to sstable before calling back with reportOnSuccess.
     * This is used to gate journal cleanup, since we skip the CommitLog for applying to the data table.
     */
    @Override
    public void ensureDurable(CommandStore commandStore, Ranges ranges, RedundantBefore reportOnSuccess, int flags)
    {
        if (commandStore.node().isReplaying() || ranges.isEmpty())
            return;

        logger.debug("{} awaiting local data durability for {}", commandStore, ranges);
        ensureDurableInternal(commandStore, reportOnSuccess, flags);
    }

    @Override
    public void ensureDurable(CommandStore commandStore, RedundantBefore reportOnSuccess, int flags)
    {
        logger.debug("{} awaiting full local data durability", commandStore);
        ensureDurableInternal(commandStore, reportOnSuccess, flags);
    }

    private void ensureDurableInternal(CommandStore commandStore, RedundantBefore redundantBefore, int flags)
    {
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(((AccordCommandStore)commandStore).tableId());
        AccordDurableOnFlush.notifyOnDurable(cfs, commandStore, ReportDurable.of(redundantBefore, flags));
    }

    public FetchResult image(Node node, SafeCommandStore safeStore, Ranges ranges, TxnId atLeast, SortedArrayList<Node.Id> readable, FetchRanges callback)
    {
        AccordFetchCoordinator coordinator;
        try
        {
            coordinator = new AccordFetchCoordinator(node, ranges, atLeast, readable, callback, safeStore.commandStore());
        }
        catch (Throwable t)
        {
            return new FetchResult.Failure(t);
        }

        coordinator.start();
        return coordinator.result();
    }

    @Override
    public FetchResult sync(Node node, SafeCommandStore safeStore, Ranges ranges, TxnId atLeast, SortedArrayList<Node.Id> readable, FetchRanges callback)
    {
        TableId tableId = ((AccordCommandStore)safeStore.commandStore()).tableId();

        ClusterMetadata cm = ClusterMetadata.current();
        TableMetadata tableMetadata = cm.schema.getTableMetadata(tableId);
        if (tableMetadata == null)
        {
            Throwable fail = new UnknownTableException("Could not find tableId " + tableId + " in ClusterMetadata", tableId);
            callback.fail(ranges, fail);
            return new FetchResult.Failure(fail);
        }

        class SyncResult extends AsyncResults.SettableResult<Ranges> implements FetchResult
        {
            @Override
            public void abort(Ranges ranges)
            {
                throw new UnsupportedOperationException("Can not abort sync task");
            }
        }

        // TODO (expected): add some automatic slicing of ranges and retry/back-off logic; but for now,
        //  since this is done at the command store level, and this is already a slice of a node, this should be fine
        SyncResult syncResult = new SyncResult();
        ProgressListener listener = new ProgressListener()
        {
            StartingRangeFetch starting = callback.starting(ranges);
            { Invariants.require(starting != null); }

            @Override
            public void progress(String tag, ProgressEvent event)
            {
                switch (event.getType())
                {
                    default: throw new UnhandledEnum(event.getType());
                    case SUCCESS:
                        callback.fetched(ranges);
                        syncResult.trySuccess(null);
                        // fall-through to ensure started
                    case START:
                        reportStarted();
                        break;
                    case PROGRESS:
                    case COMPLETE:
                    case NOTIFICATION:
                        break;
                    case ABORT:
                    case ERROR:
                        RuntimeException ex = new RuntimeException(String.format("Repair failed (%s): %s", event.getType(), event.getMessage()));
                        callback.fail(ranges, ex);
                        syncResult.tryFailure(ex);
                        break;
                }
            }

            private void reportStarted()
            {
                StartingRangeFetch start = this.starting;
                if (start == null)
                    return;
                this.starting = null;
                node.commandStores().mapReduceConsume(new RestrictedStoreSelector(ranges, 0, Long.MAX_VALUE), new MapReduceConsumeCommandStores<Ranges, Timestamp>(ranges)
                {
                    @Override public Timestamp reduce(Timestamp o1, Timestamp o2) { return Timestamp.max(o1, o2); }
                    @Override public void accept(Timestamp result, Throwable failure)
                    {
                        if (failure != null) syncResult.tryFailure(failure);
                        else start.started(result);
                    }
                    @Override public TxnId primaryTxnId() { return null; }
                    @Override public String reason() { return "Compute MaxConflict to report for fetch"; }
                    @Override protected Timestamp applyInternal(SafeCommandStore safeStore) { return safeStore.commandStore().maxConflict(TxnId.NONE, ranges); }
                });
            }
        };

        RepairCoordinator coord = StorageService.instance.newRepairCoordinator(tableMetadata.keyspace, options(tableMetadata, ranges));
        coord.addProgressListener(listener);

        ScheduledExecutors.optionalTasks.submit(coord).addCallback((s, f) -> {
            if (f != null)
                syncResult.tryFailure(f);
        });

        return syncResult;
    }

    private static RepairOption options(TableMetadata tableMetadata, Ranges accordRanges)
    {
        List<Range<Token>> ranges = new ArrayList<>();
        RangesAtEndpoint localRanges = StorageService.instance.getLocalReplicas(tableMetadata.keyspace);
        // repair validation requires that we separate by local range, even if they are adjacent;
        // unsure if this is important, so just splitting into ranges that are wholly contained by local ranges
        accordRanges.forEach(accordRange -> {
            Range<Token> range = ((TokenRange)accordRange).toKeyspaceRange();
            localRanges.ranges().forEach(localRange -> {
                if (localRange.contains(range)) ranges.add(range);
                else if (localRange.intersects(range)) ranges.addAll(range.intersectionWith(localRange));
            });
        });

        Ranges matchedWithLocal = Ranges.of(ranges.stream().map(r -> TokenRange.fromKeyspaceRange(tableMetadata.id, r)).toArray(TokenRange[]::new)).mergeTouching();
        if (!matchedWithLocal.containsAll(accordRanges))
            throw illegalArgument("Local ranges %s do not fully cover requested accord ranges %s (overlap: %s)", localRanges, accordRanges, matchedWithLocal);

        return new RepairOption(RepairParallelism.PARALLEL, // parallelism
                false,                      // primaryRange
                false,                      // incremental
                false,                      // trace
                5,                          // jobThreads
                ranges,                     // ranges
                true,                       // pullRepair
                true,                       // forceRepair
                PreviewKind.NONE,           // previewKind
                false,                      // optimiseStreams
                true,                       // ignoreUnreplicatedKeyspaces
                true,                       // repairData
                false,                      // repairPaxos
                true,                       // dontPurgeTombstones
                false,                      // repairAccord
                false
        );
    }
}
