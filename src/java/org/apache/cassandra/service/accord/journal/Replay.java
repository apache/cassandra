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

package org.apache.cassandra.service.accord.journal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.AbstractReplayer;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2LongHashMap;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.journal.Journal;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.concurrent.Semaphore;

import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static org.apache.cassandra.service.accord.JournalKey.Type.COMMAND_DIFF;
import static org.apache.cassandra.utils.FBUtilities.getAvailableProcessors;

public class Replay
{
    private static final Logger logger = LoggerFactory.getLogger(Replay.class);

    static boolean replay(AccordJournal journal, CommandStores commandStores, Object param)
    {
        Invariants.require(param == null || param.getClass() == Long2LongHashMap.class, "Param should be null or a map of commandStoreId->minSegmentId");
        final Long2LongHashMap minSegments = param == null ? new Long2LongHashMap(0L) : (Long2LongHashMap) param;

        // TODO (expected): make the parallelisms configurable
        // Replay is performed in parallel, where at most X commands can be in flight, across at most Y commands stores.
        // That is, you can limit replay parallelism to 1 command store at a time, but load multiple commands within that data store,
        // _or_ have multiple commands being loaded accross multiple data stores.
        final Semaphore commandParallelism = Semaphore.newSemaphore(getAvailableProcessors());
        final int commandStoreParallelism = Math.max(Math.max(1, Math.min(getAvailableProcessors(), 4)), getAvailableProcessors() / 4);
        final AtomicBoolean abort = new AtomicBoolean();
        final IntArrayList activeCommandStoreIds = new IntArrayList();
        final ReplayQueue pendingCommandStores = new ReplayQueue(commandStores.all());

        class ReplayStream implements Closeable
        {
            final CommandStore commandStore;
            final AbstractReplayer replayer;
            final CloseableIterator<Journal.KeyRefs<JournalKey>> iter;
            JournalKey prev;

            public ReplayStream(CommandStore commandStore, long minSegment)
            {
                this.commandStore = commandStore;
                this.replayer = (AbstractReplayer) commandStore.replayer();
                // Keys in the index are sorted by command store id, so index iteration will be sequential
                this.iter = journal.keyIterator(new JournalKey(replayer.minReplay.withoutNonIdentityFlags(), COMMAND_DIFF, commandStore.id()), new JournalKey(TxnId.MAX.withoutNonIdentityFlags(), COMMAND_DIFF, commandStore.id()), false, minSegment);
                logger.info("Beginning replay of {} with min={}, {}", commandStore, replayer.minReplay,
                            replayer.redundantBefore.map(b -> b == null ? null : b.maxBoundBoth(LOCALLY_DURABLE_TO_DATA_STORE, LOCALLY_DURABLE_TO_COMMAND_STORE), TxnId[]::new));
            }

            boolean replay()
            {
                JournalKey key;
                long[] segments;
                while (true)
                {
                    if (!iter.hasNext())
                    {
                        logger.info("Completed replay of {}", commandStore);
                        return false;
                    }

                    Journal.KeyRefs<JournalKey> ref = iter.next();
                    if (ref.key().type != COMMAND_DIFF)
                        continue;

                    key = ref.key();
                    segments = journal.rangeSearch != null && journal.rangeSearch.shouldIndex(key) ? ref.copyOfSegments() : null;
                    break;
                }

                TxnId txnId = key.id;
                Invariants.require(prev == null ||
                                   key.commandStoreId != prev.commandStoreId ||
                                   key.id.compareTo(prev.id) != 0,
                                   "duplicate key detected %s == %s", key, prev);
                prev = key;
                commandParallelism.acquireThrowUncheckedOnInterrupt(1);
                replayer.replay(txnId)
                        .map(route -> {
                            if (segments != null && route != null)
                            {
                                for (long segment : segments)
                                    journal.rangeSearch.safeNotify(index -> index.update(segment, key.commandStoreId, txnId, (Route<?>) route));
                            }
                            return null;
                        }).begin((success, fail) -> {
                            commandParallelism.release(1);
                            if (fail != null && !journal.segments.handleError("Could not replay command " + txnId, fail))
                                abort.set(true);
                        });

                return true;
            }

            @Override
            public void close()
            {
                iter.close();
            }
        }

        // Replay streams by command store id, can hold at most commandStoreParallelism items
        final Int2ObjectHashMap<ReplayStream> replayStreams = new Int2ObjectHashMap<>();
        try
        {
            // index of the store we're currently pulling from in the activeCommandStoreIds collection
            int cur = 0;
            while (!abort.get())
            {
                if (cur == activeCommandStoreIds.size())
                {
                    if (activeCommandStoreIds.size() < commandStoreParallelism && !pendingCommandStores.isEmpty())
                    {
                        CommandStore next = pendingCommandStores.next();
                        int id = next.id();
                        activeCommandStoreIds.add(id);
                        replayStreams.put(id, new ReplayStream(next, minSegments.getOrDefault(id, 0)));
                    }
                    else if (activeCommandStoreIds.isEmpty()) break;
                    else cur = 0;
                }

                int id = activeCommandStoreIds.get(cur);
                ReplayStream replayStream = replayStreams.get(id);
                while (!replayStream.replay())
                {
                    // Replay complete for this command store; close and replace
                    replayStreams.remove(id).close();
                    if (pendingCommandStores.isEmpty())
                    {
                        // no more pending to submit; remove and continue with the next remaining (if any)
                        activeCommandStoreIds.removeAt(cur);
                        if (cur == activeCommandStoreIds.size())
                            --cur;
                        if (cur < 0)
                            break;
                        id = activeCommandStoreIds.get(cur);
                    }
                    else
                    {
                        // replace it with a pending command store, and continue processing
                        CommandStore next = pendingCommandStores.next(streamId(replayStream.commandStore));
                        id = next.id();
                        activeCommandStoreIds.set(cur, id);
                        replayStreams.put(id, new ReplayStream(next, minSegments.getOrDefault(id, 0)));
                    }

                    replayStream = replayStreams.get(id);
                }

                ++cur;
            }
            return true;
        }
        catch (Throwable t)
        {
            try { FileUtils.close(replayStreams.values()); }
            catch (Throwable t2) { t.addSuppressed(t2); }
            throw t;
        }
    }

    static class ReplayQueue
    {
        final Int2ObjectHashMap<Queue<CommandStore>> byExecutor = new Int2ObjectHashMap<>();
        final Deque<Integer> nextId = new ArrayDeque<>();

        ReplayQueue(CommandStore[] commandStores)
        {
            for (CommandStore commandStore : commandStores)
            {
                byExecutor.computeIfAbsent(streamId(commandStore), ignore -> new ArrayDeque<>())
                          .add(commandStore);
            }
            nextId.addAll(byExecutor.keySet());
        }

        boolean isEmpty()
        {
            return byExecutor.isEmpty();
        }

        CommandStore next()
        {
            while (true)
            {
                if (byExecutor.isEmpty())
                    return null;

                Integer id = nextId.poll();
                if (id == null)
                {
                    nextId.addAll(byExecutor.keySet());
                    id = nextId.poll();
                }

                Queue<CommandStore> queue = byExecutor.get(id);
                if (queue != null)
                {
                    CommandStore next = queue.poll();
                    if (queue.isEmpty())
                        byExecutor.remove(id);
                    if (next != null)
                        return next;
                }
            }
        }

        CommandStore next(int streamId)
        {
            Queue<CommandStore> queue = byExecutor.get(streamId);
            if (queue == null)
                return next();

            CommandStore next = queue.poll();
            if (queue.isEmpty())
                byExecutor.remove(streamId);

            return next;
        }
    }

    private static int streamId(CommandStore commandStore)
    {
        return commandStore instanceof AccordCommandStore ? ((AccordCommandStore) commandStore).executor().executorId() : 1;
    }

    public static List<accord.api.Journal.TopologyUpdate> topologies(AccordJournal journal)
    {
        List<accord.api.Journal.TopologyUpdate> images = new ArrayList<>();
        try (CloseableIterator<accord.api.Journal.TopologyUpdate> iter = new CloseableIterator<>()
        {
            final CloseableIterator<Journal.KeyRefs<JournalKey>> iter = journal.keyIterator(TopologyRecord.journalKey(0L),
                                                                                            TopologyRecord.journalKey(Timestamp.MAX_EPOCH),
                                                                                            true, 0);
            TopologyRecord.TopologyImage prev = null;

            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public accord.api.Journal.TopologyUpdate next()
            {
                Journal.KeyRefs<JournalKey> ref = iter.next();
                MergeSerializers.TopologyMerger reader = journal.readAll(ref.key());
                if (reader.read().kind() == TopologyRecord.Kind.Repeat)
                {
                    if (prev == null)
                    {
                        logger.error("Encountered TopologyImage Repeat record for epoch {}, but no prior image record was found", ref.key().id.epoch());
                        return null;
                    }
                    prev = reader.read().asImage(Invariants.nonNull(prev.getUpdate()));
                }
                else prev = reader.read();

                return new accord.api.Journal.TopologyUpdate(prev.getUpdate().commandStores,
                                                             prev.getUpdate().global);
            }

            @Override
            public void close()
            {
                iter.close();
            }
        })
        {
            accord.api.Journal.TopologyUpdate prev = null;
            while (iter.hasNext())
            {
                accord.api.Journal.TopologyUpdate next = iter.next();
                if (next == null)
                    continue;

                Invariants.require(prev == null || next.global.epoch() > prev.global.epoch());
                // Due to partial compaction, we can clean up only some of the old epochs, creating gaps. We skip these epochs here.
                if (prev != null && next.global.epoch() > prev.global.epoch() + 1)
                    images.clear();

                images.add(next);
                prev = next;
            }
        }
        return images;
    }

}
