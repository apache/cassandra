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

package org.apache.cassandra.service.accord.debug;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.LargeBitSet;
import accord.utils.Reduce;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.primitives.Routables.Slice.Minimal;

public abstract class DebugTxnGraph<T, P>
{
    public static class TxnInfos<T> implements Comparable<TxnInfos<T>>
    {
        public final int commandStoreId;
        public final int depth;
        public final TxnId parent;
        public final List<T> infos;

        public TxnInfos(int commandStoreId, int depth, TxnId parent, List<T> infos)
        {
            this.commandStoreId = commandStoreId;
            this.depth = depth;
            this.infos = infos;
            this.parent = parent;
        }

        @Override
        public int compareTo(TxnInfos that)
        {
            int c = Integer.compare(this.depth, that.depth);
            if (c == 0) c = Integer.compare(this.commandStoreId, that.commandStoreId);
            if (c == 0) c = -this.parent.compareTo(that.parent);
            return c;
        }
    }

    public static class TxnInfo implements Comparable<TxnInfo>
    {
        public final TxnId txnId;
        public final SaveStatus saveStatus;
        public final @Nullable Timestamp executeAt;
        public final Routables<?> via;

        public TxnInfo(TxnId txnId, SaveStatus saveStatus, @Nullable Timestamp executeAt, Routables<?> via)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.executeAt = executeAt;
            this.via = via;
        }

        @Override
        public int compareTo(@Nonnull TxnInfo that)
        {
            int c = -compareExecuteAt(this.executeAt, that.executeAt);
            if (c == 0) c = -this.txnId.compareTo(that.txnId);
            return c;
        }
    }

    protected static class SaveInfo
    {
        private static final SaveInfo NONE = new SaveInfo(SaveStatus.NotDefined, null);

        public final SaveStatus saveStatus;
        public final @Nullable Timestamp executeAt;

        private SaveInfo(SaveStatus saveStatus, Timestamp executeAt)
        {
            this.saveStatus = saveStatus;
            this.executeAt = executeAt;
        }
    }

    protected static class SortInfo extends SaveInfo implements Comparable<SortInfo>
    {
        public final TxnId txnId;
        private SortInfo(TxnId txnId, SaveStatus saveStatus, Timestamp executeAt)
        {
            super(saveStatus, executeAt);
            this.txnId = txnId;
        }

        @Override
        public int compareTo(@Nonnull SortInfo that)
        {
            int c = compareExecuteAt(this.executeAt, that.executeAt);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            return c;
        }
    }

    final IAccordService service;
    final Consumer<? super TxnInfos<T>> visit;
    final TxnId root;
    final @Nullable Participants<?> intersecting;
    final TxnKindsAndDomains kinds;
    final Timestamp min;
    final int maxDepth;

    final ConcurrentLinkedQueue<AsyncChain<TxnInfos<T>>> queued = new ConcurrentLinkedQueue<>();

    public DebugTxnGraph(IAccordService service, TxnId root, TxnKindsAndDomains kinds, @Nullable Participants<?> intersecting, Timestamp min, int maxDepth, Consumer<? super TxnInfos<T>> visit)
    {
        this.service = service;
        this.visit = visit;
        this.root = root;
        this.intersecting = intersecting;
        this.kinds = kinds;
        this.min = min;
        this.maxDepth = maxDepth;
    }

    protected abstract TxnInfos<T> build(CommandStore commandStore, int depth, Command parent, List<SortInfo> sortedInfos, @Nullable Participants<?> intersecting, P param);
    protected abstract AsyncChain<TxnInfos<T>> visitRoot(SafeCommandStore safeStore, Command command);

    protected AsyncChain<TxnInfos<T>> visitRoot(SafeCommandStore safeStore, Command command, P param)
    {
        return visitParent(safeStore, command, param, new HashMap<>(), 0);
    }

    void visit(long deadlineNanos) throws TimeoutException
    {
        CommandStores commandStores = service.node().commandStores();
        if (commandStores.count() == 0)
            return;

        int[] ids = commandStores.ids();
        List<AsyncChain<TxnInfos<T>>> chains = new ArrayList<>(ids.length);
        for (int id : ids)
            chains.add(submitRoot(commandStores.forId(id), root));

        List<AsyncChain<TxnInfos<T>>> tmp = new ArrayList<>();
        Future<List<TxnInfos<T>>> next = AccordService.toFuture(AsyncChains.allOf(chains));
        while (next != null)
        {
            if (!next.awaitUntilThrowUncheckedOnInterrupt(deadlineNanos))
                throw new TimeoutException();

            next.rethrowIfFailed();
            List<TxnInfos<T>> process = next.getNow().stream()
                                            .filter(Objects::nonNull)
                                            .sorted(Comparator.naturalOrder())
                                            .collect(Collectors.toList());

            for (TxnInfos<T> txn : process)
                visit.accept(txn);

            next = drainToFuture(queued, tmp);
        }
    }

    static <V> Future<List<V>> drainToFuture(Queue<AsyncChain<V>> drain, List<AsyncChain<V>> tmp)
    {
        AsyncChain<V> next;
        while (null != (next = drain.poll()))
            tmp.add(next);
        if (tmp.isEmpty())
            return null;
        Future<List<V>> result = AccordService.toFuture(AsyncChains.allOf(List.copyOf(tmp)));
        tmp.clear();
        return result;
    }

    private AsyncChain<TxnInfos<T>> submitRoot(CommandStore commandStore, TxnId txnId)
    {
        return commandStore.chain(PreLoadContext.contextFor(txnId, "Populate txn_graph"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            if (command == null || command.saveStatus() == SaveStatus.Uninitialised)
                return AsyncChains.<TxnInfos<T>>success(null);
            return visitRoot(safeStore, command);
        }).flatMap(i -> i);
    }

    private AsyncChain<TxnInfos<T>> submitParent(CommandStore commandStore, TxnId txnId, P param, Map<TxnId, SaveInfo> infos, int depth)
    {
        return commandStore.chain(PreLoadContext.contextFor(txnId, "Populate txn_graph"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            if (command == null || command.saveStatus() == SaveStatus.Uninitialised)
                return AsyncChains.<TxnInfos<T>>success(null);
            return visitParent(safeStore, command, param, infos, depth);
        }).flatMap(i -> i);
    }

    private AsyncChain<TxnInfos<T>> visitParent(SafeCommandStore safeStore, Command command, P param, Map<TxnId, SaveInfo> infos, int depth)
    {
        CommandStore commandStore = safeStore.commandStore();
        if (depth < maxDepth)
        {
            PartialDeps deps = command.partialDeps();
            if (deps != null)
            {
                LargeBitSet recurse = new LargeBitSet(deps.txnIdCount());
                if (intersecting != null)
                {
                    if (kinds.matchesAny(Domain.Key))
                        deps.keyDeps.forEach(intersecting, recurse, null, (r, n, i) -> r.set(i));
                    if (kinds.matchesAny(Domain.Range))
                        deps.rangeDeps.forEach(intersecting, recurse, deps.keyDeps, (r, d, i) -> r.set(d.txnIdCount() + i));
                }
                else
                {
                    if (kinds.matchesAny(Domain.Key))
                        recurse.setRange(0, deps.keyDeps.txnIdCount());
                    if (kinds.matchesAny(Domain.Range))
                        recurse.setRange(deps.keyDeps.txnIdCount(), deps.txnIdCount());
                }

                List<AsyncChain<Void>> populate = new ArrayList<>();
                for (int i = recurse.nextSetBit(0, -1); i >= 0; i = recurse.nextSetBit(i + 1, -1))
                {
                    TxnId txnId = deps.txnId(i);
                    if (!kinds.matches(txnId) || txnId.compareTo(min) < 0)
                        recurse.unset(i);
                    else if (!infos.containsKey(txnId))
                        populate.add(populateTxnAsync(commandStore, txnId, infos));
                }

                if (recurse.getSetBitCount() > 0)
                {
                    AsyncChain<Void> first = populate.isEmpty() ? AsyncChains.success(null) : AsyncChains.reduce(populate, Reduce.toNull());
                    return first.flatMap(ignore -> new AsyncChains.Head<>()
                    {
                        @Override
                        protected @Nullable Cancellable start(BiConsumer<? super TxnInfos<T>, Throwable> callback)
                        {
                            List<SortInfo> list = new ArrayList<>(recurse.getSetBitCount());
                            for (int i = recurse.nextSetBit(0, -1); i >= 0; i = recurse.nextSetBit(i + 1, -1))
                            {
                                TxnId txnId = deps.txnId(i);
                                SaveInfo info = infos.get(txnId);
                                if (Invariants.expect(info != null, "populate txn_graph ordering failure; {} has no info", txnId))
                                    list.add(new SortInfo(txnId, info.saveStatus, info.executeAt));
                            }

                            list.sort(Comparator.reverseOrder());
                            visitLatestCommitted(list, command, (next, p) -> {
                                if (next.txnId.is(Txn.Kind.Read))
                                    return;

                                if (!next.saveStatus.hasBeen(Status.Committed) || next.saveStatus.hasBeen(Status.Truncated))
                                    return;

                                queued.add(submitParent(commandStore, next.txnId, param, infos, depth + 1));
                            });
                            callback.accept(build(commandStore, depth, command, list, intersecting, param), null);
                            return null;
                        }
                    });
                }
            }
        }

        return AsyncChains.success(new TxnInfos<>(commandStore.id(), depth, command.txnId(), Collections.emptyList()));
    }

    protected void visitLatestCommitted(List<SortInfo> sortedInfos, Command parent, BiConsumer<SortInfo, Participants<?>> forEach)
    {
        Participants<?> writes = parent.participants().owns();
        if (intersecting != null) writes = writes.intersecting(intersecting, Minimal);
        Participants<?> syncpoints = writes;
        boolean awaitsOnlyDeps = parent.txnId().awaitsOnlyDeps();
        for (int i = 0; i < sortedInfos.size() ; ++i)
        {
            SortInfo info = sortedInfos.get(i);
            boolean isCommitted = info.saveStatus.hasBeen(Status.Committed) && !info.saveStatus.hasBeen(Status.Invalidated);
            if (!isCommitted) continue;
            Participants<?> visit = info.txnId.isSyncPoint() ? syncpoints : writes;
            if (visit.isEmpty() || (!awaitsOnlyDeps && info.executeAt.compareTo(parent.executeAt()) > 0))
                continue;

            Participants<?> p = parent.partialDeps().participants(info.txnId);
            if (intersecting != null) p = p.intersecting(intersecting, Minimal);

            if (!p.intersects(visit))
                continue;

            forEach.accept(info, p);
            if (info.txnId.isSyncPoint()) syncpoints = syncpoints.without(p);
            if (info.txnId.isSyncPoint() || info.txnId.isWrite()) writes = writes.without(p);
            if ((parent.txnId().isSyncPoint() ? syncpoints : writes).isEmpty())
                break;
        }
    }

    private AsyncChain<Void> populateTxnAsync(CommandStore commandStore, TxnId txnId, Map<TxnId, SaveInfo> visited)
    {
        return commandStore.chain(PreLoadContext.contextFor(txnId, "Populate txn_graph"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            visited.putIfAbsent(txnId, command == null || command.saveStatus() == SaveStatus.Uninitialised ? SaveInfo.NONE : new SaveInfo(command.saveStatus(), command.executeAtIfKnown()));
        });
    }

    static int compareExecuteAt(Timestamp a, Timestamp b)
    {
        if (a == null || b == null)
            return a == b ? 0 : a == null ? -1 : 1;
        return a.compareTo(b);
    }
}
