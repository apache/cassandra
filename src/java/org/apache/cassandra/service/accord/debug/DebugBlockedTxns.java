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
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.ExecutionContext;
import accord.local.SafeCommandStore;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.LoadKeys.SYNC;
import static accord.local.LoadKeysFor.READ_WRITE;
import static java.util.Collections.emptyList;

public class DebugBlockedTxns
{
    public static class Txn implements Comparable<Txn>
    {
        public final int commandStoreId;
        public final int depth;
        public final TxnId txnId;
        public final Timestamp executeAt;
        public final SaveStatus saveStatus;
        public final RoutingKey blockedViaKey;
        public final List<TxnId> blockedBy;
        public final List<TokenKey> blockedByKey;

        public Txn(int commandStoreId, int depth, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, RoutingKey blockedViaKey, List<TxnId> blockedBy, List<TokenKey> blockedByKey)
        {
            this.commandStoreId = commandStoreId;
            this.depth = depth;
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.saveStatus = saveStatus;
            this.blockedViaKey = blockedViaKey;
            this.blockedBy = blockedBy;
            this.blockedByKey = blockedByKey;
        }

        public boolean isBlocked()
        {
            return !notBlocked();
        }

        public boolean notBlocked()
        {
            return blockedBy.isEmpty() && blockedByKey.isEmpty();
        }

        @Override
        public int compareTo(Txn that)
        {
            int c = Integer.compare(this.depth, that.depth);
            if (c == 0) c = Integer.compare(this.commandStoreId, that.commandStoreId);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            if (c == 0) c = this.blockedViaKeyString().compareTo(that.blockedViaKeyString());
            return c;
        }

        private String blockedViaKeyString()
        {
            return blockedViaKey == null ? "" : blockedViaKey.toString();
        }
    }

    final IAccordService service;
    final Consumer<Txn> visit;
    final TxnId root;
    final int maxDepth;
    final Set<Object> visited = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final ConcurrentLinkedQueue<AsyncChain<Void>> queuedKeys = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<AsyncChain<Txn>> queuedTxn = new ConcurrentLinkedQueue<>();

    public DebugBlockedTxns(IAccordService service, TxnId root, int maxDepth, Consumer<Txn> visit)
    {
        this.service = service;
        this.visit = visit;
        this.root = root;
        this.maxDepth = maxDepth;
    }

    public static void visit(IAccordService accord, TxnId txnId, int maxDepth, long deadlineNanos, Consumer<Txn> visit) throws TimeoutException
    {
        new DebugBlockedTxns(accord, txnId, maxDepth, visit).visit(deadlineNanos);
    }

    private void visit(long deadlineNanos) throws TimeoutException
    {
        CommandStores commandStores = service.node().commandStores();
        if (commandStores.count() == 0)
            return;

        int[] ids = commandStores.ids();
        List<AsyncChain<Txn>> chains = new ArrayList<>(ids.length);
        for (int id : ids)
            chains.add(visitRootTxnAsync(commandStores.forId(id), root));

        List<AsyncChain> tmp = new ArrayList<>();
        Future<List<Txn>> next = AccordService.toFuture(AsyncChains.allOf(chains));
        while (next != null)
        {
            if (!next.awaitUntilThrowUncheckedOnInterrupt(deadlineNanos))
                throw new TimeoutException();

            next.rethrowIfFailed();
            List<Txn> process = next.getNow().stream()
                                    .filter(Objects::nonNull)
                                    .sorted(Comparator.naturalOrder())
                                    .collect(Collectors.toList());

            for (Txn txn : process)
                visit.accept(txn);

            Future<List<Void>> awaitKeys = drainToFuture(queuedKeys, (List<AsyncChain<Void>>)(List)tmp);
            if (awaitKeys != null && !awaitKeys.awaitUntilThrowUncheckedOnInterrupt(deadlineNanos))
                throw new TimeoutException();

            next = drainToFuture(queuedTxn, (List<AsyncChain<Txn>>)(List)tmp);
        }
    }

    private <T> Future<List<T>> drainToFuture(Queue<AsyncChain<T>> drain, List<AsyncChain<T>> tmp)
    {
        AsyncChain<T> next;
        while (null != (next = drain.poll()))
            tmp.add(next);
        if (tmp.isEmpty())
            return null;
        Future<List<T>> result = AccordService.toFuture(AsyncChains.allOf(List.copyOf(tmp)));
        tmp.clear();
        return result;
    }

    private AsyncChain<Txn> visitRootTxnAsync(CommandStore commandStore, TxnId txnId)
    {
        return commandStore.chain(ExecutionContext.contextFor(txnId, "Populate txn_blocked_by"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            if (command == null || command.saveStatus() == SaveStatus.Uninitialised)
                return null;
            return visitTxnSync(safeStore, command, command.executeAt(), null, 0);
        });
    }

    private AsyncChain<Txn> visitTxnAsync(CommandStore commandStore, TxnId txnId, Timestamp rootExecuteAt, @Nullable TokenKey byKey, int depth, boolean recurse)
    {
        return commandStore.chain(ExecutionContext.contextFor(txnId, "Populate txn_blocked_by"), safeStore -> {
            Command command = safeStore.unsafeGetNoCleanup(txnId).current();
            if (command == null || command.saveStatus() == SaveStatus.Uninitialised)
                return null;
            return visitTxnSync(safeStore, command, rootExecuteAt, byKey, depth);
        });
    }

    private Txn visitTxnSync(SafeCommandStore safeStore, Command command, Timestamp rootExecuteAt, @Nullable TokenKey byKey, int depth)
    {
        List<TxnId> waitingOnTxnId = new ArrayList<>();
        List<TokenKey> waitingOnKey = new ArrayList<>();
        if (!command.hasBeen(Status.Applied) && command.hasBeen(Status.Stable))
        {
            // check blocking state
            Command.WaitingOn waitingOn = command.asCommitted().waitingOn();
            waitingOn.waitingOn.reverseForEach(null, null, null, null, (i1, i2, i3, i4, i) -> {
                if (i < waitingOn.txnIdCount()) waitingOnTxnId.add(waitingOn.txnId(i));
                else waitingOnKey.add((TokenKey) waitingOn.keys.get(i - waitingOn.txnIdCount()));
            });
        }

        CommandStore commandStore = safeStore.commandStore();
        if (depth < maxDepth)
        {
            for (TxnId waitingOn : waitingOnTxnId)
            {
                if (visited.add(waitingOn))
                    queuedTxn.add(visitTxnAsync(commandStore, waitingOn, rootExecuteAt, null, depth + 1, true));
            }
            for (TokenKey key : waitingOnKey)
            {
                if (visited.add(key))
                    queuedKeys.add(visitKeysAsync(commandStore, key, rootExecuteAt, depth + 1));
            }
        }

        return new Txn(commandStore.id(), depth, command.txnId(), command.executeAt(), command.saveStatus(), byKey, waitingOnTxnId, waitingOnKey);
    }


    private AsyncChain<Void> visitKeysAsync(CommandStore commandStore, TokenKey key, Timestamp rootExecuteAt, int depth)
    {
        return commandStore.chain(ExecutionContext.contextFor(RoutingKeys.of(key.toUnseekable()), SYNC, READ_WRITE, "Populate txn_blocked_by"), safeStore -> {
            visitKeysSync(safeStore, key, rootExecuteAt, depth);
        });
    }

    private void visitKeysSync(SafeCommandStore safeStore, TokenKey key, Timestamp rootExecuteAt, int depth)
    {
        SafeCommandsForKey commandsForKey = safeStore.ifLoadedAndInitialised(key);
        TxnId blocking = commandsForKey.current().blockedOnTxnId(root, rootExecuteAt);
        CommandStore commandStore = safeStore.commandStore();
        if (blocking == null)
        {
            queuedTxn.add(AsyncChains.success(new Txn(commandStore.id(), depth, null, null, null, key, emptyList(), emptyList())));
        }
        else
        {
            boolean recurse = visited.add(blocking);
            queuedTxn.add(visitTxnAsync(commandStore, blocking, rootExecuteAt, key, depth, recurse));
        }
    }
}
