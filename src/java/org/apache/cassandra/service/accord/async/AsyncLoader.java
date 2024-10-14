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
package org.apache.cassandra.service.accord.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.KeyHistory;
import accord.local.PreLoadContext;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.Observable;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCachingState;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordSafeCommandsForRanges;
import org.apache.cassandra.service.accord.AccordSafeState;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.NoSpamLogger;

public class AsyncLoader
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncLoader.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.MINUTES);

    enum State
    {
        INITIALIZED,
        SETUP,
        LOADING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    private final AccordCommandStore commandStore;

    private final Iterable<TxnId> txnIds;
    private final Unseekables<?> keysOrRanges;
    private final KeyHistory keyHistory;

    protected AsyncResult<?> readResult;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Unseekables<?> keysOrRanges, KeyHistory keyHistory)
    {
        this.commandStore = commandStore;
        this.txnIds = txnIds;
        this.keysOrRanges = keysOrRanges;
        this.keyHistory = keyHistory;
    }

    protected static Iterable<TxnId> txnIds(PreLoadContext context)
    {
        TxnId primaryid = context.primaryTxnId();
        Collection<TxnId> additionalIds = context.additionalTxnIds();
        if (primaryid == null) return additionalIds;
        if (additionalIds.isEmpty()) return Collections.singleton(primaryid);
        return Iterables.concat(Collections.singleton(primaryid), additionalIds);
    }

    private static <K, V, S extends AccordSafeState<K, V>> void referenceAndAssembleReadsForKey(K key,
                                                                                                Map<K, S> context,
                                                                                                AccordStateCache.Instance<K, V, S> cache,
                                                                                                List<AsyncChain<?>> listenChains)
    {
        referenceAndAssembleReadsForKey(key, context, cache, listenChains, null);
    }

    private static <K, V, S extends AccordSafeState<K, V>> void referenceAndAssembleReadsForKey(K key,
                                                                                                Map<K, S> context,
                                                                                                AccordStateCache.Instance<K, V, S> cache,
                                                                                                List<AsyncChain<?>> listenChains,
                                                                                                @Nullable ExecutorPlus loadExecutor)
    {
        S safeRef = cache.acquire(key, loadExecutor);
        if (context.putIfAbsent(key, safeRef) != null)
        {
            noSpamLogger.warn("Context {} contained key {} more than once", context, key);
            cache.release(safeRef);
            return;
        }
        AccordCachingState.Status status = safeRef.globalStatus(); // globalStatus() completes
        switch (status)
        {
            default: throw new IllegalStateException("Unhandled global state: " + status);
            case LOADING:
                listenChains.add(safeRef.loading());
                break;
            case SAVING:
                // make sure we work with a completed state that supports get() and set()
                listenChains.add(safeRef.saving());
                break;
            case LOADED:
            case MODIFIED:
            case FAILED_TO_SAVE:
                break;
            case FAILED_TO_LOAD:
                // TODO (required): if this triggers, we trigger some other illegal state in cache management
                throw new RuntimeException(safeRef.failure());
        }
    }

    private void referenceAndAssembleReadsForKey(RoutingKey key,
                                                 AsyncOperation.Context context,
                                                 List<AsyncChain<?>> listenChains)
    {
        referenceAndAssembleReadsForKey(key, context, listenChains, null);
    }

    private void referenceAndAssembleReadsForKey(RoutingKey key,
                                                 AsyncOperation.Context context,
                                                 List<AsyncChain<?>> listenChains,
                                                 @Nullable ExecutorPlus loadExecutor)
    {
        // recovery operations also need the deps data for their preaccept logic
        switch (keyHistory)
        {
            case TIMESTAMPS:
                referenceAndAssembleReadsForKey(key, context.timestampsForKey, commandStore.timestampsForKeyCache(), listenChains, loadExecutor);
                break;
            case COMMANDS:
            case RECOVERY:
                referenceAndAssembleReadsForKey(key, context.commandsForKey, commandStore.commandsForKeyCache(), listenChains, loadExecutor);
            case NONE:
                break;
            default: throw new IllegalArgumentException("Unhandled keyhistory: " + keyHistory);
        }
    }

    private <K, V, S extends AccordSafeState<K, V>> void referenceAndAssembleReads(Iterable<? extends K> keys,
                                                                                   Map<K, S> context,
                                                                                   AccordStateCache.Instance<K, V, S> cache,
                                                                                   List<AsyncChain<?>> listenChains)
    {
        keys.forEach(key -> referenceAndAssembleReadsForKey(key, context, cache, listenChains));
    }

    private AsyncResult<?> referenceAndDispatchReads(@Nullable TxnId primaryTxnId, AsyncOperation.Context context)
    {
        List<AsyncChain<?>> chains = new ArrayList<>();

        referenceAndAssembleReads(txnIds, context.commands, commandStore.commandCache(), chains);

        switch (keysOrRanges.domain())
        {
            case Key:
                AbstractKeys<RoutingKey> keys = (AbstractKeys<RoutingKey>) keysOrRanges;
                keys.forEach(key -> referenceAndAssembleReadsForKey(key, context, chains));
                break;
            case Range:
                chains.add(referenceAndDispatchReadsForRange(primaryTxnId, context));
                break;
            default:
                throw new UnsupportedOperationException("Unable to process keys of " + keysOrRanges.domain());
        }

        return !chains.isEmpty() ? AsyncChains.reduce(chains, (a, b) -> null).beginAsResult() : null;
    }

    private AsyncChain<?> referenceAndDispatchReadsForRange(@Nullable TxnId primaryTxnId, AsyncOperation.Context context)
    {
        if (keyHistory == KeyHistory.NONE)
            return AsyncChains.success(null);

        Ranges ranges = ((AbstractRanges) keysOrRanges).toRanges();

        List<AsyncChain<?>> root = new ArrayList<>(ranges.size() + 1);
        class Watcher implements AccordStateCache.Listener<RoutingKey, CommandsForKey>
        {
            // TODO (required): streams prohibited in hot path
            private final Set<TokenKey> cached = commandStore.commandsForKeyCache().stream()
                                                                 .map(n -> (TokenKey) n.key())
                                                                 .filter(ranges::contains)
                                                                 .collect(Collectors.toSet());

            @Override
            public void onAdd(AccordCachingState<RoutingKey, CommandsForKey> state)
            {
                TokenKey pk = (TokenKey) state.key();
                if (ranges.contains(pk))
                    cached.add(pk);
            }
        }

        // TODO (required): this needs to be optimised (e.g. to not load redundant commands, but maybe to be avoided altogether with async evaluation)
        Watcher watcher = new Watcher();
        commandStore.commandsForKeyCache().register(watcher);
        root.add(findOverlappingKeys(ranges).flatMap(keys -> {
            commandStore.commandsForKeyCache().unregister(watcher);
            if (keys.isEmpty() && watcher.cached.isEmpty())
                return AsyncChains.success(null);
            Set<? extends RoutingKey> set = ImmutableSet.<RoutingKey>builder().addAll(watcher.cached).addAll(keys).build();
            List<AsyncChain<?>> chains = new ArrayList<>();
            set.forEach(key -> referenceAndAssembleReadsForKey(key, context, chains, Stage.ACCORD_RANGE_LOADER.executor()));
            return chains.isEmpty() ? AsyncChains.success(null) : AsyncChains.reduce(chains, (a, b) -> null);
        }, commandStore));

        var chain = commandStore.diskCommandsForRanges().get(primaryTxnId, keyHistory, ranges);
        root.add(chain);
        context.commandsForRanges = new AccordSafeCommandsForRanges(ranges, chain);

        return AsyncChains.all(root);
    }

    private AsyncChain<List<? extends RoutingKey>> findOverlappingKeys(Ranges ranges)
    {
        if (ranges.isEmpty())
        {
            // During topology changes some shards may be included with empty ranges
            return AsyncChains.success(Collections.emptyList());
        }

        List<AsyncChain<List<TokenKey>>> chains = new ArrayList<>(ranges.size());
        for (Range range : ranges)
            chains.add(findOverlappingKeys(range));
        return AsyncChains.reduce(chains, (a, b) -> ImmutableList.<RoutingKey>builderWithExpectedSize(a.size() + b.size()).addAll(a).addAll(b).build());
    }

    private AsyncChain<List<TokenKey>> findOverlappingKeys(Range range)
    {
        // save to a variable as java gets confused when `.map` is called on the result of asChain
        AsyncChain<List<TokenKey>> map = Observable.asChain(callback ->
                                                               AccordKeyspace.findAllKeysBetween(commandStore.id(),
                                                                                                 (AccordRoutingKey) range.start(), range.startInclusive(),
                                                                                                 (AccordRoutingKey) range.end(), range.endInclusive(),
                                                                                                 callback),
                                                               Collectors.toList());
        return map.map(ImmutableList::copyOf);
    }

    @VisibleForTesting
    void state(State state)
    {
        this.state = state;
    }

    public boolean load(@Nullable TxnId primaryTxnId, AsyncOperation.Context context, BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running load for {} with state {}: {} {}", callback, state, txnIds, keysOrRanges);
        commandStore.checkInStoreThread();
        switch (state)
        {
            case INITIALIZED:
                state(State.SETUP);

            case SETUP:
                readResult = referenceAndDispatchReads(primaryTxnId, context);
                state(State.LOADING);

            case LOADING:
                if (readResult != null)
                {
                    if (readResult.isSuccess())
                    {
                        logger.trace("Read result succeeded for {}", callback);
                        readResult = null;
                    }
                    else
                    {
                        logger.trace("Adding callback for read result: {}", callback);
                        readResult.addCallback(callback, commandStore.executor());
                        break;
                    }
                }
                state(State.FINISHED);

            case FINISHED:
                break;

            default:
                throw new IllegalStateException("Unexpected state: " + state);
        }

        if (logger.isTraceEnabled())
            logger.trace("Exiting load for {} with state {}: {} {}", callback, state, txnIds, keysOrRanges);

        return state == State.FINISHED;
    }
}
