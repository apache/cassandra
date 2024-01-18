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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.local.KeyHistory;
import accord.local.PreLoadContext;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.Observable;
import org.apache.cassandra.service.accord.AccordCachingState;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordSafeState;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;

public class AsyncLoader
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncLoader.class);

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
    private final Seekables<?, ?> keysOrRanges;
    private final KeyHistory keyHistory;

    protected AsyncResult<?> readResult;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Seekables<?, ?> keysOrRanges, KeyHistory keyHistory)
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
        S safeRef = cache.acquire(key);
        context.put(key, safeRef);
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
                throw new RuntimeException(safeRef.failure());
        }
    }

    private void referenceAndAssembleReadsForKey(RoutableKey key,
                                                 AsyncOperation.Context context,
                                                 List<AsyncChain<?>> listenChains)
    {
        referenceAndAssembleReadsForKey(key, context.timestampsForKey, commandStore.timestampsForKeyCache(), listenChains);
        // recovery operations also need the deps data for their preaccept logic
        switch (keyHistory)
        {
            case ALL:
                referenceAndAssembleReadsForKey(key, context.allCommandsForKeys, commandStore.allCommandsForKeyCache(), listenChains);
            case DEPS:
                referenceAndAssembleReadsForKey(key, context.depsCommandsForKeys, commandStore.depsCommandsForKeyCache(), listenChains);
            case NONE:
                break;
            default: throw new IllegalArgumentException("Unhandled keyhistory: " + keyHistory);
        }
        referenceAndAssembleReadsForKey(key, context.updatesForKeys, commandStore.updatesForKeyCache(), listenChains);
    }

    private <K, V, S extends AccordSafeState<K, V>> void referenceAndAssembleReads(Iterable<? extends K> keys,
                                                                                   Map<K, S> context,
                                                                                   AccordStateCache.Instance<K, V, S> cache,
                                                                                   List<AsyncChain<?>> listenChains)
    {
        keys.forEach(key -> referenceAndAssembleReadsForKey(key, context, cache, listenChains));
    }

    private AsyncResult<?> referenceAndDispatchReads(AsyncOperation.Context context)
    {
        List<AsyncChain<?>> chains = new ArrayList<>();

        referenceAndAssembleReads(txnIds, context.commands, commandStore.commandCache(), chains);

        switch (keysOrRanges.domain())
        {
            case Key:
                // cast to Keys fails...
                Iterable<RoutableKey> keys = (Iterable<RoutableKey>) keysOrRanges;
                keys.forEach(key -> referenceAndAssembleReadsForKey(key, context, chains));
                break;
            case Range:
                chains.add(referenceAndDispatchReadsForRange(context));
                break;
            default:
                throw new UnsupportedOperationException("Unable to process keys of " + keysOrRanges.domain());
        }

        return !chains.isEmpty() ? AsyncChains.reduce(chains, (a, b) -> null).beginAsResult() : null;
    }

    private AsyncChain<?> referenceAndDispatchReadsForRange(AsyncOperation.Context context)
    {
        AsyncChain<Set<? extends RoutableKey>> overlappingKeys = findOverlappingKeys((Ranges) keysOrRanges);

        return overlappingKeys.flatMap(keys -> {
            if (keys.isEmpty())
                return AsyncChains.success(null);
            List<AsyncChain<?>> chains = new ArrayList<>();
            keys.forEach(key -> referenceAndAssembleReadsForKey(key, context, chains));
            return chains.isEmpty() ? AsyncChains.success(null) : AsyncChains.reduce(chains, (a, b) -> null);
        }, commandStore);
    }

    private AsyncChain<Set<? extends RoutableKey>> findOverlappingKeys(Ranges ranges)
    {
        Invariants.checkArgument(!ranges.isEmpty());

        List<AsyncChain<Set<PartitionKey>>> chains = new ArrayList<>(ranges.size());
        for (Range range : ranges)
            chains.add(findOverlappingKeys(range));
        return AsyncChains.reduce(chains, (a, b) -> ImmutableSet.<RoutableKey>builder().addAll(a).addAll(b).build());
    }

    private AsyncChain<Set<PartitionKey>> findOverlappingKeys(Range range)
    {
        Set<PartitionKey> cached = commandStore.depsCommandsForKeyCache().stream()
                                               .map(n -> (PartitionKey) n.key())
                                               .filter(range::contains)
                                               .collect(Collectors.toSet());
        // save to a variable as java gets confused when `.map` is called on the result of asChain
        AsyncChain<Set<PartitionKey>> map = Observable.asChain(callback ->
                                                               AccordKeyspace.findAllKeysBetween(commandStore.id(),
                                                                                                 toTokenKey(range.start()).token(), range.startInclusive(),
                                                                                                 toTokenKey(range.end()).token(), range.endInclusive(),
                                                                                                 callback),
                                                               Collectors.toSet());
        return map.map(s -> ImmutableSet.<PartitionKey>builder().addAll(s).addAll(cached).build());
    }

    private static TokenKey toTokenKey(RoutingKey start)
    {
        if (start instanceof TokenKey)
            return (TokenKey) start;
        if (start instanceof AccordRoutingKey.SentinelKey)
            return ((AccordRoutingKey.SentinelKey) start).toTokenKey();
        throw new IllegalArgumentException(String.format("Unable to convert RoutingKey %s (type %s) to TokenKey", start, start.getClass()));
    }

    @VisibleForTesting
    void state(State state)
    {
        this.state = state;
    }

    public boolean load(AsyncOperation.Context context, BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running load for {} with state {}: {} {}", callback, state, txnIds, keysOrRanges);
        commandStore.checkInStoreThread();
        switch (state)
        {
            case INITIALIZED:
                state(State.SETUP);
            case SETUP:
                readResult = referenceAndDispatchReads(context);
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

        logger.trace("Exiting load for {} with state {}: {} {}", callback, state, txnIds, keysOrRanges);
        return state == State.FINISHED;
    }
}
