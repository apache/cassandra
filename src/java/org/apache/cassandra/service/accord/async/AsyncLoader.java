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
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;


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
    private final Iterable<PartitionKey> keys;

    protected Future<?> readFuture;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys)
    {
        this.commandStore = commandStore;
        this.txnIds = txnIds;
        this.keys = keys;
    }

    private <K, V extends AccordState<K>> Future<?> referenceAndDispatch(K key,
                                                                         AccordStateCache.Instance<K, V> cache,
                                                                         Map<K, V> context,
                                                                         Function<V, Future<?>> readFunction,
                                                                         Object callback)
    {
        V item;
        Future<?> future = cache.getLoadFuture(key);
        if (future != null)
        {
            // if a load future exists for this, it must be present in the cache
            item = cache.getOrNull(key);
            Preconditions.checkState(item != null);
            context.put(key, item);
            if (logger.isTraceEnabled())
                logger.trace("Existing load future found for {} while loading for {}. ({})", item.key(), callback, item);
            return future;
        }

        item = cache.getOrCreate(key);
        context.put(key, item);
        if (item.isLoaded())
        {
            if (logger.isTraceEnabled())
                logger.trace("Cached item found for {} while loading for {}. ({})", item.key(), callback, item);
            return null;
        }

        future = readFunction.apply(item);
        cache.setLoadFuture(item.key(), future);
        if (logger.isTraceEnabled())
            logger.trace("Loading new item for {} while loading for {}. ({})", item.key(), callback, item);
        return future;
    }


    private <K, V extends AccordState<K>> List<Future<?>> referenceAndDispatchReads(Iterable<K> keys,
                                                                                           AccordStateCache.Instance<K, V> cache,
                                                                                           Map<K, V> context,
                                                                                           Function<V, Future<?>> readFunction,
                                                                                           List<Future<?>> futures,
                                                                                           Object callback)
    {
        for (K key : keys)
        {
            Future<?> future = referenceAndDispatch(key, cache, context, readFunction, callback);
            if (future == null)
                continue;

            if (futures == null)
                futures = new ArrayList<>();

            futures.add(future);
        }

        return futures;
    }

    @VisibleForTesting
    Function<AccordCommand, Future<?>> loadCommandFunction(Object callback)
    {
        return command -> Stage.READ.submit(() -> {
            try
            {
                logger.trace("Starting load of {} for {}", command.txnId(), callback);
                AccordKeyspace.loadCommand(commandStore, command);
                logger.trace("Completed load of {} for {}", command.txnId(), callback);
            }
            catch (Throwable t)
            {
                logger.error("Exception loading {} for {}", command.txnId(), callback, t);
                throw t;
            }
        });
    }

    @VisibleForTesting
    Function<AccordCommandsForKey, Future<?>> loadCommandsPerKeyFunction(Object callback)
    {
        return cfk -> Stage.READ.submit(() -> {
            try
            {
                logger.trace("Starting load of {} for {}", cfk.key(), callback);
                AccordKeyspace.loadCommandsForKey(cfk);
                logger.trace("Completed load of {} for {}", cfk.key(), callback);
            }
            catch (Throwable t)
            {
                logger.error("Exception loading {} for {}", cfk.key(), callback, t);
                throw t;
            }
        });
    }

    private Future<?> referenceAndDispatchReads(AsyncContext context, Object callback)
    {
        List<Future<?>> futures = null;

        futures = referenceAndDispatchReads(txnIds,
                                            commandStore.commandCache(),
                                            context.commands.items,
                                            loadCommandFunction(callback),
                                            futures,
                                            callback);

        futures = referenceAndDispatchReads(keys,
                                            commandStore.commandsForKeyCache(),
                                            context.commandsForKey.items,
                                            loadCommandsPerKeyFunction(callback),
                                            futures,
                                            callback);

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }

    @VisibleForTesting
    void state(State state)
    {
        this.state = state;
    }

    public boolean load(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running load for {} with state {}: {} {}", callback, state, txnIds, keys);
        commandStore.checkInStoreThread();
        switch (state)
        {
            case INITIALIZED:
                state(State.SETUP);
            case SETUP:
                // notify any pending write only groups we're loading a full instance so the pending changes aren't removed
                txnIds.forEach(commandStore.commandCache()::lockWriteOnlyGroupIfExists);
                keys.forEach(commandStore.commandsForKeyCache()::lockWriteOnlyGroupIfExists);
                readFuture = referenceAndDispatchReads(context, callback);
                state(State.LOADING);
            case LOADING:
                if (readFuture != null)
                {
                    if (readFuture.isSuccess())
                    {
                        logger.trace("Read future succeeded for {}", callback);
                        context.verifyLoaded();
                        readFuture = null;
                    }
                    else
                    {
                        logger.trace("Adding callback for read future: {}", callback);
                        readFuture.addCallback(callback, commandStore.executor());
                        break;
                    }
                }
                // apply any pending write only changes that may not have made it to disk in time to be loaded
                context.commands.items.keySet().forEach(commandStore.commandCache()::cleanupLoadFuture);
                context.commands.items.values().forEach(commandStore.commandCache()::applyAndRemoveWriteOnlyGroup);
                context.commandsForKey.items.keySet().forEach(commandStore.commandsForKeyCache()::cleanupLoadFuture);
                context.commandsForKey.items.values().forEach(commandStore.commandsForKeyCache()::applyAndRemoveWriteOnlyGroup);
                // apply blindly reported timestamps
                context.commandsForKey.items.values().forEach(AccordCommandsForKey::applyBlindWitnessedTimestamps);
                state(State.FINISHED);
            case FINISHED:
                break;
            default:
                throw new IllegalStateException("Unexpected state: " + state);
        }

        logger.trace("Exiting load for {} with state {}: {} {}", callback, state, txnIds, keys);
        return state == State.FINISHED;
    }
}
