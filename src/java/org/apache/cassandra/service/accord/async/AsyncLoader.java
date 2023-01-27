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
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static accord.utils.async.AsyncResults.ofRunnable;

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

    protected AsyncResult<?> readResult;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<PartitionKey> keys)
    {
        this.commandStore = commandStore;
        this.txnIds = txnIds;
        this.keys = keys;
    }

    private <K, V extends AccordState<K>> AsyncResult<Void> referenceAndDispatch(K key,
                                                                                 AccordStateCache.Instance<K, V> cache,
                                                                                 Map<K, V> context,
                                                                                 Function<V, AsyncResult<Void>> readFunction,
                                                                                 Object callback)
    {
        V item;
        AsyncResult<Void> result = cache.getLoadResult(key);
        if (result != null)
        {
            // if a load result exists for this, it must be present in the cache
            item = cache.getOrNull(key);
            Preconditions.checkState(item != null);
            context.put(key, item);
            if (logger.isTraceEnabled())
                logger.trace("Existing load result found for {} while loading for {}. ({})", item.key(), callback, item);
            return result;
        }

        item = cache.getOrCreate(key);
        context.put(key, item);
        if (item.isLoaded())
        {
            if (logger.isTraceEnabled())
                logger.trace("Cached item found for {} while loading for {}. ({})", item.key(), callback, item);
            return null;
        }

        result = readFunction.apply(item);
        cache.setLoadResult(item.key(), result);
        if (logger.isTraceEnabled())
            logger.trace("Loading new item for {} while loading for {}. ({})", item.key(), callback, item);
        return result;
    }


    private <K, V extends AccordState<K>> List<AsyncChain<Void>> referenceAndDispatchReads(Iterable<K> keys,
                                                                                           AccordStateCache.Instance<K, V> cache,
                                                                                           Map<K, V> context,
                                                                                           Function<V, AsyncResult<Void>> readFunction,
                                                                                           List<AsyncChain<Void>> results,
                                                                                           Object callback)
    {
        for (K key : keys)
        {
            AsyncResult<Void> result = referenceAndDispatch(key, cache, context, readFunction, callback);
            if (result == null)
                continue;

            if (results == null)
                results = new ArrayList<>();

            results.add(result);
        }

        return results;
    }

    @VisibleForTesting
    Function<AccordCommand, AsyncResult<Void>> loadCommandFunction(Object callback)
    {
        return command -> ofRunnable(Stage.READ.executor(), () -> {
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
    Function<AccordCommandsForKey, AsyncResult<Void>> loadCommandsPerKeyFunction(Object callback)
    {
        return cfk -> ofRunnable(Stage.READ.executor(), () -> {
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

    private AsyncResult<Void> referenceAndDispatchReads(AsyncContext context, Object callback)
    {
        List<AsyncChain<Void>> results = null;

        results = referenceAndDispatchReads(txnIds,
                                            commandStore.commandCache(),
                                            context.commands.items,
                                            loadCommandFunction(callback),
                                            results,
                                            callback);

        results = referenceAndDispatchReads(keys,
                                            commandStore.commandsForKeyCache(),
                                            context.commandsForKey.items,
                                            loadCommandsPerKeyFunction(callback),
                                            results,
                                            callback);

        return results != null ? AsyncResults.reduce(results, (a, b ) -> null).beginAsResult() : null;
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
                readResult = referenceAndDispatchReads(context, callback);
                state(State.LOADING);
            case LOADING:
                if (readResult != null)
                {
                    if (readResult.isSuccess())
                    {
                        logger.trace("Read result succeeded for {}", callback);
                        context.verifyLoaded();
                        readResult = null;
                    }
                    else
                    {
                        logger.trace("Adding callback for read result: {}", callback);
                        readResult.addCallback(callback, commandStore.executor());
                        break;
                    }
                }
                // apply any pending write only changes that may not have made it to disk in time to be loaded
                context.commands.items.keySet().forEach(commandStore.commandCache()::cleanupLoadResult);
                context.commands.items.values().forEach(commandStore.commandCache()::applyAndRemoveWriteOnlyGroup);
                context.commandsForKey.items.keySet().forEach(commandStore.commandsForKeyCache()::cleanupLoadResult);
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
