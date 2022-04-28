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
import java.util.function.Predicate;

import accord.txn.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordStateCache.AccordState;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;


public class AsyncLoader
{
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

    private static <K, V extends AccordState<K, V>> Future<?> referenceAndDispatch(K key,
                                                                                   AccordStateCache.Instance<K, V> cache,
                                                                                   Map<K, V> context,
                                                                                   Predicate<V> isLoaded,
                                                                                   Function<V, Future<?>> readFunction)
    {
        Future<?> future = cache.getLoadFuture(key);
        if (future != null)
            return future;

        V item = cache.getOrCreate(key);
        context.put(key, item);
        if (isLoaded.test(item))
            return null;

        future = readFunction.apply(item);
        cache.setLoadFuture(item.key(), future);
        return future;
    }


    private static <K, V extends AccordState<K, V>> List<Future<?>> referenceAndDispatchReads(Iterable<K> keys,
                                                                                              AccordStateCache.Instance<K, V> cache,
                                                                                              Map<K, V> context,
                                                                                              Predicate<V> isLoaded,
                                                                                              Function<V, Future<?>> readFunction,
                                                                                              List<Future<?>> futures)
    {
        for (K key : keys)
        {
            Future<?> future = referenceAndDispatch(key, cache, context, isLoaded, readFunction);
            if (future == null)
                continue;

            if (futures == null)
                futures = new ArrayList<>();

            futures.add(future);
        }

        return futures;
    }

    private Future<?> referenceAndDispatchReads(AsyncContext context)
    {
        List<Future<?>> futures = null;

        futures = referenceAndDispatchReads(txnIds,
                                            commandStore.commandCache(),
                                            context.commands.items,
                                            AccordCommand::isLoaded,
                                            command -> Stage.READ.submit(() -> AccordKeyspace.loadCommand(command)),
                                            futures);

        futures = referenceAndDispatchReads(keys,
                                            commandStore.commandsForKeyCache(),
                                            context.commandsForKey.items,
                                            AccordCommandsForKey::isLoaded,
                                            cfk -> Stage.READ.submit(() -> AccordKeyspace.loadCommandsForKey(cfk)),
                                            futures);

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }

    public boolean load(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        switch (state)
        {
            case INITIALIZED:
                state = State.SETUP;
            case SETUP:
                // notify any pending write only groups we're loading a full instance so the pending changes aren't removed
                txnIds.forEach(commandStore.commandCache()::lockWriteOnlyGroupIfExists);
                keys.forEach(commandStore.commandsForKeyCache()::lockWriteOnlyGroupIfExists);
                readFuture = referenceAndDispatchReads(context);
                state = State.LOADING;
            case LOADING:
                if (readFuture != null && !readFuture.isDone())
                {
                    readFuture.addCallback(callback, commandStore.executor());
                    break;
                }
                // apply any pending write only changes that may not have made it to disk in time to be loaded
                context.commands.items.values().forEach(commandStore.commandCache()::applyAndRemoveWriteOnlyGroup);
                context.commandsForKey.items.values().forEach(commandStore.commandsForKeyCache()::applyAndRemoveWriteOnlyGroup);
                state = State.FINISHED;
            case FINISHED:
                break;
            default:
                throw new IllegalStateException();
        }

        return state == State.FINISHED;
    }
}
