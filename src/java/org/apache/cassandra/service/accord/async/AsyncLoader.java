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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordLoadingState;
import org.apache.cassandra.service.accord.AccordSafeState;
import org.apache.cassandra.service.accord.AccordStateCache;
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
    private final Iterable<RoutableKey> keys;

    protected AsyncResult<?> readResult;

    public AsyncLoader(AccordCommandStore commandStore, Iterable<TxnId> txnIds, Iterable<RoutableKey> keys)
    {
        this.commandStore = commandStore;
        this.txnIds = txnIds;
        this.keys = keys;
    }

    private <K, V, S extends AccordSafeState<K, V>> void referenceAndAssembleReads(Iterable<K> keys,
                                                                                   Map<K, S> context,
                                                                                   AccordStateCache.Instance<K, V, S> cache,
                                                                                   Function<K, V> loadFunction,
                                                                                   List<Runnable> loadRunnables,
                                                                                   List<AsyncChain<?>> listenChains)
    {
        for (K key : keys)
        {
            S safeRef = cache.reference(key);
            context.put(key, safeRef);
            AccordLoadingState.LoadingState state = safeRef.loadingState();
            switch (state)
            {
                case UNINITIALIZED:
                    AsyncResults.RunnableResult<V> load = safeRef.load(loadFunction);
                    listenChains.add(load);
                    loadRunnables.add(load);
                    break;
                case PENDING:
                    listenChains.add(safeRef.listen());
                    break;
                case LOADED:
                    break;
                case FAILED:
                    throw new RuntimeException(safeRef.failure());
                default:
                    throw new IllegalStateException("Unhandled loading state: " + state);
            }
        }
    }

    @VisibleForTesting
    Function<TxnId, Command> loadCommandFunction()
    {
        return txnId -> AccordKeyspace.loadCommand(commandStore, txnId);
    }

    @VisibleForTesting
    Function<RoutableKey, CommandsForKey> loadCommandsPerKeyFunction()
    {
        return key -> AccordKeyspace.loadCommandsForKey(commandStore, (PartitionKey) key);
    }

    private AsyncResult<?> referenceAndDispatchReads(AsyncOperation.Context context)
    {
        List<Runnable> readRunnables = new ArrayList<>();
        List<AsyncChain<?>> chains = new ArrayList<>();

        referenceAndAssembleReads(txnIds,
                                  context.commands,
                                  commandStore.commandCache(),
                                  loadCommandFunction(),
                                  readRunnables,
                                  chains);

        referenceAndAssembleReads(keys,
                                  context.commandsForKeys,
                                  commandStore.commandsForKeyCache(),
                                  loadCommandsPerKeyFunction(),
                                  readRunnables,
                                  chains);

        if (chains.isEmpty())
        {
            Invariants.checkState(readRunnables.isEmpty());
            return null;
        }

        // runnable results are already contained in the chains collection
        if (!readRunnables.isEmpty())
            AsyncChains.ofRunnables(Stage.READ.executor(), readRunnables).begin(commandStore.agent());

        return !chains.isEmpty() ? AsyncChains.reduce(chains, (a, b) -> null).beginAsResult() : null;
    }

    @VisibleForTesting
    void state(State state)
    {
        this.state = state;
    }

    public boolean load(AsyncOperation.Context context, BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running load for {} with state {}: {} {}", callback, state, txnIds, keys);
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

        logger.trace("Exiting load for {} with state {}: {} {}", callback, state, txnIds, keys);
        return state == State.FINISHED;
    }
}
