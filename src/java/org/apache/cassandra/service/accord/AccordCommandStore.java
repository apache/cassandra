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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.TxnOperation;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class AccordCommandStore extends CommandStore
{
    public static long maxCacheSize()
    {
        return 5 << 20; // TODO: make configurable
    }

    private static long getThreadId(ExecutorService executor)
    {
        try
        {
            return executor.submit(() -> Thread.currentThread().getId()).get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final long threadId;
    private final ExecutorService executor;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, AccordCommand> commandCache;
    private final AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> commandsForKeyCache;
    private AsyncContext currentCtx = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;

    public AccordCommandStore(int generation,
                              int index,
                              int numShards,
                              Node.Id nodeId,
                              Function<Timestamp, Timestamp> uniqueNow,
                              Agent agent,
                              Store store,
                              KeyRanges ranges,
                              Supplier<Topology> localTopologySupplier,
                              ExecutorService executor)
    {
        super(generation, index, numShards, nodeId, uniqueNow, agent, store, ranges, localTopologySupplier);
        this.executor = executor;
        this.threadId = getThreadId(executor);
        this.stateCache = new AccordStateCache(maxCacheSize() / numShards);
        this.commandCache = stateCache.instance(TxnId.class,
                                                AccordCommand.class,
                                                txnId -> new AccordCommand(this, txnId));
        this.commandsForKeyCache = stateCache.instance(PartitionKey.class,
                                                       AccordCommandsForKey.class,
                                                       key -> new AccordCommandsForKey(this, key));
    }

    void setCacheSize(long bytes)
    {
        checkInStoreThread();
        stateCache.setMaxSize(bytes);
    }

    public void checkInStoreThread()
    {
        Preconditions.checkState(Thread.currentThread().getId() == threadId);
    }

    public void checkNotInStoreThread()
    {
        Preconditions.checkState(Thread.currentThread().getId() != threadId);
    }

    public ExecutorService executor()
    {
        return executor;
    }

    public AccordStateCache.Instance<TxnId, AccordCommand> commandCache()
    {
        return commandCache;
    }

    public AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    public void setContext(AsyncContext context)
    {
        Preconditions.checkState(currentCtx == null);
        currentCtx = context;
    }

    public AsyncContext getContext()
    {
        Preconditions.checkState(currentCtx != null);
        return currentCtx;
    }

    public void unsetContext(AsyncContext context)
    {
        Preconditions.checkState(currentCtx == context);
        currentCtx = null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }

    @Override
    public Command command(TxnId txnId)
    {
        Preconditions.checkState(currentCtx != null);
        AccordCommand command = currentCtx.commands.get(txnId);
        if (command == null)
            command = currentCtx.commands.summary(txnId);
        Preconditions.checkArgument(command != null);
        return command;
    }

    @Override
    public CommandsForKey commandsForKey(Key key)
    {
        Preconditions.checkState(currentCtx != null);
        Preconditions.checkArgument(key instanceof PartitionKey);
        AccordCommandsForKey commandsForKey = currentCtx.commandsForKey.get((PartitionKey) key);
        Preconditions.checkArgument(commandsForKey != null);
        return commandsForKey;
    }

    @Override
    public Future<Void> processSetup(Consumer<? super CommandStore> function)
    {
        AsyncPromise<Void> promise = new AsyncPromise<>();
        executor.execute(() -> {
            try
            {
                function.accept(this);
                promise.trySuccess(null);
            }
            catch (Throwable t)
            {
                promise.tryFailure(t);
            }
        });
        return promise;
    }

    @Override
    public <T> Future<T> processSetup(Function<? super CommandStore, T> function)
    {
        AsyncPromise<T> promise = new AsyncPromise<>();
        executor.execute(() -> {
            try
            {
                T result = function.apply(this);
                promise.trySuccess(result);
            }
            catch (Throwable t)
            {
                promise.tryFailure(t);
            }
        });
        return promise;
    }

    public void processBlocking(Runnable runnable)
    {
        try
        {
            executor.submit(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> Future<T> process(TxnOperation scope, Function<? super CommandStore, T> function)
    {
        AsyncOperation<T> operation = AsyncOperation.create(this, scope, function);
        executor.execute(operation);
        return operation;
    }

    @Override
    public Future<Void> process(TxnOperation scope, Consumer<? super CommandStore> consumer)
    {
        AsyncOperation<Void> operation = AsyncOperation.create(this, scope, consumer);
        executor.execute(operation);
        return operation;
    }

    @Override
    public void shutdown()
    {
        // executors are shutdown by AccordCommandStores
    }
}
