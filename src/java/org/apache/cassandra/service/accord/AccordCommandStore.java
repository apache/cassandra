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

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

import com.google.common.base.Preconditions;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.PreLoadContext;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.Clock;
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
    public final String loggingId;
    private final ExecutorService executor;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, AccordCommand> commandCache;
    private final AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> commandsForKeyCache;
    private AsyncContext currentCtx = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;

    public AccordCommandStore(int generation,
                              int index,
                              int numShards,
                              Function<Timestamp, Timestamp> uniqueNow,
                              LongSupplier currentEpoch,
                              Agent agent,
                              DataStore store,
                              ProgressLog.Factory progressLogFactory,
                              RangesForEpoch rangesForEpoch,
                              ExecutorService executor)
    {
        super(generation, index, numShards, uniqueNow, currentEpoch, agent, store, progressLogFactory, rangesForEpoch);
        this.loggingId = String.format("[%s:%s]", generation, index);
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
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }

    private AccordCommand getCommandInternal(TxnId txnId)
    {
        Preconditions.checkState(currentCtx != null);
        AccordCommand command = currentCtx.commands.get(txnId);
        if (command == null)
            throw new IllegalArgumentException("No command in context for txnId " + txnId);

        Preconditions.checkState(command.isLoaded() || (command.isReadOnly() && command.isPartiallyLoaded()));

        return command;
    }

    @Override
    public Command command(TxnId txnId)
    {
        AccordCommand command = getCommandInternal(txnId);
        if (command.isEmpty())
            command.initialize();
        return command;
    }

    @Override
    public Command ifPresent(TxnId txnId)
    {
        AccordCommand command = getCommandInternal(txnId);
        return !command.isEmpty() ? command : null;
    }

    public boolean isCommandsForKeyInContext(PartitionKey key)
    {
        return currentCtx.commandsForKey.get(key) != null;
    }

    private AccordCommandsForKey getCommandsForKeyInternal(Key key)
    {
        Objects.requireNonNull(currentCtx, "current context");
        if (!(key instanceof PartitionKey))
            throw new IllegalArgumentException("Attempted to use non-PartitionKey; given " + key.getClass());
        AccordCommandsForKey commandsForKey = currentCtx.commandsForKey.get((PartitionKey) key);
        if (commandsForKey == null)
            throw new IllegalArgumentException("No commandsForKey in context for key " + key);
        Preconditions.checkState(commandsForKey.isLoaded());
        return commandsForKey;
    }

    @Override
    public CommandsForKey commandsForKey(Key key)
    {
        AccordCommandsForKey commandsForKey = getCommandsForKeyInternal(key);
        if (commandsForKey.isEmpty())
            commandsForKey.initialize();
        return commandsForKey;
    }

    @Override
    public CommandsForKey maybeCommandsForKey(Key key)
    {
        AccordCommandsForKey commandsForKey = getCommandsForKeyInternal(key);
        return !commandsForKey.isEmpty() ? commandsForKey : null;
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
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public <T> Future<T> process(PreLoadContext loadCtx, Function<? super CommandStore, T> function)
    {
        AsyncOperation<T> operation = AsyncOperation.create(this, loadCtx, function);
        executor.execute(operation);
        return operation;
    }

    @Override
    public Future<Void> process(PreLoadContext loadCtx, Consumer<? super CommandStore> consumer)
    {
        AsyncOperation<Void> operation = AsyncOperation.create(this, loadCtx, consumer);
        executor.execute(operation);
        return operation;
    }

    @Override
    public void shutdown()
    {
        // executors are shutdown by AccordCommandStores
    }
}
