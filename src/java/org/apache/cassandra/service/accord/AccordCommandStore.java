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

import java.util.Comparator;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class AccordCommandStore extends CommandStore implements SafeCommandStore
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

    private final NodeTimeService time;
    private final Agent agent;
    private final DataStore dataStore;
    private final ProgressLog progressLog;
    private final RangesForEpoch rangesForEpoch;

    public AccordCommandStore(int id,
                              int generation,
                              int index,
                              int numShards,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              RangesForEpoch rangesForEpoch,
                              ExecutorService executor)
    {
        super(id, generation, index, numShards);
        this.time = time;
        this.agent = agent;
        this.dataStore = dataStore;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpoch = rangesForEpoch;
        this.loggingId = String.format("[%s:%s]", generation, index);
        this.executor = executor;
        this.threadId = getThreadId(this.executor);
        this.stateCache = new AccordStateCache(maxCacheSize() / numShards);
        this.commandCache = stateCache.instance(TxnId.class,
                                                AccordCommand.class,
                                                AccordCommand::new);
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

    @Override
    public Command ifLoaded(TxnId txnId)
    {
        AccordCommand command = commandCache.getOrNull(txnId);
        if (command != null && command.isLoaded())
        {
            getContext().commands.add(command);
            return command;
        }
        return null;
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
    public void addAndInvokeListener(TxnId txnId, CommandListener listener)
    {
        AccordCommand.WriteOnly command = (AccordCommand.WriteOnly) getContext().commands.getOrCreateWriteOnly(txnId, (ignore, id) -> new AccordCommand.WriteOnly(id), this);
        command.addListener(listener);
        execute(listener.listenerPreLoadContext(txnId), store -> {
            listener.onChange(store, store.command(txnId));
        });
    }

    @Override
    public CommandStore commandStore()
    {
        return this;
    }

    @Override
    public DataStore dataStore()
    {
        return dataStore;
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
    public <T> Future<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        AsyncOperation<T> operation = AsyncOperation.create(this, loadCtx, function);
        executor.execute(operation);
        return operation;
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    @Override
    public ProgressLog progressLog()
    {
        return progressLog;
    }

    @Override
    public RangesForEpoch ranges()
    {
        return rangesForEpoch;
    }

    @Override
    public long latestEpoch()
    {
        return time.epoch();
    }

    @Override
    public Timestamp preaccept(TxnId txnId, Keys keys)
    {
        Timestamp max = maxConflict(keys);
        long epoch = latestEpoch();
        if (txnId.compareTo(max) > 0 && txnId.epoch >= epoch && !agent.isExpired(txnId, time.now()))
            return txnId;

        return time.uniqueNow(max);
    }

    public Timestamp maxConflict(Keys keys)
    {
        // TODO: efficiency
        return keys.stream()
                   .map(this::maybeCommandsForKey)
                   .filter(Objects::nonNull)
                   .map(CommandsForKey::max)
                   .max(Comparator.naturalOrder())
                   .orElse(Timestamp.NONE);
    }

    @Override
    public Future<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        AsyncOperation<Void> operation = AsyncOperation.create(this, preLoadContext, consumer);
        executor.execute(operation);
        return operation;
    }


    @Override
    public void shutdown()
    {
        // executors are shutdown by AccordCommandStores
    }
}
