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
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.CommandsForKey;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.AbstractKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordCommandStore extends CommandStore
{
    public class SafeAccordCommandStore implements SafeCommandStore
    {
        final RangesForEpoch rangesForEpoch;
        final AsyncContext context;

        SafeAccordCommandStore(RangesForEpoch rangesForEpoch, AsyncContext context)
        {
            this.rangesForEpoch = rangesForEpoch;
            this.context = context;
        }

        public AsyncContext context()
        {
            return context;
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
            AccordCommand.WriteOnly command = (AccordCommand.WriteOnly) getContext().commands.getOrCreateWriteOnly(txnId, (ignore, id) -> new AccordCommand.WriteOnly(id), commandStore());
            command.addListener(listener);
            execute(listener.listenerPreLoadContext(txnId), store -> {
                listener.onChange(store, store.command(txnId));
            });
        }

        @Override
        public <T> T mapReduce(Routables<?, ?> keysOrRanges, Ranges slice, Function<CommandsForKey, T> map, BinaryOperator<T> reduce, T initialValue)
        {
            switch (keysOrRanges.kindOfContents()) {
                default:
                    throw new AssertionError();
                case Key:
                    // TODO: efficiency
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    return keys.stream()
                               .filter(slice::contains)
                               .map(this::commandsForKey)
                               .map(map)
                               .reduce(initialValue, reduce);
                case Range:
                    // TODO: implement
                    throw new UnsupportedOperationException();
            }
        }

        public <T> T mapReduce(Routables<?, ?> keysOrRanges, Function<CommandsForKey, T> map, BinaryOperator<T> reduce, T initialValue)
        {
            switch (keysOrRanges.kindOfContents()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    return keys.stream()
                               .map(this::commandsForKey)
                               .map(map)
                               .reduce(initialValue, reduce);
                case Range:
                    // TODO: implement
                    throw new UnsupportedOperationException();
            }
        }

        public void forEach(Routables<?, ?> keysOrRanges, Consumer<CommandsForKey> forEach)
        {
            switch (keysOrRanges.kindOfContents()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    keys.forEach(key -> forEach.accept(commandsForKey(key)));
                    break;
                case Range:
                    // TODO: implement
                    throw new UnsupportedOperationException();
            }
        }

        public void forEach(Routable keyOrRange, Consumer<CommandsForKey> forEach)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    forEach.accept(commandsForKey((Key) keyOrRange));
                    break;
                case Range:
                    // TODO: implement
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void forEach(Routables<?, ?> keysOrRanges, Ranges slice, Consumer<CommandsForKey> forEach)
        {
            switch (keysOrRanges.kindOfContents()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    keys.forEach(slice, key -> {
                        forEach.accept(commandsForKey(key));
                    });
                    break;
                case Range:
                    // TODO: implement
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void forEach(Routable keyOrRange, Ranges slice, Consumer<CommandsForKey> forEach)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    Key key = (Key) keyOrRange;
                    if (slice.contains(key))
                        forEach.accept(commandsForKey(key));
                    break;
                case Range:
                    // TODO: implement
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public AccordCommandStore commandStore()
        {
            return AccordCommandStore.this;
        }

        @Override
        public DataStore dataStore()
        {
            return dataStore;
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
        public Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys)
        {
            Timestamp max = maxConflict(keys);
            long epoch = latestEpoch();
            if (txnId.compareTo(max) > 0 && txnId.epoch() >= epoch && !agent.isExpired(txnId, time.now()))
                return txnId;

            return time.uniqueNow(max);
        }

        @Override
        public Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return AccordCommandStore.this.execute(context, consumer);
        }

        @Override
        public <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return AccordCommandStore.this.submit(context, function);
        }

        @Override
        public NodeTimeService time()
        {
            return time;
        }

        public Timestamp maxConflict(Seekables<?, ?> keys)
        {
            // TODO: Seekables
            // TODO: efficiency
            return ((Keys)keys).stream()
                               .map(this::maybeCommandsForKey)
                               .filter(Objects::nonNull)
                               .map(CommandsForKey::max)
                               .max(Comparator.naturalOrder())
                               .orElse(Timestamp.NONE);
        }
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
    private final RangesForEpochHolder rangesForEpochHolder;

    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              RangesForEpochHolder rangesForEpoch)
    {
        super(id);
        this.time = time;
        this.agent = agent;
        this.dataStore = dataStore;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpochHolder = rangesForEpoch;
        this.loggingId = String.format("[%s]", id);
        this.executor = executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + id + ']');
        this.threadId = getThreadId(this.executor);
        this.stateCache = new AccordStateCache(0);
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

    public SafeAccordCommandStore safeStore(AsyncContext context)
    {
        return new SafeAccordCommandStore(rangesForEpochHolder.get(), context);
    }

    public void checkInStoreThread()
    {
        Invariants.checkState(Thread.currentThread().getId() == threadId);
    }

    public void checkNotInStoreThread()
    {
        Invariants.checkState(Thread.currentThread().getId() != threadId);
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
        Invariants.checkState(currentCtx == null);
        currentCtx = context;
    }

    public AsyncContext getContext()
    {
        Invariants.checkState(currentCtx != null);
        return currentCtx;
    }

    public void unsetContext(AsyncContext context)
    {
        Invariants.checkState(currentCtx == context);
        currentCtx = null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }

    private AccordCommand getCommandInternal(TxnId txnId)
    {
        Invariants.checkState(currentCtx != null);
        AccordCommand command = currentCtx.commands.get(txnId);
        if (command == null)
            throw new IllegalArgumentException("No command in context for txnId " + txnId);
        Invariants.checkState(command.isLoaded() || (command.isReadOnly() && command.isPartiallyLoaded()));
        return command;
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
        Invariants.checkState(commandsForKey.isLoaded());
        return commandsForKey;
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
    public Future<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        AsyncOperation<Void> operation = AsyncOperation.create(this, preLoadContext, consumer);
        executor.execute(operation);
        return operation;
    }

    public void executeBlocking(Runnable runnable)
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
    public void shutdown()
    {
        executor.shutdown();
    }
}
