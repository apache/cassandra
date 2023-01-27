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
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.AbstractKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.service.accord.api.PartitionKey;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.async.AsyncContext;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.Clock;
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

        public <T> T mapReduce(Routables<?, ?> keysOrRanges, Function<CommandsForKey, T> map, BinaryOperator<T> reduce, T initialValue)
        {
            switch (keysOrRanges.domain()) {
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

        private <O> O mapReduceForKey(Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandsForKey, O, O> map, O accumulate, O terminalValue)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    // TODO: efficiency
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    for (Key key : keys)
                    {
                        if (!slice.contains(key)) continue;
                        CommandsForKey forKey = commandsForKey(key);
                        accumulate = map.apply(forKey, accumulate);
                        if (accumulate.equals(terminalValue))
                            return accumulate;
                    }
                    break;
                case Range:
                    // TODO (required): implement
                    throw new UnsupportedOperationException();
            }
            return accumulate;
        }

        @Override
        public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
        {
            accumulate = mapReduceForKey(keysOrRanges, slice, (forKey, prev) -> {
                CommandsForKey.CommandTimeseries timeseries;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case STARTED_BEFORE:
                        timeseries = forKey.byId();
                        break;
                    case EXECUTES_AFTER:
                    case MAY_EXECUTE_BEFORE:
                        timeseries = forKey.byExecuteAt();
                }
                CommandsForKey.CommandTimeseries.TestTimestamp remapTestTimestamp;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case EXECUTES_AFTER:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.AFTER;
                        break;
                    case STARTED_BEFORE:
                    case MAY_EXECUTE_BEFORE:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.BEFORE;
                }
                return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
            }, accumulate, terminalValue);

            return accumulate;
        }

        @Override
        public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
        {
            // TODO (required): support ranges
            Routables.foldl((Keys)keysOrRanges, slice, (k, v, i) -> { commandsForKey(k).register(command); return v; }, null);
        }

        @Override
        public void register(Seekable keyOrRange, Ranges slice, Command command)
        {
            // TODO (required): support ranges
            Key key = (Key) keyOrRange;
            if (slice.contains(key))
                commandsForKey(key).register(command);
        }

        public AccordCommandsForKey commandsForKey(Key key)
        {
            AccordCommandsForKey commandsForKey = getCommandsForKeyInternal(key);
            if (commandsForKey.isEmpty())
                commandsForKey.initialize();
            return commandsForKey;
        }

        public AccordCommandsForKey maybeCommandsForKey(Key key)
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
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return AccordCommandStore.this.execute(context, consumer);
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
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
    public <T> AsyncChain<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AsyncOperation.create(this, loadCtx, function);
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    @Override
    public AsyncChain<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AsyncOperation.create(this, preLoadContext, consumer);
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
