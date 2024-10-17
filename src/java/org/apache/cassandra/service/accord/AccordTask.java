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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.apache.cassandra.service.accord.AccordCachingState.Status;
import org.apache.cassandra.service.accord.AccordCommandStore.Caches;
import org.apache.cassandra.service.accord.AccordExecutor.TaskQueue;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.config.CassandraRelevantProperties.DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED;
import static org.apache.cassandra.service.accord.AccordTask.State.CANCELLED;
import static org.apache.cassandra.service.accord.AccordTask.State.FAILED;
import static org.apache.cassandra.service.accord.AccordTask.State.FINISHED;
import static org.apache.cassandra.service.accord.AccordTask.State.INITIALIZED;
import static org.apache.cassandra.service.accord.AccordTask.State.LOADING;
import static org.apache.cassandra.service.accord.AccordTask.State.PERSISTING;
import static org.apache.cassandra.service.accord.AccordTask.State.RUNNING;
import static org.apache.cassandra.service.accord.AccordTask.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_RUN;
import static org.apache.cassandra.service.accord.AccordTask.State.WAITING_TO_SCAN_RANGES;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class AccordTask<R> extends AccordExecutor.Task implements Runnable, Function<SafeCommandStore, R>, Cancellable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final boolean SANITY_CHECK = DTEST_ACCORD_JOURNAL_SANITY_CHECK_ENABLED.getBoolean();

    private static class LoggingProps
    {
        private static final String COMMAND_STORE = "command_store";
        private static final String ACCORD_TASK = "accord_task";
    }

    static class ForFunction<R> extends AccordTask<R>
    {
        private final Function<? super SafeCommandStore, R> function;

        public ForFunction(AccordCommandStore commandStore, PreLoadContext loadCtx, Function<? super SafeCommandStore, R> function)
        {
            super(commandStore, loadCtx);
            this.function = function;
        }

        @Override
        public R apply(SafeCommandStore commandStore)
        {
            return function.apply(commandStore);
        }
    }

    // TODO (desired): these anonymous ops are somewhat tricky to debug. We may want to at least give them names.
    static class ForConsumer extends AccordTask<Void>
    {
        private final Consumer<? super SafeCommandStore> consumer;

        private ForConsumer(AccordCommandStore commandStore, PreLoadContext loadCtx, Consumer<? super SafeCommandStore> consumer)
        {
            super(commandStore, loadCtx);
            this.consumer = consumer;
        }

        @Override
        public Void apply(SafeCommandStore commandStore)
        {
            consumer.accept(commandStore);
            return null;
        }
    }

    public static <T> AccordTask<T> create(CommandStore commandStore, PreLoadContext ctx, Function<? super SafeCommandStore, T> function)
    {
        return new ForFunction<>((AccordCommandStore) commandStore, ctx, function);
    }

    public static AccordTask<Void> create(CommandStore commandStore, PreLoadContext ctx, Consumer<? super SafeCommandStore> consumer)
    {
        return new ForConsumer((AccordCommandStore) commandStore, ctx, consumer);
    }

    public enum State
    {
        INITIALIZED(),
        WAITING_TO_SCAN_RANGES(INITIALIZED),
        SCANNING_RANGES(WAITING_TO_SCAN_RANGES),
        WAITING_TO_LOAD(INITIALIZED, SCANNING_RANGES),
        LOADING(INITIALIZED, SCANNING_RANGES, WAITING_TO_LOAD),
        WAITING_TO_RUN(INITIALIZED, SCANNING_RANGES, WAITING_TO_LOAD, LOADING),
        RUNNING(WAITING_TO_RUN),
        PERSISTING(RUNNING),
        FINISHED(RUNNING, PERSISTING),
        CANCELLED(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN),
        FAILED(WAITING_TO_SCAN_RANGES, SCANNING_RANGES, WAITING_TO_LOAD, LOADING, WAITING_TO_RUN, RUNNING, PERSISTING);

        private final int permittedFrom;

        State()
        {
            this.permittedFrom = 0;
        }

        State(State ... permittedFroms)
        {
            int permittedFrom = 0;
            for (State state : permittedFroms)
                permittedFrom |= 1 << state.ordinal();
            this.permittedFrom = permittedFrom;
        }

        boolean isPermittedFrom(State prev)
        {
            return (permittedFrom & (1 << prev.ordinal())) != 0;
        }

        boolean isExecuted()
        {
            return this.compareTo(PERSISTING) >= 0;
        }

        boolean isComplete()
        {
            return this.compareTo(FINISHED) >= 0;
        }
    }

    private State state = INITIALIZED;
    private final PreLoadContext preLoadContext;
    private final String loggingId;

    @Nullable Object2ObjectHashMap<TxnId, AccordSafeCommand> commands;
    @Nullable Object2ObjectHashMap<RoutingKey, AccordSafeTimestampsForKey> timestampsForKey;
    @Nullable Object2ObjectHashMap<RoutingKey, AccordSafeCommandsForKey> commandsForKey;
    @Nullable Object2ObjectHashMap<Object, AccordSafeState<?, ?>> loading;
    // TODO (expected): collection supporting faster deletes but still fast poll (e.g. some ordered collection)
    @Nullable ArrayDeque<AccordCachingState<?, ?>> waitingToLoad;
    @Nullable RangeLoader rangeLoader;
    @Nullable CommandsForRanges commandsForRanges;
    private TaskQueue queued;

    private BiConsumer<? super R, Throwable> callback;
    private List<Command> sanityCheck;
    public long createdAt = nanoTime(), loadedAt, runQueuedAt, runAt, completedAt;

    private void setLoggingIds()
    {
        MDC.put(LoggingProps.COMMAND_STORE, commandStore.loggingId);
        MDC.put(LoggingProps.ACCORD_TASK, loggingId);
    }

    private void clearLoggingIds()
    {
        MDC.remove(LoggingProps.COMMAND_STORE);
        MDC.remove(LoggingProps.ACCORD_TASK);
    }

    public AccordTask(AccordCommandStore commandStore, PreLoadContext preLoadContext)
    {
        super(commandStore);
        this.loggingId = "0x" + Integer.toHexString(System.identityHashCode(this));
        this.preLoadContext = preLoadContext;

        if (logger.isTraceEnabled())
        {
            setLoggingIds();
            logger.trace("Created {} on {}", this, commandStore);
            clearLoggingIds();
        }
    }

    @Override
    public String toString()
    {
        return "AsyncOperation{" + state + "}-" + loggingId;
    }

    public String toDescription()
    {
        return "AsyncOperation{" + state + "}-" + loggingId + ": "
               + (queued == null ? "unqueued" : queued.kind)
               + ", waitingToLoad: " + summarise(waitingToLoad)
               + ", loading:" + summarise(loading)
               + ", cfks:" + summarise(commandsForKey)
               + ", tfks:" + summarise(timestampsForKey)
               + ", txns:" + summarise(commands);

    }

    private static String summarise(Map<?, ?> map)
    {
        if (map == null)
            return "null";

        return summarise(map.keySet());
    }

    private static String summarise(Collection<?> collection)
    {
        if (collection == null)
            return "null";

        if (collection.size() < 10)
            return collection.toString();

        return collection.stream().limit(10).map(Object::toString).collect(Collectors.joining(",", "[", "...(*" + (collection.size() - 10) + ")]"));
    }

    private void state(State state)
    {
        Invariants.checkState(state.isPermittedFrom(this.state), "%s forbidden from %s", state, this.state);
        this.state = state;
        if (state == WAITING_TO_RUN) loadedAt = nanoTime();
        else if (state == RUNNING) runAt = nanoTime();
        else if (state.isExecuted()) completedAt = nanoTime();
    }

    Unseekables<?> keys()
    {
        return preLoadContext.keys();
    }

    public AsyncChain<R> chain()
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super R, Throwable> callback)
            {
                Invariants.checkState(AccordTask.this.callback == null);
                AccordTask.this.callback = callback;
                commandStore.executor().submit(AccordTask.this);
                return AccordTask.this;
            }
        };
    }

    public void setupExclusive()
    {
        setupInternal(commandStore.cachesExclusive());
        state(rangeLoader != null ? WAITING_TO_SCAN_RANGES
                                  : waitingToLoad != null ? State.WAITING_TO_LOAD
                                    : loading != null ? LOADING : WAITING_TO_RUN);
    }

    private void setupInternal(Caches caches)
    {
        for (TxnId txnId : preLoadContext.txnIds())
            setupExclusive(txnId, AccordTask::ensureCommands, caches.commands());

        if (preLoadContext.keys().isEmpty())
            return;

        switch (preLoadContext.keys().domain())
        {
            default: throw new AssertionError("Unhandled Domain: " + preLoadContext.keys().domain());
            case Key:
                switch (preLoadContext.keyHistory())
                {
                    default: throw new AssertionError("Unhandled KeyHistory: " + preLoadContext.keyHistory());
                    case NONE:
                        break;

                    case TIMESTAMPS:
                        for (RoutingKey key : (AbstractUnseekableKeys)preLoadContext.keys())
                            setupExclusive(key, AccordTask::ensureTimestampsForKey, caches.timestampsForKeys());
                        break;

                    case ASYNC:
                    case RECOVER:
                    case INCR:
                    case SYNC:
                        for (RoutingKey key : (AbstractUnseekableKeys)preLoadContext.keys())
                            setupExclusive(key, AccordTask::ensureCommandsForKey, caches.commandsForKeys());
                        break;
                }
                break;

            case Range:
                switch (preLoadContext.keyHistory())
                {
                    default: throw new AssertionError("Unhandled KeyHistory: " + preLoadContext.keyHistory());
                    case NONE:
                        break;

                    case TIMESTAMPS:
                        throw new AssertionError("TimestampsForKey unsupported for range transactions");

                    case ASYNC:
                    case RECOVER:
                    case INCR:
                    case SYNC:
                        rangeLoader = new RangeLoader(caches.commandsForKeys());
                }
                break;
        }
    }

    // expects to hold lock
    private <K, V, S extends AccordSafeState<K, V>> void setupExclusive(K k, Function<AccordTask<?>, Map<? super K, ? super S>> loaded, AccordStateCache.Type<K, V, S>.Instance cache)
    {
        S safeRef = cache.acquire(k);
        Status status = safeRef.global().status();
        Map<? super K, ? super S> map;
        switch (status)
        {
            default: throw new IllegalStateException("Unhandled global state: " + status);
            case WAITING_TO_LOAD:
            case LOADING:
                map = ensureLoading();
                break;
            case SAVING:
            case LOADED:
            case MODIFIED:
            case FAILED_TO_SAVE:
                map = loaded.apply(this);
        }

        Object prev = map.putIfAbsent(k, safeRef);
        if (prev != null)
        {
            noSpamLogger.warn("PreLoadContext {} contained key {} more than once", map, k);
            cache.release(safeRef, this);
        }
        else if (map == loading)
        {
            if (status == Status.WAITING_TO_LOAD)
                ensureWaitingToLoad().add(safeRef.global());
            safeRef.global().loadingOrWaiting().add(this);
            Invariants.checkState(safeRef.global().loadingOrWaiting().waiters().size() == safeRef.global().referenceCount());
        }
    }

    // expects to hold lock
    public boolean onLoad(AccordCachingState<?, ?> state)
    {
        Invariants.checkState(loading != null);
        AccordSafeState<?, ?> safeRef = loading.remove(state.key());
        Invariants.checkState(safeRef != null && safeRef.global() == state);
        if (safeRef.getClass() == AccordSafeCommand.class)
        {
            ensureCommands().put((TxnId)state.key(), (AccordSafeCommand) safeRef);
        }
        else if (safeRef.getClass() == AccordSafeCommandsForKey.class)
        {
            ensureCommandsForKey().put((RoutingKey) state.key(), (AccordSafeCommandsForKey) safeRef);
        }
        else
        {
            Invariants.checkState (safeRef.getClass() == AccordSafeTimestampsForKey.class);
            ensureTimestampsForKey().put((RoutingKey) state.key(), (AccordSafeTimestampsForKey) safeRef);
        }

        if (!loading.isEmpty())
            return false;

        loading = null;
        if (this.state.compareTo(State.WAITING_TO_LOAD) < 0)
            return false;

        state(WAITING_TO_RUN);
        return true;
    }

    // expects to hold lock
    public boolean onLoading(AccordCachingState<?, ?> state)
    {
        Invariants.checkState(waitingToLoad != null);
        boolean removed = waitingToLoad.remove(state);
        Invariants.checkState(removed);
        if (!waitingToLoad.isEmpty())
            return false;

        return onEmptyWaitingToLoad();
    }

    private boolean onEmptyWaitingToLoad()
    {
        waitingToLoad = null;
        if (this.state.compareTo(State.WAITING_TO_LOAD) < 0)
            return false;

        state(loading == null ? WAITING_TO_RUN : LOADING);
        return true;
    }

    public PreLoadContext preLoadContext()
    {
        return preLoadContext;
    }

    public Map<TxnId, AccordSafeCommand> commands()
    {
        return commands;
    }

    public Map<TxnId, AccordSafeCommand> ensureCommands()
    {
        if (commands == null)
            commands = new Object2ObjectHashMap<>();
        return commands;
    }

    public Map<RoutingKey, AccordSafeTimestampsForKey> timestampsForKey()
    {
        return timestampsForKey;
    }

    public Map<RoutingKey, AccordSafeTimestampsForKey> ensureTimestampsForKey()
    {
        if (timestampsForKey == null)
            timestampsForKey = new Object2ObjectHashMap<>();
        return timestampsForKey;
    }

    public Map<RoutingKey, AccordSafeCommandsForKey> commandsForKey()
    {
        return commandsForKey;
    }

    public Map<RoutingKey, AccordSafeCommandsForKey> ensureCommandsForKey()
    {
        if (commandsForKey == null)
            commandsForKey = new Object2ObjectHashMap<>();
        return commandsForKey;
    }

    private Map<Object, AccordSafeState<?, ?>> ensureLoading()
    {
        if (loading == null)
            loading = new Object2ObjectHashMap<>();
        return loading;
    }

    private ArrayDeque<AccordCachingState<?, ?>> ensureWaitingToLoad()
    {
        if (waitingToLoad == null)
            waitingToLoad = new ArrayDeque<>();
        return waitingToLoad;
    }

    public AccordCachingState<?, ?> pollWaitingToLoad()
    {
        Invariants.checkState(state == State.WAITING_TO_LOAD);
        if (waitingToLoad == null)
            return null;

        AccordCachingState<?, ?> next = waitingToLoad.poll();
        if (waitingToLoad.isEmpty())
            onEmptyWaitingToLoad();
        return next;
    }

    public AccordCachingState<?, ?> peekWaitingToLoad()
    {
        return waitingToLoad == null ? null : waitingToLoad.peek();
    }

    private void maybeSanityCheck(AccordSafeCommand safeCommand)
    {
        if (SANITY_CHECK)
        {
            if (sanityCheck == null)
                sanityCheck = new ArrayList<>(commands.size());
            sanityCheck.add(safeCommand.current());
        }
    }

    private void save(List<SavedCommand.DiffWriter> diffs, Runnable onFlush)
    {
        if (sanityCheck != null)
        {
            Invariants.checkState(SANITY_CHECK);
            Condition condition = Condition.newOneTimeCondition();
            this.commandStore.appendCommands(diffs, condition::signal);
            condition.awaitUninterruptibly();

            for (Command check : sanityCheck)
                this.commandStore.sanityCheckCommand(check);

            if (onFlush != null) onFlush.run();
        }
        else
        {
            this.commandStore.appendCommands(diffs, onFlush);
        }
    }

    @Override
    protected void preRunExclusive()
    {
        state(RUNNING);
        if (rangeLoader != null)
        {
            commandsForRanges = rangeLoader.finish(commandStore.cachesExclusive());
            rangeLoader = null;
        }
    }

    @Override
    public void run()
    {
        setLoggingIds();
        logger.trace("Running {} with state {}", this, state);
        AccordSafeCommandStore safeStore = null;
        try
        {
            if (state != RUNNING)
                throw new IllegalStateException("Unexpected state " + state);

            if (commands != null)
                commands.values().forEach(AccordSafeState::preExecute);
            if (commandsForKey != null)
                commandsForKey.values().forEach(AccordSafeState::preExecute);
            if (timestampsForKey != null)
                timestampsForKey.values().forEach(AccordSafeState::preExecute);

            safeStore = commandStore.begin(this, commandsForRanges);
            R result = apply(safeStore);

            // TODO (required): currently, we are not very efficient about ensuring that we persist the absolute minimum amount of state. Improve that.
            List<SavedCommand.DiffWriter> diffs = null;
            if (commands != null)
            {
                for (AccordSafeCommand safeCommand : commands.values())
                {
                    SavedCommand.DiffWriter diff = safeCommand.diff();
                    if (diff == null)
                        continue;

                    if (diffs == null)
                        diffs = new ArrayList<>(commands.size());
                    diffs.add(diff);

                    maybeSanityCheck(safeCommand);
                }
            }

            boolean flush = diffs != null || safeStore.fieldUpdates() != null;
            if (flush)
            {
                state(PERSISTING);
                Runnable onFlush = () -> finish(result, null);
                if (safeStore.fieldUpdates() != null)
                    commandStore.persistFieldUpdates(safeStore.fieldUpdates(), diffs == null ? onFlush : null);
                if (diffs != null)
                    save(diffs, onFlush);
            }

            commandStore.complete(safeStore);
            safeStore = null;
            if (!flush)
                finish(result, null);
        }
        catch (Throwable t)
        {
            if (safeStore != null)
            {
                revert();
                commandStore.abort(safeStore);
            }
            throw t;
        }
        finally
        {
            logger.trace("Exiting {}", this);
            clearLoggingIds();
        }
    }

    // expects to hold cache lock
    public void fail(Throwable throwable)
    {
        commandStore.agent().onUncaughtException(throwable);
        if (state.isComplete())
            return;

        if (commandStore.hasSafeStore())
            commandStore.agent().onUncaughtException(new IllegalStateException(String.format("Failure to cleanup safe store for %s; status=%s", this, state), throwable));

        finish(null, throwable);
    }

    protected void cleanupExclusive()
    {
        releaseResources(commandStore.cachesExclusive());
    }

    public RangeLoader rangeLoader()
    {
        return rangeLoader;
    }

    public boolean hasRanges()
    {
        return rangeLoader != null;
    }

    @Override
    public void cancel()
    {
        commandStore.executor().cancel(this);
    }

    public void cancelExclusive()
    {
        releaseResources(commandStore.cachesExclusive());
        state(CANCELLED);
        if (callback != null)
            callback.accept(null, new CancellationException());
    }

    public State state()
    {
        return state;
    }

    private void finish(R result, Throwable failure)
    {
        state(failure == null ? FINISHED : FAILED);
        if (callback != null)
            callback.accept(result, failure);
    }

    void releaseResources(Caches caches)
    {
        // TODO (expected): we should destructively iterate to avoid invoking second time in fail; or else read and set to null
        if (commands != null)
        {
            commands.forEach((k, v) -> caches.commands().release(v, this));
            commands.clear();
            commands = null;
        }
        if (timestampsForKey != null)
        {
            timestampsForKey.forEach((k, v) -> caches.timestampsForKeys().release(v, this));
            timestampsForKey.clear();
            timestampsForKey = null;
        }
        if (commandsForKey != null)
        {
            commandsForKey.forEach((k, v) -> caches.commandsForKeys().release(v, this));
            commandsForKey.clear();
            commandsForKey = null;
        }
        if (loading != null)
        {
            loading.forEach((k, v) -> caches.global().release(v, this));
            loading.clear();
            loading = null;
        }
        waitingToLoad = null;
    }

    void revert()
    {
        if (commands != null)
            commands.forEach((k, v) -> v.revert());
        if (timestampsForKey != null)
            timestampsForKey.forEach((k, v) -> v.revert());
        if (commandsForKey != null)
            commandsForKey.forEach((k, v) -> v.revert());
    }

    public class RangeLoader implements Runnable
    {
        class KeyWatcher implements AccordStateCache.Listener<RoutingKey, CommandsForKey>
        {
            @Override
            public void onAdd(AccordCachingState<RoutingKey, CommandsForKey> state)
            {
                if (ranges.contains(state.key()))
                    reference(state);
            }
        }

        class CommandWatcher implements AccordStateCache.Listener<TxnId, Command>
        {
            @Override
            public void onUpdate(AccordCachingState<TxnId, Command> state)
            {
                CommandsForRangesLoader.Summary summary = summaryLoader.from(state);
                if (summary != null)
                    summaries.put(summary.txnId, summary);
            }
        }

        final ConcurrentHashMap<TxnId, CommandsForRangesLoader.Summary> summaries = new ConcurrentHashMap<>();
        // TODO (expected): produce key summaries to avoid locking all in memory
        final Set<AccordRoutingKey.TokenKey> intersectingKeys = new ObjectHashSet<>();
        final KeyWatcher keyWatcher = new KeyWatcher();
        final CommandWatcher commandWatcher = new CommandWatcher();
        final Ranges ranges = ((AbstractRanges) preLoadContext.keys()).toRanges();
        final AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache;

        public RangeLoader(AccordStateCache.Type<RoutingKey, CommandsForKey, AccordSafeCommandsForKey>.Instance commandsForKeyCache)
        {
            this.commandsForKeyCache = commandsForKeyCache;
        }

        CommandsForRangesLoader.Loader summaryLoader;
        boolean scanned;

        @Override
        public void run()
        {
            try
            {
                for (Range range : ranges)
                {
                    AccordKeyspace.findAllKeysBetween(commandStore.id(),
                                                      (AccordRoutingKey) range.start(), range.startInclusive(),
                                                      (AccordRoutingKey) range.end(), range.endInclusive(),
                                                      intersectingKeys::add);
                }

                Collection<TxnId> txnIds = summaryLoader.intersects();
                for (TxnId txnId : txnIds)
                {
                    if (summaries.containsKey(txnId))
                        continue;

                    CommandsForRangesLoader.Summary summary = summaryLoader.load(txnId);
                    if (summary != null)
                        summaries.putIfAbsent(txnId, summary);
                }
            }
            catch (Throwable t)
            {
                commandStore.executor().onScannedRanges(AccordTask.this, t);
                throw t;
            }
            commandStore.executor().onScannedRanges(AccordTask.this, null);
        }

        private void reference(AccordCachingState<RoutingKey, CommandsForKey> state)
        {
            switch (state.status())
            {
                default: throw new AssertionError("Unhandled Status: " + state.status());
                case WAITING_TO_LOAD:
                case LOADING:
                    if (scanned)
                        // if we've finished scanning and not already taken a reference we shouldn't need to witness (unless modified)
                        return;
                    if (loading != null && loading.containsKey(state.key()))
                        return;
                    ensureLoading().put(state.key(), commandsForKeyCache.acquire(state));
                    if (state.status() == Status.WAITING_TO_LOAD)
                        ensureWaitingToLoad().add(state);
                    state.loadingOrWaiting().add(AccordTask.this);
                    return;

                case MODIFIED:
                case SAVING:
                case LOADED:
                case FAILED_TO_SAVE:
                    if (commandsForKey != null && commandsForKey.containsKey(state.key()))
                        return;
                    ensureCommandsForKey().putIfAbsent(state.key(), commandsForKeyCache.acquire(state));
            }
        }

        public void start(Function<Runnable, Future<?>> executor)
        {
            Caches caches = commandStore.cachesExclusive();
            state(SCANNING_RANGES);

            for (RoutingKey key : caches.commandsForKeys().keySet())
            {
                if (ranges.contains(key))
                    intersectingKeys.add((AccordRoutingKey.TokenKey) key);
            }

            summaryLoader = commandStore.diskCommandsForRanges().loader(preLoadContext.primaryTxnId(), preLoadContext.keyHistory(), ranges);
            summaryLoader.forEachInCache(summary -> summaries.put(summary.txnId, summary), caches);
            caches.commandsForKeys().register(keyWatcher);
            caches.commands().register(commandWatcher);
            executor.apply(this);
        }

        public void scanned()
        {
            Invariants.checkState(state == SCANNING_RANGES);
            scanned = true;
            if (loading == null) state(WAITING_TO_RUN);
            else if (waitingToLoad == null) state(LOADING);
            else state(State.WAITING_TO_LOAD);
        }

        CommandsForRanges finish(Caches caches)
        {
            caches.commandsForKeys().unregister(keyWatcher);
            caches.commands().unregister(commandWatcher);
            return CommandsForRanges.create(ranges, new TreeMap<>(summaries));
        }
    }

    protected void addToQueue(TaskQueue queue)
    {
        Invariants.checkState(queue.kind == state || (queue.kind == State.WAITING_TO_LOAD && state == WAITING_TO_SCAN_RANGES), "Invalid queue type: %s vs %s", queue.kind, this);
        Invariants.checkState(this.queued == null, "Already queued with state: " + this);
        queued = queue;
        queue.append(this);
    }

    TaskQueue<?> queued()
    {
        return queued;
    }

    TaskQueue<?> unqueue()
    {
        TaskQueue<?> wasQueued = queued;
        queued.remove(this);
        queued = null;
        return wasQueued;
    }

    TaskQueue<?> unqueueIfQueued()
    {
        if (queued == null)
            return null;
        return unqueue();
    }
}
